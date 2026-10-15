"""`pipeline run`: collector + uploader + local processor threads.

Mode logic
  online   : the last cloud request succeeded less than OFFLINE_AFTER seconds ago
  offline  : otherwise. The device processes windows itself (mode "offline").
While the cloud is still catching up after an outage, the device also keeps
producing offline results, so a result for every second exists within seconds
on at least one side.
"""
import gzip
import json
import logging
import signal
import threading
import time
import urllib.error
import urllib.request

from .collector import Collector
from .processing import Processor
from .store import Store, fetch_range, connect

log = logging.getLogger("daemon")

OFFLINE_AFTER = 4.0       # s without a successful cloud request -> offline
REQUEST_TIMEOUT = 3.0
PROBE_INTERVAL = 1.0
CATCHUP_RECORDS = 40      # backlog (records) above which the device keeps processing locally


class Uploader(threading.Thread):
    """Sends the record log to the cloud in seq order. The cloud deduplicates on
    (session, sensor, kind, first_idx), so re-sending after a crash is harmless:
    every sample ends up in the cloud exactly once."""

    def __init__(self, store, cfg, stop, metrics):
        super().__init__(daemon=True, name="uploader")
        self.store, self.cfg, self.stop, self.m = store, cfg, stop, metrics
        self.url = cfg["cloud_url"].rstrip("/")
        self.acked = int(store.get_meta("acked_seq", 0))
        self.last_ok = None
        self.mode = "offline"
        self.store.set_meta("mode", "offline")

    def _post(self, path, payload):
        body = gzip.compress(json.dumps(payload, separators=(",", ":")).encode(), 5)
        req = urllib.request.Request(self.url + path, data=body, method="POST", headers={
            "Content-Type": "application/json", "Content-Encoding": "gzip",
            "Authorization": "Bearer " + self.cfg.get("token", "")})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as r:
            out = json.loads(r.read())
        self.m["net_bytes_sent"] = self.m.get("net_bytes_sent", 0) + len(body) + 300
        return out

    def _set_mode(self, mode):
        if mode != self.mode:
            log.warning("mode %s -> %s", self.mode, mode)
            self.mode = mode
            self.store.set_meta("mode", mode)
            self.store.set_meta("mode_changed_wall", time.time())
            self.m["mode_switches"] = self.m.get("mode_switches", 0) + 1

    def run(self):
        while not self.stop.is_set():
            ok = False
            sent_any = False
            try:
                rows = self.store.records_after(self.acked)
                if rows:
                    recs = [{"seq": r[0], "kind": r[1], "session": r[2], "sensor": r[3], "first": r[4],
                             "last": r[5], "reason": r[6], "data": r[7], "crc": r[8]} for r in rows]
                    res = self._post("/ingest", {"device": self.cfg.get("device_id", "pi"),
                                                 "records": recs})
                    if res.get("ok"):
                        self.acked = rows[-1][0]
                        # NORMAL durability is fine here: losing an ack only causes a resend
                        self.store.set_meta("acked_seq", self.acked)
                        ok = sent_any = True
                else:
                    res = self._post("/ping", {"device": self.cfg.get("device_id", "pi")})
                    ok = bool(res.get("ok"))
            except (urllib.error.URLError, OSError, ValueError) as e:
                self.m["upload_errors"] = self.m.get("upload_errors", 0) + 1
                log.debug("cloud request failed: %s", e)
            now = time.monotonic()
            if ok:
                self.last_ok = now
                self._set_mode("online")
            elif self.last_ok is None or now - self.last_ok > OFFLINE_AFTER:
                self._set_mode("offline")
            self.m["backlog_records"] = self.store.max_seq() - self.acked
            if not sent_any:
                self.stop.wait(0.25 if ok else PROBE_INTERVAL)

    def needs_local_processing(self):
        return self.mode == "offline" or self.m.get("backlog_records", 0) > CATCHUP_RECORDS


class LocalProcessor(threading.Thread):
    """Runs section-3 processing on the device for the current session. Results are
    stored (mode 'offline') only for windows finalised while the cloud is unavailable
    or catching up."""

    def __init__(self, store, uploader, stop, metrics):
        super().__init__(daemon=True, name="processor")
        self.path, self.up, self.stop, self.m = store.path, uploader, stop, metrics
        self.db = None
        self.session = None
        self.proc = None
        self.seen_seq = 0
        self.catching_up = True

    def run(self):
        self.db = connect(self.path, durable=False)
        from .processing import Coverage
        while not self.stop.is_set():
            try:
                r = self.db.execute("SELECT v FROM meta WHERE k='current_session'").fetchone()
                sid = r[0] if r else None
                if sid and sid != self.session:
                    self.session, self.cov, self.seen_seq = sid, Coverage(), 0
                    self.proc = Processor(sid, lambda s, a, b, sid=sid: fetch_range(self.db, sid, s, a, b),
                                          self.cov, "offline")
                    self.catching_up = True        # replay after (re)start: do not emit
                if self.proc:
                    for seq, kind, sensor, a, b in self.db.execute(
                            "SELECT seq,kind,sensor,first_idx,last_idx FROM records WHERE session=? "
                            "AND seq>? ORDER BY seq", (self.session, self.seen_seq)):
                        self.cov.add_record(kind, sensor, a, b)
                        self.seen_seq = seq
                    results = self.proc.step()
                    emit = (not self.catching_up) and self.up.needs_local_processing()
                    if emit and results:
                        self.db.execute("BEGIN")
                        for res in results:
                            res["produced_at"] = round(time.time(), 3)
                            self.db.execute("INSERT OR IGNORE INTO results VALUES(?,?,?,?)",
                                            (res["session"], res["window"], "offline", json.dumps(res)))
                        self.db.execute("COMMIT")
                        self.m["offline_results"] = self.m.get("offline_results", 0) + len(results)
                    self.catching_up = False
            except Exception:
                log.exception("local processing error")
            self.stop.wait(0.5)


def run(cfg):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(threadName)s %(levelname)s %(message)s")
    store = Store(cfg["data_dir"])
    stop = threading.Event()
    metrics = {"started_wall": time.time()}
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    col = Collector(store, cfg.get("mock_host", "127.0.0.1"), int(cfg.get("mock_port", 7100)), stop, metrics)
    up_store = Store(cfg["data_dir"])          # separate connection per thread
    up = Uploader(up_store, cfg, stop, metrics)
    lp = LocalProcessor(store, up, stop, metrics)
    for t in (col, up, lp):
        t.start()
    hb_store = Store(cfg["data_dir"], durable=False)
    while not stop.is_set():
        hb_store.set_meta("heartbeat_mono", time.monotonic())
        hb_store.set_meta("heartbeat_wall", time.time())
        hb_store.set_meta("metrics", json.dumps(metrics))
        stop.wait(1.0)
    log.info("stopping")
