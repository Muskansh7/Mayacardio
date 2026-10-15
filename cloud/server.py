"""Cloud side: receives the device's record log, stores it exactly once, and runs
the section-3 processing (mode "online") as soon as each window is resolved.

Standard library only: runs on any free-tier Linux VM (e.g. Oracle Cloud Always
Free Ampere A1, or Google Cloud e2-micro), behind a free TLS terminator
(Caddy or Cloudflare Tunnel).

Endpoints (all need `Authorization: Bearer $PIPELINE_TOKEN`):
  POST /ingest   {"device", "records":[...]}  (gzip body allowed) -> {"ok":true,"stored":n}
  POST /ping                                  -> {"ok":true}
  GET  /sessions
  GET  /status?session=ID
  GET  /export?session=ID    one JSON line per sample, then per loss
  GET  /results?session=ID   one JSON line per window
"""
import gzip
import json
import logging
import os
import sys
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from pipeline.processing import Coverage, Processor  # noqa: E402  (same code as the device)
from pipeline.store import connect, fetch_range, export_lines  # noqa: E402

log = logging.getLogger("cloud")
TOKEN = os.environ.get("PIPELINE_TOKEN", "")
DB_PATH = os.environ.get("PIPELINE_CLOUD_DB", "cloud.db")

CLOUD_EXTRA = """
CREATE TABLE IF NOT EXISTS received(session TEXT, sensor INTEGER, kind TEXT, first_idx INTEGER,
  received_at REAL, PRIMARY KEY(session, sensor, kind, first_idx));
"""


class State:
    def __init__(self, path):
        self.path = path
        self.local = threading.local()
        db = self.db()
        db.executescript(CLOUD_EXTRA)
        self.lock = threading.Lock()          # serialises ingest
        self.dirty = set()
        self.wake = threading.Event()
        self.cov = {}                          # session -> Coverage
        self.procs = {}
        for (sid,) in db.execute("SELECT DISTINCT session FROM records"):
            cov = Coverage()
            for kind, sensor, a, b in db.execute(
                    "SELECT kind,sensor,first_idx,last_idx FROM records WHERE session=? ORDER BY seq", (sid,)):
                cov.add_record(kind, sensor, a, b)
            self.cov[sid] = cov
            self.dirty.add(sid)

    def db(self):
        if not hasattr(self.local, "db"):
            # synchronous=FULL: an acknowledged ingest is durable in the cloud
            self.local.db = connect(self.path, durable=True)
        return self.local.db

    def ingest(self, records):
        db = self.db()
        stored = 0
        now = time.time()
        applied = []
        with self.lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                for r in records:
                    if r["kind"] == "data":
                        if format(zlib.crc32(r["data"].encode()) & 0xFFFFFFFF, "08x") != r["crc"]:
                            raise ValueError("crc mismatch in record seq %s" % r.get("seq"))
                        n = len(json.loads(r["data"]))
                        if n != r["last"] - r["first"] + 1:
                            raise ValueError("length mismatch")
                    cur = db.execute(
                        "INSERT OR IGNORE INTO records(kind,session,sensor,first_idx,last_idx,reason,data,crc)"
                        " VALUES(?,?,?,?,?,?,?,?)",
                        (r["kind"], r["session"], r["sensor"], r["first"], r["last"], r.get("reason"),
                         r.get("data"), r.get("crc")))
                    if cur.rowcount:
                        stored += 1
                        db.execute("INSERT OR IGNORE INTO received VALUES(?,?,?,?,?)",
                                   (r["session"], r["sensor"], r["kind"], r["first"], now))
                        db.execute("INSERT OR IGNORE INTO sessions(session, opened_seq) VALUES(?, ?)",
                                   (r["session"], r.get("seq", 0)))
                        applied.append(r)
                db.execute("COMMIT")
                # coverage is advanced only after the rows are committed and visible
                for r in applied:
                    self.cov.setdefault(r["session"], Coverage()).add_record(
                        r["kind"], r["sensor"], r["first"], r["last"])
                    self.dirty.add(r["session"])
            except BaseException:
                db.execute("ROLLBACK")
                raise
        self.wake.set()
        return stored

    def worker(self):
        db = connect(self.path, durable=True)
        while True:
            self.wake.wait(1.0)
            self.wake.clear()
            with self.lock:
                todo = list(self.dirty)
                self.dirty.clear()
            for sid in todo:
                try:
                    p = self.procs.get(sid)
                    if p is None:
                        p = Processor(sid, lambda s, a, b, sid=sid: fetch_range(db, sid, s, a, b),
                                      self.cov[sid], "online")
                        self.procs[sid] = p
                    while True:
                        res = p.step(limit=200)
                        if not res:
                            break
                        now = time.time()
                        db.execute("BEGIN")
                        for r in res:
                            r["produced_at"] = round(now, 3)
                            db.execute("INSERT OR IGNORE INTO results VALUES(?,?,?,?)",
                                       (sid, r["window"], "online", json.dumps(r)))
                        db.execute("COMMIT")
                except Exception:
                    log.exception("processing failed for %s", sid)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    state = None

    def log_message(self, fmt, *args):
        pass

    def _auth(self):
        if TOKEN and self.headers.get("Authorization") != "Bearer " + TOKEN:
            self._send(401, {"error": "unauthorized"})
            return False
        return True

    def _send(self, code, obj=None, body=None, ctype="application/json"):
        if body is None:
            body = json.dumps(obj).encode()
        if "gzip" in self.headers.get("Accept-Encoding", "") and len(body) > 2048:
            body = gzip.compress(body, 5)
            self.send_response(code)
            self.send_header("Content-Encoding", "gzip")
        else:
            self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if not self._auth():
            return
        n = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(n)
        try:
            if self.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            payload = json.loads(raw or b"{}")
            path = urlparse(self.path).path
            if path == "/ping":
                return self._send(200, {"ok": True, "time": time.time()})
            if path == "/ingest":
                stored = self.state.ingest(payload.get("records", []))
                return self._send(200, {"ok": True, "stored": stored})
            self._send(404, {"error": "not found"})
        except ValueError as e:
            self._send(400, {"ok": False, "error": str(e)})

    def do_GET(self):
        if not self._auth():
            return
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        db = self.state.db()
        sid = q.get("session")
        if u.path == "/health":
            return self._send(200, {"ok": True})
        if u.path == "/sessions":
            return self._send(200, [r[0] for r in db.execute("SELECT session FROM sessions ORDER BY rowid")])
        if u.path == "/status":
            n = db.execute("SELECT IFNULL(SUM(last_idx-first_idx+1),0) FROM records WHERE kind='data' "
                           "AND session=?", (sid,)).fetchone()[0]
            w = db.execute("SELECT COUNT(*) FROM results WHERE session=?", (sid,)).fetchone()[0]
            return self._send(200, {"session": sid, "samples": n, "result_windows": w})
        if u.path == "/export":
            body = "".join(l + "\n" for l in export_lines(db, sid)).encode()
            return self._send(200, body=body, ctype="application/x-ndjson")
        if u.path == "/results":
            rows = db.execute("SELECT json FROM results WHERE session=? ORDER BY window", (sid,))
            body = "".join(j + "\n" for (j,) in rows).encode()
            return self._send(200, body=body, ctype="application/x-ndjson")
        self._send(404, {"error": "not found"})


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    host = os.environ.get("PIPELINE_HOST", "0.0.0.0")
    port = int(os.environ.get("PIPELINE_PORT", "8080"))
    Handler.state = State(DB_PATH)
    threading.Thread(target=Handler.state.worker, daemon=True, name="processor").start()
    srv = ThreadingHTTPServer((host, port), Handler)
    srv.daemon_threads = True
    log.info("cloud service on %s:%s db=%s", host, port, DB_PATH)
    srv.serve_forever()


if __name__ == "__main__":
    main()
