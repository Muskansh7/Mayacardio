"""pipeline — device-side command line.

  pipeline run                                   start the collector/uploader/processor
  pipeline status                                one JSON line
  pipeline export --session ID --from device|cloud
  pipeline results --session ID [--from device|cloud|both]
  pipeline sessions                              list known sessions
  pipeline verify                                re-check the CRC of every stored chunk

Configuration: JSON file given by $PIPELINE_CONFIG (default /etc/pipeline/config.json,
then ./config.json). Keys: data_dir, mock_host, mock_port, cloud_url, token, device_id.
"""
import argparse
import gzip
import json
import os
import sys
import time
import urllib.parse
import urllib.request

from .store import Store, export_lines, crc_hex


def load_config():
    for p in (os.environ.get("PIPELINE_CONFIG"), "/etc/pipeline/config.json", "config.json"):
        if p and os.path.exists(p):
            with open(p) as f:
                cfg = json.load(f)
            cfg.setdefault("data_dir", "/var/lib/pipeline")
            return cfg
    sys.exit("no config found (set PIPELINE_CONFIG)")


def cloud_get(cfg, path, params, timeout=30):
    url = cfg["cloud_url"].rstrip("/") + path + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + cfg.get("token", ""),
                                               "Accept-Encoding": "gzip"})
    r = urllib.request.urlopen(req, timeout=timeout)
    if r.headers.get("Content-Encoding") == "gzip":
        return gzip.GzipFile(fileobj=r)
    return r


def resolve_session(store, s):
    return store.latest_session() if s in (None, "latest") else s


def cmd_status(cfg, a):
    st = Store(cfg["data_dir"], durable=False)
    acked = int(st.get_meta("acked_seq", 0))
    saved = st.sample_count()
    in_cloud = st.sample_count(acked)
    hb = st.get_meta("heartbeat_mono")
    alive = hb is not None and 0 <= time.monotonic() - float(hb) < 5
    metrics = json.loads(st.get_meta("metrics", "{}"))
    losses = st.db.execute("SELECT COUNT(*), IFNULL(SUM(CASE WHEN last_idx IS NULL THEN 0 ELSE "
                           "last_idx-first_idx+1 END),0) FROM records WHERE kind='loss'").fetchone()
    out = {"mode": st.get_meta("mode", "offline") if alive else "stopped",
           "catching_up": alive and metrics.get("backlog_records", 0) > 40,
           "session": st.latest_session(),
           "samples_saved": saved, "samples_in_cloud": in_cloud, "backlog": saved - in_cloud,
           "backlog_records": st.max_seq() - acked,
           "loss_ranges": losses[0], "samples_lost_bounded": losses[1],
           "process_running": alive}
    print(json.dumps(out))


def cmd_export(cfg, a):
    st = Store(cfg["data_dir"], durable=False)
    sid = resolve_session(st, a.session)
    out = sys.stdout
    if a.source == "device":
        for line in export_lines(st.db, sid):
            out.write(line + "\n")
    else:
        r = cloud_get(cfg, "/export", {"session": sid}, timeout=600)
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            out.write(chunk.decode())


def cmd_results(cfg, a):
    st = Store(cfg["data_dir"], durable=False)
    sid = resolve_session(st, a.session)
    rows = []
    if a.source in ("device", "both"):
        rows += [json.loads(j) for (j,) in st.db.execute(
            "SELECT json FROM results WHERE session=? ORDER BY window", (sid,))]
    if a.source in ("cloud", "both"):
        try:
            r = cloud_get(cfg, "/results", {"session": sid})
            rows += [json.loads(l) for l in r.read().decode().splitlines() if l.strip()]
        except OSError as e:
            print(json.dumps({"warning": "cloud unreachable; device results only", "error": str(e)}),
                  file=sys.stderr)
    rows.sort(key=lambda r: (r["window"], r["mode"]))
    for r in rows:
        print(json.dumps(r))


def cmd_sessions(cfg, a):
    st = Store(cfg["data_dir"], durable=False)
    for s in st.sessions():
        print(s)


def cmd_verify(cfg, a):
    st = Store(cfg["data_dir"], durable=False)
    bad = 0
    n = 0
    for seq, data, crc in st.db.execute("SELECT seq,data,crc FROM records WHERE kind='data'"):
        n += 1
        if crc_hex(data) != crc:
            bad += 1
            print(json.dumps({"corrupt_record_seq": seq}))
    print(json.dumps({"chunks_checked": n, "corrupt": bad}))
    sys.exit(1 if bad else 0)


def main(argv=None):
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run")
    sub.add_parser("status")
    e = sub.add_parser("export")
    e.add_argument("--session", default="latest")
    e.add_argument("--from", dest="source", choices=["device", "cloud"], default="device")
    r = sub.add_parser("results")
    r.add_argument("--session", default="latest")
    r.add_argument("--from", dest="source", choices=["device", "cloud", "both"], default="both")
    sub.add_parser("sessions")
    sub.add_parser("verify")
    a = p.parse_args(argv)
    cfg = load_config()
    if a.cmd == "run":
        from .daemon import run
        run(cfg)
    else:
        {"status": cmd_status, "export": cmd_export, "results": cmd_results,
         "sessions": cmd_sessions, "verify": cmd_verify}[a.cmd](cfg, a)


if __name__ == "__main__":
    main()
