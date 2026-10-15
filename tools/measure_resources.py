#!/usr/bin/env python3
"""Sample a running `pipeline run` for N seconds: avg CPU %, peak RSS, MB written, MB sent.
  python3 tools/measure_resources.py PID 3600"""
import json, os, sys, time
pid, dur = int(sys.argv[1]), float(sys.argv[2])
tick = os.sysconf("SC_CLK_TCK")
def cpu(): f = open(f"/proc/{pid}/stat").read().split(")")[-1].split(); return (int(f[11]) + int(f[12])) / tick
def io(): return {l.split(":")[0]: int(l.split(":")[1]) for l in open(f"/proc/{pid}/io")}
def hwm(): return next(int(l.split()[1]) for l in open(f"/proc/{pid}/status") if l.startswith("VmHWM"))
c0, i0, t0 = cpu(), io(), time.monotonic()
time.sleep(dur)
c1, i1, t1 = cpu(), io(), time.monotonic()
print(json.dumps({"seconds": round(t1 - t0), "avg_cpu_pct": round(100 * (c1 - c0) / (t1 - t0), 1),
                  "peak_rss_mb": round(hwm() / 1024, 1),
                  "disk_written_mb": round((i1["write_bytes"] - i0["write_bytes"]) / 1e6, 1),
                  "note": "network MB: see metrics.net_bytes_sent in the device DB meta table"}))
