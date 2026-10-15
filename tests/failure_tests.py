#!/usr/bin/env python3
"""Automated failure tests. Each scenario starts a fresh cloud service, a network-cut
proxy, the stand-in mock and the pipeline, injects faults, then checks the device and
cloud exports against the mock's ground truth and inspects results/latency/mode switches.

  sudo python3 tests/failure_tests.py [scenario ...]      (root needed only for
                                                           disk_full (tmpfs) and clock_change)
Writes evidence/failure_tests.json and evidence/logs/<scenario>/*.
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
EVID = os.path.join(ROOT, "evidence")
PY = sys.executable


class Env:
    port_base = 9000

    def __init__(self, name, mock_args, data_dir=None):
        Env.port_base += 10
        self.name, self.mock_args = name, mock_args
        self.dir = os.path.join(EVID, "logs", name)
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir)
        self.p = {"cloud": Env.port_base, "proxy": Env.port_base + 1, "mock": Env.port_base + 2}
        self.cutfile = os.path.join(self.dir, "CUT")
        self.data_dir = data_dir or os.path.join("/tmp/pipeline_tests", name, "data")
        shutil.rmtree(self.data_dir, ignore_errors=True)
        os.makedirs(self.data_dir, exist_ok=True)
        self.cfg = os.path.join(self.dir, "config.json")
        json.dump({"data_dir": self.data_dir, "mock_host": "127.0.0.1", "mock_port": self.p["mock"],
                   "cloud_url": "http://127.0.0.1:%d" % self.p["proxy"], "token": "test-token",
                   "device_id": "pi-" + name}, open(self.cfg, "w"))
        self.truth = os.path.join(self.dir, "truth.json")
        self.procs = {}
        self.env = dict(os.environ, PIPELINE_CONFIG=self.cfg, PIPELINE_TOKEN="test-token",
                        PIPELINE_PORT=str(self.p["cloud"]),
                        PIPELINE_CLOUD_DB=os.path.join("/tmp/pipeline_tests", name, "cloud.db"))
        try:
            os.remove(self.env["PIPELINE_CLOUD_DB"])
        except FileNotFoundError:
            pass
        self.events = []
        self.t0 = time.time()

    def ev(self, what, **kw):
        e = {"t": round(time.time() - self.t0, 2), "event": what}
        e.update(kw)
        self.events.append(e)
        print("  [%s %6.1fs] %s %s" % (self.name, e["t"], what, kw or ""), flush=True)

    def _spawn(self, key, args):
        log = open(os.path.join(self.dir, key + ".log"), "a")
        self.procs[key] = subprocess.Popen(args, cwd=ROOT, env=self.env, stdout=log, stderr=subprocess.STDOUT,
                                           start_new_session=True)

    def start_cloud(self):
        self._spawn("cloud", [PY, "cloud/server.py"])

    def start_proxy(self):
        self._spawn("proxy", [PY, "tests/netcut_proxy.py", str(self.p["proxy"]), "127.0.0.1",
                              str(self.p["cloud"]), self.cutfile])

    def start_mock(self):
        self._spawn("mock", [PY, "standin_mock/biosignal_mock_standin.py", "--port", str(self.p["mock"]),
                             "--truth", self.truth] + self.mock_args)

    def start_pipeline(self):
        self._spawn("pipeline", [PY, "-m", "pipeline", "run"])

    def kill(self, key, sig=signal.SIGKILL):
        p = self.procs.get(key)
        if p and p.poll() is None:
            os.killpg(p.pid, sig)
            p.wait()

    def start_all(self):
        self.start_cloud()
        self.start_proxy()
        time.sleep(0.5)
        self.start_mock()
        time.sleep(0.3)
        self.start_pipeline()
        self.ev("started")

    def stop_all(self):
        for k in ("pipeline", "mock", "proxy", "cloud"):
            self.kill(k, signal.SIGTERM if k == "pipeline" else signal.SIGKILL)

    def cut(self):
        open(self.cutfile, "w").close()
        self.ev("network_cut")

    def restore(self):
        os.remove(self.cutfile)
        self.ev("network_restored")

    def cli(self, *args, timeout=600):
        r = subprocess.run([PY, "-m", "pipeline"] + list(args), cwd=ROOT, env=self.env, capture_output=True,
                           text=True, timeout=timeout)
        return r.stdout

    def status(self):
        try:
            return json.loads(self.cli("status").strip().splitlines()[-1])
        except Exception:
            return {}

    def sessions(self):
        return [s for s in self.cli("sessions").split() if s]

    def check(self, session, source):
        path = os.path.join(self.dir, "export_%s_%s.jsonl" % (source, session))
        with open(path, "w") as f:
            subprocess.run([PY, "-m", "pipeline", "export", "--session", session, "--from", source], cwd=ROOT,
                           env=self.env, stdout=f, timeout=900)
        r = subprocess.run([PY, "standin_mock/check_ground_truth_standin.py", "--truth", self.truth, "--export",
                            path, "--session", session], cwd=ROOT, capture_output=True, text=True)
        os.remove(path) if os.path.getsize(path) > 30_000_000 else None
        try:
            return json.loads(r.stdout)
        except ValueError:
            return {"pass": False, "error": r.stdout + r.stderr}

    def results(self, session):
        out = self.cli("results", "--session", session, "--from", "both")
        return [json.loads(l) for l in out.splitlines() if l.strip()]

    def wait_mode(self, mode, limit):
        t = time.time()
        while time.time() - t < limit:
            if self.status().get("mode") == mode:
                return round(time.time() - t, 2)
            time.sleep(0.25)
        return None

    def wait_synced(self, limit):
        t = time.time()
        while time.time() - t < limit:
            s = self.status()
            if s.get("backlog_records") == 0 and s.get("mode") == "online":
                return round(time.time() - t, 1)
            time.sleep(1)
        return None


def online_latency(env, session, results):
    truth = json.load(open(env.truth))
    sess = next(s for s in truth["sessions"] if s["session_id"] == session)
    lat = [r["produced_at"] - (sess["start_wall"] + (r["window"] + 1) / sess.get("speed", 1))
           for r in results if r["mode"] == "online" and r["session"] == session]
    if not lat:
        return {}
    lat.sort()
    return {"windows": len(lat), "max_s": round(lat[-1], 2), "p95_s": round(lat[int(len(lat) * 0.95) - 1], 2),
            "median_s": round(lat[len(lat) // 2], 2), "over_5s": sum(1 for x in lat if x > 5)}


def metrics(env):
    import sqlite3
    db = sqlite3.connect(os.path.join(env.data_dir, "pipeline.db"))
    r = db.execute("SELECT v FROM meta WHERE k='metrics'").fetchone()
    return json.loads(r[0]) if r else {}


def check_sessions(env, sources=("device", "cloud")):
    out = {}
    for s in env.sessions():
        for src in sources:
            out["%s/%s" % (s, src)] = env.check(s, src)
    return out


def same_results(results):
    """Where both an offline (device) and online (cloud) result exist, they must be identical."""
    by = {}
    for r in results:
        by.setdefault((r["session"], r["window"]), {})[r["mode"]] = r
    both = diff = 0
    for k, m in by.items():
        if "online" in m and "offline" in m:
            both += 1
            a = {x: y for x, y in m["online"].items() if x not in ("mode", "produced_at")}
            b = {x: y for x, y in m["offline"].items() if x not in ("mode", "produced_at")}
            diff += a != b
    return {"windows_with_both": both, "differences": diff}


# ------------------------------------------------------------------ scenarios
def sc_baseline():
    e = Env("baseline_online", ["--mode", "signal", "--duration", "90"])
    e.start_all()
    time.sleep(95)
    s = e.sessions()[0]
    res = e.results(s)
    out = {"checks": check_sessions(e), "status": e.status(), "online_latency": online_latency(e, s, res),
           "online_windows": sum(r["mode"] == "online" for r in res),
           "heart_rates_sample": [r["heart_rate"] for r in res if r["mode"] == "online"][4:12]}
    e.stop_all()
    return e, out


def sc_mock_faults():
    e = Env("mock_faults", ["--mode", "ramp", "--duration", "60",
                            "--faults", "corrupt@8,dup@12,corrupt@13,stall@16:5,dropout@24:3:2,dup@27,restart@33"])
    e.start_all()
    time.sleep(70)
    out = {"checks": check_sessions(e), "status": e.status(),
           "pipeline_warnings": [l.strip() for l in open(os.path.join(e.dir, "pipeline.log"))
                                 if "WARNING" in l][:25]}
    e.stop_all()
    return e, out


def sc_process_kill():
    e = Env("process_kill", ["--mode", "ramp", "--duration", "75"])
    e.start_all()
    time.sleep(15)
    e.kill("pipeline")
    e.ev("pipeline SIGKILL (short, 3 s: within mock's 10 s buffer)")
    time.sleep(3)
    e.start_pipeline()
    e.ev("pipeline restarted")
    time.sleep(15)
    e.kill("pipeline")
    e.ev("pipeline SIGKILL (long, 15 s: exceeds mock's 10 s buffer)")
    time.sleep(15)
    e.start_pipeline()
    e.ev("pipeline restarted")
    time.sleep(40)
    out = {"checks": check_sessions(e), "status": e.status()}
    e.stop_all()
    return e, out


def sc_network_cut():
    e = Env("network_cut", ["--mode", "signal", "--duration", "150"])
    e.start_all()
    time.sleep(25)
    e.cut()
    t_off = e.wait_mode("offline", 30)
    e.ev("mode=offline observed", after_s=t_off)
    time.sleep(45)
    e.restore()
    t_on = e.wait_mode("online", 30)
    e.ev("mode=online observed", after_s=t_on)
    t_sync = e.wait_synced(120)
    e.ev("cloud caught up", after_s=t_sync)
    time.sleep(max(0, 160 - (time.time() - e.t0)))
    s = e.sessions()[0]
    res = e.results(s)
    off = sorted(r["window"] for r in res if r["mode"] == "offline")
    out = {"switch_to_offline_s": t_off, "switch_to_online_s": t_on, "catchup_s": t_sync,
           "checks": check_sessions(e), "status": e.status(),
           "offline_result_windows": [off[0], off[-1], len(off)] if off else [],
           "online_result_windows": sum(r["mode"] == "online" for r in res),
           "offline_vs_online_identical": same_results(res)}
    e.stop_all()
    return e, out


def sc_power_cut():
    e = Env("power_cut_sim", ["--mode", "ramp", "--duration", "80"])
    e.start_all()
    time.sleep(20)
    e.kill("pipeline")
    e.kill("mock")
    e.ev("SIGKILL mock + pipeline together (simulated power cut: device and mock die)")
    time.sleep(4)
    e.start_mock()
    time.sleep(0.3)
    e.start_pipeline()
    e.ev("both restarted (mock begins a new session)")
    time.sleep(30)
    out = {"checks": check_sessions(e), "status": e.status(),
           "old_session_losses": [l for l in open(os.path.join(e.dir, "pipeline.log")) if "loss" in l][:6]}
    e.stop_all()
    return e, out


def sc_disk_full():
    mnt = "/tmp/pipeline_tests/disk_full_mnt"
    os.makedirs(mnt, exist_ok=True)
    subprocess.run(["umount", mnt], capture_output=True)
    r = subprocess.run(["mount", "-t", "tmpfs", "-o", "size=24m", "tmpfs", mnt], capture_output=True, text=True)
    if r.returncode:
        return None, {"skipped": "cannot mount tmpfs: " + r.stderr}
    e = Env("disk_full", ["--mode", "ramp", "--duration", "90"], data_dir=os.path.join(mnt, "data"))
    e.start_all()
    time.sleep(10)

    def fill(name):
        st = os.statvfs(mnt)
        free = st.f_bavail * st.f_frsize
        subprocess.run(["dd", "if=/dev/zero", "of=" + os.path.join(mnt, name), "bs=64k",
                        "count=%d" % (free // 65536 + 2)], capture_output=True)
    fill("filler1")
    e.ev("disk filled (pipeline should release its 4 MB reserve)")
    time.sleep(6)
    fill("filler2")
    e.ev("disk filled again (reserve already used): samples cannot be saved")
    time.sleep(12)
    for f in ("filler1", "filler2"):
        os.remove(os.path.join(mnt, f))
    e.ev("space freed")
    time.sleep(30)
    out = {"checks": check_sessions(e), "status": e.status(),
           "metrics": metrics(e)}
    e.stop_all()
    subprocess.run(["umount", mnt], capture_output=True)
    return e, out


def sc_clock_change():
    e = Env("clock_change", ["--mode", "ramp", "--duration", "60"])
    e.start_all()
    time.sleep(15)
    r = subprocess.run(["date", "-s", "+3 hours"], capture_output=True, text=True)
    if r.returncode:
        e.stop_all()
        return e, {"skipped": r.stderr}
    e.ev("system clock +3 h")
    time.sleep(10)
    subprocess.run(["date", "-s", "-5 hours"], capture_output=True)
    e.ev("system clock -5 h")
    time.sleep(10)
    subprocess.run(["date", "-s", "+2 hours"], capture_output=True)
    e.ev("system clock restored (+2 h)")
    time.sleep(30)
    out = {"checks": check_sessions(e), "status": e.status(),
           "mode_switches": metrics(e).get("mode_switches")}
    e.stop_all()
    return e, out


SCENARIOS = {"baseline": sc_baseline, "mock_faults": sc_mock_faults, "process_kill": sc_process_kill,
             "network_cut": sc_network_cut, "power_cut": sc_power_cut, "disk_full": sc_disk_full,
             "clock_change": sc_clock_change}


def verdict(out):
    checks = out.get("checks", {})
    return bool(checks) and all(c.get("pass") for c in checks.values())


def main():
    names = sys.argv[1:] or list(SCENARIOS)
    path = os.path.join(EVID, "failure_tests.json")
    allres = json.load(open(path)) if os.path.exists(path) else {}
    for n in names:
        print("== scenario", n, flush=True)
        e, out = SCENARIOS[n]()
        out["data_checks_pass"] = verdict(out) if "skipped" not in out else None
        out["events"] = e.events if e else []
        out["environment"] = "x86-64 Linux container (1 vCPU), stand-in mock; NOT the Raspberry Pi 4"
        allres[n] = out
        json.dump(allres, open(path, "w"), indent=1)
        print(json.dumps({k: v for k, v in out.items() if k not in ("events",)}, indent=1)[:3000], flush=True)


if __name__ == "__main__":
    main()
