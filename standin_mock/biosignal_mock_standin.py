#!/usr/bin/env python3
"""STAND-IN for the provided `biosignal-mock` (whose download link was blank in the
brief). Written from section 6 of the brief only, so the pipeline could be tested
before the official binary is available. The official mock is what judging uses.

  python3 biosignal_mock_standin.py --mode ramp --seed 7 --duration 3600 --port 7100 \
      [--speed 1] [--faults "corrupt@20,dup@25,stall@30:4,dropout@40:3:2,restart@60"] \
      [--truth truth.json]

Fault spec (seconds of wall time since start):
  corrupt@T            next reply gets a wrong crc32
  dup@T                next reply is sent twice
  stall@T:D            stop answering for D seconds
  dropout@T:D:S        sensor S produces nothing for D seconds (reported in `lost`)
  restart@T            new session: new session_id, indexes restart at 0
"""
import argparse
import json
import os
import socket
import sys
import threading
import time
import uuid
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from standin_signal import SignalModel  # noqa: E402

RATE = 400
BUFFER_S = 10


class Mock:
    def __init__(self, a):
        self.a = a
        self.model = SignalModel(a.mode, a.seed, a.duration)
        self.lock = threading.Lock()
        self.faults = []
        for f in filter(None, (a.faults or "").split(",")):
            name, rest = f.split("@")
            parts = [float(x) for x in rest.split(":")]
            self.faults.append([name, parts, False])
        self.t0 = time.monotonic()
        self.truth = {"mode": a.mode, "seed": a.seed, "duration": a.duration, "sessions": []}
        self.pending = {"corrupt": 0, "dup": 0}
        self.stall_until = 0.0
        self._new_session()

    def _new_session(self):
        self.session = uuid.uuid4().hex[:8]
        self.s_t0 = time.monotonic()
        self.cur = {1: 0, 2: 0}
        self.dropouts = {1: [], 2: []}       # [from, to] index ranges never produced
        self.sess = {"session_id": self.session, "start_wall": time.time(), "speed": self.a.speed,
                     "generated": {"1": 0, "2": 0}, "dropouts": {"1": [], "2": []}}
        self.truth["sessions"].append(self.sess)

    def total(self):
        return int(self.a.duration * RATE)

    def generated(self):
        return min(self.total(), int((time.monotonic() - self.s_t0) * RATE * self.a.speed))

    def tick(self):
        now = time.monotonic() - self.t0
        with self.lock:
            for f in self.faults:
                name, p, done = f
                if done or now < p[0]:
                    continue
                f[2] = True
                if name in ("corrupt", "dup"):
                    self.pending[name] += 1
                elif name == "stall":
                    self.stall_until = time.monotonic() + p[1]
                elif name == "dropout":
                    s = int(p[2])
                    g = self.generated()
                    rng = [g, g + int(p[1] * RATE * self.a.speed) - 1]
                    self.dropouts[s].append(rng)
                    self.sess["dropouts"][str(s)].append(rng)
                elif name == "restart":
                    self._record()
                    self._new_session()
            self._record()

    def _record(self):
        g = self.generated()
        self.sess["generated"] = {"1": g, "2": g}

    def read(self, sensor, from_index=None):
        with self.lock:
            g = self.generated()
            if from_index is not None:
                self.cur[sensor] = max(0, min(int(from_index), g))
            cur = self.cur[sensor]
            oldest = max(0, g - BUFFER_S * RATE)
            lost = []
            if cur < oldest:
                lost.append({"from_index": cur, "to_index": oldest - 1, "reason": "buffer_overflow"})
                cur = oldest
            # skip dropout ranges at the cursor
            changed = True
            while changed:
                changed = False
                for a, b in self.dropouts[sensor]:
                    if a <= cur <= b and cur < g:
                        end = min(b, g - 1)
                        lost.append({"from_index": cur, "to_index": end, "reason": "sensor_dropout"})
                        cur = end + 1
                        changed = True
            end = min(cur + RATE, g)
            for a, b in self.dropouts[sensor]:
                if cur < a < end:
                    end = a
            data = [self.model.sample(sensor, i) for i in range(cur, end)]
            self.cur[sensor] = end
            session_end = end >= self.total()
            data_text = json.dumps(data)
            crc = format(zlib.crc32(data_text.encode()) & 0xFFFFFFFF, "08x")
            dup = False
            if self.pending["corrupt"]:
                self.pending["corrupt"] -= 1
                crc = format((int(crc, 16) ^ 0x5A5A) & 0xFFFFFFFF, "08x")
            if self.pending["dup"]:
                self.pending["dup"] -= 1
                dup = True
            line = ('{"session_id": "%s", "sensor": %d, "first_index": %d, "count": %d, "lost": %s, '
                    '"session_end": %s, "crc32": "%s", "data": %s}\n' % (
                        self.session, sensor, cur, len(data), json.dumps(lost),
                        "true" if session_end else "false", crc, data_text))
            return line, dup

    def write_truth(self):
        if self.a.truth:
            tmp = self.a.truth + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.truth, f)
            os.replace(tmp, self.a.truth)


def serve(m, port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(4)
    current = {"conn": None}

    def handle(conn):
        f = conn.makefile("rb")
        try:
            for raw in f:
                while time.monotonic() < m.stall_until:
                    time.sleep(0.05)
                try:
                    req = json.loads(raw)
                except ValueError:
                    continue
                if req.get("cmd") != "read" or req.get("sensor") not in (1, 2):
                    conn.sendall(b'{"error": "bad request"}\n')
                    continue
                line, dup = m.read(req["sensor"], req.get("from_index"))
                conn.sendall(line.encode())
                if dup:
                    conn.sendall(line.encode())
        except OSError:
            pass
        finally:
            conn.close()

    while True:
        conn, _ = srv.accept()
        if current["conn"]:                      # one client at a time: newest wins
            try:
                current["conn"].shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        current["conn"] = conn
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["signal", "ramp"], default="signal")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--duration", type=float, default=3600)
    p.add_argument("--port", type=int, default=7100)
    p.add_argument("--speed", type=float, default=1.0)
    p.add_argument("--faults", default="")
    p.add_argument("--truth", default="")
    a = p.parse_args()
    m = Mock(a)
    threading.Thread(target=serve, args=(m, a.port), daemon=True).start()
    last_w = 0
    while True:
        m.tick()
        if time.monotonic() - last_w > 0.5:
            m.write_truth()
            last_w = time.monotonic()
        time.sleep(0.02)


if __name__ == "__main__":
    main()
