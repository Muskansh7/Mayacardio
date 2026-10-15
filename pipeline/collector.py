"""Reads both sensors from the mock and saves every sample durably.

Fault handling (section 6.3):
  * no answer           -> per-request timeout, re-send; reconnect after repeated timeouts
  * corrupt reply (crc) -> discard, re-read with from_index
  * duplicate reply     -> replies are self-describing; anything below next_idx is dropped
  * sensor dropout      -> the mock's `lost` ranges are saved as loss records
  * mock restart        -> new session_id: unread data of the old one saved as an
                           open-ended loss (to_index null); indexes restart at 0
  * unexplained gap     -> re-read once with from_index; if still missing, recorded as lost
  * disk full           -> keep draining the mock, track the dropped range in memory,
                           write it as a loss record as soon as space returns
                           (a 4 MB ballast file is released to guarantee that write)
"""
import json
import logging
import os
import socket
import sqlite3
import threading
import time
import zlib

from .store import MAX_CHUNK

log = logging.getLogger("collector")
INT24_MIN, INT24_MAX = -(1 << 23), (1 << 23) - 1
NCH = {1: 3, 2: 2}
BALLAST_BYTES = 4 * 1024 * 1024


class CorruptReply(Exception):
    pass


def extract_data_text(line):
    """Return the raw text of the `data` array exactly as sent, from [ to the matching ]."""
    k = line.find('"data"')
    if k < 0:
        raise CorruptReply("no data field")
    j = line.find("[", k)
    depth = 0
    for p in range(j, len(line)):
        c = line[p]
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                return line[j:p + 1]
    raise CorruptReply("unbalanced data array")


def parse_reply(line):
    try:
        obj = json.loads(line)
        raw = extract_data_text(line)
        crc = format(zlib.crc32(raw.encode()) & 0xFFFFFFFF, "08x")
        if crc != str(obj["crc32"]).lower().rjust(8, "0"):
            raise CorruptReply("crc mismatch")
        sensor, first, count, data = obj["sensor"], obj["first_index"], obj["count"], obj["data"]
        if sensor not in NCH or not isinstance(first, int) or first < 0 or count != len(data):
            raise CorruptReply("bad header")
        for s in data:
            if len(s) != NCH[sensor] or any(not isinstance(v, int) or isinstance(v, bool)
                                            or v < INT24_MIN or v > INT24_MAX for v in s):
                raise CorruptReply("bad sample")
        return obj
    except CorruptReply:
        raise
    except Exception as e:  # malformed JSON, missing keys...
        raise CorruptReply(str(e))


class Collector(threading.Thread):
    def __init__(self, store, host, port, stop, metrics):
        super().__init__(daemon=True, name="collector")
        self.store, self.host, self.port, self.stop, self.m = store, host, port, stop, metrics
        self.sock = None
        self.rbuf = b""
        cur = store.cursors()
        self.session = store.get_meta("current_session")
        self.next = {}
        self.closed = {}
        if self.session:
            for s in (1, 2):
                n, c = cur.get((self.session, s), (0, False))
                self.next[s], self.closed[s] = n, c
        self.old_sessions = set(store.sessions()) - {self.session}
        self.retry_pos = {}
        self.disk_full = {}            # (session, sensor) -> [from, to] dropped because the disk is full
        self.pending_ops = []          # non-sample records that could not be written yet
        self.ballast = os.path.join(store.dir, "ballast.bin")
        self._ensure_ballast()

    # ---------------- disk-full reserve ----------------
    def _ensure_ballast(self):
        try:
            if not os.path.exists(self.ballast):
                st = os.statvfs(self.store.dir)
                if st.f_bavail * st.f_frsize > 4 * BALLAST_BYTES:
                    with open(self.ballast, "wb") as f:
                        f.write(b"\0" * BALLAST_BYTES)
                        f.flush()
                        os.fsync(f.fileno())
        except OSError:
            pass

    # ---------------- connection ----------------
    def _connect(self):
        self._close()
        self.sock = socket.create_connection((self.host, self.port), timeout=2)
        self.sock.settimeout(2.0)
        self.rbuf = b""
        log.info("connected to mock %s:%s", self.host, self.port)

    def _close(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None

    def _send(self, obj):
        self.sock.sendall((json.dumps(obj) + "\n").encode())

    def _readline(self, timeout):
        deadline = time.monotonic() + timeout
        while b"\n" not in self.rbuf:
            left = deadline - time.monotonic()
            if left <= 0:
                raise socket.timeout()
            self.sock.settimeout(left)
            chunk = self.sock.recv(1 << 20)
            if not chunk:
                raise ConnectionError("mock closed connection")
            self.rbuf += chunk
        line, self.rbuf = self.rbuf.split(b"\n", 1)
        return line.decode("utf-8", "replace")

    # ---------------- main loop ----------------
    def run(self):
        backoff = 0.2
        timeouts = 0
        while not self.stop.is_set():
            try:
                if self.sock is None:
                    self._connect()
                    backoff = 0.2
                got_full = False
                for sensor in (1, 2):
                    req = {"cmd": "read", "sensor": sensor}
                    if self.session and not self.closed.get(sensor, True):
                        req["from_index"] = self.next[sensor]
                    self._send(req)
                    full = self._await(sensor)
                    got_full = got_full or full
                timeouts = 0
                self.m["mock_ok"] = time.monotonic()
                if not got_full:
                    self.stop.wait(0.1)
            except socket.timeout:
                timeouts += 1
                self.m["mock_timeouts"] = self.m.get("mock_timeouts", 0) + 1
                if timeouts >= 3:
                    log.warning("mock not answering; reconnecting")
                    self._close()
                    timeouts = 0
            except (OSError, ConnectionError) as e:
                log.warning("mock connection problem: %s", e)
                self._close()
                self.stop.wait(backoff)
                backoff = min(backoff * 2, 2.0)

    def _await(self, sensor):
        """Read lines until a valid reply for `sensor` arrives (others are processed too)."""
        deadline = time.monotonic() + 2.0
        while True:
            line = self._readline(max(0.05, deadline - time.monotonic()))
            try:
                obj = parse_reply(line)
            except CorruptReply as e:
                self.m["corrupt_replies"] = self.m.get("corrupt_replies", 0) + 1
                log.warning("discarding corrupt reply (%s); will re-read", e)
                return False      # next loop re-requests from next_idx
            full = self._handle(obj)
            if obj["sensor"] == sensor:
                return full

    # ---------------- reply integration ----------------
    def _handle(self, obj):
        sid, sensor = str(obj["session_id"]), obj["sensor"]
        if sid in self.old_sessions:
            return False                          # stale duplicate from a past session
        ops = []
        if sid != self.session:
            ops += self._open_session(sid)
        if self.closed.get(sensor):
            if ops:
                self._commit(ops)
            return False
        first, data = obj["first_index"], obj["data"]
        segs = []
        for l in obj.get("lost") or []:
            segs.append((l["from_index"], l.get("to_index"), "loss", l.get("reason") or "reported_lost_by_mock"))
        if data:
            segs.append((first, first + len(data) - 1, "data", None))
        segs.sort(key=lambda s: s[0])
        pos = self.next[sensor]
        gap_pending = False
        for a, b, kind, reason in segs:
            if b is not None and b < pos:
                continue                          # duplicate / already handled
            if a > pos:
                # samples pos..a-1 neither delivered nor reported lost
                if self.retry_pos.get(sensor) != pos:
                    self.retry_pos[sensor] = pos  # re-read once with from_index
                    gap_pending = True
                    break
                ops.append(self._loss(sensor, pos, a - 1, "not_delivered_by_mock"))
                pos = a
            if kind == "data":
                start = pos - first
                vals = data[start:b - first + 1]
                for c in range(0, len(vals), MAX_CHUNK):
                    part = vals[c:c + MAX_CHUNK]
                    ops.append({"kind": "data", "session": sid, "sensor": sensor, "first": pos + c,
                                "last": pos + c + len(part) - 1,
                                "data": json.dumps(part, separators=(",", ":"))})
                pos = b + 1
            else:
                ops.append(self._loss(sensor, pos, b, reason))
                if b is None:
                    self.closed[sensor] = True
                    break
                pos = b + 1
        self.next[sensor] = pos
        if obj.get("session_end") and not self.closed.get(sensor) and not gap_pending:
            if True:
                ops.append({"kind": "end", "session": sid, "sensor": sensor, "first": pos - 1, "last": pos - 1})
                self.closed[sensor] = True
        self._commit(ops)
        return len(data) >= 400

    def _loss(self, sensor, a, b, reason):
        self.m["loss_records"] = self.m.get("loss_records", 0) + 1
        log.warning("loss session=%s sensor=%s %s..%s reason=%s", self.session, sensor, a, b, reason)
        return {"kind": "loss", "session": self.session, "sensor": sensor, "first": a, "last": b,
                "reason": reason}

    def _open_session(self, sid):
        ops = []
        if self.session:
            for s in (1, 2):
                if not self.closed.get(s):
                    ops.append(self._loss(s, self.next[s], None,
                                          "mock_session_restarted:unread_data_of_old_session"))
                    self.closed[s] = True
            self.old_sessions.add(self.session)
        log.info("new mock session %s", sid)
        self.session = sid
        self.next = {1: 0, 2: 0}
        self.closed = {1: False, 2: False}
        self.retry_pos = {}
        ops.insert(0, {"kind": "session", "session": sid})
        return ops

    def _cursor_state(self):
        return {(self.session, s): (self.next[s], self.closed[s]) for s in (1, 2)}

    def _commit(self, ops):
        """Persist ops + cursors in one transaction; survive disk-full without silent loss."""
        old_cursors = {}
        if any(op["kind"] == "loss" and op.get("last") is None for op in ops):
            # open-ended losses of an old session: also close its cursors
            for op in ops:
                if op["kind"] == "loss" and op.get("last") is None:
                    old_cursors[(op["session"], op["sensor"])] = (op["first"], True)
        cursors = dict(old_cursors)
        cursors.update(self._cursor_state())
        if self.disk_full or self.pending_ops:
            ops = self.pending_ops + self._disk_full_prefix() + ops
            for op in self.pending_ops:
                if op["kind"] == "loss" and op.get("last") is None:
                    cursors.setdefault((op["session"], op["sensor"]), (op["first"], True))
        try:
            self.store.write_batch(ops, cursors)
            self.store.set_meta("current_session", self.session)
            if self.disk_full:
                log.warning("disk space recovered")
                self.disk_full = {}
                self.pending_ops = []
                self._ensure_ballast()
            self.m["saved_batches"] = self.m.get("saved_batches", 0) + 1
        except sqlite3.OperationalError as e:
            if "full" not in str(e).lower() and "disk" not in str(e).lower():
                raise
            self._on_disk_full(ops)

    def _disk_full_prefix(self):
        out = []
        for (sid, s), (a, b) in sorted(self.disk_full.items()):
            out.append({"kind": "loss", "session": sid, "sensor": s, "first": a, "last": b,
                        "reason": "device_disk_full"})
        return out

    def _on_disk_full(self, ops):
        """The transaction failed: these samples are not on disk. Track them as lost."""
        self.m["disk_full_events"] = self.m.get("disk_full_events", 0) + 1
        if os.path.exists(self.ballast):
            os.remove(self.ballast)      # frees space so the loss record itself can be written
            log.error("disk full: released ballast")
        keep = []
        for op in ops:
            if op["kind"] == "loss" and op.get("reason") == "device_disk_full":
                continue                         # regenerated from self.disk_full next time
            bounded = op["kind"] == "data" or (op["kind"] == "loss" and op.get("last") is not None)
            if not bounded:
                keep.append(op)                  # session / end / open-ended loss: retry verbatim
                continue
            key = (op["session"], op["sensor"])
            a, b = op["first"], op["last"]
            if key in self.disk_full:
                self.disk_full[key] = [min(self.disk_full[key][0], a), max(self.disk_full[key][1], b)]
            else:
                self.disk_full[key] = [a, b]
        self.pending_ops = keep
        # Mock-reported losses inside a dropped range are folded into it and labelled
        # device_disk_full: still honest (not on disk), never invented.
