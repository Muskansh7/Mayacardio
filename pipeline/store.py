"""Durable append-only record log on the device (SQLite, WAL, synchronous=FULL).

One table holds everything the cloud must eventually have, in arrival order:
  kind='data' : a run of consecutive samples  [first_idx .. last_idx]
  kind='loss' : a range that will never exist [first_idx .. last_idx|NULL] + reason
  kind='end'  : the session ended; first_idx = index of its last sample
`seq` gives a total order used for upload acknowledgement.

Invariant kept by the collector: for every (session, sensor), records cover
indices 0 .. next_idx-1 exactly once, without overlap. So each index is either
saved once or listed once as lost, and nothing is silently missing.
"""
import json
import os
import sqlite3
import zlib

from .processing import Coverage

SCHEMA = """
CREATE TABLE IF NOT EXISTS records(
  seq       INTEGER PRIMARY KEY AUTOINCREMENT,
  kind      TEXT    NOT NULL,
  session   TEXT    NOT NULL,
  sensor    INTEGER NOT NULL,
  first_idx INTEGER NOT NULL,
  last_idx  INTEGER,
  reason    TEXT,
  data      TEXT,
  crc       TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS records_key ON records(session, sensor, kind, first_idx);
CREATE TABLE IF NOT EXISTS cursors(
  session TEXT NOT NULL, sensor INTEGER NOT NULL,
  next_idx INTEGER NOT NULL, closed INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(session, sensor)
);
CREATE TABLE IF NOT EXISTS sessions(session TEXT PRIMARY KEY, opened_seq INTEGER);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS results(
  session TEXT NOT NULL, window INTEGER NOT NULL, mode TEXT NOT NULL, json TEXT NOT NULL,
  PRIMARY KEY(session, window, mode)
);
"""

MAX_CHUNK = 4000


def crc_hex(text):
    return format(zlib.crc32(text.encode()) & 0xFFFFFFFF, "08x")


def connect(path, durable=True):
    db = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    # FULL: every COMMIT is fsync'd before it returns, so it survives a power cut
    db.execute("PRAGMA synchronous=FULL" if durable else "PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(SCHEMA)
    return db


class Store:
    def __init__(self, data_dir, durable=True):
        os.makedirs(data_dir, exist_ok=True)
        self.dir = data_dir
        self.path = os.path.join(data_dir, "pipeline.db")
        self.db = connect(self.path, durable)

    # ---------- meta ----------
    def get_meta(self, k, default=None):
        r = self.db.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return default if r is None else r[0]

    def set_meta(self, k, v):
        self.db.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                        (k, str(v)))

    # ---------- writes (collector) ----------
    def write_batch(self, ops, cursors):
        """Atomically append records and advance cursors. ops: list of dicts."""
        db = self.db
        db.execute("BEGIN IMMEDIATE")
        try:
            for op in ops:
                if op["kind"] == "session":
                    db.execute("INSERT OR IGNORE INTO sessions(session, opened_seq) VALUES(?, "
                               "(SELECT IFNULL(MAX(seq),0) FROM records))", (op["session"],))
                    continue
                db.execute("INSERT INTO records(kind,session,sensor,first_idx,last_idx,reason,data,crc)"
                           " VALUES(?,?,?,?,?,?,?,?)",
                           (op["kind"], op["session"], op["sensor"], op["first"], op.get("last"),
                            op.get("reason"), op.get("data"),
                            crc_hex(op["data"]) if op.get("data") is not None else None))
            for (session, sensor), (nxt, closed) in cursors.items():
                db.execute("INSERT INTO cursors(session,sensor,next_idx,closed) VALUES(?,?,?,?) "
                           "ON CONFLICT(session,sensor) DO UPDATE SET next_idx=excluded.next_idx,"
                           " closed=excluded.closed", (session, sensor, nxt, int(closed)))
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise

    def cursors(self):
        return {(s, n): (i, bool(c)) for s, n, i, c in
                self.db.execute("SELECT session,sensor,next_idx,closed FROM cursors")}

    def sessions(self):
        return [r[0] for r in self.db.execute("SELECT session FROM sessions ORDER BY opened_seq, rowid")]

    def latest_session(self):
        s = self.sessions()
        return s[-1] if s else None

    # ---------- reads ----------
    def records_after(self, seq, limit=500, max_samples=40000):
        rows = self.db.execute(
            "SELECT seq,kind,session,sensor,first_idx,last_idx,reason,data,crc FROM records "
            "WHERE seq>? ORDER BY seq LIMIT ?", (seq, limit)).fetchall()
        out, n = [], 0
        for r in rows:
            out.append(r)
            if r[1] == "data":
                n += r[5] - r[4] + 1
            if n >= max_samples:
                break
        return out

    def max_seq(self):
        return self.db.execute("SELECT IFNULL(MAX(seq),0) FROM records").fetchone()[0]

    def coverage(self, session, upto_seq=None):
        cov = Coverage()
        q = "SELECT kind,sensor,first_idx,last_idx FROM records WHERE session=?"
        args = [session]
        if upto_seq is not None:
            q += " AND seq<=?"
            args.append(upto_seq)
        for kind, sensor, a, b in self.db.execute(q + " ORDER BY seq", args):
            cov.add_record(kind, sensor, a, b)
        return cov

    def fetch(self, session, sensor, lo, hi):
        return fetch_range(self.db, session, sensor, lo, hi)

    def sample_count(self, upto_seq=None):
        q = "SELECT IFNULL(SUM(last_idx-first_idx+1),0) FROM records WHERE kind='data'"
        if upto_seq is None:
            return self.db.execute(q).fetchone()[0]
        return self.db.execute(q + " AND seq<=?", (upto_seq,)).fetchone()[0]


def fetch_range(db, session, sensor, lo, hi):
    """Values for indices lo..hi (None where unavailable)."""
    out = [None] * (hi - lo + 1)
    rows = db.execute(
        "SELECT first_idx,last_idx,data FROM records WHERE kind='data' AND session=? AND sensor=?"
        " AND first_idx BETWEEN ? AND ? AND last_idx>=?",
        (session, sensor, lo - MAX_CHUNK, hi, lo)).fetchall()
    for a, b, data in rows:
        vals = json.loads(data)
        for i in range(max(a, lo), min(b, hi) + 1):
            out[i - lo] = tuple(vals[i - a])
    return out


def export_lines(db, session):
    """Yield the export format: one line per sample, then one per loss."""
    for sensor in (1, 2):
        for a, b, data in db.execute(
                "SELECT first_idx,last_idx,data FROM records WHERE kind='data' AND session=? "
                "AND sensor=? ORDER BY first_idx", (session, sensor)):
            for k, v in enumerate(json.loads(data)):
                yield json.dumps({"sensor": sensor, "index": a + k, "values": v}, separators=(", ", ": "))
    for sensor, a, b, reason in db.execute(
            "SELECT sensor,first_idx,last_idx,reason FROM records WHERE kind='loss' AND session=? "
            "ORDER BY sensor, first_idx", (session,)):
        yield json.dumps({"sensor": sensor, "from_index": a, "to_index": b, "reason": reason},
                         separators=(", ", ": "))
