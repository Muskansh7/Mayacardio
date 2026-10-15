# Design note

## Architecture
The **device** runs one program with three threads sharing a SQLite database (WAL mode, `synchronous=FULL`).

1. The **collector** reads both sensors from the mock. It checks every reply's CRC-32, sample shape and 24-bit range. Each reply is then appended as a *record* to an append-only log in one transaction, together with the per-sensor read cursor. There are three record kinds:
   - `data`: a run of consecutive samples
   - `loss`: an index range plus a reason; the end index can be null if unknown
   - `end`: the session has ended
2. The **uploader** sends records to the cloud in `seq` order (gzip JSON over HTTPS, bearer token) and stores the highest acknowledged `seq`. The mode is set by contact with the cloud: offline after 4 s without a successful request, and online on the first success. Probes run every 1 s, so a switch happens well within 10 s.
3. The **local processor** runs the section-3 logic for the current session. It stores results with `mode: offline` while offline, and while the cloud is still catching up.

The **cloud** (`cloud/server.py`) stores records with `INSERT OR IGNORE` on (session, sensor, kind, first index). It then runs the *same* processing module with `mode: online` as soon as each window is resolved.

## Guarantees
- **Exactly once on disk.** The collector keeps the invariant that, for each sensor, indices 0…cursor−1 are covered once by data or loss records. The cursor moves in the same transaction as the data. After a restart, it re-reads from the cursor with `from_index`, so a process kill shorter than the mock's 10 s buffer loses nothing.
- **No silent loss, no invented values.** Every gap becomes a loss record with a reason:
  - `buffer_overflow` or `sensor_dropout`: reported by the mock
  - `not_delivered_by_mock`: a gap still present after one re-read
  - `mock_session_restarted…` with `to_index: null`
  - `device_disk_full`
- **Exactly once in the cloud.** Uploads are idempotent, so resends after crashes or network cuts are deduplicated. Catch-up sends the whole backlog in order.
- **Identical results on both sides.** A window is finalised only when every index it depends on is present, recorded as lost, or past the session end. Results therefore do not depend on arrival order or on where they are computed. This is checked against an independent naive implementation (`evidence/processing_equivalence.json`), and the heart rate reproduces 60/90/72 bpm.
- **Robust to clock changes.** Timing logic uses the monotonic clock only.
- **Handles a full disk.** A 4 MB reserve file is released on `SQLITE_FULL` so the loss record can always be written. Samples read while the disk stays full are tracked in memory and written as one `device_disk_full` range once space returns.

## Not guaranteed
- **Power cut while the disk is full.** The in-memory range is lost. On restart it appears as a re-read gap, and is still recorded with a reason, not lost silently.
- **Online latency during catch-up.** Processing is sequential, because the peak rule depends on the previous peak. While a backlog is replayed, new cloud results wait. The device keeps producing offline results in the meantime.
- **SD-card durability.** This relies on the card honouring fsync. A high-endurance card is recommended.

## Maximum data loss on a power cut: ≤ 1 s
A power cut kills the device and the mock together. Committed data is fsync'd. What is lost is what the mock produced but had not yet delivered and committed. With a polling interval of 0.1–0.25 s, plus one reply of at most 1 s of data, plus fsync time, this stays under 1 s. That range is listed as an open loss (`to_index: null`) of the old session.

## Interpretations (flagged)
- Sensor 1, channel 2 is `values[1]`.
- `heart_rate` is null only if a **sensor-1** sample is missing in the 5 s span.
- Rounding is half away from zero; min and max stay integers.
- The mean gap is (last peak − first peak) / (n − 1).
- In a session's final partial window, samples that don't exist count as missing.
