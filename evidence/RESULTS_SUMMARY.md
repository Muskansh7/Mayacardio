# Evidence summary (honest status)
Environment: x86-64 Linux container with 1 vCPU, using the **stand-in** mock. This is **not** a Raspberry Pi 4 and **not** a real cloud account.

**Completed**
- **Processing equivalence** (`processing_equivalence.json`): all 6 scenarios pass with 0 mismatches against the naive reference. The scenarios include gaps, out-of-order arrival and a mid-window session end. Heart rate averages exactly 60, 90 and 72 bpm across the schedule.
- **Baseline online run** (`failure_tests.json` → baseline): device and cloud exports both pass the ground-truth check, with 0 duplicates and 0 silent loss. Online result latency is median 0.18 s and max 0.36 s, against a 5 s limit.

**Not yet run (scripts are ready)**
Run `sudo python3 tests/failure_tests.py mock_faults process_kill network_cut power_cut disk_full clock_change` to cover:
- mock faults
- process kill
- network cut
- simulated power cut
- disk full
- clock change

**Still to do on real hardware**
- The 2-hour outage catch-up test.
- The 1-hour resource figures. Use `tools/measure_resources.py`, or `pidstat`, `/proc/<pid>/io`, `VmHWM` and the `metrics.net_bytes_sent` value from status.
- The demo video (see `DEMO_SCRIPT.md`).
- Deployment to a free-tier cloud account.
