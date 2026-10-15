"""Criterion 7 check: incremental processor (device == cloud code) vs the naive reference.

Scenarios: clean signal, random gaps (single samples, window boundaries, long runs,
gaps near peaks), out-of-order arrival, sessions ending mid-window, ramp data.
Run: python3 tests/test_processing.py   (writes evidence/processing_equivalence.json)
"""
import json
import os
import random
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "standin_mock"))
from pipeline.processing import Coverage, Processor  # noqa: E402
from standin_signal import SignalModel  # noqa: E402
from reference_impl import reference  # noqa: E402

KEYS = ["missing_1", "missing_2", "complete", "mean_1", "min_1", "max_1", "mean_2", "min_2", "max_2",
        "magnitude", "heart_rate"]


def make(mode, seconds, seed, gaps_spec, extra=0):
    m = SignalModel(mode, seed, seconds + 5)
    n = seconds * 400 + extra
    s1 = [tuple(m.sample(1, i)) for i in range(n)]
    s2 = [tuple(m.sample(2, i)) for i in range(n)]
    for sensor, a, b in gaps_spec:
        arr = s1 if sensor == 1 else s2
        for i in range(a, min(b, n - 1) + 1):
            arr[i] = None
    return s1, s2


def run_incremental(s1, s2, shuffle_seed=None, chunk=137):
    """Feed records (data runs + loss runs + end) in possibly shuffled order."""
    recs = []
    for sensor, arr in ((1, s1), (2, s2)):
        i = 0
        n = len(arr)
        while i < n:
            present = arr[i] is not None
            j = i
            while j < n and (arr[j] is not None) == present and j - i < chunk:
                j += 1
            recs.append(("data" if present else "loss", sensor, i, j - 1))
            i = j
        recs.append(("end", sensor, n - 1, n - 1))
    rng = random.Random(shuffle_seed)
    if shuffle_seed is not None:
        rng.shuffle(recs)
    cov = Coverage()
    store = {1: {}, 2: {}}

    def fetch(sensor, lo, hi):
        d = store[sensor]
        return [d.get(i) for i in range(lo, hi + 1)]

    p = Processor("t", fetch, cov, "test")
    out = []
    for kind, sensor, a, b in recs:
        if kind == "data":
            arr = s1 if sensor == 1 else s2
            for i in range(a, b + 1):
                store[sensor][i] = arr[i]
        cov.add_record(kind, sensor, a, b)
        out += p.step()
    return out


def compare(name, s1, s2, shuffle_seed=None):
    n_windows = (max(len(s1), len(s2)) - 1) // 400 + 1
    ref = reference(s1, s2, n_windows)
    got = run_incremental(s1, s2, shuffle_seed)
    mism = []
    if len(got) != len(ref):
        mism.append({"windows_ref": len(ref), "windows_got": len(got)})
    for r, g in zip(ref, got):
        for k in KEYS:
            if r[k] != g[k]:
                mism.append({"window": r["window"], "key": k, "ref": r[k], "got": g[k]})
    hr = [g["heart_rate"] for g in got if g["heart_rate"] is not None]
    return {"scenario": name, "windows": len(ref), "mismatches": len(mism), "examples": mism[:5],
            "hr_windows": len(hr), "pass": not mism}


def main():
    res = []
    s1, s2 = make("signal", 60, 7, [])
    res.append(compare("clean signal 60 s", s1, s2))
    rng = random.Random(1)
    gaps = []
    for _ in range(25):
        s = rng.choice([1, 2])
        a = rng.randrange(0, 120 * 400)
        gaps.append((s, a, a + rng.choice([0, 0, 1, 5, 99, 400, 1200])))
    gaps += [(1, 400 * 10, 400 * 10), (1, 400 * 20 - 1, 400 * 20 - 1), (2, 400 * 30, 400 * 31 - 1),
             (1, 400 * 40, 400 * 41 - 1), (2, 0, 399)]
    s1, s2 = make("signal", 120, 7, gaps)
    res.append(compare("signal 120 s with 30 gaps, in order", s1, s2))
    res.append(compare("same, records shuffled (out-of-order arrival)", s1, s2, shuffle_seed=3))
    s1, s2 = make("signal", 30, 9, [(1, 5000, 5300)], extra=137)
    res.append(compare("session ending mid-window", s1, s2, shuffle_seed=5))
    s1, s2 = make("ramp", 20, 7, [(2, 100, 900)])
    res.append(compare("ramp 20 s", s1, s2))
    # heart-rate plausibility on clean signal across the 60/90/72 schedule
    s1, s2 = make("signal", 400, 7, [])
    got = run_incremental(s1, s2)
    def avg(a, b):
        v = [g["heart_rate"] for g in got if a <= g["window"] < b and g["heart_rate"] is not None]
        return round(sum(v) / len(v), 1) if v else None
    plaus = {"minutes_0_2_expected_60": avg(10, 115), "minutes_2_4_expected_90": avg(130, 235),
             "minutes_4_6_expected_72": avg(250, 355)}
    res.append(compare("clean signal 400 s (heart-rate schedule)", s1, s2) | {"hr_means": plaus})
    out = {"all_pass": all(r["pass"] for r in res), "scenarios": res}
    os.makedirs(os.path.join(ROOT, "evidence"), exist_ok=True)
    with open(os.path.join(ROOT, "evidence", "processing_equivalence.json"), "w") as f:
        json.dump(out, f, indent=1)
    print(json.dumps(out, indent=1))
    sys.exit(0 if out["all_pass"] else 1)


if __name__ == "__main__":
    main()
