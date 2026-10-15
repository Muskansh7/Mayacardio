#!/usr/bin/env python3
"""STAND-IN for the provided check_ground_truth tool.

  python3 check_ground_truth_standin.py --truth truth.json --export export.jsonl [--session ID]

Checks, for one session:
  * every exported sample value equals the generator's value (unchanged)
  * no sample is exported twice (exactly once)
  * no exported sample lies in a range the mock never produced (dropouts)
  * every generated index is either exported or listed in a loss range (no silent loss)
  * loss ranges do not overlap exported samples
Exit code 0 = pass.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from standin_signal import SignalModel  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--truth", required=True)
    p.add_argument("--export", required=True)
    p.add_argument("--session", required=True)
    a = p.parse_args()
    truth = json.load(open(a.truth))
    sess = next((s for s in truth["sessions"] if s["session_id"] == a.session), None)
    if sess is None:
        sys.exit("session not in truth file")
    model = SignalModel(truth["mode"], truth["seed"], truth["duration"])
    seen = {1: set(), 2: set()}
    losses = {1: [], 2: []}
    errors = []
    dups = wrong = 0
    for line in open(a.export):
        r = json.loads(line)
        if "index" in r:
            s, i = r["sensor"], r["index"]
            if i in seen[s]:
                dups += 1
            seen[s].add(i)
            if r["values"] != model.sample(s, i):
                wrong += 1
        else:
            losses[r["sensor"]].append((r["from_index"], r["to_index"], r["reason"]))
    report = {"session": a.session}
    for s in (1, 2):
        gen = sess["generated"][str(s)]
        drop = sess["dropouts"][str(s)]
        in_drop = sum(1 for i in seen[s] if any(x <= i <= y for x, y in drop))
        lost_set = 0
        silent = []
        overlap = 0
        def lost(i):
            return any(x <= i and (y is None or i <= y) for x, y, _ in losses[s])
        for i in seen[s]:
            if lost(i):
                overlap += 1
        cur = None
        for i in range(gen):
            if i in seen[s]:
                continue
            if lost(i):
                lost_set += 1
                continue
            if cur and cur[1] == i - 1:
                cur[1] = i
            else:
                cur = [i, i]
                silent.append(cur)
        beyond = sum(1 for i in seen[s] if i >= gen + 400)   # truth file is written every 0.5 s
        report["sensor%d" % s] = {"generated": gen, "exported": len(seen[s]), "listed_lost": lost_set,
                                  "silent_loss_ranges": silent[:20], "silent_lost": sum(b - a + 1 for a, b in silent),
                                  "exported_from_dropout": in_drop, "loss_overlapping_data": overlap,
                                  "loss_reasons": sorted({r for *_, r in losses[s]}),
                                  "exported_beyond_generated": beyond}
        if silent or in_drop or overlap or beyond:
            errors.append("sensor %d problems" % s)
    report["duplicates"] = dups
    report["wrong_values"] = wrong
    if dups or wrong:
        errors.append("duplicates/wrong values")
    report["pass"] = not errors
    print(json.dumps(report))
    sys.exit(0 if not errors else 1)


if __name__ == "__main__":
    main()
