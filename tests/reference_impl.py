"""Independent, deliberately naive implementation of section 3, written straight from
the brief's wording (no incremental state). Used only to check pipeline.processing."""
import math
from decimal import Decimal, ROUND_HALF_UP


def rnd(x):
    return None if x is None else float(Decimal(x).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


def reference(s1, s2, n_windows):
    """s1, s2: lists indexed by sample index; entry is a tuple or None (missing).
    Indices beyond the list are missing."""
    def get(arr, i):
        return arr[i] if 0 <= i < len(arr) else None

    # peaks, scanning forward
    peaks = []
    last = None
    for i in range(len(s1)):
        if i < 800 or get(s1, i) is None or get(s1, i + 1) is None:
            continue
        before = [get(s1, j) for j in range(i - 800, i)]
        if any(b is None for b in before):
            continue
        v = s1[i][1]
        if not (v > s1[i - 1][1] and v > s1[i + 1][1]):
            continue
        if not (v > max(b[1] for b in before) / 2):
            continue
        if last is not None and i - last < 100:
            continue
        peaks.append(i)
        last = i
    out = []
    for w in range(n_windows):
        lo, hi = 400 * w, 400 * w + 399
        a1 = [get(s1, i) for i in range(lo, hi + 1) if get(s1, i) is not None]
        a2 = [get(s2, i) for i in range(lo, hi + 1) if get(s2, i) is not None]
        def st(rows, n):
            if not rows:
                return [None] * n, [None] * n, [None] * n
            return ([rnd(Decimal(sum(r[c] for r in rows)) / Decimal(len(rows))) for c in range(n)],
                    [min(r[c] for r in rows) for c in range(n)], [max(r[c] for r in rows) for c in range(n)])
        mean1, min1, max1 = st(a1, 3)
        mean2, min2, max2 = st(a2, 2)
        mag = rnd(math.fsum(math.sqrt(x * x + y * y) for x, y in a2) / len(a2)) if a2 else None
        hr = None
        a, b = 400 * (w - 4), 400 * (w + 1) - 1
        if w >= 4 and all(get(s1, i) is not None for i in range(a, b + 1)):
            p = [x for x in peaks if a <= x <= b]
            if len(p) >= 2:
                gaps = [(p[k + 1] - p[k]) / 400 for k in range(len(p) - 1)]
                mean_gap = Decimal(p[-1] - p[0]) / Decimal(400 * (len(p) - 1))   # exact mean of gaps
                hr = rnd(Decimal(60) / mean_gap)
        out.append({"window": w, "missing_1": 400 - len(a1), "missing_2": 400 - len(a2),
                    "complete": len(a1) == 400 and len(a2) == 400,
                    "mean_1": mean1, "min_1": min1, "max_1": max1, "mean_2": mean2, "min_2": min2,
                    "max_2": max2, "magnitude": mag, "heart_rate": hr})
    return out
