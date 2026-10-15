"""Section 3 processing logic, shared unchanged by the device (offline mode)
and the cloud (online mode).

The processor is incremental: it finalises window w only when every sample
index it depends on is *resolved*: present, recorded as lost, or beyond the
end of the session. Because it never guesses, results are identical whatever
order the data arrived in, and identical on the device and in the cloud.

Interpretations of the brief (also stated in DESIGN.md):
  * "sensor 1, channel 2" = values[1] of a sensor-1 sample.
  * heart_rate is null if any SENSOR-1 sample in the 5-second span is missing
    (the heart rate is derived from sensor 1 only).
  * min/max are integers; mean, magnitude and heart_rate are rounded half away
    from zero to 1 decimal place.
  * "Mean gap between consecutive peaks" = (last - first) / (n - 1).
  * A window that runs past the end of a session counts the non-existent
    samples as missing.
"""
from collections import deque
from decimal import Decimal, ROUND_HALF_UP
import math

WIN = 400            # samples per window (1 s at 400 Hz)
LOOKBACK = 800       # samples that must precede a peak
MIN_PEAK_GAP = 100   # samples between peaks
HR_WINDOWS = 5       # heart rate uses the last 5 seconds
INF = float("inf")


def r1(x):
    """Round to 1 decimal place, half away from zero."""
    if x is None:
        return None
    d = x if isinstance(x, Decimal) else Decimal(x)
    return float(d.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


class Intervals:
    """Sorted, merged closed intervals [a, b]; b may be INF."""

    def __init__(self):
        self.iv = []

    def add(self, a, b):
        iv = self.iv
        if iv and a >= iv[-1][0]:
            if a <= iv[-1][1] + 1:          # touches/overlaps the last interval
                iv[-1][1] = max(iv[-1][1], b)
                return
            iv.append([a, b])               # strictly after: append
            return
        iv.append([a, b])                   # out of order: full merge
        iv.sort()
        out = []
        for x, y in iv:
            if out and x <= out[-1][1] + 1:
                out[-1][1] = max(out[-1][1], y)
            else:
                out.append([x, y])
        self.iv = out

    def resolved_until(self):
        """R such that every index < R is covered, contiguously from 0."""
        if not self.iv or self.iv[0][0] > 0:
            return 0
        return self.iv[0][1] + 1


class Coverage:
    """What is known about one session: which indices are resolved, per sensor."""

    def __init__(self):
        self.iv = {1: Intervals(), 2: Intervals()}
        self.max_present = {1: -1, 2: -1}
        self.terminal = {1: False, 2: False}
        self.end_idx = {1: None, 2: None}

    def add_record(self, kind, sensor, first, last):
        if kind == "data":
            self.iv[sensor].add(first, last)
            self.max_present[sensor] = max(self.max_present[sensor], last)
        elif kind == "loss":
            self.iv[sensor].add(first, INF if last is None else last)
            if last is None:
                self.terminal[sensor] = True
        elif kind == "end":
            # `first` is the index of the session's last sample
            self.iv[sensor].add(first + 1, INF)
            self.end_idx[sensor] = first
            self.terminal[sensor] = True

    def R(self, sensor):
        return self.iv[sensor].resolved_until()

    def last_window(self):
        """Index of the final window once the session is over, else None."""
        if not (self.terminal[1] and self.terminal[2]):
            return None
        last = max(self.max_present[1], self.max_present[2],
                   -1 if self.end_idx[1] is None else self.end_idx[1],
                   -1 if self.end_idx[2] is None else self.end_idx[2])
        return last // WIN if last >= 0 else -1


class Processor:
    """Incremental implementation of section 3 for one session.

    fetch(sensor, lo, hi) returns a list of length hi-lo+1 holding each
    sample's value tuple, or None where the sample is not available.
    """

    def __init__(self, session, fetch, coverage, mode):
        self.session, self.fetch, self.cov, self.mode = session, fetch, coverage, mode
        self.next_w = 0
        self.last_peak = None
        self.peaks = deque()
        self.run_start = 0       # first index of the current unbroken run of sensor-1 samples
        self.dq = deque()        # monotonic deque of (idx, value): max of the previous <=800 values
        self.prev = None         # sensor-1 channel-2 value at i-1
        self.missing_hist = deque(maxlen=HR_WINDOWS)

    def ready(self, w):
        lw = self.cov.last_window()
        if lw is not None and w > lw:
            return False
        # sensor 1 needs the sample after the window (peak test); sensor 2 does not
        return self.cov.R(1) > WIN * w + WIN and self.cov.R(2) > WIN * w + WIN - 1

    def step(self, limit=None):
        out = []
        while (limit is None or len(out) < limit) and self.ready(self.next_w):
            out.append(self._window(self.next_w))
            self.next_w += 1
        return out

    def _window(self, w):
        lo = WIN * w
        s1 = self.fetch(1, lo, lo + WIN)        # 401 entries
        s2 = self.fetch(2, lo, lo + WIN - 1)
        ch2 = [None if s is None else s[1] for s in s1]

        # ---- peak detection for indices lo .. lo+399 ----
        dq, run_start, prev, last_peak = self.dq, self.run_start, self.prev, self.last_peak
        for k in range(WIN):
            i = lo + k
            v = ch2[k]
            if v is None:
                run_start = i + 1
                dq.clear()
                prev = None
                continue
            if i - LOOKBACK >= run_start:       # the 800 samples before i are available
                nxt = ch2[k + 1]
                if (nxt is not None and v > prev and v > nxt and 2 * v > dq[0][1]
                        and (last_peak is None or i - last_peak >= MIN_PEAK_GAP)):
                    last_peak = i
                    self.peaks.append(i)
            while dq and dq[-1][1] <= v:
                dq.pop()
            dq.append((i, v))
            while dq[0][0] < i + 1 - LOOKBACK:
                dq.popleft()
            prev = v
        self.run_start, self.prev, self.last_peak = run_start, prev, last_peak

        # ---- per-window statistics ----
        a1 = [s for s in s1[:WIN] if s is not None]
        a2 = [s for s in s2 if s is not None]
        m1, m2 = WIN - len(a1), WIN - len(a2)

        def stats(rows, nch):
            if not rows:
                return [None] * nch, [None] * nch, [None] * nch
            n = Decimal(len(rows))
            cols = list(zip(*rows))
            return ([r1(Decimal(sum(c)) / n) for c in cols],
                    [min(c) for c in cols], [max(c) for c in cols])

        mean1, min1, max1 = stats(a1, 3)
        mean2, min2, max2 = stats(a2, 2)
        mag = (r1(math.fsum(math.sqrt(x * x + y * y) for x, y in a2) / len(a2))
               if a2 else None)

        # ---- heart rate over windows w-4 .. w ----
        self.missing_hist.append(m1)
        start = WIN * (w - 4)
        while self.peaks and self.peaks[0] < start:
            self.peaks.popleft()
        hr = None
        if w >= 4 and sum(self.missing_hist) == 0 and len(self.peaks) >= 2:
            p = self.peaks
            # mean gap (s) = (p_last - p_first) / (n-1) / 400 ; bpm = 60 / gap
            hr = r1(Decimal(60 * WIN * (len(p) - 1)) / Decimal(p[-1] - p[0]))

        return {"session": self.session, "window": w, "mode": self.mode,
                "missing_1": m1, "missing_2": m2, "complete": m1 == 0 and m2 == 0,
                "mean_1": mean1, "min_1": min1, "max_1": max1,
                "mean_2": mean2, "min_2": min2, "max_2": max2,
                "magnitude": mag, "heart_rate": hr}
