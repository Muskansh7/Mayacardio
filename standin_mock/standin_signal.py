"""Deterministic synthetic sensor values for the stand-in mock (any index, any order)."""
import bisect
import math

RATE = 400
MOD = 8388608
LIM = (1 << 23) - 1


def _noise(seed, sensor, i):
    """Deterministic pseudo-random integer in [-1000, 1000] (splitmix64)."""
    z = (seed * 0x9E3779B97F4A7C15 + sensor * 0xBF58476D1CE4E5B9 + i * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    z ^= z >> 31
    return (z % 2001) - 1000


def hr_at(t):
    m = (t / 60.0) % 6
    return 60 if m < 2 else (90 if m < 4 else 72)


class SignalModel:
    def __init__(self, mode, seed, duration):
        self.mode, self.seed = mode, seed
        self.beats = []
        if mode == "signal":
            t = 0.37
            while t < duration + 2:
                self.beats.append(t * RATE)
                t += 60.0 / hr_at(t)

    def sample(self, sensor, i):
        if self.mode == "ramp":
            if sensor == 1:
                return [(3 * i + c) % MOD for c in (1, 2, 3)]
            return [(2 * i + c) % MOD for c in (1, 2)]
        if sensor == 1:
            v = self._ecg(i)
            return [max(-LIM, min(LIM, int(round(v * k)))) for k in (0.6, 1.0, 1.3)]
        t = i / RATE
        c1 = 400000 + 20000 * math.sin(2 * math.pi * t / 30.0) + _noise(self.seed, 21, i)
        c2 = 150000 + 8000 * math.sin(2 * math.pi * t / 47.0 + 1.0) + _noise(self.seed, 22, i)
        return [int(round(c1)), int(round(c2))]

    def _ecg(self, i):
        v = 30000 * math.sin(2 * math.pi * i / (RATE * 7.0))       # baseline wander
        v += _noise(self.seed, 1, i) * 8                           # +-8000 noise
        k = bisect.bisect_right(self.beats, i + 200)
        for b in self.beats[max(0, k - 3):k]:
            d = i - b
            v += 1500000 * math.exp(-0.5 * (d / 3.0) ** 2)          # sharp R peak
            v += 350000 * math.exp(-0.5 * ((d - 110) / 22.0) ** 2)  # T wave
            v -= 150000 * math.exp(-0.5 * ((d + 12) / 4.0) ** 2)    # Q dip
        return v
