"""Shared validation for dashboard/relay commands (kept tiny so the poller can import it)."""
import math

RESET_CAP_MIN = 1.0
RESET_CAP_MAX = 1_000_000.0

def valid_capital(v):
    """Return a starting balance rounded to cents, or None if invalid.

    Rejects negatives (e.g. -5), zero, NaN/inf, booleans, non-numeric values,
    and amounts outside [$1, $1,000,000].
    """
    if isinstance(v, bool):
        return None
    try:
        if isinstance(v, str):
            s = v.strip().replace(",", "").replace(" ", "")
            if s.startswith("$"):
                s = s[1:]
            if s.endswith("$"):
                s = s[:-1]
            v = s
        x = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x) or x < RESET_CAP_MIN or x > RESET_CAP_MAX:
        return None
    return round(x, 2)
