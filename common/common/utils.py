from datetime import datetime, timezone
from typing import Any, Optional
from dateutil import parser as time_parser


def _parse_string_datetime(s: str) -> Optional[datetime]:
    """Try to parse a datetime string. Uses stdlib first, then dateutil if available."""
    try:
        # try ISO formats and common variants
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return time_parser.parse(s)
        except Exception:
            return None


def to_timestamp(ts: Any, default: Any = None) -> Optional[datetime]:
    """Convert various timestamp formats to a `datetime` or return `default`.

    Supported inputs:
    - `None` -> returns `default`
    - `datetime` -> returned as-is
    - `int`/`float` -> treated as Unix timestamp in seconds or milliseconds
      (heuristic: > 1e12 -> milliseconds)
    - `str` -> parsed with `datetime.fromisoformat`, falling back to
      `dateutil.parser.parse` if available.
    """
    if ts is None:
        return default
    if isinstance(ts, datetime):
        return ts
    try:
        if isinstance(ts, (int, float)):
            # Handle Unix timestamp in seconds or milliseconds
            # ALWAYS use UTC timezone to prevent local time conversion
            if ts > 1e12:  # Likely in milliseconds
                return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
            else:  # Likely in seconds
                return datetime.fromtimestamp(ts, tz=timezone.utc)
        elif isinstance(ts, str):
            parsed = _parse_string_datetime(ts)
            return parsed if parsed is not None else default
        else:
            return default
    except Exception:
        return default


def ts_gap(ts1: Any, ts2: Any) -> Optional[int]:
    """Calculate the gap in seconds between two timestamps.

    Accepts `datetime`, numeric unix timestamps (s or ms), or parseable strings.
    Returns integer number of seconds `(ts2 - ts1)` or `None` when inputs are invalid.
    """
    if ts1 is None or ts2 is None:
        return None
    t1 = to_timestamp(ts1)
    t2 = to_timestamp(ts2)
    if t1 is None or t2 is None:
        return None
    return int((t2 - t1).total_seconds())

