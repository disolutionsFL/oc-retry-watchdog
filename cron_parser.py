"""Standard 5-field cron expression parser. Pure stdlib.

Supports the common syntax used by Unix cron:
  - `*`                  any value in the field's range
  - `N`                  single integer
  - `N-M`                inclusive range
  - `N,M,K`              comma-separated list (each item can also be N-M)
  - `*/K`                step over the full range
  - `N-M/K`              step over a sub-range
  - `@hourly` / `@daily` / `@weekly` / `@monthly` / `@yearly` aliases

Field order: minute hour day-of-month month day-of-week.

What's NOT supported (would need croniter or similar):
  - Day-of-week names (MON/TUE/...) — pre-translate or use integers (0-7, both Sun)
  - L (last), W (weekday), # (nth weekday) — extended cron forms
  - Seconds field (Quartz-style 6-field)

Day-of-month and day-of-week semantics: Vixie cron rule applies. If BOTH
DOM and DOW are restricted (not `*`), a fire matches if EITHER condition
is true. If only one is restricted, only that field's restriction applies.

Time zone is the caller's responsibility. The parser deals in naive
integers; fire_times() expects timezone-aware datetimes for the window
and emits timezone-aware datetimes for the matches.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable
from zoneinfo import ZoneInfo


_FIELD_RANGES = [
    (0, 59),   # minute
    (0, 23),   # hour
    (1, 31),   # day of month
    (1, 12),   # month
    (0, 6),    # day of week (0 = Sunday). Vixie cron also accepts 7 = Sunday.
]

_ALIASES = {
    "@yearly":   "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly":  "0 0 1 * *",
    "@weekly":   "0 0 * * 0",
    "@daily":    "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly":   "0 * * * *",
}


@dataclass(frozen=True)
class CronExpr:
    minutes: frozenset[int]
    hours: frozenset[int]
    doms: frozenset[int]
    months: frozenset[int]
    dows: frozenset[int]
    # True iff the original expression had unrestricted DOM (or DOW). Tracks
    # which side of the Vixie-cron DOM/DOW OR-rule applies.
    dom_unrestricted: bool
    dow_unrestricted: bool


def _expand_field(spec: str, low: int, high: int) -> frozenset[int]:
    out: set[int] = set()
    for piece in spec.split(","):
        piece = piece.strip()
        if not piece:
            raise ValueError(f"empty term in field {spec!r}")
        step = 1
        if "/" in piece:
            piece, step_str = piece.split("/", 1)
            try:
                step = int(step_str)
            except ValueError:
                raise ValueError(f"invalid step in {step_str!r}")
            if step < 1:
                raise ValueError(f"step must be >= 1 in {spec!r}")
        if piece == "*":
            start, end = low, high
        elif "-" in piece:
            try:
                a, b = piece.split("-", 1)
                start, end = int(a), int(b)
            except ValueError:
                raise ValueError(f"invalid range {piece!r}")
            if start > end:
                raise ValueError(f"reversed range {piece!r}")
        else:
            try:
                start = end = int(piece)
            except ValueError:
                raise ValueError(f"non-integer term {piece!r}")
        if start < low or end > high:
            raise ValueError(
                f"value(s) out of range for field [{low},{high}]: {piece!r}"
            )
        for v in range(start, end + 1, step):
            out.add(v)
    if not out:
        raise ValueError(f"no values produced for field {spec!r}")
    return frozenset(out)


def parse(expr: str) -> CronExpr:
    """Parse a 5-field cron expression (or @alias). Raises ValueError on bad input."""
    s = expr.strip()
    if not s:
        raise ValueError("empty cron expression")
    if s.startswith("@"):
        if s not in _ALIASES:
            raise ValueError(f"unknown cron alias {s!r}")
        s = _ALIASES[s]
    fields = s.split()
    if len(fields) != 5:
        raise ValueError(
            f"expected 5 cron fields, got {len(fields)}: {expr!r}"
        )
    minutes = _expand_field(fields[0], *_FIELD_RANGES[0])
    hours   = _expand_field(fields[1], *_FIELD_RANGES[1])
    doms    = _expand_field(fields[2], *_FIELD_RANGES[2])
    months  = _expand_field(fields[3], *_FIELD_RANGES[3])
    dow_field = fields[4]
    # Normalise Sunday=7 to 0 before expansion
    dows_pre = _expand_field(dow_field, 0, 7)
    dows = frozenset((d % 7) for d in dows_pre)
    return CronExpr(
        minutes=minutes,
        hours=hours,
        doms=doms,
        months=months,
        dows=dows,
        dom_unrestricted=(fields[2].strip() == "*"),
        dow_unrestricted=(dow_field.strip() == "*"),
    )


def _matches(expr: CronExpr, dt: datetime) -> bool:
    """Return True iff dt is a fire moment for expr (minute granularity)."""
    if dt.minute not in expr.minutes:
        return False
    if dt.hour not in expr.hours:
        return False
    if dt.month not in expr.months:
        return False
    # Vixie cron DOM/DOW OR-rule
    dow = (dt.weekday() + 1) % 7   # Python's weekday(): Mon=0..Sun=6. Convert to Sun=0..Sat=6.
    dom_ok = dt.day in expr.doms
    dow_ok = dow in expr.dows
    if expr.dom_unrestricted and expr.dow_unrestricted:
        return True   # both fields wildcarded — always match
    if expr.dom_unrestricted:
        return dow_ok
    if expr.dow_unrestricted:
        return dom_ok
    return dom_ok or dow_ok


def fire_times(expr: CronExpr, tz_name: str,
               since: datetime, until: datetime) -> list[datetime]:
    """Enumerate fire times of expr in the [since, until) window.

    `since` and `until` MUST be timezone-aware. Returned datetimes are in
    the supplied tz. Iteration is at minute granularity; for a 24h window
    that's at most 1440 evaluations per cron.
    """
    if since.tzinfo is None or until.tzinfo is None:
        raise ValueError("since/until must be timezone-aware")
    if until <= since:
        return []
    tz = ZoneInfo(tz_name) if tz_name else since.tzinfo
    start = since.astimezone(tz).replace(second=0, microsecond=0)
    if start < since.astimezone(tz):
        start = start + timedelta(minutes=1)
    end = until.astimezone(tz)
    out: list[datetime] = []
    cur = start
    while cur < end:
        if _matches(expr, cur):
            out.append(cur)
        cur = cur + timedelta(minutes=1)
    return out
