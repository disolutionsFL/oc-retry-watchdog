"""Predicate framework for verifying side-effects of "successful" crons.

When a cron exits with status=ok but didn't actually do its work (the agent
reported success without invoking the tool, or the script ran but produced
no output), OpenClaw's webhook won't fire. Predicates close that gap by
asserting that real side effects happened.

Predicate types:
  - file_mtime: file at <path> has mtime within <max_age_minutes>
  - file_grew:  file at <path> grew since the last evaluation (size delta > 0)
  - json_field_count: load JSON at <path>, count entries in a list matching
                      a field condition; assert count_min/count_max bounds
  - http_health: GET <url>, expect <expected_status> (default 200)

Path placeholders:
  {TODAY}     -> current date YYYY-MM-DD in configured timezone
  {YESTERDAY} -> current date - 1 day, YYYY-MM-DD in configured timezone

Each predicate dict carries a human-readable `description` that surfaces in
alert email bodies when the predicate fails.
"""
from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


def _expand_path(path: str, tz_name: str) -> str:
    """Substitute {TODAY}/{YESTERDAY} placeholders + ~ expansion."""
    today = datetime.now(ZoneInfo(tz_name)).date()
    yesterday = today - timedelta(days=1)
    expanded = (path
                .replace("{TODAY}", today.isoformat())
                .replace("{YESTERDAY}", yesterday.isoformat()))
    return os.path.expanduser(expanded)


def evaluate(pred: dict, *, tz_name: str, state_get, state_set) -> tuple[bool, str]:
    """Evaluate a single predicate. Returns (passed, message).

    `state_get(key) -> dict|None` and `state_set(key, dict)` are persistence
    callbacks for stateful predicates (file_grew tracks last-known size).
    """
    ptype = pred.get("type")
    desc = pred.get("description", "")
    try:
        if ptype == "file_mtime":
            return _file_mtime(pred, tz_name, desc)
        if ptype == "file_grew":
            return _file_grew(pred, tz_name, desc, state_get, state_set)
        if ptype == "json_field_count":
            return _json_field_count(pred, tz_name, desc)
        if ptype == "http_health":
            return _http_health(pred, desc)
        return False, f"unknown predicate type {ptype!r}"
    except Exception as e:
        return False, f"{desc or ptype} raised {type(e).__name__}: {e}"


# ----- type implementations -----

def _file_mtime(pred: dict, tz_name: str, desc: str) -> tuple[bool, str]:
    path = _expand_path(pred["path"], tz_name)
    if not os.path.exists(path):
        return False, f"{desc or 'file_mtime'}: {path} does not exist"

    mtime = os.path.getmtime(path)
    age_seconds = time.time() - mtime
    max_age_min = float(pred.get("max_age_minutes", 60))
    if age_seconds > max_age_min * 60:
        age_min = age_seconds / 60
        return False, (f"{desc or 'file_mtime'}: {path} mtime is "
                       f"{age_min:.1f} min old, max allowed {max_age_min:.1f}")

    min_size = pred.get("min_size_bytes")
    if min_size is not None:
        size = os.path.getsize(path)
        if size < int(min_size):
            return False, (f"{desc or 'file_mtime'}: {path} size {size} < "
                           f"min {min_size}")

    return True, f"ok ({desc})"


def _file_grew(pred: dict, tz_name: str, desc: str,
               state_get, state_set) -> tuple[bool, str]:
    path = _expand_path(pred["path"], tz_name)
    if not os.path.exists(path):
        return False, f"{desc or 'file_grew'}: {path} does not exist"

    size = os.path.getsize(path)
    last_state = state_get("file_grew") or {}
    last_size = last_state.get("last_size")

    state_set("file_grew", {"last_size": size, "last_checked_at": time.time()})

    if last_size is None:
        # First run after deploy — baseline only, don't fail
        return True, f"baseline (first observation, size={size})"

    if size <= last_size:
        return False, (f"{desc or 'file_grew'}: {path} did not grow "
                       f"(was {last_size}, now {size})")
    return True, f"ok (grew from {last_size} to {size})"


def _json_field_count(pred: dict, tz_name: str, desc: str) -> tuple[bool, str]:
    """Count list-entries matching a field filter; assert count_min/count_max.

    Schema:
      path:        JSON file to load (placeholders allowed)
      list_path:   optional dot-path to the list inside the JSON. Empty/missing
                   = the file itself is a list at root.
      field:       name of the field to inspect on each list item
      filter:      one of:
                     "non_null"           — field has a value
                     "null"               — field is null/missing
                     {"equals": <value>}  — field == value
                     {"in": [<v1>, ...]}  — field ∈ values
      count_min:   inclusive lower bound on matches (optional)
      count_max:   inclusive upper bound on matches (optional)
    """
    path = _expand_path(pred["path"], tz_name)
    if not os.path.exists(path):
        return False, f"{desc or 'json_field_count'}: {path} does not exist"

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    list_path = pred.get("list_path", "")
    items = data
    for part in [p for p in list_path.split(".") if p]:
        if not isinstance(items, dict):
            return False, f"{desc}: list_path {list_path!r} did not resolve"
        items = items.get(part)
    if not isinstance(items, list):
        return False, f"{desc}: target is not a list (got {type(items).__name__})"

    field = pred.get("field")
    flt = pred.get("filter", "non_null")
    matcher = _build_field_matcher(field, flt)
    matched = sum(1 for it in items if isinstance(it, dict) and matcher(it))

    count_min = pred.get("count_min")
    count_max = pred.get("count_max")
    if count_min is not None and matched < int(count_min):
        return False, (f"{desc or 'json_field_count'}: matched={matched} < "
                       f"count_min={count_min} (filter={flt})")
    if count_max is not None and matched > int(count_max):
        return False, (f"{desc or 'json_field_count'}: matched={matched} > "
                       f"count_max={count_max} (filter={flt})")
    return True, f"ok (matched={matched})"


def _build_field_matcher(field: str | None, flt):
    """Return a predicate fn (item -> bool) implementing the filter."""
    if field is None:
        return lambda item: True
    if flt == "non_null":
        return lambda item: item.get(field) is not None
    if flt == "null":
        return lambda item: item.get(field) is None
    if isinstance(flt, dict):
        if "equals" in flt:
            target = flt["equals"]
            return lambda item: item.get(field) == target
        if "in" in flt and isinstance(flt["in"], list):
            targets = set(flt["in"])
            return lambda item: item.get(field) in targets
    # Unknown filter — default to non_null
    return lambda item: item.get(field) is not None


def _http_health(pred: dict, desc: str) -> tuple[bool, str]:
    url = pred["url"]
    timeout = float(pred.get("timeout_seconds", 5))
    expected = int(pred.get("expected_status", 200))
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status == expected:
                return True, f"ok ({resp.status})"
            return False, (f"{desc or 'http_health'}: {url} returned "
                           f"{resp.status}, expected {expected}")
    except Exception as e:
        return False, f"{desc or 'http_health'}: {url} -> {type(e).__name__}: {e}"
