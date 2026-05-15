"""AI-assisted predicate suggestion.

Reads OpenClaw's `openclaw.json` to discover configured providers + models,
calls a chosen model via the OpenAI-compatible chat-completions endpoint,
and parses the response into validated predicate dicts.

The model used is operator-configurable in Settings (primary + fallback)
— independent of which model each cron uses for its own work. Lets the
operator pick a smart model (e.g. qwen3.6-35b) for predicate suggestion
regardless of what individual crons run on.
"""
from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# ----- openclaw.json discovery ---------------------------------------------

def read_openclaw_models(openclaw_config_path: str) -> list[dict[str, Any]]:
    """Walk openclaw.json's providers + their models, return a flat list of
    options the operator can pick from. Each entry:

      {
        "key":          "<provider_id>/<model_id>",   # matches cron payload.model
        "label":        "<model_name> @ <provider_id>",
        "provider_id":  "vllm-c3po",
        "model_id":     "qwen3.6-35b",
        "base_url":     "http://...:8100/v1",
        "api_kind":     "openai-completions" | "ollama" | ...,
        "context_window": int | None,
        "max_tokens":   int | None,
        "has_api_key":  bool,        # whether the provider has apiKey set
      }
    """
    p = Path(os.path.expanduser(openclaw_config_path))
    if not p.exists():
        log.warning("openclaw config not found at %s", p)
        return []
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("could not parse openclaw config %s: %s", p, e)
        return []

    providers = (((d.get("models") or {}).get("providers")) or {})
    out: list[dict[str, Any]] = []
    for prov_id, prov in providers.items():
        if not isinstance(prov, dict):
            continue
        base_url = prov.get("baseUrl")
        api_kind = prov.get("api")
        has_key = bool(prov.get("apiKey"))
        for m in prov.get("models") or []:
            if not isinstance(m, dict):
                continue
            mid = m.get("id")
            if not mid:
                continue
            out.append({
                "key": f"{prov_id}/{mid}",
                "label": f"{m.get('name', mid)} @ {prov_id}",
                "provider_id": prov_id,
                "model_id": mid,
                "base_url": base_url,
                "api_kind": api_kind,
                "context_window": m.get("contextWindow"),
                "max_tokens": m.get("maxTokens"),
                "has_api_key": has_key,
            })
    return out


def get_model_endpoint(openclaw_config_path: str, model_key: str
                       ) -> dict[str, Any] | None:
    """Return the full descriptor for a model_key like 'vllm-c3po/qwen3.6-35b',
    including the apiKey if present. Used at call time."""
    p = Path(os.path.expanduser(openclaw_config_path))
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    if "/" not in model_key:
        return None
    prov_id, mid = model_key.split("/", 1)
    prov = (((d.get("models") or {}).get("providers")) or {}).get(prov_id)
    if not isinstance(prov, dict):
        return None
    for m in prov.get("models") or []:
        if m.get("id") == mid:
            return {
                "provider_id": prov_id,
                "model_id": mid,
                "base_url": prov.get("baseUrl"),
                "api_kind": prov.get("api"),
                "api_key": prov.get("apiKey"),
                "max_tokens": m.get("maxTokens"),
            }
    return None


# ----- OpenAI-compatible chat call -----------------------------------------

def chat_completion(*, base_url: str, model: str, messages: list[dict],
                    api_key: str | None = None, max_tokens: int = 1024,
                    timeout_seconds: int = 60, temperature: float = 0.2
                    ) -> tuple[bool, str]:
    """POST to <base_url>/chat/completions. Returns (ok, content_or_error)."""
    url = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = ""
        return False, f"HTTP {e.code}: {err_body[:500]}"
    except urllib.error.URLError as e:
        return False, f"URLError: {e}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

    choices = data.get("choices") or []
    if not choices:
        return False, f"no choices in response: {str(data)[:500]}"
    msg = choices[0].get("message") or {}
    content = msg.get("content")
    # Qwen3-family "thinking" mode (and similar) sometimes returns the actual
    # response in `reasoning` with `content: null`. Fall back to reasoning so
    # the parser can still find JSON in it. The `/no_think` directive in our
    # user prompt typically prevents this, but not all models honor it.
    if not content:
        content = msg.get("reasoning") or ""
    if not content:
        return False, f"empty content+reasoning in choice 0: {str(choices[0])[:500]}"
    return True, content


# ----- prompt + parsing -----------------------------------------------------

_SYSTEM = """You are an SRE helping configure side-effect predicates for an OpenClaw cron.
A predicate runs after every status=ok cron completion. If any predicate fails, the watchdog re-fires the cron and emails the operator. The goal is to detect cases where the cron exits cleanly but its expected side effects (file writes, data mutations, output deliveries, etc.) did NOT happen — agents sometimes claim success in their narrative without actually invoking the tools.

Output ONLY a JSON array. No prose, no markdown, no code fences. The array must contain 1-4 predicate objects. Available types:

1. file_mtime
   {"type":"file_mtime", "path":"...", "max_age_minutes":N, "min_size_bytes":N (optional), "description":"..."}
   Asserts the file at `path` was modified within max_age_minutes of NOW.
   Path supports {TODAY} and {YESTERDAY} placeholders (resolved server-side in the cron's timezone, YYYY-MM-DD).

2. file_grew
   {"type":"file_grew", "path":"...", "description":"..."}
   Asserts the file size strictly increased since the last scan.

3. json_field_count
   {"type":"json_field_count", "path":"...", "list_path":"" (optional, dot-path to the list inside the JSON if not at root), "field":"...", "filter": "non_null" | "null" | {"equals":X} | {"in":[X,Y]}, "count_min":N (optional), "count_max":N (optional), "description":"..."}
   Loads JSON, locates a list, counts items where `field` matches the filter, asserts bounds.

4. http_health
   {"type":"http_health", "url":"...", "timeout_seconds":N (optional, default 5), "expected_status":N (optional, default 200), "description":"..."}
   GETs the URL, expects the status code.

Each `description` is one short sentence describing what business outcome the predicate verifies; this shows up in the operator's alert email if the predicate trips.

Choose predicates that are SPECIFIC to this cron's actual outputs — not generic. If the cron writes a JSON file, prefer json_field_count over file_mtime alone."""


def build_messages(*, cron_name: str, agent: str, schedule: str,
                   cron_prompt: str, recent_summaries: list[str],
                   existing_predicates: list[dict]) -> list[dict]:
    # The "/no_think" directive tells Qwen3-family models to skip chain-of-
    # thought reasoning and emit the final answer directly. Saves latency
    # and avoids the trap where the JSON ends up in the `reasoning` field
    # with `content: null`.
    user = f"""/no_think

Cron name: {cron_name}
Agent: {agent}
Schedule: {schedule}

Cron prompt:
\"\"\"
{cron_prompt[:4000]}
\"\"\"

Recent successful run summaries (most recent first):
{chr(10).join(f"- {s[:300]}" for s in recent_summaries[:5]) or "(none)"}

Existing predicates for this cron:
{json.dumps(existing_predicates, indent=2) if existing_predicates else "(none — fresh suggestions)"}

Suggest predicates that would catch a status=ok run where this cron's real side effects did NOT happen. Return ONLY a JSON array."""
    return [
        {"role": "system", "content": _SYSTEM},
        {"role": "user", "content": user},
    ]


_VALID_TYPES = {"file_mtime", "file_grew", "json_field_count", "http_health"}
_REQUIRED_FIELDS = {
    "file_mtime": {"path", "max_age_minutes", "description"},
    "file_grew": {"path", "description"},
    "json_field_count": {"path", "field", "filter", "description"},
    "http_health": {"url", "description"},
}


def parse_predicates(raw: str) -> tuple[list[dict] | None, str]:
    """Parse the model's response into a validated predicate list.
    Returns (predicates, error). On success error is empty."""
    # Strip code fences if the model added them despite instructions
    s = raw.strip()
    m = re.search(r"```(?:json)?\s*\n(.*?)\n```", s, re.DOTALL)
    if m:
        s = m.group(1).strip()
    # Find the array
    start = s.find("[")
    end = s.rfind("]")
    if start == -1 or end == -1 or end < start:
        return None, f"no JSON array found in model output (first 200 chars: {raw[:200]!r})"
    try:
        arr = json.loads(s[start:end + 1])
    except json.JSONDecodeError as e:
        return None, f"JSON parse failed: {e}"
    if not isinstance(arr, list):
        return None, "top-level value is not an array"
    out = []
    for i, p in enumerate(arr):
        if not isinstance(p, dict):
            return None, f"predicate[{i}] is not an object"
        t = p.get("type")
        if t not in _VALID_TYPES:
            return None, f"predicate[{i}] has unknown type {t!r}"
        missing = _REQUIRED_FIELDS[t] - set(p.keys())
        if missing:
            return None, f"predicate[{i}] of type {t!r} missing required field(s): {sorted(missing)}"
        out.append(p)
    return out, ""
