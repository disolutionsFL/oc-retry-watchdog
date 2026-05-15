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


# ----- per-model tuning registry -------------------------------------------
# Different model families need different knobs to produce clean JSON output.
# Patterns are matched (case-insensitive) against the model_id portion of
# `<provider>/<model_id>` keys. First match wins. Operators can override or
# add families via `ai.tunings` in config.json.
#
# Each tuning may contain:
#   extra_body            — dict merged into the chat-completions request body
#   temperature           — overrides the default 0.0
#   max_tokens            — overrides the default 1024
#   system_prompt_prefix  — text prepended to the user message
#   notes                 — human-readable description of why this tuning exists

DEFAULT_TUNING = {
    "name": "default",
    "notes": "Conservative defaults — works for most chat-completion endpoints.",
    "extra_body": {},
    "temperature": 0.0,
    "max_tokens": 1024,
}

BUILTIN_TUNINGS = [
    {
        "name": "qwen3",
        "match": r"qwen3(\.\d+)?",         # qwen3, qwen3.5, qwen3.6, ...
        "notes": ("Qwen3 family in vLLM: pass chat_template_kwargs."
                  "enable_thinking=false so the chat template skips the "
                  "chain-of-thought prefix. Without this, the actual JSON "
                  "answer goes into the reasoning field with content: null."),
        "extra_body": {"chat_template_kwargs": {"enable_thinking": False}},
        "temperature": 0.0,
    },
    {
        "name": "qwen2",
        "match": r"qwen2(\.\d+)?",
        "notes": "Qwen2 / Qwen2.5 family — no thinking mode, standard chat completions.",
        "extra_body": {},
        "temperature": 0.1,
    },
    {
        "name": "openai-style",
        "match": r"(gpt-?oss|gpt-3|gpt-4|gpt-5|o1|o3)",
        "notes": ("OpenAI / GPT-OSS family: response_format json_object is "
                  "reliable for forcing JSON-only output."),
        "extra_body": {"response_format": {"type": "json_object"}},
        "temperature": 0.0,
    },
    {
        "name": "deepseek",
        "match": r"deepseek",
        "notes": ("DeepSeek family — reasoning models. response_format usually "
                  "works; reasoning may still appear in `reasoning` field "
                  "which our content-fallback handles."),
        "extra_body": {"response_format": {"type": "json_object"}},
        "temperature": 0.0,
    },
    {
        "name": "glm",
        "match": r"glm",
        "notes": "GLM family (4.x+) responds well to a /no_think text directive.",
        "system_prompt_prefix": "/no_think\n\n",
        "extra_body": {},
        "temperature": 0.0,
    },
    {
        "name": "gemma",
        "match": r"gemma",
        "notes": "Gemma family — no special directives needed in current testing.",
        "extra_body": {},
        "temperature": 0.2,
    },
    {
        "name": "mistral",
        "match": r"mistral|mixtral",
        "notes": "Mistral / Mixtral family — no special directives.",
        "extra_body": {},
        "temperature": 0.2,
    },
    {
        "name": "nemotron",
        "match": r"nemotron",
        "notes": "Nvidia Nemotron family — no special directives in current testing.",
        "extra_body": {},
        "temperature": 0.1,
    },
]


def resolve_tuning(model_key: str, overrides: dict | None = None) -> dict:
    """Resolve the active tuning for a model.

    Resolution order:
      1. Exact match of full model_key (provider/model_id) in overrides
      2. Exact match of model_id (after slash) in overrides
      3. Built-in pattern match against model_id
      4. DEFAULT_TUNING

    Each lookup result is merged onto DEFAULT_TUNING so partial overrides
    inherit unspecified fields. The returned dict includes a `_source` key
    so callers (and the UI) can show which tuning was selected.
    """
    overrides = overrides or {}
    model_id = model_key.split("/", 1)[-1] if "/" in model_key else model_key

    if model_key in overrides and isinstance(overrides[model_key], dict):
        return {**DEFAULT_TUNING, **overrides[model_key],
                "_source": f"config-override[{model_key}]"}
    if model_id in overrides and isinstance(overrides[model_id], dict):
        return {**DEFAULT_TUNING, **overrides[model_id],
                "_source": f"config-override[{model_id}]"}

    for t in BUILTIN_TUNINGS:
        try:
            if re.search(t["match"], model_id, re.IGNORECASE):
                merged = {**DEFAULT_TUNING, **t}
                merged["_source"] = f"builtin[{t['name']}]"
                return merged
        except re.error:
            continue

    return {**DEFAULT_TUNING, "_source": "builtin[default]"}


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

def is_endpoint_reachable(base_url: str, api_key: str | None = None,
                          timeout_seconds: int = 2) -> bool:
    """Quick GET <base_url>/models to verify the endpoint is alive.
    Returns True iff a 2xx response comes back within timeout_seconds.
    Used to fast-fail offline models so the primary doesn't burn its
    full chat-completion timeout before falling back to secondary."""
    url = base_url.rstrip("/") + "/models"
    req = urllib.request.Request(url, method="GET")
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def check_models_availability(models: list[dict], timeout_seconds: int = 2,
                              max_workers: int = 10) -> dict[str, bool]:
    """Ping each model's endpoint in parallel. Returns {model_key: online}.
    All checks share the same timeout, so the total wall-clock cost is
    bounded by max(timeout_seconds, slowest endpoint)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    if not models:
        return {}
    # De-duplicate by base_url so we don't hit the same endpoint multiple
    # times when it hosts several models
    by_url: dict[str, list[str]] = {}
    for m in models:
        by_url.setdefault(m["base_url"], []).append(m["key"])
    results: dict[str, bool] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(is_endpoint_reachable, url, None, timeout_seconds): url
            for url in by_url
        }
        for f in as_completed(futures):
            url = futures[f]
            ok = False
            try:
                ok = f.result()
            except Exception:
                ok = False
            for k in by_url[url]:
                results[k] = ok
    return results


def chat_completion(*, base_url: str, model: str, messages: list[dict],
                    api_key: str | None = None,
                    tuning: dict | None = None,
                    max_tokens: int | None = None,
                    timeout_seconds: int = 30,
                    temperature: float | None = None,
                    ) -> tuple[bool, str]:
    """POST to <base_url>/chat/completions. Returns (ok, content_or_error).

    `tuning` (from resolve_tuning) supplies extra_body / temperature /
    max_tokens defaults. Explicit max_tokens / temperature kwargs override
    the tuning values. When tuning is None, the absolute defaults apply.
    """
    tuning = tuning or DEFAULT_TUNING
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens if max_tokens is not None else tuning.get("max_tokens", 1024),
        "temperature": temperature if temperature is not None else tuning.get("temperature", 0.0),
    }
    # Merge tuning-defined extra body fields (e.g. chat_template_kwargs,
    # response_format) into the request payload. Unknown fields are
    # silently ignored by endpoints that don't recognize them.
    for k, v in (tuning.get("extra_body") or {}).items():
        payload[k] = v

    url = base_url.rstrip("/") + "/chat/completions"
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

_SYSTEM = """You are a JSON API for an SRE tool. You receive cron metadata and respond with a JSON array of side-effect predicates. Your ENTIRE response must be a valid JSON array. No prose. No markdown. No code fences. No analysis. No "Here are the predicates". Begin your response with `[` and end with `]`.

WHAT PREDICATES ARE: A predicate runs after every status=ok cron run. If any predicate fails, the watchdog re-fires the cron and emails the operator. They detect cases where the cron exits cleanly but didn't actually do its work — agents sometimes claim success in their narrative without invoking the tools.

PREDICATE TYPES:

- file_mtime — assert file at `path` was modified within `max_age_minutes` of NOW. Path supports {TODAY} / {YESTERDAY} placeholders.
  {"type":"file_mtime","path":"...","max_age_minutes":N,"min_size_bytes":N (optional),"description":"..."}

- file_grew — assert file size strictly increased since last scan.
  {"type":"file_grew","path":"...","description":"..."}

- json_field_count — load JSON, count list items matching filter, assert bounds.
  {"type":"json_field_count","path":"...","list_path":"" (optional dot-path inside the JSON),"field":"...","filter":"non_null"|"null"|{"equals":X}|{"in":[X,Y]},"count_min":N (optional),"count_max":N (optional),"description":"..."}

- http_health — GET URL, expect status code.
  {"type":"http_health","url":"...","timeout_seconds":N (optional, default 5),"expected_status":N (optional, default 200),"description":"..."}

Each `description` is one short sentence describing the business outcome the predicate verifies. Choose 1-4 predicates that are SPECIFIC to THIS cron's actual outputs.

EXAMPLE — given a cron that grades yesterday's picks and writes results to a JSON file:

[
  {"type":"file_mtime","path":"/home/user/data/picks-stored/{YESTERDAY}.json","max_age_minutes":1440,"description":"Yesterday's picks file must have been written within the last 24h"},
  {"type":"json_field_count","path":"/home/user/data/picks-stored/{YESTERDAY}.json","field":"result","filter":"non_null","count_min":1,"description":"At least one pick must have a non-null result after grading"}
]

Now respond with predicates for the cron described next. JSON array only."""


def build_messages(*, cron_name: str, agent: str, schedule: str,
                   cron_prompt: str, recent_summaries: list[str],
                   existing_predicates: list[dict],
                   tuning: dict | None = None) -> list[dict]:
    # Per-tuning prompt prefix lets families like GLM (which honor /no_think
    # text directives) opt in without affecting families that don't.
    prefix = (tuning or {}).get("system_prompt_prefix", "")
    user = f"""{prefix}Cron name: {cron_name}
Agent: {agent}
Schedule: {schedule}

Cron prompt:
\"\"\"
{cron_prompt[:4000]}
\"\"\"

Recent successful run summary:
{chr(10).join(f"- {s[:300]}" for s in recent_summaries[:3]) or "(none)"}

Existing predicates for this cron:
{json.dumps(existing_predicates, indent=2) if existing_predicates else "(none)"}

Respond with the JSON array now. Begin with `[`."""
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
