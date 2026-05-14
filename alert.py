"""Email alert sender. Wraps a configurable CLI binary (e.g. gog-send)."""
from __future__ import annotations

import os
import subprocess
from typing import Mapping


def send_email(
    *,
    sender_binary: str,
    sender_account: str,
    recipient: str,
    subject: str,
    body: str,
    extra_env: Mapping[str, str] | None = None,
    timeout_seconds: int = 30,
) -> tuple[bool, str]:
    """Invoke the sender binary as a subprocess.

    Returns (success, error_message). success=True only if exit code is 0.
    On non-zero exit, error_message contains stderr (truncated to 2000 chars).
    """
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    cmd = [
        os.path.expanduser(sender_binary),
        "gmail", "send",
        f"--account={sender_account}",
        f"--to={recipient}",
        f"--subject={subject}",
        f"--body={body}",
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return False, f"timeout after {timeout_seconds}s"
    except FileNotFoundError:
        return False, f"sender binary not found: {sender_binary}"
    except OSError as e:
        return False, f"OSError: {e}"

    if result.returncode == 0:
        return True, ""
    err = (result.stderr or result.stdout or f"exit {result.returncode}")[:2000]
    return False, err


def format_failure_body(*, cron: dict, error: str, failure_source: str,
                        retry_history: list[dict], suggested_cron_run: str,
                        ui_url: str) -> str:
    """Compose the body of an ultimate-failure email.

    Plain text — multiple subscribers' clients render differently. Stay simple.
    """
    lines = [
        f"OpenClaw cron failed and max retries are exhausted.",
        "",
        f"Cron name:    {cron.get('name') or cron['cron_id']}",
        f"Cron ID:      {cron['cron_id']}",
        f"Schedule:     {cron.get('schedule') or '(unknown)'}",
        f"Agent:        {cron.get('agent') or '(unknown)'}",
        f"Failure src:  {failure_source}",
        f"Max retries:  {cron['max_retries']}",
        "",
        "Error:",
        "  " + (error or "(no error text)").replace("\n", "\n  "),
        "",
    ]
    if retry_history:
        lines.append("Recent retry history:")
        for h in retry_history[:10]:
            lines.append(
                f"  - {h.get('received_at','?')} outcome={h.get('outcome','?')} "
                f"source={h.get('failure_source','?')}"
            )
        lines.append("")
    lines += [
        "Manual recovery:",
        f"  {suggested_cron_run}",
        "",
        f"Watchdog UI: {ui_url}",
    ]
    return "\n".join(lines)
