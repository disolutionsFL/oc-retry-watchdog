# oc-retry-watchdog

A small Python daemon that catches OpenClaw cron failures, retries them once (configurable), and emails an operator when retries are exhausted.

OpenClaw's built-in cron scheduler supports failure alerting via webhook but doesn't retry. Most cron failures in agent-driven setups are transient — model produced no completion, upstream API hiccup, dependency busy — and one retry typically clears them. This bolt-on closes that gap. A web UI lets you tune `max_retries` and alert recipients per cron.

> **Status:** v0.1 (early). Webhook + retry + alert flow. Predicate-based side-effect verification (v0.2) and missed-run heartbeat detection (v0.3) are coming.

## Features (v0.1)

- Receives OpenClaw failure webhooks at `POST /webhook`
- Auto-registers crons on first failure
- Retries via `openclaw cron run <id>` up to `max_retries` per cron per day
- Sends a single ultimate-failure email when the limit is hit
- Per-cron settings: `enabled`, `max_retries`, `alert_recipient`
- JSON HTTP API + minimal Web UI (Tailscale-friendly)
- SQLite-backed history of every retry and alert
- Zero non-stdlib dependencies (Python 3.11+)

## Coming soon

- **v0.2** — Predicate framework: verify that a "successful" cron actually mutated its expected output (file mtime / JSON field count / size delta). Catches the "agent reports status=ok but did nothing" failure mode.
- **v0.3** — Heartbeat scanner: every 5 min, compare expected cron fire times against actual run records. Detects silent missed runs (host rebooted, scheduler stalled).
- **v0.4** — Full web UI with predicate editor, heartbeat dashboard, history drill-down.

## Architecture

```
                       ┌───────────────────────────────┐
  openclaw-gateway ───▶│ POST /webhook                 │
  (failure-alert        │                              │
   webhook)             │  Watchdog daemon (port 9095) │
                        │   - retry  → openclaw cron run│
                        │   - alert  → email subprocess │
                        │   - SQLite history            │
                        │   - HTTP API + Web UI         │
                        └────────────┬──────────────────┘
                                     │
                                     ▼
                          gog-send  (or any CLI that
                          accepts --account/--to/
                          --subject/--body)
```

## Quickstart

Requirements: Python 3.11+, an email-sending CLI (e.g. [gog-send](https://github.com/disolutionsFL/gog) — anything that accepts `--account/--to/--subject/--body` works).

```bash
# 1. Clone
git clone https://github.com/disolutionsFL/oc-retry-watchdog.git
cd oc-retry-watchdog

# 2. Configure
cp config.example.json config.json
# Edit config.json: set alert.default_recipient, alert.sender_binary,
# alert.sender_account, openclaw_cli path. Defaults work otherwise.

# 3. Initialize the SQLite DB
python3 server.py --init-db

# 4. Run
python3 server.py
# (or install the systemd unit — see retry-watchdog.service.example)

# 5. Smoke test the API
curl http://localhost:9095/api/health
curl http://localhost:9095/api/settings
curl http://localhost:9095/api/crons
```

## Wiring an OpenClaw cron

For each cron you want covered, configure OpenClaw to POST failures to the watchdog:

```bash
openclaw cron edit <cron_id> \
    --failure-alert \
    --failure-alert-mode webhook \
    --failure-alert-to http://localhost:9095/webhook \
    --failure-alert-after 1
```

After the first failure, the cron will auto-register in the watchdog with defaults from your config. Tune via UI or `PATCH /api/crons/<cron_id>`.

## Configuration

`config.json` (created from `config.example.json`):

| Key | Default | Purpose |
|---|---|---|
| `server.port` | 9095 | Listen port |
| `server.ui_bind` | `0.0.0.0` | UI bind (Tailscale-friendly) |
| `server.timezone` | `America/New_York` | TZ for "today" counting |
| `db.path` | `~/.openclaw/retry-watchdog/retry.db` | SQLite location |
| `alert.default_recipient` | *(empty)* | Email address — must be set for alerts |
| `alert.sender_binary` | `gog-send` | CLI used to send mail |
| `alert.sender_account` | *(empty)* | Sender account passed to the CLI |
| `alert.sender_env` | `{}` | Extra env vars for the sender subprocess |
| `retries.default_max` | 1 | Default `max_retries` for newly-registered crons |
| `openclaw_cli` | `openclaw` | Path to openclaw CLI |
| `ui_url` | `http://localhost:9095/` | Embedded in failure emails as a link |

You can override `config.json`'s location via the `RETRY_WATCHDOG_CONFIG` env var or `--config <path>` flag. Recommended for production: keep the public code in one place and a private `config.json` (with real recipient/sender) somewhere else.

## API

All endpoints return JSON.

| Method | Path | Purpose |
|---|---|---|
| POST | `/webhook` | OpenClaw failure receiver |
| GET | `/api/health` | Liveness + version |
| GET | `/api/settings` | Current global defaults |
| PATCH | `/api/settings` | Update `default_alert_recipient` or `default_max_retries` |
| GET | `/api/crons` | All registered crons + 30-day counts |
| PATCH | `/api/crons/<id>` | Update `enabled`, `max_retries`, or `alert_recipient` |
| POST | `/api/crons/<id>/retry-now` | Manually fire `openclaw cron run <id>` |
| POST | `/api/crons/<id>/test-alert` | Send a synthetic alert email |
| GET | `/api/crons/<id>/history` | Last 10 retry + 10 alert events for the cron |

## Security

v0.1 has no auth. Bind the daemon to `127.0.0.1` and front it with a trusted-network ACL (Tailscale, VPN, etc.). The webhook is intended to receive POSTs only from the local OpenClaw gateway.

## License

MIT — see [LICENSE](LICENSE).
