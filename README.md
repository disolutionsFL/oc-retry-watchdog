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
# (or install the systemd unit — see "Install as a systemd service" below)

# 5. Smoke test the API
curl http://localhost:9095/api/health
curl http://localhost:9095/api/settings
curl http://localhost:9095/api/crons
```

## Install as a systemd service

The included `retry-watchdog.service.example` assumes a deliberate layout: code lives at `~/.openclaw/retry-watchdog/code/` (a clone of this repo) and `config.json` lives at `~/.openclaw/retry-watchdog/config.json`. With that layout the daemon updates cleanly via `git pull` without touching your config.

```bash
# One-time install
INSTALL_DIR="$HOME/.openclaw/retry-watchdog"
mkdir -p "$INSTALL_DIR"
git clone https://github.com/disolutionsFL/oc-retry-watchdog.git "$INSTALL_DIR/code"

# Drop your config in place (edit before or after copying)
cp "$INSTALL_DIR/code/config.example.json" "$INSTALL_DIR/config.json"
$EDITOR "$INSTALL_DIR/config.json"

# Install the user-level systemd unit
mkdir -p "$HOME/.config/systemd/user"
cp "$INSTALL_DIR/code/retry-watchdog.service.example" \
   "$HOME/.config/systemd/user/retry-watchdog.service"
systemctl --user daemon-reload

# Initialize the DB, enable + start, smoke-test
RETRY_WATCHDOG_CONFIG="$INSTALL_DIR/config.json" \
    python3 "$INSTALL_DIR/code/server.py" --init-db
systemctl --user enable --now retry-watchdog.service
sleep 2
systemctl --user status retry-watchdog.service --no-pager | head -20
curl -sf http://localhost:9095/api/health && echo
curl -sf http://localhost:9095/api/settings && echo
```

### Updating the code

```bash
INSTALL_DIR="$HOME/.openclaw/retry-watchdog"
git -C "$INSTALL_DIR/code" pull
systemctl --user restart retry-watchdog.service
curl -sf http://localhost:9095/api/health && echo
```

### Verify after a config edit

```bash
INSTALL_DIR="$HOME/.openclaw/retry-watchdog"
systemctl --user restart retry-watchdog.service
sleep 2
curl -sf http://localhost:9095/api/settings && echo
```

(Settings table values are seeded from `config.json` only on first init; the UI-editable defaults persist in SQLite afterward. Restart picks up everything else: paths, sender binary, sender env, healthcheck/predicate rules.)

## Running on Windows + WSL2

WSL2 Ubuntu is the typical host for OpenClaw on a Windows machine. The watchdog runs cleanly there but a few WSL-specific gotchas are worth knowing about.

### Invoking commands from PowerShell

Wrap any of the commands in the sections above to target your WSL distro:

```powershell
wsl -u <user> -- bash -c "<command>"
```

For multi-line scripts (file edits, deploys), stage the script to disk via a PowerShell here-string and invoke from WSL via the `/mnt/c/` path. Out-File writes CRLF endings — strip them inside WSL before running:

```powershell
@'
#!/bin/bash
# your bash script here
'@ | Out-File -Encoding ASCII C:\temp\my-script.sh

wsl -u <user> -- bash -c "sed -i 's/\r\$//' /mnt/c/temp/my-script.sh && bash /mnt/c/temp/my-script.sh"
```

### Mirrored-mode publish failures (the most likely problem you'll hit)

WSL2's mirrored networking mode has a known bug ([Microsoft/WSL #12703](https://github.com/microsoft/WSL/issues/12703), [#40287](https://github.com/microsoft/WSL/pull/40287)) where a successful `0.0.0.0:<port>` bind *inside* WSL fails to publish that port to the host's external network interfaces. Windows-side `localhost:<port>` reaches the WSL service, but inbound from another machine on your LAN / Tailscale / wireguard times out.

Symptoms — only the first of these tests will succeed:

```powershell
Test-NetConnection localhost -Port 9095 -InformationLevel Quiet                   # True
Test-NetConnection <your-LAN-or-Tailscale-IP> -Port 9095 -InformationLevel Quiet  # False
```

while inside WSL:

```bash
ss -tlnp | grep 9095   # Shows 0.0.0.0:9095 LISTEN — bind is fine
```

The community workaround (and what we run in production): have the daemon bind a different *internal* port, and add a Windows-side `netsh portproxy` listener on the *external* port that forwards to the internal port. Clients still hit the external port — the shift is invisible to them.

**Step 1**. Edit `config.json` to bind an internal port (e.g. `9094`):

```json
{ "server": { "port": 9094, "ui_bind": "0.0.0.0", ... } }
```

**Step 2**. Add the portproxy listener on the external port (admin PowerShell):

```powershell
netsh interface portproxy add v4tov4 `
    listenport=9095 listenaddress=0.0.0.0 `
    connectport=9094 connectaddress=127.0.0.1
```

**Step 3**. Restart the daemon and verify:

```powershell
wsl -u <user> -- bash -c "systemctl --user restart retry-watchdog.service"
netsh interface portproxy show all
Test-NetConnection <your-LAN-or-Tailscale-IP> -Port 9095 -InformationLevel Quiet  # Now True
```

> **Important — do NOT use the same port for the portproxy listener and the WSL bind.** In mirrored mode the port namespace is shared between Windows and WSL. If both try to listen on the same port, one wins silently and the other appears to work but accepts no traffic. Always use different ports (we use 9094 internal / 9095 external).

If the portproxy stops responding after a network change or sleep / wake, delete + re-add:

```powershell
netsh interface portproxy delete v4tov4 listenport=9095 listenaddress=0.0.0.0
netsh interface portproxy add v4tov4 listenport=9095 listenaddress=0.0.0.0 connectport=9094 connectaddress=127.0.0.1
```

Both the portproxy rule and any firewall rules persist across reboots.

### Windows Firewall

For each port you expose externally, add an inbound rule (admin PowerShell):

```powershell
New-NetFirewallRule -DisplayName "oc-retry-watchdog (9095)" `
    -Direction Inbound -Protocol TCP -LocalPort 9095 `
    -Action Allow -Profile Any
```

`-Profile Any` works on Private/Public/Domain. Verify the rule is enabled and binds the right protocol:

```powershell
Get-NetFirewallRule -DisplayName "oc-retry-watchdog (9095)" |
    Format-List Name, Enabled, Profile, Direction, Action
```

### `gog-send` TLS error on WSL

If your sender binary is a Go program (e.g. `gog-send`) and the alert email fails with `x509: certificate signed by unknown authority`, set `SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt` in the subprocess environment. Add it under `alert.sender_env` in `config.json`:

```json
"alert": {
  ...
  "sender_env": {
    "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt"
  }
}
```

The included `retry-watchdog.service.example` also sets this on the service environment as a backup.

### Linger for boot-time startup

User-level systemd services only start when the user logs in by default. To start the watchdog automatically at boot (and survive logout), enable linger once:

```bash
sudo loginctl enable-linger $USER
```

### Where to find logs

The systemd unit pipes stdout/stderr to the user journal:

```bash
journalctl --user-unit retry-watchdog.service -f         # tail live
journalctl --user-unit retry-watchdog.service -n 100     # last 100 lines
journalctl --user-unit retry-watchdog.service --since "1 hour ago"
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
