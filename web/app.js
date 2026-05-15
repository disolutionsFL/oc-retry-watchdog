// oc-retry-watchdog UI

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

// ----- theme -----
// Initial theme is applied by an inline script in <head> (prevents FOUC).
// This handler just toggles + persists on click.
const THEME_KEY = "oc-retry-watchdog-theme";

function setTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  try { localStorage.setItem(THEME_KEY, theme); } catch (e) { /* private mode */ }
}

document.getElementById("theme-toggle").addEventListener("click", () => {
  const current = document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
  setTheme(current === "light" ? "dark" : "light");
});

// ----- agent badge colors -----
// Deterministic hash → 0..7 color slot (matches CSS .agent-chip.cN definitions).
function agentColorIndex(agent) {
  const s = String(agent || "");
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = ((h * 31) + s.charCodeAt(i)) >>> 0;
  }
  return h % 8;
}

// ----- Admin modal: OpenClaw Integration -----

async function loadAdminData() {
  try {
    return await api("GET", "/api/openclaw-jobs");
  } catch (e) {
    toast(`Load OpenClaw jobs failed: ${e.message}`, "bad");
    return { jobs: [], orphans: [], expected_webhook: "" };
  }
}

function renderAdminModal(data) {
  const jobs = data.jobs || [];
  const orphans = data.orphans || [];
  const expected = data.expected_webhook || "";

  $("#admin-jobs-count").textContent = `(${jobs.length} jobs · ${jobs.filter(j => j.webhook_wired_here).length} wired here)`;
  const tbody = $("#admin-jobs-tbody");
  tbody.innerHTML = jobs.map(j => {
    let wireStatus, wireDetail;
    if (j.webhook_wired_here) {
      wireStatus = `<span class="wire-status wired" title="Wired to this watchdog">wired</span>`;
      wireDetail = `<div class="wire-url">→ ${escapeHtml(expected)} (after ${j.webhook_after || 1})</div>`;
    } else if (j.webhook_wired_elsewhere) {
      wireStatus = `<span class="wire-status other" title="Webhook points elsewhere — clicking Wire will overwrite">other</span>`;
      wireDetail = `<div class="wire-url">→ ${escapeHtml(j.webhook_url || "?")}</div>`;
    } else {
      wireStatus = `<span class="wire-status unwired" title="No failure-alert webhook configured">unwired</span>`;
      wireDetail = "";
    }
    const inDb = j.in_watchdog_db
      ? `<span class="in-watchdog-yes" title="Registered in watchdog DB">✓</span>`
      : `<span class="in-watchdog-no" title="Not yet registered (will auto-register on first failure or when wired)">—</span>`;
    const predBadge = (j.predicates_count || 0) > 0
      ? `<span class="badge-pred active" data-action="edit-predicates" data-id="${j.cron_id}" title="Click to edit predicates">${j.predicates_count}</span>`
      : `<span class="badge-pred" data-action="edit-predicates" data-id="${j.cron_id}" title="Click to add predicates">0</span>`;
    const wireBtn = j.webhook_wired_here
      ? `<button class="btn-mini danger" data-action="unwire-cron" data-id="${j.cron_id}">Unwire</button>`
      : `<button class="btn-mini success" data-action="wire-cron" data-id="${j.cron_id}">Wire</button>`;
    return `
      <tr>
        <td>
          <strong>${escapeHtml(j.name || j.cron_id)}</strong>
          <br><small><code>${escapeHtml(j.cron_id || "")}</code></small>
        </td>
        <td>${j.agent ? `<span class="agent-chip c${agentColorIndex(j.agent)}">${escapeHtml(j.agent)}</span>` : "—"}</td>
        <td><code>${escapeHtml(j.schedule || "?")}</code></td>
        <td style="text-align:center;">${j.enabled ? "✓" : "—"}</td>
        <td>${wireStatus}${wireDetail}</td>
        <td style="text-align:center;">${inDb}</td>
        <td style="text-align:center;">${predBadge}</td>
        <td>${wireBtn}</td>
      </tr>
    `;
  }).join("") || `<tr><td colspan="8" style="text-align:center;color:var(--muted);padding:20px;">No jobs found in jobs.json. Is the path in <code>heartbeat.jobs_json_path</code> correct?</td></tr>`;

  // Orphans
  $("#admin-orphans-count").textContent = `(${orphans.length})`;
  const obody = $("#admin-orphans-tbody");
  const oempty = $("#admin-orphans-empty");
  if (orphans.length === 0) {
    obody.innerHTML = "";
    oempty.textContent = "No orphans — every cron in this watchdog is also in OpenClaw.";
  } else {
    oempty.textContent = "";
    obody.innerHTML = orphans.map(o => `
      <tr class="row-orphan">
        <td><strong>${escapeHtml(o.name || "(unknown)")}</strong></td>
        <td><code>${escapeHtml(o.cron_id)}</code></td>
        <td><small>${fmtDate(o.first_seen_at)}</small></td>
        <td style="text-align:center;">${o.predicates_count || 0}</td>
        <td>
          <button class="btn-mini danger" data-action="delete-orphan" data-id="${o.cron_id}">Remove</button>
        </td>
      </tr>
    `).join("");
  }
}

async function openAdminModal() {
  const data = await loadAdminData();
  renderAdminModal(data);
  $("#admin-modal").showModal();
}


// ----- predicate editor schema -----
// Per-type editable fields. Each field: {name, label, kind, req?, hint?}
// kind: "text" | "number" | "json" (json values parsed/serialized on save/load)
const PREDICATE_TYPES = {
  file_mtime: {
    label: "File mtime",
    fields: [
      { name: "path", label: "Path", kind: "text", req: true, hint: "Path to file. Supports {TODAY} and {YESTERDAY} placeholders (resolved in server.timezone)." },
      { name: "max_age_minutes", label: "Max age (minutes)", kind: "number", req: true, hint: "Predicate fails if the file's mtime is older than this many minutes." },
      { name: "min_size_bytes", label: "Min size (bytes)", kind: "number", hint: "Optional. If set, file size below this fails the predicate." },
      { name: "description", label: "Description", kind: "text", req: true, hint: "Shown in tooltips and the alert email body." },
    ],
  },
  file_grew: {
    label: "File grew",
    fields: [
      { name: "path", label: "Path", kind: "text", req: true, hint: "File must have grown (size increased) since the previous scan. State persisted in predicate_history table." },
      { name: "description", label: "Description", kind: "text", req: true },
    ],
  },
  json_field_count: {
    label: "JSON field count",
    fields: [
      { name: "path", label: "Path", kind: "text", req: true, hint: "Path to JSON file. Supports {TODAY}/{YESTERDAY} placeholders." },
      { name: "list_path", label: "List path", kind: "text", hint: "Empty if the file's root is a list. Dot-path for nested (e.g. \"picks\")." },
      { name: "field", label: "Field", kind: "text", req: true, hint: "Field name to inspect on each list item." },
      { name: "filter", label: "Filter", kind: "json", req: true, hint: "non_null | null | {\"equals\":X} | {\"in\":[X,Y]}. JSON, no quotes for keywords." },
      { name: "count_min", label: "Count min", kind: "number", hint: "Inclusive lower bound on matching entries." },
      { name: "count_max", label: "Count max", kind: "number", hint: "Inclusive upper bound on matching entries." },
      { name: "description", label: "Description", kind: "text", req: true },
    ],
  },
  http_health: {
    label: "HTTP health",
    fields: [
      { name: "url", label: "URL", kind: "text", req: true, hint: "Full URL to GET." },
      { name: "timeout_seconds", label: "Timeout (seconds)", kind: "number", hint: "Default 5." },
      { name: "expected_status", label: "Expected status", kind: "number", hint: "Default 200." },
      { name: "description", label: "Description", kind: "text", req: true },
    ],
  },
};

// `kind` is "predicates" or "healthchecks" — same modal, different
// endpoints + semantics. Set at open time.
let predEditorState = { cronId: null, predicates: [], kind: "predicates" };

function renderPredicateField(predIdx, field, value) {
  const id = `pred-${predIdx}-${field.name}`;
  let inputHtml = "";
  if (field.kind === "number") {
    const v = (value === null || value === undefined) ? "" : value;
    inputHtml = `<input type="number" id="${id}" data-field="${field.name}" value="${escapeAttr(String(v))}" step="any">`;
  } else if (field.kind === "json") {
    let stringified;
    if (typeof value === "string") stringified = value;
    else if (value === null || value === undefined) stringified = "";
    else stringified = JSON.stringify(value);
    inputHtml = `<input type="text" id="${id}" data-field="${field.name}" data-kind="json" value="${escapeAttr(stringified)}">`;
  } else {
    inputHtml = `<input type="text" id="${id}" data-field="${field.name}" value="${escapeAttr(value == null ? "" : String(value))}">`;
  }
  const cls = field.req ? "lbl req" : "lbl";
  return `
    <label>
      <span class="${cls}">${escapeHtml(field.label)}</span>
      ${inputHtml}
    </label>
    ${field.hint ? `<div class="pred-hint">${escapeHtml(field.hint)}</div>` : ""}
  `;
}

function renderPredicateCard(idx, pred) {
  const type = pred.type || "file_mtime";
  const schema = PREDICATE_TYPES[type] || PREDICATE_TYPES.file_mtime;
  const fieldsHtml = schema.fields.map(f => renderPredicateField(idx, f, pred[f.name])).join("");
  const typeOptions = Object.entries(PREDICATE_TYPES)
    .map(([k, v]) => `<option value="${k}" ${k === type ? "selected" : ""}>${escapeHtml(v.label)}</option>`).join("");
  return `
    <div class="pred-card" data-idx="${idx}">
      <div class="pred-card-row">
        <strong>#${idx + 1}</strong>
        <select class="pred-type" data-idx="${idx}">${typeOptions}</select>
        <span class="grow"></span>
        <button type="button" class="pred-remove" data-idx="${idx}" title="Remove this predicate">&times;</button>
      </div>
      ${fieldsHtml}
    </div>
  `;
}

function renderPredicateModal() {
  const list = $("#pred-modal-list");
  list.innerHTML = predEditorState.predicates.map((p, i) => renderPredicateCard(i, p)).join("")
    || `<p class="hint">No predicates yet. Click + Add predicate below to start.</p>`;
}

function readPredicatesFromForm() {
  const cards = $$("#pred-modal-list .pred-card");
  const result = [];
  for (const card of cards) {
    const idx = parseInt(card.dataset.idx, 10);
    const type = card.querySelector(".pred-type").value;
    const schema = PREDICATE_TYPES[type] || PREDICATE_TYPES.file_mtime;
    const pred = { type };
    for (const f of schema.fields) {
      const input = card.querySelector(`input[data-field="${f.name}"]`);
      if (!input) continue;
      const raw = input.value;
      if (raw === "" || raw == null) {
        if (f.req) throw new Error(`predicate #${idx + 1}: '${f.label}' is required`);
        continue;  // skip empty optional
      }
      if (f.kind === "number") {
        const n = Number(raw);
        if (Number.isNaN(n)) throw new Error(`predicate #${idx + 1}: '${f.label}' must be a number`);
        pred[f.name] = n;
      } else if (f.kind === "json") {
        // try parse as JSON; if it fails, treat as a bare string keyword
        const t = raw.trim();
        if (t.startsWith("{") || t.startsWith("[") || t.startsWith("\"")) {
          try { pred[f.name] = JSON.parse(t); }
          catch (e) { throw new Error(`predicate #${idx + 1}: '${f.label}' is not valid JSON (${e.message})`); }
        } else {
          pred[f.name] = t;
        }
      } else {
        pred[f.name] = raw;
      }
    }
    result.push(pred);
  }
  return result;
}

// `kind` = "predicates" (default) or "healthchecks". Same modal — title,
// hint text, and endpoint URLs vary by kind.
function openChecksEditor(cronId, kind = "predicates") {
  const cron = state.crons.find(c => c.cron_id === cronId);
  if (!cron) {
    toast(`Cron ${cronId} not loaded`, "bad");
    return;
  }
  predEditorState = { cronId, predicates: [], kind };

  // Title + hint reflect the kind
  if (kind === "healthchecks") {
    $("#pred-modal-title").textContent = "Edit healthchecks";
    $("#pred-modal-hint").innerHTML =
      "Healthchecks run <strong>before</strong> the watchdog retries a failed cron. " +
      "If any healthcheck fails, the retry is skipped — the watchdog assumes a dependency is down " +
      "and alerts directly rather than burning the retry. " +
      "Changes here write to <code>config.json</code> on the daemon host. " +
      "<em>Enforcement during retries lands in a follow-up release; for now this manages + AI-suggests the rules.</em>";
  } else {
    $("#pred-modal-title").textContent = "Edit predicates";
    $("#pred-modal-hint").innerHTML =
      "Predicates run after every <code>status=ok</code> finished run for this cron. " +
      "If any predicate fails, the watchdog fires the same retry/alert flow as a webhook failure. " +
      "Changes here write to <code>config.json</code> on the daemon host and take effect immediately. " +
      "They do not auto-sync to any source repo you may have.";
  }

  api("GET", `/api/crons/${cronId}/${kind}`).then(items => {
    predEditorState.predicates = Array.isArray(items) ? JSON.parse(JSON.stringify(items)) : [];
    $("#pred-modal-cron-name").textContent = cron.name || cron.cron_id;
    $("#pred-modal-cron-id").textContent = cron.cron_id;
    renderPredicateModal();
    const aiBtn = $("#pred-suggest-btn");
    if (state.settings.ai_enabled && state.settings.ai_primary_model) {
      aiBtn.classList.remove("hidden");
      aiBtn.disabled = false;
    } else {
      aiBtn.classList.add("hidden");
    }
    $("#predicates-modal").showModal();
  }).catch(e => toast(`Load ${kind} failed: ${e.message}`, "bad"));
}

// Backwards-compatible alias for existing callers
function openPredicateEditor(cronId) { openChecksEditor(cronId, "predicates"); }
function openHealthchecksEditor(cronId) { openChecksEditor(cronId, "healthchecks"); }

// ----- agent filter -----
const AGENT_FILTER_KEY = "oc-retry-watchdog-agent-filter";

function loadAgentFilter() {
  // Returns Set of *disabled* agent names. Empty Set = show all.
  try {
    const raw = localStorage.getItem(AGENT_FILTER_KEY);
    if (!raw) return new Set();
    const arr = JSON.parse(raw);
    return new Set(Array.isArray(arr) ? arr : []);
  } catch { return new Set(); }
}
function saveAgentFilter(disabledSet) {
  try { localStorage.setItem(AGENT_FILTER_KEY, JSON.stringify(Array.from(disabledSet))); }
  catch { /* private mode */ }
}

const disabledAgents = loadAgentFilter();

const state = {
  crons: [],
  settings: {},
};

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  let payload;
  try { payload = await res.json(); } catch { payload = null; }
  if (!res.ok) {
    const err = (payload && payload.error) || `HTTP ${res.status}`;
    throw new Error(err);
  }
  return payload;
}

// ----- themed confirm dialog (replaces window.confirm) -----
let _confirmResolver = null;

function appConfirm({ title, message, confirmLabel = "Confirm", confirmKind = "primary", cancelLabel = "Cancel" }) {
  $("#confirm-title").textContent = title || "Confirm";
  // message can include simple inline HTML (e.g. <code>UUID</code>)
  $("#confirm-message").innerHTML = message || "";
  const ok = $("#confirm-ok");
  ok.textContent = confirmLabel;
  ok.className = confirmKind;
  $("#confirm-cancel").textContent = cancelLabel;
  $("#confirm-modal").showModal();
  return new Promise(resolve => { _confirmResolver = resolve; });
}

function _confirmResolve(value) {
  $("#confirm-modal").close();
  if (_confirmResolver) {
    const r = _confirmResolver;
    _confirmResolver = null;
    r(value);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  $("#confirm-ok").addEventListener("click", () => _confirmResolve(true));
  $("#confirm-cancel").addEventListener("click", () => _confirmResolve(false));
  // Esc-close or backdrop-close
  $("#confirm-modal").addEventListener("close", () => {
    if (_confirmResolver) _confirmResolve(false);
  });
});
// Fallback: if DOMContentLoaded already fired (script at end of body), wire now too
if (document.readyState !== "loading") {
  const ok = $("#confirm-ok"), cancel = $("#confirm-cancel"), modal = $("#confirm-modal");
  if (ok && !ok.dataset.wired) {
    ok.dataset.wired = "1";
    ok.addEventListener("click", () => _confirmResolve(true));
    cancel.addEventListener("click", () => _confirmResolve(false));
    modal.addEventListener("close", () => { if (_confirmResolver) _confirmResolve(false); });
  }
}

function toast(msg, kind = "") {
  const el = $("#toast");
  el.className = kind;
  el.textContent = msg;
  el.show();
  clearTimeout(window._toastTimer);
  window._toastTimer = setTimeout(() => el.close(), 3500);
}

function fmtDate(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (isNaN(d)) return iso;
  const y = d.getFullYear(), m = String(d.getMonth() + 1).padStart(2, "0"),
    day = String(d.getDate()).padStart(2, "0"),
    hh = String(d.getHours()).padStart(2, "0"),
    mm = String(d.getMinutes()).padStart(2, "0");
  return `${y}-${m}-${day} ${hh}:${mm}`;
}

function uptimeStr(seconds) {
  if (!seconds && seconds !== 0) return "(unknown)";
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (d) return `${d}d ${h}h ${m}m`;
  if (h) return `${h}h ${m}m`;
  return `${m}m`;
}

function rowClass(c) {
  if (!c.enabled) return "row-disabled";
  if (c.alerts_today > 0) return "row-alerted";
  if (c.retries_today > 0) return "row-retried-only";
  if (c.max_retries === 0) return "row-alert-only";
  return "";
}

function renderAgentFilter() {
  const bar = $("#agent-filter-bar");
  const chipsEl = $("#agent-filter-chips");
  const agents = Array.from(new Set(state.crons.map(c => c.agent || "").filter(Boolean))).sort();
  if (agents.length === 0) {
    bar.classList.add("hidden");
    chipsEl.innerHTML = "";
    return;
  }
  bar.classList.remove("hidden");
  chipsEl.innerHTML = agents.map(a => {
    const idx = agentColorIndex(a);
    const active = !disabledAgents.has(a);
    return `<button class="agent-filter-chip c${idx}${active ? " active" : ""}" data-agent="${escapeAttr(a)}" title="${active ? "Hide" : "Show"} ${escapeHtml(a)} rows">${escapeHtml(a)}</button>`;
  }).join("");
}

function renderCrons() {
  const tbody = $("#cron-tbody");
  if (state.crons.length === 0) {
    $("#cron-table").classList.add("hidden");
    $("#empty-state").classList.remove("hidden");
    return;
  }
  $("#empty-state").classList.add("hidden");
  $("#cron-table").classList.remove("hidden");
  tbody.innerHTML = "";
  for (const c of state.crons) {
    if (c.agent && disabledAgents.has(c.agent)) continue;   // filtered out
    const tr = document.createElement("tr");
    tr.className = rowClass(c);
    const predCount = c.predicates_count || 0;
    const predDescs = c.predicates_descriptions || [];
    const predTitle = predCount > 0
      ? `${predCount} predicate(s) checked after every successful run:\n\n` +
        predDescs.map((d, i) => `${i + 1}. ${d}`).join("\n\n")
      : "No predicates configured — only webhook (status=error) failures trigger an alert for this cron. Add per-cron rules to config.json + restart to enable side-effect verification.";
    const predBadge = predCount > 0
      ? `<span class="badge-pred active" title="${escapeAttr(predTitle)}\n\n(Click to edit)" data-action="edit-predicates" data-id="${c.cron_id}">${predCount}</span>`
      : `<span class="badge-pred" title="${escapeAttr(predTitle)}\n\n(Click to add predicates)" data-action="edit-predicates" data-id="${c.cron_id}">0</span>`;

    const hcCount = c.healthchecks_count || 0;
    const hcDescs = c.healthchecks_descriptions || [];
    const hcTitle = hcCount > 0
      ? `${hcCount} healthcheck(s) run before any retry:\n\n` +
        hcDescs.map((d, i) => `${i + 1}. ${d}`).join("\n\n")
      : "No healthchecks configured — the watchdog will retry without checking dependencies first. Add http_health rules to skip retries when an upstream service is down.";
    const hcBadge = hcCount > 0
      ? `<span class="badge-pred active" title="${escapeAttr(hcTitle)}\n\n(Click to edit)" data-action="edit-healthchecks" data-id="${c.cron_id}">${hcCount}</span>`
      : `<span class="badge-pred" title="${escapeAttr(hcTitle)}\n\n(Click to add healthchecks)" data-action="edit-healthchecks" data-id="${c.cron_id}">0</span>`;
    const agentCell = c.agent
      ? `<span class="agent-chip c${agentColorIndex(c.agent)}" title="Agent: ${escapeAttr(c.agent)}">${escapeHtml(c.agent)}</span>`
      : `<span class="muted" style="font-size:11px;">&mdash;</span>`;
    tr.innerHTML = `
      <td data-label="Cron"><strong>${escapeHtml(c.name || c.cron_id)}</strong><br><small><code>${c.cron_id}</code></small></td>
      <td data-label="Agent">${agentCell}</td>
      <td data-label="Schedule" class="col-hide-md"><code>${escapeHtml(c.schedule || "?")}</code></td>
      <td data-label="Predicates" class="num">${predBadge}</td>
      <td data-label="Healthchecks" class="num">${hcBadge}</td>
      <td data-label="Max Retries" class="num col-hide-sm"><input class="inline num" type="number" min="0" max="10" value="${c.max_retries}" data-field="max_retries" data-id="${c.cron_id}"></td>
      <td data-label="Alert Recipient" class="col-hide-lg"><input class="inline" type="text" value="${escapeAttr(c.alert_recipient || "")}" placeholder="(use default)" data-field="alert_recipient" data-id="${c.cron_id}"></td>
      <td data-label="Retries (today / 30d)" class="num">${c.retries_today || 0} / ${c.retries_30d || 0}</td>
      <td data-label="Alerts (today / 30d)" class="num">${c.alerts_today || 0} / ${c.alerts_30d || 0}</td>
      <td data-label="Last Retried" class="col-hide-md"><small>${fmtDate(c.last_retried_at)}</small></td>
      <td data-label="Last Alerted" class="col-hide-md"><small>${fmtDate(c.last_alerted_at)}</small></td>
      <td data-label="Enabled" class="col-hide-sm"><input type="checkbox" data-field="enabled" data-id="${c.cron_id}" ${c.enabled ? "checked" : ""}></td>
      <td data-label="Actions" class="actions">
        <button data-action="retry-now" data-id="${c.cron_id}">Retry now</button>
        <button data-action="test-alert" data-id="${c.cron_id}">Test alert</button>
      </td>
    `;
    tbody.appendChild(tr);
  }
}

async function loadHeartbeat() {
  try {
    const rows = await api("GET", "/api/heartbeat");
    const last = (rows && rows.length) ? rows[0] : null;
    if (!last) {
      $("#hb-last").innerHTML = "Last scan: <em>(no scans yet)</em>";
      $("#hb-stats").textContent = "";
      return;
    }
    $("#hb-last").innerHTML = `Last scan: <strong>${fmtDate(last.scanned_at)}</strong>`;
    const failedTxt = last.predicates_failed > 0
      ? `<span style="color: var(--bad); font-weight: 600;">${last.predicates_failed} predicate failure(s)</span>`
      : `${last.predicates_failed} failures`;
    $("#hb-stats").innerHTML =
      `${last.crons_checked} cron(s) checked · ${failedTxt} · ${last.duration_ms}ms`;
  } catch (e) {
    $("#hb-last").innerHTML = `Last scan: <em>(error: ${e.message})</em>`;
  }
}

function escapeHtml(s) {
  return String(s || "").replace(/[&<>"]/g, c =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}
function escapeAttr(s) { return escapeHtml(s); }

async function loadAll() {
  try {
    const [crons, settings] = await Promise.all([
      api("GET", "/api/crons"),
      api("GET", "/api/settings"),
    ]);
    state.crons = crons || [];
    state.settings = settings || {};
    renderAgentFilter();
    renderCrons();
    updateHeader();
    loadHeartbeat();   // async, fire-and-forget
  } catch (e) {
    toast(`Load failed: ${e.message}`, "bad");
  }
}

function updateHeader() {
  const v = state.settings;
  $("#version-pill").textContent = `v${v.daemon_version || "?"} · ${uptimeStr(v.daemon_uptime_seconds)}`;
  const instance = (v.openclaw_instance_name || "").trim();
  const el = $("#instance-name");
  if (instance) {
    el.textContent = instance;
    el.classList.remove("hidden");
  } else {
    el.textContent = "";
    el.classList.add("hidden");
  }
}

// ----- handlers

document.addEventListener("change", async (e) => {
  const t = e.target;
  if (!t.dataset || !t.dataset.field) return;
  const id = t.dataset.id, field = t.dataset.field;
  let value;
  if (t.type === "checkbox") value = t.checked ? 1 : 0;
  else if (t.type === "number") value = parseInt(t.value, 10);
  else value = t.value;
  if (field === "alert_recipient" && value === "") value = null;
  try {
    await api("PATCH", `/api/crons/${id}`, { [field]: value });
    toast(`${field} saved`, "good");
  } catch (err) {
    toast(`Save failed: ${err.message}`, "bad");
    loadAll();
  }
});

document.addEventListener("click", async (e) => {
  const t = e.target;
  if (t.dataset?.action === "edit-predicates") {
    openPredicateEditor(t.dataset.id);
    return;
  }
  if (t.dataset?.action === "edit-healthchecks") {
    openHealthchecksEditor(t.dataset.id);
    return;
  }
  if (t.dataset?.action === "wire-cron") {
    if (!await appConfirm({
      title: "Wire failure-alert webhook?",
      message: `Configure cron <code>${escapeHtml(t.dataset.id)}</code> to POST its failure-alert at this watchdog. Auto-registers the cron in the watchdog DB on success.`,
      confirmLabel: "Wire",
      confirmKind: "success",
    })) return;
    t.disabled = true;
    try {
      const r = await api("POST", `/api/openclaw-jobs/${t.dataset.id}/wire`);
      toast(r.ok ? "Wired" : `Wire failed: ${r.output || "?"}`, r.ok ? "good" : "bad");
      const data = await loadAdminData();
      renderAdminModal(data);
      loadAll();
    } catch (err) { toast(`Wire failed: ${err.message}`, "bad"); t.disabled = false; }
    return;
  }
  if (t.dataset?.action === "unwire-cron") {
    if (!await appConfirm({
      title: "Unwire failure-alert webhook?",
      message: `Remove the failure-alert webhook from cron <code>${escapeHtml(t.dataset.id)}</code>. The cron stays in OpenClaw and the watchdog DB; only the alert-routing is removed.`,
      confirmLabel: "Unwire",
      confirmKind: "danger",
    })) return;
    t.disabled = true;
    try {
      const r = await api("POST", `/api/openclaw-jobs/${t.dataset.id}/unwire`);
      if (!r.ok) {
        toast(`Unwire failed: ${r.output || "?"}`, "bad");
        t.disabled = false;
      } else {
        toast("Unwired", "good");
        // Follow-up prompt: also remove from watchdog DB so it stops
        // appearing in the dashboard?
        const removeFromWatchdog = await appConfirm({
          title: "Also remove from watchdog dashboard?",
          message: `The cron is unwired from OpenClaw but still appears in the watchdog dashboard (with any history + predicates intact). Remove it from the watchdog DB and clear its predicates? Retry/alert event rows stay for audit.`,
          confirmLabel: "Remove",
          confirmKind: "danger",
          cancelLabel: "Keep in dashboard",
        });
        if (removeFromWatchdog) {
          try {
            await api("DELETE", `/api/crons/${t.dataset.id}`);
            toast("Removed from watchdog", "good");
          } catch (err) {
            toast(`Remove failed: ${err.message}`, "bad");
          }
        }
        const data = await loadAdminData();
        renderAdminModal(data);
        loadAll();
      }
    } catch (err) { toast(`Unwire failed: ${err.message}`, "bad"); t.disabled = false; }
    return;
  }
  if (t.dataset?.action === "delete-orphan") {
    if (!await appConfirm({
      title: "Remove orphan?",
      message: `Remove cron <code>${escapeHtml(t.dataset.id)}</code> from the watchdog DB and clear its predicates from <code>config.json</code>. Retry/alert history rows are kept for forensics.`,
      confirmLabel: "Remove",
      confirmKind: "danger",
    })) return;
    t.disabled = true;
    try {
      const r = await api("DELETE", `/api/crons/${t.dataset.id}`);
      toast(r.ok ? "Removed orphan" : "Remove failed", r.ok ? "good" : "bad");
      const data = await loadAdminData();
      renderAdminModal(data);
      loadAll();
    } catch (err) { toast(`Remove failed: ${err.message}`, "bad"); t.disabled = false; }
    return;
  }
  if (t.dataset?.action === "retry-now") {
    if (!await appConfirm({
      title: "Manual retry?",
      message: `Invoke <code>openclaw cron run ${escapeHtml(t.dataset.id)}</code>. This actually re-fires the cron; any emails or side-effects it produces will happen.`,
      confirmLabel: "Retry now",
      confirmKind: "primary",
    })) return;
    try {
      const r = await api("POST", `/api/crons/${t.dataset.id}/retry-now`);
      toast(`Retry: ${r.action}${r.run_id ? ` (run ${r.run_id})` : ""}`, r.action === "retried" ? "good" : "bad");
      setTimeout(loadAll, 500);
    } catch (err) { toast(`Failed: ${err.message}`, "bad"); }
  } else if (t.dataset?.action === "test-alert") {
    if (!await appConfirm({
      title: "Send test alert?",
      message: `Send a synthetic alert email for this cron to verify the email delivery path. No real failure is recorded.`,
      confirmLabel: "Send",
      confirmKind: "success",
    })) return;
    try {
      const r = await api("POST", `/api/crons/${t.dataset.id}/test-alert`);
      toast(r.ok ? `Test alert sent to ${r.recipient}` : `Alert failed: ${r.error || "?"}`,
            r.ok ? "good" : "bad");
      setTimeout(loadAll, 500);
    } catch (err) { toast(`Failed: ${err.message}`, "bad"); }
  }
});

$("#refresh-btn").addEventListener("click", loadAll);

// Admin modal
$("#admin-btn").addEventListener("click", openAdminModal);
$("#admin-close-btn").addEventListener("click", () => $("#admin-modal").close());
$("#admin-refresh-btn").addEventListener("click", async () => {
  const data = await loadAdminData();
  renderAdminModal(data);
});

// Agent filter — toggle on click, persist, re-render
$("#agent-filter-chips").addEventListener("click", (e) => {
  const chip = e.target.closest(".agent-filter-chip");
  if (!chip) return;
  const agent = chip.dataset.agent;
  if (!agent) return;
  if (disabledAgents.has(agent)) {
    disabledAgents.delete(agent);
  } else {
    disabledAgents.add(agent);
  }
  saveAgentFilter(disabledAgents);
  renderAgentFilter();
  renderCrons();
});

// ----- Predicate editor modal handlers -----

$("#pred-suggest-btn").addEventListener("click", async () => {
  const btn = $("#pred-suggest-btn");
  const kind = predEditorState.kind || "predicates";
  try { predEditorState.predicates = readPredicatesFromForm(); } catch {}
  btn.disabled = true;
  const originalText = btn.textContent;
  btn.textContent = "✨ Thinking…";
  try {
    const r = await api("POST", `/api/crons/${predEditorState.cronId}/${kind}/suggest`);
    if (!r.ok || !Array.isArray(r.predicates) || r.predicates.length === 0) {
      let detail = r.error || "no suggestions returned";
      if (Array.isArray(r.tried) && r.tried.length) {
        const lines = r.tried.map(t => `${t.slot}: ${(t.error || "?").toString().slice(0, 200)}`);
        detail += " — " + lines.join(" | ");
      }
      toast(`AI suggest failed: ${detail}`, "bad");
      return;
    }
    predEditorState.predicates = predEditorState.predicates.concat(r.predicates);
    renderPredicateModal();
    toast(`Added ${r.predicates.length} ${kind === "healthchecks" ? "healthcheck" : "predicate"} suggestion(s) from ${r.model_used} (${r.slot}). Review + edit before saving.`, "good");
  } catch (e) {
    toast(`AI suggest failed: ${e.message}`, "bad");
  } finally {
    btn.disabled = false;
    btn.textContent = originalText;
  }
});

$("#pred-add-btn").addEventListener("click", () => {
  // Snapshot in-progress form values into state before re-render
  try {
    predEditorState.predicates = readPredicatesFromForm();
  } catch (e) {
    // Validation will fire on Save; ignore here to allow adding new rows mid-edit
  }
  predEditorState.predicates.push({ type: "file_mtime", path: "", max_age_minutes: 60, description: "" });
  renderPredicateModal();
});

$("#pred-modal-list").addEventListener("click", (e) => {
  const t = e.target;
  if (t.classList.contains("pred-remove")) {
    const idx = parseInt(t.dataset.idx, 10);
    try { predEditorState.predicates = readPredicatesFromForm(); } catch {}
    predEditorState.predicates.splice(idx, 1);
    renderPredicateModal();
  }
});

$("#pred-modal-list").addEventListener("change", (e) => {
  if (e.target.classList.contains("pred-type")) {
    const idx = parseInt(e.target.dataset.idx, 10);
    try { predEditorState.predicates = readPredicatesFromForm(); } catch {}
    // Reset to new type with empty fields (preserve description if any)
    const oldDesc = predEditorState.predicates[idx]?.description || "";
    predEditorState.predicates[idx] = { type: e.target.value, description: oldDesc };
    renderPredicateModal();
  }
});

$("#pred-cancel-btn").addEventListener("click", () => {
  $("#predicates-modal").close();
});

$("#pred-save-btn").addEventListener("click", async () => {
  let payload;
  try {
    payload = readPredicatesFromForm();
  } catch (e) {
    toast(e.message, "bad");
    return;
  }
  const kind = predEditorState.kind || "predicates";
  try {
    await api("PUT", `/api/crons/${predEditorState.cronId}/${kind}`, payload);
    const cron = state.crons.find(c => c.cron_id === predEditorState.cronId);
    const label = cron && cron.name
      ? `${cron.name} (${predEditorState.cronId})`
      : predEditorState.cronId;
    const noun = kind === "healthchecks" ? "healthcheck" : "predicate";
    toast(`Saved ${payload.length} ${noun}(s) for ${label}`, "good");
    $("#predicates-modal").close();
    loadAll();
  } catch (e) {
    toast(`Save failed: ${e.message}`, "bad");
  }
});

$("#agent-filter-reset").addEventListener("click", () => {
  disabledAgents.clear();
  saveAgentFilter(disabledAgents);
  renderAgentFilter();
  renderCrons();
});

$("#hb-scan-now").addEventListener("click", async () => {
  try {
    const stats = await api("POST", "/api/heartbeat/scan-now");
    const msg = `Scanned ${stats.crons_checked} cron(s) — ${stats.predicates_failed} failure(s) (${stats.duration_ms}ms)`;
    toast(msg, stats.predicates_failed > 0 ? "bad" : "good");
    loadAll();
  } catch (e) { toast(`Scan failed: ${e.message}`, "bad"); }
});

$("#settings-btn").addEventListener("click", async () => {
  $("#setting-recipient").value = state.settings.default_alert_recipient || "";
  $("#setting-max-retries").value = state.settings.default_max_retries ?? 1;
  $("#setting-sender").textContent = state.settings.sender_account || "(not configured)";
  $("#setting-version").textContent = `v${state.settings.daemon_version || "?"}`;
  $("#setting-uptime").textContent = uptimeStr(state.settings.daemon_uptime_seconds);
  // AI section
  $("#setting-ai-enabled").checked = !!state.settings.ai_enabled;
  updateAIConfigVisibility();
  await loadAndRenderAIModels({ initialPrimary: state.settings.ai_primary_model || "",
                                initialFallback: state.settings.ai_fallback_model || "" });
  $("#settings-modal").showModal();
});

$("#settings-save").addEventListener("click", async () => {
  try {
    await api("PATCH", "/api/settings", {
      default_alert_recipient: $("#setting-recipient").value,
      default_max_retries: parseInt($("#setting-max-retries").value, 10),
      ai_enabled: $("#setting-ai-enabled").checked,
      ai_primary_model: $("#setting-ai-primary").value || "",
      ai_fallback_model: $("#setting-ai-fallback").value || "",
    });
    toast("Settings saved", "good");
    $("#settings-modal").close();
    loadAll();
  } catch (e) { toast(`Save failed: ${e.message}`, "bad"); }
});

$("#settings-cancel").addEventListener("click", () => $("#settings-modal").close());

// Toggle the AI sub-section's visibility when the master switch flips.
function updateAIConfigVisibility() {
  const on = !!$("#setting-ai-enabled").checked;
  $("#ai-config-section").classList.toggle("hidden", !on);
}
$("#setting-ai-enabled").addEventListener("change", updateAIConfigVisibility);

// Fetch the latest model list from /api/ai/models and re-populate both
// dropdowns. Detects stale selections (current value not in the new list)
// and adds a synthetic "(no longer in openclaw.json)" option so the
// operator sees the situation rather than the value silently resetting.
//
// Opts:
//   initialPrimary / initialFallback — when re-rendering after a refresh,
//     pass the current select.value so user mid-edits aren't clobbered.
async function loadAndRenderAIModels({ initialPrimary, initialFallback } = {}) {
  const t0 = Date.now();
  let models;
  try {
    models = await api("GET", "/api/ai/models");
  } catch (e) {
    toast(`Could not load model list: ${e.message}`, "bad");
    return;
  }
  state.aiModels = models || [];
  const took = Date.now() - t0;
  const onlineCount = state.aiModels.filter(m => m.online).length;
  $("#ai-models-stamp").textContent =
    `${state.aiModels.length} models · ${onlineCount} online · refreshed ${took}ms ago`;

  const baseOpts = state.aiModels.map(m => {
    let suffix = "";
    if (m.online === false) suffix = "  — offline";
    return `<option value="${escapeAttr(m.key)}" title="${escapeAttr((m.tuning_notes || "").slice(0, 200))}">${escapeHtml(m.label + suffix)}</option>`;
  });

  // Build the option HTML for a slot, prepending a synthetic stale option
  // if `currentValue` isn't in the fresh model list.
  function buildOpts(currentValue) {
    let html = `<option value="">— none —</option>`;
    if (currentValue && !state.aiModels.some(m => m.key === currentValue)) {
      html += `<option value="${escapeAttr(currentValue)}" class="opt-stale">${escapeHtml(currentValue)} — no longer in openclaw.json</option>`;
    }
    html += baseOpts.join("");
    return html;
  }

  function renderSlot(slot, fallbackInitial) {
    const sel = $(`#setting-ai-${slot}`);
    // Prefer the currently-displayed value (user may have changed it mid-edit)
    // over the initial value passed in.
    const current = sel.value || fallbackInitial || "";
    sel.innerHTML = buildOpts(current);
    sel.value = current;
    renderTuningInfo(slot);
  }
  renderSlot("primary", initialPrimary);
  renderSlot("fallback", initialFallback);
}

$("#ai-refresh-models").addEventListener("click", async () => {
  const btn = $("#ai-refresh-models");
  btn.disabled = true;
  const orig = btn.textContent;
  btn.textContent = "↻ Refreshing…";
  try {
    await loadAndRenderAIModels();
    toast("Model list refreshed", "good");
  } finally {
    btn.disabled = false;
    btn.textContent = orig;
  }
});

// Render the tuning info line beneath a model dropdown for the currently
// selected model. Empty when no model is selected.
function renderTuningInfo(slot) {
  const sel = $(`#setting-ai-${slot}`);
  const info = $(`#${slot}-tuning-info`);
  const key = sel.value;
  if (!key || !state.aiModels) { info.innerHTML = ""; return; }
  const m = state.aiModels.find(x => x.key === key);
  if (!m) {
    // Selection exists but isn't in the current model list — stale config
    info.innerHTML = `<div class="tuning-stale">&#9888; <strong>${escapeHtml(key)}</strong> is no longer in openclaw.json. Pick another model or update openclaw config and refresh.</div>`;
    return;
  }
  const name = m.tuning_name || "default";
  const notes = (m.tuning_notes || "").trim();
  let statusLine = "";
  if (m.online === false) {
    statusLine = `<div class="tuning-offline">&#9888; Endpoint not reachable right now — fallback will be used if available.</div>`;
  }
  // Context-budget line — imported from openclaw.json:
  //   context window | model max output | resolved effective output | reserveTokens
  let budgetLine = "";
  if (m.context_window) {
    const ctx = (m.context_window / 1024).toFixed(0) + "K";
    const reserve = m.compaction && m.compaction.reserveTokens
      ? `${(m.compaction.reserveTokens / 1024).toFixed(0)}K reserve`
      : "";
    const mmt = m.model_max_tokens
      ? `model max ${(m.model_max_tokens / 1024).toFixed(0)}K out`
      : "";
    const eff = m.effective_max_tokens
      ? `using ${m.effective_max_tokens} out`
      : "";
    const parts = [`${ctx} context`, mmt, reserve, eff].filter(Boolean);
    budgetLine = `<div class="tuning-budget">${escapeHtml(parts.join(" · "))}</div>`;
  }
  info.innerHTML = `
    ${statusLine}
    Tuning: <span class="tuning-name">${escapeHtml(name)}</span>
    ${m.tuning_source ? `<span class="tuning-src">(${escapeHtml(m.tuning_source)})</span>` : ""}
    ${notes ? `<br><span>${escapeHtml(notes)}</span>` : ""}
    ${budgetLine}
  `;
}

$("#setting-ai-primary").addEventListener("change", () => renderTuningInfo("primary"));
$("#setting-ai-fallback").addEventListener("change", () => renderTuningInfo("fallback"));

// ----- bootstrap

loadAll();
setInterval(loadAll, 30000);
