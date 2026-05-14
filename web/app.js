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
      ? `<span class="badge-pred active" title="${escapeAttr(predTitle)}">${predCount}</span>`
      : `<span class="badge-pred" title="${escapeAttr(predTitle)}">0</span>`;
    const agentCell = c.agent
      ? `<span class="agent-chip c${agentColorIndex(c.agent)}" title="Agent: ${escapeAttr(c.agent)}">${escapeHtml(c.agent)}</span>`
      : `<span class="muted" style="font-size:11px;">&mdash;</span>`;
    tr.innerHTML = `
      <td><strong>${escapeHtml(c.name || c.cron_id)}</strong><br><small><code>${c.cron_id}</code></small></td>
      <td>${agentCell}</td>
      <td class="col-hide-md"><code>${escapeHtml(c.schedule || "?")}</code></td>
      <td class="num">${predBadge}</td>
      <td class="num col-hide-sm"><input class="inline num" type="number" min="0" max="10" value="${c.max_retries}" data-field="max_retries" data-id="${c.cron_id}"></td>
      <td class="col-hide-lg"><input class="inline" type="text" value="${escapeAttr(c.alert_recipient || "")}" placeholder="(use default)" data-field="alert_recipient" data-id="${c.cron_id}"></td>
      <td class="num">${c.retries_today || 0} / ${c.retries_30d || 0}</td>
      <td class="num">${c.alerts_today || 0} / ${c.alerts_30d || 0}</td>
      <td class="col-hide-md"><small>${fmtDate(c.last_retried_at)}</small></td>
      <td class="col-hide-md"><small>${fmtDate(c.last_alerted_at)}</small></td>
      <td class="col-hide-sm"><input type="checkbox" data-field="enabled" data-id="${c.cron_id}" ${c.enabled ? "checked" : ""}></td>
      <td class="actions">
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
  if (t.dataset?.action === "retry-now") {
    if (!confirm(`Trigger 'openclaw cron run ${t.dataset.id}'?`)) return;
    try {
      const r = await api("POST", `/api/crons/${t.dataset.id}/retry-now`);
      toast(`Retry: ${r.action}${r.run_id ? ` (run ${r.run_id})` : ""}`, r.action === "retried" ? "good" : "bad");
      setTimeout(loadAll, 500);
    } catch (err) { toast(`Failed: ${err.message}`, "bad"); }
  } else if (t.dataset?.action === "test-alert") {
    if (!confirm(`Send a test alert email for this cron?`)) return;
    try {
      const r = await api("POST", `/api/crons/${t.dataset.id}/test-alert`);
      toast(r.ok ? `Test alert sent to ${r.recipient}` : `Alert failed: ${r.error || "?"}`,
            r.ok ? "good" : "bad");
      setTimeout(loadAll, 500);
    } catch (err) { toast(`Failed: ${err.message}`, "bad"); }
  }
});

$("#refresh-btn").addEventListener("click", loadAll);

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

$("#settings-btn").addEventListener("click", () => {
  $("#setting-recipient").value = state.settings.default_alert_recipient || "";
  $("#setting-max-retries").value = state.settings.default_max_retries ?? 1;
  $("#setting-sender").textContent = state.settings.sender_account || "(not configured)";
  $("#setting-version").textContent = `v${state.settings.daemon_version || "?"}`;
  $("#setting-uptime").textContent = uptimeStr(state.settings.daemon_uptime_seconds);
  $("#settings-modal").showModal();
});

$("#settings-save").addEventListener("click", async () => {
  try {
    await api("PATCH", "/api/settings", {
      default_alert_recipient: $("#setting-recipient").value,
      default_max_retries: parseInt($("#setting-max-retries").value, 10),
    });
    toast("Settings saved", "good");
    $("#settings-modal").close();
    loadAll();
  } catch (e) { toast(`Save failed: ${e.message}`, "bad"); }
});

$("#settings-cancel").addEventListener("click", () => $("#settings-modal").close());

// ----- bootstrap

loadAll();
setInterval(loadAll, 30000);
