// oc-retry-watchdog v0.1 UI

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

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
    const tr = document.createElement("tr");
    tr.className = rowClass(c);
    const predCount = c.predicates_count || 0;
    const predBadge = predCount > 0
      ? `<span class="badge-pred active" title="${predCount} predicate(s) configured">${predCount}</span>`
      : `<span class="badge-pred" title="No predicates configured">0</span>`;
    tr.innerHTML = `
      <td><strong>${escapeHtml(c.name || c.cron_id)}</strong><br><small><code>${c.cron_id}</code></small></td>
      <td><code>${escapeHtml(c.schedule || "?")}</code></td>
      <td class="num">${predBadge}</td>
      <td class="num"><input class="inline num" type="number" min="0" max="10" value="${c.max_retries}" data-field="max_retries" data-id="${c.cron_id}"></td>
      <td><input class="inline" type="text" value="${escapeAttr(c.alert_recipient || "")}" placeholder="(use default)" data-field="alert_recipient" data-id="${c.cron_id}"></td>
      <td class="num">${c.retries_today || 0} / ${c.retries_30d || 0}</td>
      <td class="num">${c.alerts_today || 0} / ${c.alerts_30d || 0}</td>
      <td><small>${fmtDate(c.last_retried_at)}</small></td>
      <td><small>${fmtDate(c.last_alerted_at)}</small></td>
      <td><input type="checkbox" data-field="enabled" data-id="${c.cron_id}" ${c.enabled ? "checked" : ""}></td>
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
