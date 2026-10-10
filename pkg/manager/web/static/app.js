// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

/* go-helix 管理控制台前端逻辑（原生 JS，无框架依赖）
 *
 * 视图结构：所有可复用片段在 index.html 中以 <template> 声明，本文件通过
 * cloneNode 复制模板并填充数据；列表/卡片的点击统一由容器上的事件委托
 * 处理，重渲染后无需重新绑定。 */

const TOKEN_KEY = "gohelix_token";

const state = {
  token: sessionStorage.getItem(TOKEN_KEY) || "",
  view: "clusters",
  clusters: [],
  raftNodes: [],
  expanded: new Set(),
  busy: false,
  switchLogs: [],
  selectedLog: null,
  selectedSource: "",
  liveTimer: null,
  // 审计视图：当前分段（audit=操作审计 / switch=切换记录）与两份列表数据。
  auditSeg: "audit",
  auditLogs: [],
  switchRecords: [],
  // Names of the cluster cards currently in the DOM, in order. Used to tell
  // whether a background refresh can patch cards in place (no flicker) or has
  // to rebuild the whole list.
  renderedClusterNames: null,
};

/* ---------------- 网络层 ---------------- */

// request 是统一入口：同源相对路径 + 自动附加 Bearer Token。
async function request(path, options = {}) {
  const headers = Object.assign({}, options.headers || {});
  if (state.token) headers["Authorization"] = "Bearer " + state.token;

  let body = options.body;
  if (options.json !== undefined) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(options.json);
  }

  const resp = await fetch(path, { method: options.method || "GET", headers, body });
  if (resp.status === 401) {
    const err = new Error("未授权（401）");
    err.code = 401;
    throw err;
  }
  return resp;
}

async function getJSON(path) {
  const r = await request(path);
  if (!r.ok) throw new Error(`${path} -> HTTP ${r.status}`);
  return r.json();
}

async function postJSON(path, payload) {
  const r = await request(path, { method: "POST", json: payload });
  const text = await r.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { raw: text }; }
  return { ok: r.ok, status: r.status, data };
}

/* ---------------- Toast ---------------- */

function toast(message, type = "info", timeout = 3500) {
  const box = document.getElementById("toastBox");
  const el = document.createElement("div");
  el.className = `toast toast-${type}`;
  el.textContent = message;
  box.appendChild(el);
  setTimeout(() => {
    el.style.opacity = "0";
    el.style.transition = "opacity .3s";
    setTimeout(() => el.remove(), 300);
  }, timeout);
}

/* ---------------- 工具 ---------------- */

function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function fmtTime(t) {
  if (!t) return "-";
  const d = new Date(t);
  if (isNaN(d.getTime())) return "-";
  const p = (n) => String(n).padStart(2, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

// fmtDateTime 渲染完整时间戳为 "MM-DD HH:mm:ss"（同年）或带年份。
function fmtDateTime(t) {
  if (!t) return "-";
  const d = new Date(t);
  if (isNaN(d.getTime())) return "-";
  const p = (n) => String(n).padStart(2, "0");
  const hm = `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
  const md = `${p(d.getMonth() + 1)}-${p(d.getDate())}`;
  if (d.getFullYear() !== new Date().getFullYear()) {
    return `${d.getFullYear()}-${md} ${hm}`;
  }
  return `${md} ${hm}`;
}

function setConn(text, kind = "muted") {
  const el = document.getElementById("connBadge");
  el.className = `badge badge-${kind}`;
  el.textContent = text;
}

/* ---------------- 模板辅助 ---------------- */

// tpl clones the single root element declared inside a <template>.
function tpl(id) {
  const t = document.getElementById(id);
  return t.content.firstElementChild.cloneNode(true);
}

// makeBadge builds a colored pill, e.g. makeBadge("green","健康").
function makeBadge(kind, text) {
  const b = tpl("tpl-badge");
  b.classList.add("badge-" + kind);
  b.textContent = text;
  return b;
}

// priorityBadge builds the cluster-priority pill (P0 red .. P3 gray).
function priorityBadge(p) {
  const kinds = { P0: "red", P1: "orange", P2: "blue", P3: "muted" };
  const value = p || "P3";
  return makeBadge(kinds[value] || "muted", value);
}

// buildPriorityPill returns a priority badge carrying the data-priority
// marker so clicks on it open the editor (it sits in the clickable head).
function buildPriorityPill(p) {
  const b = priorityBadge(p);
  b.classList.add("card-priority");
  b.setAttribute("data-priority", "");
  return b;
}

// applyRoleTag fills an existing .role-tag element for the given role.
function applyRoleTag(tag, role) {
  if (role === "master") {
    tag.classList.add("role-master");
    tag.textContent = "MASTER";
  } else if (role === "slave") {
    tag.classList.add("role-slave");
    tag.textContent = "SLAVE";
  } else {
    tag.classList.add("role-other");
    tag.textContent = role || "-";
  }
}

// makeRoleTag builds a standalone role pill from the role-tag template.
function makeRoleTag(role) {
  const tag = tpl("tpl-role-tag");
  applyRoleTag(tag, role);
  return tag;
}

// makeLine builds a <div> composed of mixed text and styled spans.
// Segments are either plain strings (text nodes) or { text, cls } spans.
function makeLine(segments) {
  const div = document.createElement("div");
  segments.forEach((seg) => {
    if (typeof seg === "string") {
      div.appendChild(document.createTextNode(seg));
    } else {
      const s = document.createElement("span");
      if (seg.cls) s.className = seg.cls;
      s.textContent = seg.text;
      div.appendChild(s);
    }
  });
  return div;
}

/* ---------------- Token 弹窗 ---------------- */

const tokenModal = document.getElementById("tokenModal");
function openTokenModal() { tokenModal.classList.add("show"); document.getElementById("tokenInput").focus(); }
function closeTokenModal() { tokenModal.classList.remove("show"); }

document.getElementById("btnToken").addEventListener("click", openTokenModal);
document.getElementById("tokenSave").addEventListener("click", saveToken);
document.getElementById("tokenInput").addEventListener("keydown", (e) => {
  if (e.key === "Enter") saveToken();
});

async function saveToken() {
  state.token = document.getElementById("tokenInput").value.trim();
  if (state.token) sessionStorage.setItem(TOKEN_KEY, state.token);
  else sessionStorage.removeItem(TOKEN_KEY);
  closeTokenModal();
  await bootstrap();
}

/* ---------------- 确认弹窗 ---------------- */

const confirmModal = document.getElementById("confirmModal");
let confirmHandler = null;

function confirmAction({
  title, body, bodyHTML = "", bodyNode = null, showPreferred = true,
  okText = "确认执行", okDisabled = false, onOk,
}) {
  document.getElementById("confirmTitle").textContent = title;
  const bodyEl = document.getElementById("confirmBody");
  if (bodyNode) {
    bodyEl.replaceChildren(bodyNode);
  } else if (bodyHTML) {
    bodyEl.innerHTML = bodyHTML;
  } else {
    bodyEl.textContent = body;
  }
  const prefField = document.getElementById("confirmPreferred").closest(".field");
  prefField.style.display = showPreferred ? "block" : "none";
  document.getElementById("confirmPreferred").value = "";
  const okBtn = document.getElementById("confirmOk");
  okBtn.textContent = okText;
  okBtn.disabled = okDisabled;
  confirmHandler = onOk;
  confirmModal.classList.add("show");
  if (showPreferred) document.getElementById("confirmPreferred").focus();
}

function closeConfirm() { confirmModal.classList.remove("show"); confirmHandler = null; }

document.getElementById("confirmCancel").addEventListener("click", closeConfirm);
document.getElementById("confirmOk").addEventListener("click", async () => {
  if (!confirmHandler) return closeConfirm();
  const btn = document.getElementById("confirmOk");
  const preferred = document.getElementById("confirmPreferred").value.trim();
  btn.disabled = true;
  try {
    await confirmHandler(preferred);
    closeConfirm();
  } catch (e) {
    toast("操作失败：" + e.message, "error", 5000);
    btn.disabled = false;
  }
});

/* ---------------- 视图切换 ---------------- */

// switchTab activates a top-level view by name (used by both clicks and the
// automatic jump after triggering a switch).
function switchTab(viewName) {
  document.querySelectorAll(".tab").forEach((t) =>
    t.classList.toggle("active", t.dataset.view === viewName));
  document.querySelectorAll(".view").forEach((v) => v.classList.remove("active"));
  state.view = viewName;
  const view = document.getElementById("view-" + viewName);
  if (view) view.classList.add("active");
}

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    stopLiveLog();
    switchTab(tab.dataset.view);
    if (tab.dataset.view === "switchlogs") loadSwitchLogs(true);
    if (tab.dataset.view === "audit") loadActiveAuditSeg();
    if (tab.dataset.view === "notify") loadNotify().catch((e) => toast(e.message, "error"));
  });
});

/* ---------------- 数据加载 ---------------- */

// namesEqual compares two name lists (same clusters, same order).
function namesEqual(a, b) {
  return a.length === b.length && a.every((n, i) => n === b[i]);
}

// populateClusterDatalist fills the hosttools cluster-name suggestions.
function populateClusterDatalist(clusters) {
  const dl = document.getElementById("clusterNameList");
  if (!dl) return;
  dl.textContent = "";
  clusters.forEach((c) => {
    const opt = document.createElement("option");
    opt.value = c.cluster_name;
    dl.appendChild(opt);
  });
}

async function loadClusters() {
  const data = await getJSON("/clusters");
  state.clusters = data.clusters || [];
  populateClusterDatalist(state.clusters);

  // Background refreshes every 5s must not rebuild the card DOM (that made
  // the topology visibly flash). When the cluster set is unchanged we only
  // patch the dynamic header fields in place.
  const names = state.clusters.map((c) => c.cluster_name);
  if (namesEqual(names, state.renderedClusterNames || [])) {
    updateClusterCardsInPlace();
  } else {
    renderClusters();
  }

  // 策略徽标位于卡片内部，必须等卡片就位后再加载，避免渲染竞态。
  await loadTopologyPolicies();

  // 展开中的集群刷新详情
  await Promise.all([...state.expanded].map((name) => loadClusterDetail(name)));
}

async function loadClusterDetail(name) {
  const q = "/status?" + new URLSearchParams({ cluster: name }).toString();
  const data = await getJSON(q);
  renderClusterDetail(name, data.nodes || []);
}

async function loadRaft() {
  const data = await getJSON("/raft/nodes");
  state.raftNodes = data.nodes || [];
  renderRaft(data.leader_id, data.tier);
}

// topologyPolicyPath builds the per-cluster topology policy endpoint.
function topologyPolicyPath(cluster) {
  return "/config/topology?" + new URLSearchParams({ cluster }).toString();
}

// policyValue maps one topology policy field from the API response.
function policyValue(p, key) {
  if (key === "detach") return !!p.detach_lost_replicas;
  if (key === "preserve") return !!p.preserve_hierarchy;
  return !!p.intermediate_master_recovery;
}

// renderClusterPolicy syncs the three per-row toggles in the card with the
// effective policy. User interaction is separate (see bindCardToggles): here
// we only update a toggle's checked state when the server value differs, so
// background polls do not fight the user or make the switches flash.
function renderClusterPolicy(name, p) {
  const card = document.querySelector(
    `.cluster-card[data-cluster="${CSS.escape(name)}"]`);
  if (!card) return;

  card.querySelectorAll(".tp-card-toggle").forEach((input) => {
    const key = input.dataset.policy;
    const want = policyValue(p, key);
    if (input.checked !== want && !input.dataset.locked) input.checked = want;
  });
}

function renderClusterPolicyError(name) {
  // Toggles stay in their last known state; surface the failure via toast
  // instead of swapping the whole row layout.
  toast(`集群「${name}」拓扑策略读取失败`, "error");
}

// loadTopologyPolicies resolves the effective policy for EVERY cluster card.
// It must run after the cards exist in the DOM. A 401 is rethrown so
// bootstrap can prompt for the token; other per-cluster failures are shown
// without aborting the remaining clusters.
async function loadTopologyPolicies() {
  const outcomes = await Promise.allSettled(state.clusters.map(async (c) => {
    try {
      const p = await getJSON(topologyPolicyPath(c.cluster_name));
      renderClusterPolicy(c.cluster_name, p);
    } catch (e) {
      if (e && e.code === 401) throw e;
      renderClusterPolicyError(c.cluster_name);
    }
  }));
  const authFail = outcomes.find(
    (r) => r.status === "rejected" && r.reason && r.reason.code === 401);
  if (authFail) throw authFail.reason;
}

/* ---- topology policy edit modal ---- */

const topoPolicyModal = document.getElementById("topoPolicyModal");
// Cluster currently being edited; the modal is shared but every save is scoped
// to exactly this cluster.
let topoEditingCluster = "";

// openTopoPolicyModal loads one cluster's current policy and shows the editor.
async function openTopoPolicyModal(cluster) {
  if (!cluster) { toast("未指定要编辑的集群", "error"); return; }
  topoEditingCluster = cluster;
  try {
    const p = await getJSON(topologyPolicyPath(cluster));
    document.getElementById("tpDetach").checked = !!p.detach_lost_replicas;
    document.getElementById("tpPreserve").checked = !!p.preserve_hierarchy;
    document.getElementById("tpIntermediate").checked = !!p.intermediate_master_recovery;
    document.getElementById("topoPolicyTitle").textContent = "编辑拓扑策略 · " + cluster;
    topoPolicyModal.classList.add("show");
  } catch (e) {
    toast("读取拓扑策略失败：" + e.message, "error");
  }
}

function closeTopoPolicyModal() {
  topoPolicyModal.classList.remove("show");
  topoEditingCluster = "";
}

// saveTopoPolicy commits one cluster's edited policy (POST runs on the leader;
// the browser request is proxied when this node is a follower), then patches
// that card's badges to reflect the committed values.
async function saveTopoPolicy() {
  const cluster = topoEditingCluster;
  if (!cluster) { toast("未指定要编辑的集群", "error"); return; }

  const payload = {
    detach_lost_replicas: document.getElementById("tpDetach").checked,
    preserve_hierarchy: document.getElementById("tpPreserve").checked,
    intermediate_master_recovery: document.getElementById("tpIntermediate").checked,
  };

  const saveBtn = document.getElementById("tpSave");
  saveBtn.disabled = true;
  try {
    const r = await postJSON(topologyPolicyPath(cluster), payload);
    if (!r.ok) throw new Error("HTTP " + r.status);
    closeTopoPolicyModal();
    toast(`集群「${cluster}」拓扑策略已保存并同步`, "success");
    renderClusterPolicy(cluster, payload);
  } catch (e) {
    toast("保存拓扑策略失败：" + e.message, "error");
  } finally {
    saveBtn.disabled = false;
  }
}

/* ---- cluster priority edit modal ---- */

const priorityModal = document.getElementById("priorityModal");
let priorityCluster = "";

// openPriorityModal loads the cluster's current priority and shows the editor.
function openPriorityModal(cluster) {
  const cur = state.clusters.find((c) => c.cluster_name === cluster);
  const value = (cur && cur.cluster_priority) || "P3";
  priorityCluster = cluster;
  const radio = document.querySelector(`#priorityModal input[value="${value}"]`);
  if (radio) radio.checked = true;
  document.getElementById("priorityModalTitle").textContent =
    "修改集群优先级 · " + cluster;
  priorityModal.classList.add("show");
}

function closePriorityModal() {
  priorityModal.classList.remove("show");
  priorityCluster = "";
}

// savePriority commits the chosen priority, then patches the card pill.
async function savePriority() {
  const cluster = priorityCluster;
  const radio = document.querySelector(
    '#priorityModal input[name="priorityOption"]:checked');
  if (!cluster || !radio) return;
  const value = radio.value;

  const saveBtn = document.getElementById("prioritySave");
  saveBtn.disabled = true;
  try {
    const path = "/config/priority?" + new URLSearchParams({ cluster }).toString();
    const r = await postJSON(path, { cluster_priority: value });
    if (!r.ok) throw new Error("HTTP " + r.status);
    const cur = state.clusters.find((c) => c.cluster_name === cluster);
    if (cur) cur.cluster_priority = value;
    const pill = document.querySelector(
      `.cluster-card[data-cluster="${CSS.escape(cluster)}"] .card-priority`);
    if (pill) pill.replaceWith(buildPriorityPill(value));
    closePriorityModal();
    toast(`集群「${cluster}」优先级已设置为 ${value}`, "success");
  } catch (e) {
    toast("保存优先级失败：" + e.message, "error");
  } finally {
    saveBtn.disabled = false;
  }
}

/* ---- access endpoint edit modal (none/vip/domain) ---- */

const endpointModal = document.getElementById("endpointModal");
let endpointCluster = "";

// currentEndpointType returns the cluster's stored endpoint type (default none).
function currentEndpointType(cluster) {
  const cur = state.clusters.find((c) => c.cluster_name === cluster);
  return (cur && cur.endpoint_type) || "none";
}

// openEndpointModal loads the cluster's current endpoint type/value.
function openEndpointModal(cluster) {
  const cur = state.clusters.find((c) => c.cluster_name === cluster);
  const type = currentEndpointType(cluster);
  endpointCluster = cluster;

  const radio = document.querySelector(`#endpointModal input[value="${type}"]`);
  if (radio) radio.checked = true;

  const input = document.getElementById("endpointValueInput");
  input.value = (cur && cur.endpoint_value) || "";

  document.getElementById("endpointModalTitle").textContent =
    "编辑接入端点 · " + cluster;
  updateEndpointValueState();
  endpointModal.classList.add("show");
}

function closeEndpointModal() {
  endpointModal.classList.remove("show");
  endpointCluster = "";
}

// selectedEndpointType returns the chosen type radio value.
function selectedEndpointType() {
  const r = document.querySelector(
    '#endpointModal input[name="endpointTypeOption"]:checked');
  return r ? r.value : "none";
}

// updateEndpointValueState enables/relabels the value input by type.
function updateEndpointValueState() {
  const type = selectedEndpointType();
  const field = document.querySelector(".endpoint-value-field");
  const label = document.getElementById("endpointValueLabel");
  const input = document.getElementById("endpointValueInput");

  if (type === "none") {
    field.style.opacity = ".5";
    input.disabled = true;
    input.value = "";
    label.textContent = "未启用端点";
    return;
  }
  field.style.opacity = "1";
  input.disabled = false;
  if (type === "vip") {
    label.textContent = "VIP（浮动 IP）";
    input.placeholder = "如 10.0.0.1";
  } else {
    label.textContent = "域名";
    input.placeholder = "如 cluster1.db.internal";
  }
}

// saveEndpoint persists type+value via the cluster registration endpoint.
async function saveEndpoint() {
  const cluster = endpointCluster;
  const type = selectedEndpointType();
  const value = document.getElementById("endpointValueInput").value.trim();

  if (type !== "none" && !value) {
    toast(type === "vip" ? "请填写 VIP" : "请填写域名", "error");
    return;
  }

  const cur = state.clusters.find((c) => c.cluster_name === cluster);
  const priority = (cur && cur.cluster_priority) || "P3";

  const saveBtn = document.getElementById("endpointSave");
  saveBtn.disabled = true;
  try {
    const r = await postJSON("/cluster/register", {
      cluster_name: cluster,
      cluster_priority: priority,
      endpoint_type: type,
      endpoint_value: type === "none" ? "" : value,
    });
    if (!r.ok) throw new Error("HTTP " + r.status);

    // Reflect in state + card without waiting for the next poll.
    if (cur) {
      cur.endpoint_type = type;
      cur.endpoint_value = type === "none" ? "" : value;
    }
    const card = document.querySelector(
      `.cluster-card[data-cluster="${CSS.escape(cluster)}"]`);
    if (card) renderCardEndpoint(card, { endpoint_type: type, endpoint_value: value });

    closeEndpointModal();
    toast(`接入端点已保存：${type === "none" ? "未启用" : type + "=" + value}`, "success");
  } catch (e) {
    toast("保存接入端点失败：" + e.message, "error");
  } finally {
    saveBtn.disabled = false;
  }
}

/* ---------------- 渲染：数据库集群 ---------------- */

function healthBadge(c) {
  if (c.healthy) return makeBadge("green", "健康");
  if (c.master_count !== 1) return makeBadge("red", "异常");
  return makeBadge("orange", "部分节点离线");
}

// clusterMetaText renders the one-line summary shown under a cluster name.
function clusterMetaText(c) {
  return `节点 ${c.node_count} · 主 ${c.master_count} · 从 ${c.slave_count}` +
    ` · 在线 ${c.online_count} · 最近心跳 ${fmtTime(c.last_heartbeat)}`;
}

// endpointBadges renders the endpoint TYPE pill plus the value pill.
// The type pill always shows VIP/域名/未配置 so the row distinguishes
// the endpoint kind at a glance, even before reading the value.
function endpointBadges(c) {
  const frag = document.createDocumentFragment();

  if (!c.endpoint_type || c.endpoint_type === "none") {
    frag.appendChild(makeBadge("muted", "未配置"));
    return frag;
  }

  if (c.endpoint_type === "vip") {
    frag.appendChild(makeBadge("blue", "VIP"));
    frag.appendChild(makeBadge("blue", c.endpoint_value || "?"));
  } else {
    frag.appendChild(makeBadge("green", "域名"));
    if (c.endpoint_value) frag.appendChild(makeBadge("green", c.endpoint_value));
  }
  return frag;
}

// renderCardEndpoint fills the endpoint row (type + value) inside one card.
function renderCardEndpoint(card, c) {
  const box = card.querySelector(".card-endpoint-badges");
  if (box) box.replaceChildren(endpointBadges(c));
}

// updateClusterCardsInPlace patches only the health badge and summary line of
// the existing cards. It performs no structural DOM mutation, so periodic
// auto-refresh is visually seamless.
function updateClusterCardsInPlace() {
  const box = document.getElementById("clusterList");
  state.clusters.forEach((c) => {
    const card = box.querySelector(
      `.cluster-card[data-cluster="${CSS.escape(c.cluster_name)}"]`);
    if (!card) return;
    card.querySelector(".card-health").replaceChildren(healthBadge(c));
    const pEl = card.querySelector(".card-priority");
    if (pEl) {
      const incoming = buildPriorityPill(c.cluster_priority);
      if (pEl.outerHTML !== incoming.outerHTML) pEl.replaceWith(incoming);
    }
    card.querySelector(".cluster-meta").textContent = clusterMetaText(c);
    renderCardEndpoint(card, c);
  });
}

function renderClusters() {
  const box = document.getElementById("clusterList");
  if (!state.clusters.length) {
    box.replaceChildren(tpl("tpl-cluster-empty"));
    state.renderedClusterNames = [];
    return;
  }

  const cards = state.clusters.map((c) => {
    const card = tpl("tpl-cluster-card");
    card.dataset.cluster = c.cluster_name;
    if (state.expanded.has(c.cluster_name)) card.classList.add("open");

    card.querySelector(".cluster-head").dataset.toggle = c.cluster_name;
    card.querySelector(".card-health").replaceChildren(healthBadge(c));
    card.querySelector(".card-name").textContent = c.cluster_name;
    card.querySelector(".card-priority").replaceWith(buildPriorityPill(c.cluster_priority));
    card.querySelector(".cluster-meta").textContent = clusterMetaText(c);
    renderCardEndpoint(card, c);

    card.querySelector(".topo").id = "topo-" + c.cluster_name;
    card.querySelector(".detail-body").id = "detail-" + c.cluster_name;
    return card;
  });

  box.replaceChildren(...cards);
  state.renderedClusterNames = state.clusters.map((c) => c.cluster_name);
}

// toggleCard expands or collapses one card and loads detail on first expand.
async function toggleCard(name) {
  const box = document.getElementById("clusterList");
  const card = box.querySelector(
    `.cluster-card[data-cluster="${CSS.escape(name)}"]`);
  if (state.expanded.has(name)) {
    state.expanded.delete(name);
    card.classList.remove("open");
  } else {
    state.expanded.add(name);
    card.classList.add("open");
    await loadClusterDetail(name);
  }
}

/* ---------------- 渲染：集群拓扑 ---------------- */

// nodeAddr returns a node's canonical database address.
function nodeAddr(n) { return `${n.ip}:${n.port}`; }

// topoNode builds one topology node card from n. isIntermediate marks a slave
// that itself has downstream replicas.
function topoNode(n, isIntermediate) {
  const online = n.status === "ONLINE";
  const el = tpl("tpl-topo-node");
  el.classList.add("topo-node-" + (n.role || "other"));
  if (isIntermediate) el.classList.add("topo-node-intermediate");
  if (!online) el.classList.add("is-offline");

  el.querySelector(".topo-dot").classList.add(online ? "dot-on" : "dot-off");

  // Role label distinguishes master / intermediate / slave leaf.
  const roleEl = el.querySelector(".topo-node-role");
  if (n.role === "master") {
    roleEl.textContent = "MASTER";
  } else if (isIntermediate) {
    roleEl.textContent = "中间主";
  } else {
    roleEl.textContent = (n.role || "unknown").toUpperCase();
  }

  el.querySelector(".topo-node-addr").textContent = nodeAddr(n);
  el.querySelector(".topo-node-meta").textContent =
    `${n.db_version || "-"} · 延迟 ${n.seconds_behind_master || 0}s` +
    (online ? "" : " · 已隔离");
  return el;
}

// buildTopology renders the replication tree from the master_host edges,
// recursively so cascaded sub-trees keep their visible hierarchy. It falls
// back to a flat layout only when no node has a resolvable parent chain.
function buildTopology(nodes) {
  if (!nodes.length) return null;

  const byAddr = new Map();
  nodes.forEach((n) => byAddr.set(nodeAddr(n), n));

  // children edges keyed by upstream address, only when both ends resolve.
  const children = new Map();
  const hasParent = new Set();
  nodes.forEach((n) => {
    if (!n.master_host) return;
    const upAddr = `${n.master_host}:${n.master_port}`;
    if (!byAddr.has(upAddr)) return; // edge outside the reported set
    if (!children.has(upAddr)) children.set(upAddr, []);
    children.get(upAddr).push(n);
    hasParent.add(nodeAddr(n));
  });

  const isIntermediate = (n) =>
    n.role === "slave" && (children.get(nodeAddr(n)) || []).length > 0;

  // Roots = nodes that never appear as a child (prefer the marked master).
  let roots = nodes.filter((n) => !hasParent.has(nodeAddr(n)));
  const masters = roots.filter((n) => n.role === "master");
  if (masters.length) roots = masters;

  // No usable parent chain at all -> flat layout.
  if (!roots.length || !children.size) {
    const flat = document.createElement("div");
    flat.className = "topo-flat";
    nodes.forEach((n) => flat.appendChild(topoNode(n, isIntermediate(n))));
    return flat;
  }

  const tree = tpl("tpl-topo-tree");
  const rootBox = tree.querySelector(".topo-roots");
  roots.forEach((n) => rootBox.appendChild(topoNode(n, isIntermediate(n))));

  // Recursively attach each root's subtree into the root <li>.
  const rootLi = tree.querySelector(".topo-tree > li");
  const attachSubtree = (parentAddr, parentLi) => {
    const kids = children.get(parentAddr) || [];
    if (!kids.length) return;
    const ul = document.createElement("ul");
    ul.className = "topo-children";
    kids.forEach((kid) => {
      const li = document.createElement("li");
      li.appendChild(topoNode(kid, isIntermediate(kid)));
      const arrow = document.createElement("span");
      arrow.className = "topo-arrow";
      li.appendChild(arrow);
      ul.appendChild(li);
      attachSubtree(nodeAddr(kid), li);
    });
    parentLi.appendChild(ul);
  };
  roots.forEach((n) => attachSubtree(nodeAddr(n), rootLi));

  return tree;
}

// renderTopology draws the tree above the node table. It replaces the DOM only
// when deep structure changed, so a stable topology refreshes invisibly.
function renderTopology(name, nodes) {
  const box = document.getElementById("topo-" + name);
  if (!box) return;
  const incoming = buildTopology(nodes);
  if (incoming === null) {
    box.replaceChildren();
    return;
  }
  const current = box.firstElementChild;
  if (!current || !current.isEqualNode(incoming)) {
    box.replaceChildren(incoming);
  }
}

/* ---------------- 渲染：节点详情表 ---------------- */

// nodeRow builds one node-table row from n.
function nodeRow(n) {
  const row = tpl("tpl-node-row");
  const addrFull = `${n.ip}:${n.port}`;
  const addr = row.querySelector(".cell-addr");
  addr.textContent = addrFull;
  addr.title = addrFull;

  applyRoleTag(row.querySelector(".cell-role .role-tag"), n.role);
  row.querySelector(".cell-status").textContent = n.status;
  row.querySelector(".cell-version").textContent = n.db_version || "-";
  return row;
}

function renderClusterDetail(name, nodes) {
  const tb = document.getElementById("detail-" + name);
  if (!tb) return;

  const rows = nodes.length ? nodes.map(nodeRow) : [tpl("tpl-node-empty")];
  const same = tb.rows.length === rows.length &&
    rows.every((r, i) => tb.rows[i].isEqualNode(r));
  if (!same) tb.replaceChildren(...rows);

  renderTopology(name, nodes);
}

/* ---------------- 渲染：Manager / Raft ---------------- */

// raftRoleBadge builds the colored role pill for a raft node.
function raftRoleBadge(role) {
  const map = {
    leader: "green",
    follower: "blue",
    "non-voter": "orange",
    pending: "muted",
  };
  return makeBadge(map[role] || "muted", role);
}

// raftRow builds one raft-table row from n.
function raftRow(n) {
  const row = tpl("tpl-raft-row");
  row.querySelector(".node-id").textContent = n.node_id;
  if (n.is_self) {
    const self = row.querySelector(".self-note");
    self.textContent = " (本机)";
    self.hidden = false;
  }
  row.querySelector(".cell-raft-addr").textContent = n.raft_addr || "-";
  row.querySelector(".cell-api-addr").textContent = n.api_addr || "-";
  row.querySelector(".cell-role").appendChild(raftRoleBadge(n.role));
  row.querySelector(".cell-suffrage").textContent = n.suffrage || "-";
  row.querySelector(".cell-incluster").textContent = n.in_cluster ? "是" : "否";

  const canRemove = n.is_self || !n.in_cluster;
  if (canRemove) {
    row.querySelector(".cell-op").appendChild(tpl("tpl-remove-btn-disabled"));
  } else {
    const b = tpl("tpl-remove-btn");
    b.dataset.leave = n.node_id;
    row.querySelector(".cell-op").appendChild(b);
  }
  return row;
}

function renderRaft(leaderID, tier) {
  const tb = document.getElementById("raftTableBody");
  const rows = state.raftNodes.length
    ? state.raftNodes.map(raftRow)
    : [tpl("tpl-raft-empty")];
  const same = tb.rows.length === rows.length &&
    rows.every((r, i) => tb.rows[i].isEqualNode(r));
  if (!same) tb.replaceChildren(...rows);

  // Service tier of this Manager cluster (startup-fixed).
  const tierEl = document.querySelector(".raft-tier");
  if (tierEl && tier) {
    const kinds = { P0: "red", P1: "orange", P2: "blue", P3: "muted" };
    tierEl.textContent = tier;
    tierEl.className = "raft-tier badge badge-" + (kinds[tier] || "muted");
  }

  if (leaderID) setConn("Leader: " + leaderID, "green");
  else setConn("当前无 Leader", "red");
}

/* ---------------- 操作：切换 / 移除 ---------------- */

function askSwitch(cluster, graceful) {
  confirmAction({
    title: graceful ? "计划切换（Switchover）" : "紧急切换（Failover）",
    body: graceful
      ? `将对集群「${cluster}」执行无损计划切换：追平数据后提升新 Master，旧 Master 降级为 Slave。`
      : `将对集群「${cluster}」执行紧急故障切换。请确认当前 Master 已不可用，切换可能导致未同步数据丢失。`,
    showPreferred: true,
    okText: graceful ? "执行计划切换" : "执行紧急切换",
    onOk: async (preferred) => {
      const res = await postJSON(graceful ? "/switchover" : "/failover", {
        cluster_name: cluster,
        preferred_node: preferred,
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      toast((graceful ? "计划切换" : "紧急切换") + "已发起：" + cluster, "success");

      // Jump to the switch-log view and live-tail this switch's log.
      const kind = graceful ? "switchover" : "failover";
      switchTab("switchlogs");
      document.getElementById("switchlogKind").value = kind;
      document.getElementById("switchlogFilter").value = cluster;
      await loadSwitchLogs(true);
      startLiveLog(kind, cluster);
      setTimeout(refreshAll, 1000);
    },
  });
}

function askLeave(nodeID) {
  confirmAction({
    title: "移除 Raft 节点",
    body: `确认将节点「${nodeID}」永久移出 Raft 集群？移除后 Leader 将停止向其重连，该节点需清理 raft_dir 后重新加入。`,
    showPreferred: false,
    okText: "确认移除",
    onOk: async () => {
      const res = await postJSON("/leave", { node_id: nodeID });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      toast("已移除节点：" + nodeID, "success");
      await loadRaft();
    },
  });
}

/* ---------------- 主从恢复 ---------------- */

// requestRecover posts one recovery then jumps to the recover log live view.
async function requestRecover(cluster, mode, targetNode = "") {
  const res = await postJSON("/recover", {
    cluster_name: cluster,
    mode,
    target_node: targetNode,
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  toast("主从恢复已发起：" + cluster, "success");

  switchTab("switchlogs");
  // The log kind now equals the recover mode, so the filter and tailed file
  // id ({mode}/{cluster}_{date}.log) match the operation actually triggered.
  document.getElementById("switchlogKind").value = mode;
  document.getElementById("switchlogFilter").value = cluster;
  await loadSwitchLogs(true);
  startLiveLog(mode, cluster);
  setTimeout(() => refreshAll(true), 1000);
}

// askRecoverReplicaCluster is the card-level entry. The target slave is taken
// from the preferred field; when left blank it auto-selects the slave only if
// the cluster has exactly one, otherwise the user must specify it.
function askRecoverReplicaCluster(cluster) {
  confirmAction({
    title: "恢复从库复制",
    body: "修复指定从库到当前主库的复制链路：停止复制 → 重新指向主库（repl 账号、GTID 自动定位）→ 启动复制并等待追平。可在下方填写目标从库 host:port；留空且集群仅有一个从库时自动选择。",
    showPreferred: true,
    okText: "恢复复制",
    onOk: async (preferred) => {
      let target = preferred.trim();
      if (!target) {
        const q = "/status?" + new URLSearchParams({ cluster }).toString();
        const data = await getJSON(q);
        const slaves = (data.nodes || []).filter((n) => n.role === "slave");
        if (!slaves.length) throw new Error("集群中没有可恢复的从库");
        if (slaves.length > 1) {
          throw new Error("存在多个从库，请填写目标 host:port");
        }
        target = `${slaves[0].ip}:${slaves[0].port}`;
      }
      return requestRecover(cluster, "replica", target);
    },
  });
}

// askRecoverTopology converges the whole cluster to one master.
function askRecoverTopology(cluster) {
  confirmAction({
    title: "重建集群拓扑",
    body: `将把集群「${cluster}」收敛为一主多从。可在下方填写作为主的节点 host:port（留空则使用当前主库）；其余节点全部重新指向该主库并等待追平。`,
    showPreferred: true,
    okText: "重建拓扑",
    onOk: (preferred) => requestRecover(cluster, "topology", preferred.trim()),
  });
}

/* ---------------- 回滚预检预览 ---------------- */

// pfNode builds one compact node snapshot card. Missing nodes get the dashed
// "已移除" style; restore nodes additionally show replication details.
function pfNode(title, v, isRestore) {
  const node = tpl("tpl-pf-node");
  node.querySelector(".pf-node-title").textContent = title;
  const addr = node.querySelector(".pf-node-addr");
  const body = node.querySelector(".pf-node-body");

  if (!v || !v.present) {
    node.classList.add("pf-node-missing");
    addr.remove();
    body.replaceChildren(tpl("tpl-pf-miss"));
    return node;
  }

  addr.textContent = v.addr;
  body.appendChild(makeLine(
    ["角色：", { text: v.role || "-" }, " · 状态：", { text: v.status || "-" }]));

  if (isRestore) {
    body.appendChild(makeLine(["上游：", { text: v.upstream || "未配置" }]));
    body.appendChild(makeLine([
      "接收线程：",
      { text: v.receiver_running ? "运行" : "停止", cls: v.receiver_running ? "pf-ok" : "pf-miss" },
      " · 重放线程：",
      { text: v.applier_running ? "运行" : "停止", cls: v.applier_running ? "pf-ok" : "pf-miss" },
    ]));
    body.appendChild(makeLine(["复制延迟：" + String(v.seconds_behind) + "s"]));
  }
  return node;
}

// rollbackPreview builds the full preflight body as a DocumentFragment.
function rollbackPreview(pf) {
  const frag = document.createDocumentFragment();

  const banner = tpl("tpl-pf-banner");
  banner.classList.add(pf.feasible ? "pf-banner-ok" : "pf-banner-bad");
  banner.textContent = pf.feasible
    ? "✓ 预检通过：可无损回滚，不会丢失数据"
    : "✗ 预检未通过：存在不满足的前置条件，回滚按钮已禁用";
  frag.appendChild(banner);

  const r = pf.record || {};
  const rec = tpl("tpl-pf-record");
  rec.textContent = `回滚记录：#${String(r.id)} ${r.operation || ""}` +
    ` @ ${r.create_time || ""} · ${r.old_master || ""} → ${r.new_master || ""}`;
  frag.appendChild(rec);

  const nodes = tpl("tpl-pf-nodes");
  nodes.appendChild(pfNode("当前主库", pf.current, false));
  nodes.appendChild(pfNode("待恢复主库", pf.restore, true));
  frag.appendChild(nodes);

  const checks = tpl("tpl-pf-checks");
  pf.checks.forEach((c) => {
    const li = tpl("tpl-pf-check");
    const icon = li.querySelector(".pf-icon");
    icon.className = "pf-icon " + (c.ok ? "pf-ok" : "pf-miss");
    icon.textContent = c.ok ? "✓" : "✗";
    li.querySelector(".pf-check-name").textContent = c.name;
    li.querySelector(".pf-check-detail").textContent = c.detail || "";
    // Advisory (non-blocking) checks are visually de-emphasized and tagged.
    if (!c.required) {
      li.classList.add("pf-check-advisory");
      li.querySelector(".pf-check-name").textContent = c.name + "（参考）";
    }
    checks.appendChild(li);
  });
  frag.appendChild(checks);
  return frag;
}

function askRecoverRollback(cluster) {
  getJSON("/recover/preflight?" + new URLSearchParams({ cluster }).toString())
    .then((pf) => {
      confirmAction({
        title: "回滚最近一次切换",
        bodyNode: rollbackPreview(pf),
        showPreferred: false,
        okText: "确认回滚",
        okDisabled: !pf.feasible,
        onOk: () => requestRecover(cluster, "rollback"),
      });
    })
    .catch((err) => {
      // Preflight itself failed (e.g. metadata store issue): show the plain
      // confirmation so the operation is not completely blocked.
      if (err.code === 401) {
        toast("会话已过期，请重新输入 Token", "error");
        return;
      }
      confirmAction({
        title: "回滚最近一次切换",
        body: `预检失败（${err.message}）。仍可尝试将集群「${cluster}」回滚到切换前的原主库，但存在风险。`,
        showPreferred: false,
        okText: "仍要回滚",
        onOk: () => requestRecover(cluster, "rollback"),
      });
    });
}

/* ---------------- 切换日志 ---------------- */

function todayStamp() {
  const d = new Date();
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}`;
}

// loadSwitchLogs fetches the historical list using the current filters. When
// the aggregate checkbox is on it collects logs from every Raft node.
async function loadSwitchLogs(keepSelection = false) {
  const kind = document.getElementById("switchlogKind").value;
  const cluster = document.getElementById("switchlogFilter").value.trim();
  const aggregated = document.getElementById("aggregateSwitchLogs").checked;

  const params = new URLSearchParams();
  if (kind) params.set("kind", kind);
  if (cluster) params.set("cluster", cluster);

  const endpoint = aggregated ? "/switchlogs/aggregate" : "/switchlogs";
  const data = await getJSON(endpoint + "?" + params.toString());
  state.switchLogs = data.logs || [];
  renderSwitchLogs(keepSelection);

  if (aggregated && Array.isArray(data.unreachable) && data.unreachable.length) {
    toast("部分节点不可达：" + data.unreachable.join(", "), "error", 6000);
  }
}

// kindBadge builds the type pill for the log list. failover is red,
// switchover blue, and every recovery variant orange.
function kindBadge(kind) {
  if (kind === "switchover") return makeBadge("blue", "计划");
  if (kind === "topology") return makeBadge("orange", "拓扑");
  if (kind === "rollback") return makeBadge("orange", "回滚");
  if (kind === "replica" || kind === "recover") return makeBadge("orange", "恢复");
  return makeBadge("red", "紧急");
}

// kindLabel returns the Chinese label used in the content head title. It maps
// 1:1 to the action button that produced the log.
function kindLabel(kind) {
  if (kind === "switchover") return "计划切换";
  if (kind === "topology") return "重建拓扑";
  if (kind === "rollback") return "回滚切换";
  if (kind === "replica" || kind === "recover") return "恢复复制";
  return "紧急切换";
}

// fmtDateStamp turns "20261005" into "2026-10-05".
function fmtDateStamp(date) {
  date = String(date || "");
  if (/^\d{8}$/.test(date)) {
    return date.slice(0, 4) + "-" + date.slice(4, 6) + "-" + date.slice(6, 8);
  }
  return date;
}

// logHeadTitle builds the friendly content head, e.g.
// "集群 cluster1 · 计划切换 · 2026-10-05".
function logHeadTitle(cluster, kind, date) {
  let s = `集群 ${cluster} · ${kindLabel(kind)}`;
  const d = fmtDateStamp(date);
  if (d) s += " · " + d;
  return s;
}

// rowKey distinguishes aggregated rows: same file id may exist on different
// source nodes with different content.
function rowKey(id, source) {
  return source ? id + "@" + source : id;
}

// switchlogRow builds one log-list row from m.
function switchlogRow(m) {
  const row = tpl("tpl-switchlog-row");
  row.dataset.id = m.id;
  row.dataset.source = m.source;
  if (rowKey(state.selectedLog, state.selectedSource) ===
      rowKey(m.id, m.source)) {
    row.classList.add("selected");
  }

  row.querySelector(".cell-kind").appendChild(kindBadge(m.kind));
  row.querySelector(".cell-cluster strong").textContent = m.cluster;
  row.querySelector(".cell-date").textContent = m.date;
  const src = row.querySelector(".cell-source");
  src.textContent = m.source || "-";
  src.title = m.source || "";

  const btn = tpl("tpl-view-btn");
  btn.dataset.viewLog = m.id;
  btn.dataset.viewSource = m.source;
  row.querySelector(".cell-op").appendChild(btn);
  return row;
}

// selectLogRow selects one row and loads its content.
function selectLogRow(id, source) {
  stopLiveLog();
  state.selectedLog = id;
  state.selectedSource = source;
  const tb = document.getElementById("switchlogList");
  const key = rowKey(id, source);
  tb.querySelectorAll(".switchlog-row").forEach((r) =>
    r.classList.toggle("selected",
      rowKey(r.dataset.id, r.dataset.source) === key));
  showLogContent(id, false, source);
}

function renderSwitchLogs(keepSelection = false) {
  const tb = document.getElementById("switchlogList");
  const rows = state.switchLogs.length
    ? state.switchLogs.map(switchlogRow)
    : [tpl("tpl-switchlog-empty")];
  const same = tb.rows.length === rows.length &&
    rows.every((r, i) => tb.rows[i].isEqualNode(r));
  if (!same) tb.replaceChildren(...rows);

  if (keepSelection && state.selectedLog) {
    showLogContent(state.selectedLog, !!state.liveTimer, state.selectedSource);
  }
}

async function fetchLogText(id, source = "", bust = false) {
  const params = new URLSearchParams({ id });
  if (source) params.set("node", source);
  // Live tail polls the same file repeatedly; a unique param bypasses the
  // browser/proxy cache so we never re-render a stale first snapshot.
  if (bust) params.set("_t", String(Date.now()));
  const r = await request("/switchlog/content?" + params.toString());
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.text();
}

// showLogContent renders one log file's content into the right panel. source
// routes the read to the node holding the file (aggregated listings).
async function showLogContent(id, live = false, source = "") {
  const title = document.getElementById("switchlogTitle");
  const liveTag = document.getElementById("switchlogLive");
  const box = document.getElementById("switchlogContent");
  // Prefer the friendly "集群 · 类型 · 日期" head resolved from the list
  // metadata; keep the raw file id as hover text and a fallback.
  const meta = state.switchLogs.find(
    (m) => rowKey(m.id, m.source) === rowKey(id, source));
  title.textContent = meta
    ? logHeadTitle(meta.cluster, meta.kind, meta.date)
    : (source ? rowKey(id, source) : id);
  title.title = source ? rowKey(id, source) : id;
  liveTag.hidden = !live;
  try {
    const text = await fetchLogText(id, source);
    const stick = box.scrollHeight - box.scrollTop - box.clientHeight < 60;
    box.textContent = text || "（空文件）";
    if (stick) box.scrollTop = box.scrollHeight;
  } catch (e) {
    box.textContent = "读取日志失败：" + e.message;
  }
}

// isLogFinished detects the terminator the monitor writes at sequence end.
// Log files append every switch of the same day, so a *previous* run's End
// marker must not terminate the tail of the run that just started: only the
// section at or after the last Start banner is inspected.
function isLogFinished(text) {
  const startRe = /=+\s*(?:Failover|Switchover|Recover|Replica|Topology|Rollback)\s+Start\s*=+/g;
  let lastStart = -1;
  let m;
  while ((m = startRe.exec(text)) !== null) lastStart = m.index;
  const tail = lastStart >= 0 ? text.slice(lastStart) : text;
  return /End \((SUCCESS|FAILED)\)/.test(tail);
}

// startLiveLog tails the log of the switch just triggered. The target file is
// "{kind}/{cluster}_{today}.log". Polling stops once an End marker appears.
function startLiveLog(kind, cluster) {
  stopLiveLog();
  const id = `${kind}/${cluster}_${todayStamp()}.log`;
  state.selectedLog = id;
  state.selectedSource = "";

  const box = document.getElementById("switchlogContent");
  const title = document.getElementById("switchlogTitle");
  const liveTag = document.getElementById("switchlogLive");

  // Show a waiting hint immediately; otherwise, when the file is not yet
  // readable the panel keeps the static "click an entry" placeholder text.
  title.textContent = logHeadTitle(cluster, kind, todayStamp());
  title.title = id;
  liveTag.hidden = false;
  box.textContent = "等待日志生成…";

  let missCount = 0;
  // Last content already painted. The panel is rewritten only when the file
  // actually grew, so a steady log never reflows or flickers.
  let lastText = null;
  const updatedEl = document.getElementById("switchlogUpdated");
  const poll = async () => {
    try {
      const text = await fetchLogText(id, "", true);
      missCount = 0;

      if (text !== lastText) {
        lastText = text;
        const stick = box.scrollHeight - box.scrollTop - box.clientHeight < 60;
        box.textContent = text || "日志文件已创建，暂无内容…";
        if (stick) box.scrollTop = box.scrollHeight;
      }

      // Freshness marker: proves the tail is still live and shows last fetch.
      updatedEl.textContent = "更新于 " + fmtTime(new Date());
      updatedEl.hidden = false;

      if (isLogFinished(text)) stopLiveLog();
    } catch (_) {
      // File not yet readable (just triggered, or switch aborted before the
      // first line). Keep waiting, but make the state visible instead of
      // silently showing the old placeholder.
      missCount++;
      if (missCount === 15) {
        box.textContent =
          "仍未读到日志文件（已等待约 15s）。若持续如此，请确认切换已真正开始、Manager 日志目录可写，以及当前节点为执行切换的 Leader。";
      }
    }
  };

  poll();
  state.liveTimer = setInterval(poll, 1000);
  // Safety cap: never tail longer than the overall operation window.
  setTimeout(stopLiveLog, 5 * 60 * 1000);
}

function stopLiveLog() {
  if (state.liveTimer) {
    clearInterval(state.liveTimer);
    state.liveTimer = null;
  }
  const liveTag = document.getElementById("switchlogLive");
  if (liveTag) liveTag.hidden = true;
  const updatedEl = document.getElementById("switchlogUpdated");
  if (updatedEl) updatedEl.hidden = true;
}

/* ---------------- 审计日志：操作审计 + 切换记录 ---------------- */

// auditPageLimit 是控制台一次拉取的条数（后端默认 100、上限 1000）。
const auditPageLimit = 200;

// categoryBadge 渲染审计类别徽标。
function categoryBadge(cat) {
  const map = {
    lifecycle: ["blue", "生命周期"],
    config: ["muted", "配置"],
    cluster: ["orange", "集群"],
    security: ["red", "安全"],
  };
  const [kind, label] = map[cat] || ["muted", cat || "-"];
  return makeBadge(kind, label);
}

// auditResultBadge 渲染审计结果徽标。
function auditResultBadge(result) {
  const map = {
    success: ["green", "成功"],
    started: ["blue", "开始"],
    failed: ["red", "失败"],
    aborted: ["orange", "中止"],
  };
  const [kind, label] = map[result] || ["muted", result || "-"];
  return makeBadge(kind, label);
}

// switchOperationBadge 渲染切换记录操作徽标（recover 按恢复模式细分）。
function switchOperationBadge(op, mode) {
  if (op === "failover") return makeBadge("red", "紧急切换");
  if (op === "switchover") return makeBadge("blue", "计划切换");
  const label = {
    topology: "重建拓扑", rollback: "回滚切换", replica: "恢复复制",
  }[mode] || "恢复";
  return makeBadge("orange", label);
}

// switchResultBadge 渲染切换记录结果徽标。
function switchResultBadge(result) {
  if (result === "SUCCESS") return makeBadge("green", "成功");
  if (result === "FAILED") return makeBadge("red", "失败");
  return makeBadge("muted", result || "-");
}

// switchAuditSeg 切换操作审计 / 切换记录分段，并加载对应列表。
function switchAuditSeg(seg) {
  if (seg !== "audit" && seg !== "switch") return;
  state.auditSeg = seg;
  document.querySelectorAll("#auditSegment .seg-btn").forEach((b) =>
    b.classList.toggle("active", b.dataset.seg === seg));
  document.querySelectorAll("[data-seg-filters]").forEach((f) =>
    (f.hidden = f.dataset.segFilters !== seg));
  document.querySelectorAll("[data-seg-panel]").forEach((p) =>
    (p.hidden = p.dataset.segPanel !== seg));
  loadActiveAuditSeg();
}

// loadActiveAuditSeg 加载当前分段对应的列表（失败统一以 toast 提示）。
function loadActiveAuditSeg() {
  if (state.auditSeg === "switch") {
    loadSwitchRecords().catch((e) => toast(e.message, "error"));
  } else {
    loadAuditLogs().catch((e) => toast(e.message, "error"));
  }
}

// loadAuditLogs 按当前类别筛选拉取操作审计并渲染。
async function loadAuditLogs() {
  const category = document.getElementById("auditCategory").value;

  const params = new URLSearchParams();
  if (category) params.set("category", category);
  params.set("limit", String(auditPageLimit));

  const data = await getJSON("/audit-logs?" + params.toString());
  state.auditLogs = data.audit_logs || [];
  renderAuditLogs();
}

// auditRow 构造一条操作审计行。
function auditRow(a) {
  const row = tpl("tpl-audit-row");
  row.querySelector(".cell-time").textContent = fmtDateTime(a.time);
  row.querySelector(".cell-category").appendChild(categoryBadge(a.category));
  row.querySelector(".cell-action").textContent = a.action;
  row.querySelector(".cell-cluster").textContent = a.cluster || "-";
  row.querySelector(".cell-target").textContent = a.target || "-";
  row.querySelector(".cell-result").appendChild(auditResultBadge(a.result));
  // 来源由源 IP 与执行节点组合（缺项自动省略）。
  const src = [a.source_ip, a.node].filter(Boolean).join(" · ");
  row.querySelector(".cell-source").textContent = src || "-";
  const detailCell = row.querySelector(".cell-detail");
  detailCell.textContent = a.detail || "";
  detailCell.title = a.detail || "";
  return row;
}

function renderAuditLogs() {
  const tb = document.getElementById("auditTableBody");
  const rows = state.auditLogs.length
    ? state.auditLogs.map(auditRow)
    : [tpl("tpl-audit-empty")];
  tb.replaceChildren(...rows);
}

// loadSwitchRecords 按当前操作筛选拉取结构化切换记录并渲染。
async function loadSwitchRecords() {
  const operation = document.getElementById("switchRecOperation").value;

  const params = new URLSearchParams();
  if (operation) params.set("operation", operation);
  params.set("limit", String(auditPageLimit));

  const data = await getJSON("/switch-records?" + params.toString());
  state.switchRecords = data.records || [];
  renderSwitchRecords();
}

// switchRecordRow 构造一条切换记录行。
function switchRecordRow(r) {
  const row = tpl("tpl-switch-record-row");
  row.querySelector(".cell-time").textContent = fmtDateTime(r.time);
  row.querySelector(".cell-cluster strong").textContent = r.cluster;
  row.querySelector(".cell-operation").appendChild(
    switchOperationBadge(r.operation, r.recover_mode));
  row.querySelector(".cell-old").textContent = r.old_master || "-";
  row.querySelector(".cell-new").textContent = r.new_master || "-";
  row.querySelector(".cell-code").textContent = r.analysis_code || "-";
  row.querySelector(".cell-result").appendChild(switchResultBadge(r.result));
  row.querySelector(".cell-node").textContent = r.node || "-";
  return row;
}

function renderSwitchRecords() {
  const tb = document.getElementById("switchRecordBody");
  const rows = state.switchRecords.length
    ? state.switchRecords.map(switchRecordRow)
    : [tpl("tpl-switch-record-empty")];
  tb.replaceChildren(...rows);
}

/* ---------------- 刷新与启动 ---------------- */

async function refreshAll(silent = true) {
  // loadClusters 内部已在卡片渲染后加载每个集群的策略，不再单独请求全局策略。
  const tasks = [loadClusters(), loadRaft()];
  const results = await Promise.allSettled(tasks);
  const rejected = results.find((r) => r.status === "rejected");
  if (rejected) {
    const err = rejected.reason;
    if (err.code === 401) throw err; // 交给 bootstrap 处理
    if (!silent) toast("刷新失败：" + err.message, "error");
    setConn("刷新失败", "red");
  }
}

async function bootstrap() {
  setConn("连接中…", "muted");
  try {
    await refreshAll(false);
  } catch (e) {
    if (e.code === 401) {
      setConn("需要 Token", "orange");
      openTokenModal();
    } else {
      setConn("连接失败", "red");
      toast("初始化失败：" + e.message, "error", 6000);
    }
  }
}

/* ---------------- 静态控件事件 ---------------- */

document.getElementById("btnRefreshClusters")
  .addEventListener("click", () => loadClusters().catch((e) => toast(e.message, "error")));
document.getElementById("tpCancel")
  .addEventListener("click", closeTopoPolicyModal);
document.getElementById("tpSave")
  .addEventListener("click", () => saveTopoPolicy().catch((e) => toast(e.message, "error")));
topoPolicyModal.addEventListener("click", (e) => {
  if (e.target === topoPolicyModal) closeTopoPolicyModal();
});
document.getElementById("priorityCancel")
  .addEventListener("click", closePriorityModal);
document.getElementById("prioritySave")
  .addEventListener("click", () => savePriority().catch((e) => toast(e.message, "error")));
priorityModal.addEventListener("click", (e) => {
  if (e.target === priorityModal) closePriorityModal();
});
document.getElementById("endpointCancel")
  .addEventListener("click", closeEndpointModal);
document.getElementById("endpointSave")
  .addEventListener("click", () => saveEndpoint().catch((e) => toast(e.message, "error")));
endpointModal.addEventListener("click", (e) => {
  if (e.target === endpointModal) closeEndpointModal();
});
document.querySelectorAll('#endpointModal input[name="endpointTypeOption"]')
  .forEach((r) => r.addEventListener("change", updateEndpointValueState));
document.getElementById("btnRefreshManagers")
  .addEventListener("click", () => loadRaft().catch((e) => toast(e.message, "error")));
document.getElementById("btnRefreshSwitchLogs")
  .addEventListener("click", () => loadSwitchLogs(true).catch((e) => toast(e.message, "error")));
document.getElementById("btnBackToClusters")
  .addEventListener("click", async () => {
    stopLiveLog();
    switchTab("clusters");
    try {
      await loadClusters();
    } catch (e) {
      toast(e.message, "error");
    }
  });
document.getElementById("switchlogKind")
  .addEventListener("change", () => loadSwitchLogs().catch((e) => toast(e.message, "error")));
document.getElementById("aggregateSwitchLogs")
  .addEventListener("change", () => loadSwitchLogs().catch((e) => toast(e.message, "error")));
document.getElementById("switchlogFilter")
  .addEventListener("keyup", (e) => {
    if (e.key === "Enter") loadSwitchLogs().catch((err) => toast(err.message, "error"));
  });

// 审计日志：分段切换、手动刷新与筛选。
document.querySelectorAll("#auditSegment .seg-btn").forEach((b) =>
  b.addEventListener("click", () => switchAuditSeg(b.dataset.seg)));
document.getElementById("btnRefreshAudit")
  .addEventListener("click", () => loadActiveAuditSeg());
document.getElementById("auditCategory")
  .addEventListener("change", () => loadAuditLogs().catch((e) => toast(e.message, "error")));
document.getElementById("switchRecOperation")
  .addEventListener("change", () => loadSwitchRecords().catch((e) => toast(e.message, "error")));

/* ---------------- 列表事件委托 ---------------- */

// 集群卡片：展开、四种操作、节点级恢复，统一在容器上识别 data-* 目标。
document.getElementById("clusterList").addEventListener("click", (e) => {
  // Priority pill sits inside the clickable head: handle it before the
  // expand toggle so changing priority does not fold/unfold the card.
  const prio = e.target.closest("[data-priority]");
  if (prio) {
    const cluster = prio.closest(".cluster-card").dataset.cluster;
    openPriorityModal(cluster);
    return;
  }

  const toggle = e.target.closest("[data-toggle]");
  if (toggle) {
    toggleCard(toggle.dataset.toggle);
    return;
  }
  const actBtn = e.target.closest("[data-act]");
  if (actBtn) {
    const name = actBtn.closest(".cluster-card").dataset.cluster;
    switch (actBtn.dataset.act) {
      case "failover": askSwitch(name, false); break;
      case "switchover": askSwitch(name, true); break;
      case "rebuild": askRecoverTopology(name); break;
      case "rollback": askRecoverRollback(name); break;
      case "recover-replica": askRecoverReplicaCluster(name);
      case "edit-endpoint": openEndpointModal(name); break;
    }
    return;
  }
});

// Card topology switches: flipping a row immediately persists that one
// policy field. On failure the switch reverts to its previous state.
document.getElementById("clusterList").addEventListener("change", async (e) => {
  const input = e.target.closest(".tp-card-toggle");
  if (!input) return;

  const card = input.closest(".cluster-card");
  const cluster = card.dataset.cluster;
  const key = input.dataset.policy;
  const previous = !input.checked;

  input.dataset.locked = "1";
  input.disabled = true;
  try {
    // Read current values, then overwrite just the toggled field so the
    // other two switches stay as they are.
    const cur = await getJSON(topologyPolicyPath(cluster));
    const body = {
      detach_lost_replicas: cur.detach_lost_replicas,
      preserve_hierarchy: cur.preserve_hierarchy,
      intermediate_master_recovery: cur.intermediate_master_recovery,
    };
    if (key === "detach") body.detach_lost_replicas = input.checked;
    else if (key === "preserve") body.preserve_hierarchy = input.checked;
    else body.intermediate_master_recovery = input.checked;

    const r = await postJSON(topologyPolicyPath(cluster), body);
    if (!r.ok) throw new Error("HTTP " + r.status);
    toast(`集群「${cluster}」策略已更新`, "success");
  } catch (err) {
    input.checked = previous;
    toast("保存拓扑策略失败：" + err.message, "error");
  } finally {
    delete input.dataset.locked;
    input.disabled = false;
  }
});

// Raft 表：移除节点。
document.getElementById("raftTableBody").addEventListener("click", (e) => {
  const b = e.target.closest("[data-leave]");
  if (b) askLeave(b.dataset.leave);
});

// 切换日志列表：查看某条（按钮或整行）。
document.getElementById("switchlogList").addEventListener("click", (e) => {
  const v = e.target.closest("[data-view-log]");
  if (v) {
    selectLogRow(v.dataset.viewLog, v.dataset.viewSource);
    return;
  }
  const r = e.target.closest(".switchlog-row");
  if (r) selectLogRow(r.dataset.id, r.dataset.source);
});

// 自动轮询（5s）
setInterval(() => {
  if (document.getElementById("autoRefresh").checked && !state.busy) {
    refreshAll(true).catch(() => {});
  }
}, 5000);

/* ---------------- 响应式：动态适配浏览器大小 ---------------- */

// applyResponsive 根据视口宽度切换密度档位。三行 Grid 外壳与内部
// Grid/Flex 已承担绝大部分流体伸缩，这里只补充需要 JS 感知的密度调整，
// 保证窗口缩小时工具栏项仍可见、不溢出。
function applyResponsive() {
  const w = window.innerWidth;
  document.body.classList.toggle("compact", w < 760);
  document.body.classList.toggle("ultra-compact", w < 480);
}

let resizeTimer = null;
window.addEventListener("resize", () => {
  // debounce：拖拽改变窗口大小期间不频繁执行，停稳后一次到位。
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(applyResponsive, 120);
});
// 横竖屏切换也立即响应。
window.addEventListener("orientationchange", applyResponsive);
applyResponsive();

/* ---------------- 主机工具：流式执行 + 文件拉取 ---------------- */

// streamNDJSON posts body to path and invokes onRecord for each newline-
// delimited JSON object. Returns an abort function; aborting cancels the fetch
// and (server-side) the running process.
function streamNDJSON(path, body, onRecord) {
  const ctrl = new AbortController();
  const headers = { "Content-Type": "application/json" };
  if (state.token) headers["Authorization"] = "Bearer " + state.token;

  (async () => {
    let resp;
    try {
      resp = await fetch(path, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
        signal: ctrl.signal,
      });
    } catch (e) {
      if (e.name !== "AbortError") onRecord({ error: e.message });
      return;
    }

    if (!resp.ok || !resp.body) {
      const text = await resp.text().catch(() => "");
      onRecord({ error: `HTTP ${resp.status}: ${text}` });
      return;
    }

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let nl;
        while ((nl = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, nl).trim();
          buf = buf.slice(nl + 1);
          if (line) onRecord(JSON.parse(line));
        }
      }
      const tail = buf.trim();
      if (tail) onRecord(JSON.parse(tail));
    } catch (e) {
      if (e.name !== "AbortError") onRecord({ error: e.message });
    }
  })();

  return () => ctrl.abort();
}

// parseArgs splits a raw argument string on whitespace (the whitelisted
// commands accept simple flags; validateShellArgs remains the security gate).
function parseArgs(raw) {
  return raw.trim() ? raw.trim().split(/\s+/) : [];
}

let execAbort = null;
const esOut = document.getElementById("esOutput");
const esStatus = document.getElementById("esStatus");
const esCancelBtn = document.getElementById("btnExecStreamCancel");

document.getElementById("btnExecStream").addEventListener("click", () => {
  const cluster = document.getElementById("esCluster").value.trim();
  const node = document.getElementById("esNode").value.trim();
  const command = document.getElementById("esCommand").value;
  const args = parseArgs(document.getElementById("esArgs").value);
  if (!cluster || !node) { toast("请填写集群与目标节点", "error"); return; }

  esOut.textContent = "";
  esStatus.textContent = "执行中…";
  document.getElementById("btnExecStream").disabled = true;
  esCancelBtn.disabled = false;

  execAbort = streamNDJSON("/agent/exec-stream",
    { cluster, node, command, args }, (rec) => {
      if (rec.error) {
        esOut.textContent += "错误：" + rec.error + "\n";
      } else {
        // 被拦截时 Agent 把原因放在 line 里且同条记录带 final=true，
        // 必须先输出文本，否则拦截原因会被吞掉（历史 bug：exit=undefined）。
        // channel: 0=STDOUT（省略时即 undefined），1=STDERR。
        if (rec.line) {
          esOut.textContent += (rec.channel === 1 ? "[stderr] " : "") +
            rec.line + "\n";
        }
        if (rec.final) {
          // exit_code 为 0 时 proto omitempty 会省略该字段，按 0 兜底。
          esStatus.textContent = `结束（exit=${rec.exit_code ?? 0}）`;
        }
      }
      esOut.scrollTop = esOut.scrollHeight;
      if (rec.final || rec.error) finishExecStream();
    });
});

esCancelBtn.addEventListener("click", () => {
  if (execAbort) execAbort();
  finishExecStream();
  esStatus.textContent = "已取消";
});

function finishExecStream() {
  execAbort = null;
  document.getElementById("btnExecStream").disabled = false;
  esCancelBtn.disabled = true;
}

document.getElementById("btnFetchFile").addEventListener("click", () => {
  const cluster = document.getElementById("ffCluster").value.trim();
  const node = document.getElementById("ffNode").value.trim();
  const source_url = document.getElementById("ffSourceUrl").value.trim();
  const target_dir = document.getElementById("ffTargetDir").value.trim();
  const filename = document.getElementById("ffFilename").value.trim();
  const sha256 = document.getElementById("ffSha256").value.trim();
  const max_bytes = Number(document.getElementById("ffMaxBytes").value || 0) * 1024 * 1024;
  const limit_kbps = Number(document.getElementById("ffLimitKbps").value || 0);

  if (!cluster || !node || !source_url || !target_dir || !filename) {
    toast("请填写集群、节点、源URL、目标目录与文件名", "error"); return;
  }

  const ffOut = document.getElementById("ffOutput");
  const ffStatus = document.getElementById("ffStatus");
  ffOut.textContent = "";
  ffStatus.textContent = "拉取中…";

  streamNDJSON("/agent/fetch-file", {
    cluster, node, source_url, target_dir, filename,
    sha256, max_bytes, limit_kbps,
  }, (rec) => {
    if (rec.phase === "error" || rec.error) {
      ffOut.textContent += "错误：" + (rec.message || rec.error) + "\n";
      ffStatus.textContent = "失败";
      return;
    }
    if (rec.phase === "done" || rec.done) {
      ffOut.textContent += "完成：" + rec.message + "\n";
      ffStatus.textContent = "完成";
      return;
    }
    const pct = rec.total > 0 ? `（${Math.round(rec.loaded * 100 / rec.total)}%）` : "";
    ffOut.textContent = `已下载 ${rec.loaded}/${rec.total} bytes ${pct} · ${rec.speed_bps} B/s · ${rec.phase}\n`;
  });
});

/* ---------------- 主题：白色 / 黑色 ---------------- */

// 主题选择持久化在 localStorage（跨会话保留），与只存 sessionStorage 的
// token 不同：主题是纯界面偏好，无需登录即可应用。
const THEME_KEY = "gohelix_theme";

// 月亮图标（当前为白色，点击切到黑色）/ 太阳图标（当前为黑色，点击切白）。
const ICON_MOON =
  `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none"
     stroke="currentColor" stroke-width="2" stroke-linecap="round"
     stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3
     7 7 0 0 0 21 12.79z"/></svg>`;
const ICON_SUN =
  `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none"
     stroke="currentColor" stroke-width="2" stroke-linecap="round"
     stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path
     d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></svg>`;

// currentTheme 以 <html data-theme> 为单一真实状态源。
function currentTheme() {
  return document.documentElement.dataset.theme === "dark" ? "dark" : "light";
}

// applyTheme 只写 data-theme 属性并同步按钮图标/提示；所有颜色变化
// 完全由 CSS 变量层完成，JS 不直接操作任何颜色。
function applyTheme(theme) {
  if (theme === "dark") {
    document.documentElement.setAttribute("data-theme", "dark");
  } else {
    document.documentElement.removeAttribute("data-theme");
  }
  const btn = document.getElementById("btnTheme");
  if (btn) {
    btn.innerHTML = theme === "dark" ? ICON_SUN : ICON_MOON;
    btn.title = theme === "dark" ? "切换为白色主题" : "切换为黑色主题";
  }
}

document.getElementById("btnTheme").addEventListener("click", () => {
  const next = currentTheme() === "dark" ? "light" : "dark";
  try { localStorage.setItem(THEME_KEY, next); } catch (e) {}
  applyTheme(next);
});

// 与 index.html 头部内联脚本保持同一规则：未显式选过黑色即为白色。
let initialTheme = "light";
try {
  if (localStorage.getItem(THEME_KEY) === "dark") initialTheme = "dark";
} catch (e) {}
applyTheme(initialTheme);

/* ---------------- 告警通知 ---------------- */

const NOTIFY_CHANNELS = [
  { id: "email", name: "邮件（SMTP）" },
  { id: "wechat", name: "企业微信群机器人" },
  { id: "dingtalk", name: "钉钉群机器人" },
  { id: "feishu", name: "飞书群机器人" },
];

// notifyData holds the latest GET /config/notify (masked) view, used to build
// full PUT bodies that preserve the other channels and unchanged secrets.
let notifyData = null;

async function loadNotify() {
  const data = await getJSON("/config/notify");
  renderNotify(data);
}

function renderNotify(data) {
  notifyData = data;

  // 总开关状态
  const dot = document.getElementById("notifyMasterDot");
  const masterState = document.getElementById("notifyMasterState");
  const hint = document.getElementById("notifyMasterHint");
  const masterSwitch = document.getElementById("notifyMasterSwitch");
  dot.className = "notify-master-dot " + (data.enabled ? "on" : "off");
  masterState.textContent = data.enabled ? "通知总开关已启用" : "通知总开关关闭";
  // Programmatic .checked assignment does not fire change, so this won't
  // recursively trigger a save while re-rendering.
  masterSwitch.checked = !!data.enabled;
  hint.textContent = data.enabled
    ? "故障检测与切换事件会推送到已启用且配置完整的渠道；重复性告警按 10 分钟节流。可随时在卡片上编辑或测试。"
    : "打开右侧总开关，或点击渠道卡片的编辑按钮配置后启用；保存经 Raft 同步并持久化。";

  const list = document.getElementById("notifyChannelList");
  list.textContent = "";
  NOTIFY_CHANNELS.forEach((ch) => {
    list.appendChild(buildNotifyChannel(ch, data[ch.id] || {}));
  });
}

function notifyRow(label, value) {
  const row = document.createElement("div");
  row.className = "notify-row";
  const k = document.createElement("span");
  k.className = "notify-row-key";
  k.textContent = label;
  const v = document.createElement("span");
  v.className = "notify-row-val";
  v.textContent = value === "" || value == null ? "-" : String(value);
  row.appendChild(k);
  row.appendChild(v);
  return row;
}

function buildNotifyChannel(ch, c) {
  const card = document.createElement("div");
  card.className = "notify-channel" + (c.enabled ? " is-on" : "");

  const head = document.createElement("div");
  head.className = "notify-channel-head";
  const title = document.createElement("strong");
  title.textContent = ch.name;
  const badge = makeBadge(c.enabled ? "green" : "muted", c.enabled ? "已启用" : "未启用");
  const editBtn = document.createElement("button");
  editBtn.className = "btn btn-sm btn-ghost";
  editBtn.textContent = "编辑";
  editBtn.addEventListener("click", () => openNotifyEdit(ch.id));
  const testBtn = document.createElement("button");
  testBtn.className = "btn btn-sm btn-ghost";
  testBtn.textContent = "测试";
  testBtn.addEventListener("click", () => sendNotifyTest(ch.id, testBtn));
  head.appendChild(title);
  head.appendChild(badge);
  head.appendChild(editBtn);
  head.appendChild(testBtn);

  const body = document.createElement("div");
  body.className = "notify-channel-body";
  if (ch.id === "email") {
    body.appendChild(notifyRow("SMTP 服务器", c.host ? `${c.host}:${c.port || ""}` : ""));
    body.appendChild(notifyRow("发件人", c.from));
    body.appendChild(notifyRow("收件人", Array.isArray(c.to) ? c.to.join(", ") : ""));
    body.appendChild(notifyRow("加密方式", c.use_tls ? "隐式 TLS" : "STARTTLS（若支持）"));
    body.appendChild(notifyRow("登录密码", c.has_password ? "已配置" : "未配置"));
  } else {
    body.appendChild(notifyRow("Webhook 地址", c.url || ""));
    body.appendChild(notifyRow("加签密钥", c.has_secret ? "已配置" : "未配置"));
  }

  card.appendChild(head);
  card.appendChild(body);
  return card;
}

async function sendNotifyTest(channel, btn) {
  if (btn) btn.disabled = true;
  try {
    const payload = channel ? { channel } : {};
    const r = await postJSON("/config/notify", payload);
    renderTestResults(r.data && r.data.results);
    if (!r.ok) toast("测试请求失败（HTTP " + r.status + "）", "error");
  } catch (e) {
    toast("发送测试通知失败：" + e.message, "error");
  } finally {
    if (btn) btn.disabled = false;
  }
}

function renderTestResults(results) {
  const box = document.getElementById("notifyTestResults");
  box.textContent = "";
  if (!results || !results.length) {
    box.className = "notify-test-results muted";
    box.textContent = "没有可测试的渠道（可能未启用或未配置）。";
    return;
  }
  box.className = "notify-test-results";
  results.forEach((r) => {
    const row = document.createElement("div");
    row.className = "notify-test-row " + (r.success ? "ok" : "bad");
    row.textContent = `${r.name}：` + (r.success ? "发送成功" : "失败 - " + r.error);
    box.appendChild(row);
  });
}

document.getElementById("btnRefreshNotify")
  .addEventListener("click", () => loadNotify().catch((e) => toast(e.message, "error")));
document.getElementById("btnTestAll")
  .addEventListener("click", () => sendNotifyTest("", null));

/* ---- notify edit modal + persistence ---- */

async function putJSON(path, payload) {
  const r = await request(path, { method: "PUT", json: payload });
  const text = await r.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { raw: text }; }
  return { ok: r.ok, status: r.status, data };
}

const notifyEditModal = document.getElementById("notifyEditModal");
let editingChannel = null;

// webhookPart builds one webhook section of a PUT body from the masked GET
// view: non-secret enabled is known, url/secret stay null (preserve) unless
// the operator types a replacement in the edit form.
function webhookPart(c) {
  return { enabled: !!(c && c.enabled), url: null, secret: null };
}

// buildBaseNotifyBody reconstructs the FULL PUT body from the last GET view so
// saving one channel never wipes the others. All secrets are null = preserve.
function buildBaseNotifyBody() {
  const d = notifyData || {};
  const e = d.email || {};
  return {
    enabled: !!d.enabled,
    email: {
      enabled: !!e.enabled,
      host: e.host || "",
      port: e.port || 0,
      username: e.username || "",
      password: null,
      from: e.from || "",
      to: Array.isArray(e.to) ? e.to : [],
      use_tls: !!e.use_tls,
    },
    wechat: webhookPart(d.wechat),
    dingtalk: webhookPart(d.dingtalk),
    feishu: webhookPart(d.feishu),
  };
}

function parseRecipients(s) {
  return s.split(/[\n,;]+/).map((x) => x.trim()).filter(Boolean);
}

function openNotifyEdit(channelId) {
  const ch = NOTIFY_CHANNELS.find((x) => x.id === channelId);
  if (!ch || !notifyData) return;
  editingChannel = channelId;
  const c = notifyData[channelId] || {};

  document.getElementById("notifyEditTitle").textContent = "编辑：" + ch.name;
  document.getElementById("neEnabled").checked = !!c.enabled;
  const isEmail = channelId === "email";
  document.getElementById("neEmailFields").hidden = !isEmail;
  document.getElementById("neWebhookFields").hidden = isEmail;

  if (isEmail) {
    document.getElementById("neHost").value = c.host || "";
    document.getElementById("nePort").value = c.port || "";
    document.getElementById("neUsername").value = c.username || "";
    document.getElementById("nePassword").value = ""; // blank => keep current
    document.getElementById("neFrom").value = c.from || "";
    document.getElementById("neTo").value = Array.isArray(c.to) ? c.to.join("\n") : "";
    document.getElementById("neUseTLS").checked = !!c.use_tls;
  } else {
    document.getElementById("neURL").value = ""; // blank => keep current
    document.getElementById("neSecret").value = "";
  }
  notifyEditModal.classList.add("show");
}

function closeNotifyEdit() {
  notifyEditModal.classList.remove("show");
  editingChannel = null;
}

async function submitNotifyEdit() {
  if (!editingChannel) return;
  const body = buildBaseNotifyBody();

  if (editingChannel === "email") {
    body.email.enabled = document.getElementById("neEnabled").checked;
    body.email.host = document.getElementById("neHost").value.trim();
    const portRaw = document.getElementById("nePort").value.trim();
    body.email.port = portRaw ? parseInt(portRaw, 10) || 0 : 0;
    body.email.username = document.getElementById("neUsername").value.trim();
    body.email.from = document.getElementById("neFrom").value.trim();
    body.email.to = parseRecipients(document.getElementById("neTo").value);
    body.email.use_tls = document.getElementById("neUseTLS").checked;
    const pw = document.getElementById("nePassword").value;
    if (pw !== "") body.email.password = pw; // *string => replace
  } else {
    const part = body[editingChannel];
    part.enabled = document.getElementById("neEnabled").checked;
    const u = document.getElementById("neURL").value.trim();
    const sec = document.getElementById("neSecret").value.trim();
    if (u !== "") part.url = u;
    if (sec !== "") part.secret = sec;
  }

  const r = await putJSON("/config/notify", body);
  if (!r.ok) {
    toast("保存通知配置失败（HTTP " + r.status + "）", "error");
    return false;
  }
  renderNotify(r.data);
  closeNotifyEdit();
  toast("通知渠道配置已保存", "success");
  return true;
}

document.getElementById("neCancel").addEventListener("click", closeNotifyEdit);
document.getElementById("neSave").addEventListener("click", () => {
  const btn = document.getElementById("neSave");
  btn.disabled = true;
  submitNotifyEdit()
    .catch((e) => toast("保存失败：" + e.message, "error"))
    .finally(() => { btn.disabled = false; });
});
notifyEditModal.addEventListener("click", (e) => {
  if (e.target === notifyEditModal) closeNotifyEdit();
});

// Master switch: toggles the global enabled flag and persists, preserving all
// channels and secrets. On failure the checkbox reverts to the prior state.
document.getElementById("notifyMasterSwitch").addEventListener("change", async (e) => {
  const wanted = e.target.checked;
  const body = buildBaseNotifyBody();
  body.enabled = wanted;
  try {
    const r = await putJSON("/config/notify", body);
    if (!r.ok) throw new Error("HTTP " + r.status);
    renderNotify(r.data);
    toast(wanted ? "通知总开关已启用" : "通知总开关已关闭", "success");
  } catch (err) {
    e.target.checked = !wanted;
    toast("切换总开关失败：" + err.message, "error");
  }
});

bootstrap();
