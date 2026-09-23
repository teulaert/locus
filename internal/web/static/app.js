// Locus PWA client. All operations go through the versioned API and modify
// the real filesystem on the connected peer; nothing is mirrored into browser
// storage beyond session settings.
const api = {
  token: localStorage.getItem("locus.token") || "",
  async req(method, url, body, headers = {}) {
    const h = { Authorization: "Bearer " + api.token, ...headers };
    const res = await fetch(url, { method, headers: h, body });
    if (res.status === 401) {
      const t = prompt("Locus API token:");
      if (t) {
        localStorage.setItem("locus.token", t);
        api.token = t;
        return api.req(method, url, body, headers);
      }
      throw new Error("unauthorized");
    }
    if (!res.ok) {
      let msg = res.statusText;
      try { msg = (await res.json()).error || msg; } catch {}
      throw new Error(msg);
    }
    const ct = res.headers.get("content-type") || "";
    return ct.includes("json") ? res.json() : res;
  },
  get: (u) => api.req("GET", u),
  post: (u, body, headers) => api.req("POST", u, body, headers),
  del: (u) => api.req("DELETE", u),
};

const state = { root: null, path: "/", view: "list", results: [], status: null, selected: null };

const $ = (id) => document.getElementById(id);

function toast(msg, isErr = false) {
  const t = $("toast");
  t.textContent = msg;
  t.className = "toast" + (isErr ? " error" : "");
  setTimeout(() => t.classList.add("hidden"), 4000);
}

function qparam(v) { return "&" + new URLSearchParams({ root: state.root }).toString(); }

async function boot() {
  try {
    const st = await api.get("/api/v1/status");
    state.status = st;
    const sel = $("rootSelect");
    sel.innerHTML = "";
    for (const r of st.roots) {
      const opt = document.createElement("option");
      opt.value = r.path;
      opt.textContent = r.path;
      sel.appendChild(opt);
    }
    if (st.roots.length) state.root = st.roots[0].path;
    sel.onchange = () => { state.root = sel.value; state.path = "/"; loadFiles(); };
    renderStatus(st);
    await loadFiles();
  } catch (e) {
    toast("Cannot reach this Locus peer: " + e.message, true);
  }
}

function renderStatus(st) {
  $("statusPeer").textContent = "peer: " + (st.hostname || st.peer_id.slice(0, 8));
  const online = (st.peers || []).filter(p => p.state === "online" || p.state === "synchronizing").length;
  const syncState = st.roots?.some(r => r.sync_state === "synchronizing") ? "synchronizing" : `in sync · ${online} peer(s)`;
  $("statusSync").textContent = "sync: " + syncState;
  $("statusIndex").textContent = `index: ${st.index.documents} docs · ${st.index.chunks} chunks`;
  const badge = $("statusConflict");
  badge.textContent = "⚠ " + st.conflicts;
  badge.classList.toggle("hidden", st.conflicts === 0);
}

async function loadFiles() {
  if (!state.root) return;
  show("filesPane");
  const data = await api.get(`/api/v1/files?path=${encodeURIComponent(state.path)}${qparam()}`);
  renderBreadcrumbs(data.path);
  renderFiles(data.entries || []);
}

function renderBreadcrumbs(path) {
  const c = $("breadcrumbs");
  c.innerHTML = "";
  const parts = path.split("/").filter(Boolean);
  const home = document.createElement("button");
  home.textContent = "home";
  home.onclick = () => { state.path = "/"; loadFiles(); };
  c.appendChild(home);
  let acc = "";
  for (const p of parts) {
    acc += "/" + p;
    const sep = document.createElement("span");
    sep.textContent = " / ";
    c.appendChild(sep);
    const b = document.createElement("button");
    b.textContent = p;
    const target = acc;
    b.onclick = () => { state.path = target; loadFiles(); };
    c.appendChild(b);
  }
}

function renderFiles(entries) {
  const list = $("fileList");
  list.innerHTML = "";
  list.className = "filelist " + (state.view === "grid" ? "grid" : "");
  entries.sort((a, b) => (a.is_dir === b.is_dir ? a.name.localeCompare(b.name) : a.is_dir ? -1 : 1));
  for (const e of entries) {
    const li = document.createElement("li");
    li.className = "file" + (state.selected === e.path ? " selected" : "");
    li.innerHTML = `
      <span class="icon">${e.is_dir ? "📁" : iconFor(e.name)}</span>
      <span class="name"></span>
      <span class="meta">${e.is_dir ? "" : fmtSize(e.size)}</span>
      <button class="row-menu">⋮</button>`;
    li.querySelector(".name").textContent = e.name;
    li.onclick = () => openEntry(e);
    li.querySelector(".row-menu").onclick = (ev) => { ev.stopPropagation(); showRowMenu(e); };
    list.appendChild(li);
  }
  if (!entries.length) {
    const li = document.createElement("li");
    li.className = "empty";
    li.textContent = "empty";
    list.appendChild(li);
  }
}

function iconFor(name) {
  const ext = name.split(".").pop().toLowerCase();
  if (ext === "pdf") return "📕";
  if (["md", "txt", "log"].includes(ext)) return "📄";
  if (["png", "jpg", "jpeg", "gif", "webp"].includes(ext)) return "🖼";
  return "📎";
}

function fmtSize(n) {
  if (n < 1024) return n + " B";
  if (n < 1048576) return (n / 1024).toFixed(1) + " KB";
  return (n / 1048576).toFixed(1) + " MB";
}

function openEntry(e) {
  if (e.is_dir) {
    state.path = e.path;
    loadFiles();
  } else {
    previewFile(e);
  }
}

async function previewFile(e) {
  state.selected = e.path;
  $("preview").classList.remove("hidden");
  $("previewTitle").textContent = e.name;
  const url = `/api/v1/files/content?path=${encodeURIComponent(e.path)}${qparam()}`;
  $("previewDownload").href = url;
  const body = $("previewBody");
  const ext = e.name.split(".").pop().toLowerCase();
  if (ext === "pdf") {
    body.innerHTML = `<iframe src="${url}" title="PDF preview"></iframe>`;
  } else if (["txt", "md", "log", "json", "csv", "yaml", "yml"].includes(ext) && e.size < 5_000_000) {
    const res = await fetch(url, { headers: { Authorization: "Bearer " + api.token } });
    const text = await res.text();
    const pre = document.createElement("pre");
    pre.textContent = text.length > 200000 ? text.slice(0, 200000) + "\n… (truncated)" : text;
    body.innerHTML = "";
    body.appendChild(pre);
  } else if (["png", "jpg", "jpeg", "gif", "webp"].includes(ext)) {
    body.innerHTML = `<img src="${url}" alt="">`;
  } else {
    body.innerHTML = `<p>No preview available for this document type.</p>
      <p><a class="btn" href="${url}" download="${e.name}">Download ${e.name}</a></p>`;
  }
  $("previewRename").onclick = () => renameEntry(e);
  $("previewMove").onclick = () => moveEntry(e);
  $("previewDelete").onclick = () => deleteEntry(e);
}

async function renameEntry(e) {
  const base = e.name.includes(".") ? e.name.slice(0, e.name.lastIndexOf(".")) : e.name;
  const ext = e.name.includes(".") ? e.name.slice(e.name.lastIndexOf(".")) : "";
  const name = prompt("New name:", e.name);
  if (!name || name === e.name) return;
  const dir = e.path.slice(0, e.path.length - e.name.length);
  await api.post("/api/v1/files/move", JSON.stringify({ from: e.path, to: dir + name, root: state.root }), { "Content-Type": "application/json" });
  toast("Renamed");
  closePreview();
  loadFiles();
}

async function moveEntry(e) {
  const dest = prompt("Move into directory (e.g. /Archive):", "/");
  if (!dest) return;
  const to = (dest.endsWith("/") ? dest : dest + "/") + e.name;
  await api.post("/api/v1/files/move", JSON.stringify({ from: e.path, to, root: state.root }), { "Content-Type": "application/json" });
  toast("Moved");
  closePreview();
  loadFiles();
}

async function deleteEntry(e) {
  if (!confirm(`Delete ${e.name}? This propagates to all peers.`)) return;
  await api.del(`/api/v1/files?path=${encodeURIComponent(e.path)}${qparam()}`);
  toast("Deleted");
  closePreview();
  loadFiles();
}

function closePreview() {
  $("preview").classList.add("hidden");
  $("previewBody").innerHTML = "";
  state.selected = null;
}

async function doSearch() {
  const q = $("searchInput").value.trim();
  if (!q) { show("filesPane"); return; }
  const type = $("typeFilter").value;
  const data = await api.get(`/api/v1/search?q=${encodeURIComponent(q)}&type=${type}&limit=20${qparam()}`);
  state.results = data.results || [];
  const box = $("results");
  box.innerHTML = "";
  for (const r of state.results) {
    const card = document.createElement("article");
    card.className = "result";
    const h = document.createElement("h3");
    h.textContent = r.path;
    h.onclick = () => openSearchResult(r);
    card.appendChild(h);
    for (const c of (r.chunks || []).slice(0, 2)) {
      const p = document.createElement("p");
      const label = c.page ? `p. ${c.page}` : (c.section || "");
      p.innerHTML = `<span class="chunk-label">${label}</span>`;
      p.appendChild(document.createTextNode(c.snippet));
      card.appendChild(p);
    }
    box.appendChild(card);
  }
  if (!state.results.length) box.innerHTML = "<p class='empty'>No matches.</p>";
  show("searchPane");
}

function openSearchResult(r) {
  const name = r.path.split("/").pop();
  previewFile({ name, path: r.path, is_dir: false, size: 0 });
}

async function uploadFiles(files) {
  const prog = $("uploadProgress");
  prog.classList.remove("hidden");
  let done = 0;
  for (const f of files) {
    try {
      await api.post(`/api/v1/files?path=${encodeURIComponent(state.path)}&name=${encodeURIComponent(f.name)}${qparam()}`, f, {
        "Content-Type": "application/octet-stream",
        "X-Locus-Filename": f.name,
      });
    } catch (e) {
      toast(`Upload failed for ${f.name}: ${e.message}`, true);
    }
    done++;
    prog.value = Math.round(100 * done / files.length);
  }
  setTimeout(() => prog.classList.add("hidden"), 600);
  toast(`Uploaded ${done} file(s)`);
  loadFiles();
}

function showRowMenu(e) {
  openEntry(e);
}

function show(pane) {
  $("filesPane").classList.toggle("hidden", pane !== "filesPane");
  $("searchPane").classList.toggle("hidden", pane !== "searchPane");
}

$("searchInput").addEventListener("keydown", (ev) => {
  if (ev.key === "Enter") doSearch().catch(e => toast(e.message, true));
});
$("refreshBtn").onclick = () => { loadFiles(); };
$("uploadBtn").onclick = () => $("uploadInput").click();
$("uploadInput").onchange = (ev) => uploadFiles([...ev.target.files]);
$("newFolderBtn").onclick = async () => {
  const name = prompt("Folder name:");
  if (!name) return;
  const path = (state.path === "/" ? "" : state.path) + "/" + name;
  await api.post("/api/v1/directories", JSON.stringify({ path, root: state.root }), { "Content-Type": "application/json" });
  toast("Folder created");
  loadFiles();
};
$("previewClose").onclick = closePreview;
$("listViewBtn").onclick = () => {
  state.view = "list";
  $("listViewBtn").classList.add("active");
  $("gridViewBtn").classList.remove("active");
  loadFiles();
};
$("gridViewBtn").onclick = () => { state.view = "grid"; $("gridViewBtn").classList.add("active"); $("listViewBtn").classList.remove("active"); loadFiles(); };

const main = $("main");
main.addEventListener("dragover", (ev) => { ev.preventDefault(); $("dropHint").classList.add("visible"); });
main.addEventListener("dragleave", () => $("dropHint").classList.remove("visible"));
main.addEventListener("drop", (ev) => {
  ev.preventDefault();
  $("dropHint").classList.remove("visible");
  if (ev.dataTransfer.files.length) uploadFiles([...ev.dataTransfer.files]);
});

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("/sw.js").catch(() => {});
}
boot();
