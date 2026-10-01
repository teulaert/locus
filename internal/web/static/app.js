// Locus PWA client. All operations go through the versioned API and modify
// the real filesystem on the connected peer; nothing is mirrored into browser
// storage beyond session settings.
//
// File names, paths and extracted headings come from other peers and from
// arbitrary documents, so they are untrusted: render them with textContent /
// setAttribute, never by interpolating them into innerHTML.

// The API token lives in sessionStorage so it does not outlive the tab. Drop
// any copy an older build left in localStorage.
try { localStorage.removeItem("locus.token"); } catch {}
const api = {
  token: sessionStorage.getItem("locus.token") || "",
  // raw returns the Response untouched instead of decoding JSON bodies.
  async req(method, url, body, headers = {}, raw = false) {
    const h = { Authorization: "Bearer " + api.token, ...headers };
    const res = await fetch(url, { method, headers: h, body });
    if (res.status === 401) {
      const t = prompt("Locus API token:");
      if (t) {
        sessionStorage.setItem("locus.token", t);
        api.token = t;
        return api.req(method, url, body, headers, raw);
      }
      throw new Error("unauthorized");
    }
    if (!res.ok) {
      let msg = res.statusText;
      try { msg = (await res.json()).error || msg; } catch {}
      throw new Error(msg);
    }
    if (raw) return res;
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

// apiURL builds an API URL with every query parameter encoded by
// URLSearchParams. The selected root is added only when one is set, so an
// unset root never reaches the server as the literal string "null".
function apiURL(path, params = {}) {
  const q = new URLSearchParams(params);
  if (state.root) q.set("root", state.root);
  return `${path}?${q}`;
}

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
  const data = await api.get(apiURL("/api/v1/files", { path: state.path }));
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

// File contents sit behind the bearer token like every other API route, and
// a browser navigation (<iframe src>, <img src>, <a href>) never carries an
// Authorization header. So contents are always fetched through api.req and
// handed to the page as a blob: URL; the API URL itself is never put into a
// src or href attribute.
function contentURL(path) {
  return apiURL("/api/v1/files/content", { path });
}

async function fetchContent(path) {
  const res = await api.req("GET", contentURL(path), undefined, {}, true);
  return res.blob();
}

// Object URLs created for the open preview; revoked when it changes or closes.
let previewObjectURLs = [];

function objectURLFor(blob) {
  const objURL = URL.createObjectURL(blob);
  previewObjectURLs.push(objURL);
  return objURL;
}

function revokePreviewURLs() {
  for (const u of previewObjectURLs) URL.revokeObjectURL(u);
  previewObjectURLs = [];
}

async function downloadFile(e) {
  try {
    const blob = await fetchContent(e.path);
    const objURL = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = objURL;
    a.download = e.name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(objURL), 60000);
  } catch (err) {
    toast(`Download failed for ${e.name}: ${err.message}`, true);
  }
}

async function previewFile(e) {
  state.selected = e.path;
  revokePreviewURLs();
  $("preview").classList.remove("hidden");
  $("previewTitle").textContent = e.name;
  $("previewDownload").onclick = () => downloadFile(e);
  $("previewRename").onclick = () => renameEntry(e);
  $("previewMove").onclick = () => moveEntry(e);
  $("previewDelete").onclick = () => deleteEntry(e);
  const body = $("previewBody");
  const ext = e.name.split(".").pop().toLowerCase();
  body.innerHTML = "";
  // A newer preview may have replaced this one while the bytes were loading.
  const load = async () => {
    const blob = await fetchContent(e.path);
    if (state.selected !== e.path) throw new Error("superseded");
    return blob;
  };
  try {
    if (ext === "pdf") {
      const blob = await load();
      const frame = document.createElement("iframe");
      frame.title = "PDF preview";
      frame.src = objectURLFor(blob);
      body.appendChild(frame);
    } else if (["txt", "md", "log", "json", "csv", "yaml", "yml"].includes(ext) && e.size < 5_000_000) {
      const text = await (await load()).text();
      const pre = document.createElement("pre");
      pre.textContent = text.length > 200000 ? text.slice(0, 200000) + "\n… (truncated)" : text;
      body.appendChild(pre);
    } else if (["png", "jpg", "jpeg", "gif", "webp"].includes(ext)) {
      const blob = await load();
      const img = document.createElement("img");
      img.alt = "";
      img.src = objectURLFor(blob);
      body.appendChild(img);
    } else {
      const note = document.createElement("p");
      note.textContent = "No preview available for this document type.";
      const btn = document.createElement("button");
      btn.className = "btn";
      btn.textContent = "Download " + e.name;
      btn.onclick = () => downloadFile(e);
      const wrap = document.createElement("p");
      wrap.appendChild(btn);
      body.append(note, wrap);
    }
  } catch (err) {
    if (state.selected !== e.path) return;
    body.innerHTML = "";
    const p = document.createElement("p");
    p.className = "empty";
    p.textContent = "Preview failed: " + err.message;
    body.appendChild(p);
  }
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
  await api.del(apiURL("/api/v1/files", { path: e.path }));
  toast("Deleted");
  closePreview();
  loadFiles();
}

function closePreview() {
  revokePreviewURLs();
  $("preview").classList.add("hidden");
  $("previewBody").innerHTML = "";
  state.selected = null;
}

async function doSearch() {
  const q = $("searchInput").value.trim();
  if (!q) { show("filesPane"); return; }
  const type = $("typeFilter").value;
  const data = await api.get(apiURL("/api/v1/search", { q, type, limit: 20 }));
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
      const span = document.createElement("span");
      span.className = "chunk-label";
      span.textContent = label;
      p.appendChild(span);
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
  let done = 0, ok = 0;
  for (const f of files) {
    try {
      await api.post(apiURL("/api/v1/files", { path: state.path, name: f.name }), f, {
        "Content-Type": "application/octet-stream",
        "X-Locus-Filename": f.name,
      });
      ok++;
    } catch (e) {
      toast(`Upload failed for ${f.name}: ${e.message}`, true);
    }
    done++;
    prog.value = Math.round(100 * done / files.length);
  }
  setTimeout(() => prog.classList.add("hidden"), 600);
  const failed = files.length - ok;
  if (failed) toast(`Uploaded ${ok} of ${files.length} file(s); ${failed} failed`, true);
  else toast(`Uploaded ${ok} file(s)`);
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
