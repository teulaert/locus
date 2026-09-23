// Locus service worker: caches only the UI shell. Document contents are
// always fetched live; nothing is mirrored into browser storage.
const SHELL = ["/", "/app.js", "/style.css", "/manifest.webmanifest"];
const CACHE = "locus-shell-v1";

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)));
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
  );
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  const isShell = SHELL.includes(url.pathname) || url.pathname === "/";
  if (isShell && event.request.method === "GET") {
    event.respondWith(
      fetch(event.request)
        .then((res) => {
          const copy = res.clone();
          caches.open(CACHE).then((c) => c.put(event.request, copy));
          return res;
        })
        .catch(() => caches.match(event.request))
    );
    return;
  }
  // Everything else (files, search, API) is never cached.
  event.respondWith(fetch(event.request));
});
