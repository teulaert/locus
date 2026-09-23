// Package web embeds the production PWA build served by Locus. Running
// Node.js is not required on a deployed peer.
package web

import _ "embed"

//go:embed static/index.html
var indexHTML []byte

//go:embed static/app.js
var appJS []byte

//go:embed static/style.css
var stylesheet []byte

//go:embed static/manifest.webmanifest
var manifest []byte

//go:embed static/sw.js
var serviceWorker []byte

// Index returns the application shell.
func Index() []byte { return indexHTML }

// AppJS returns the client application.
func AppJS() []byte { return appJS }

// Stylesheet returns the UI stylesheet.
func Stylesheet() []byte { return stylesheet }

// Manifest returns the web app manifest.
func Manifest() []byte { return manifest }

// ServiceWorker returns the offline shell cache. Document contents are never
// cached here: only the UI shell.
func ServiceWorker() []byte { return serviceWorker }
