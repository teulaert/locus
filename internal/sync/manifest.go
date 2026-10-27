package sync

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	gosync "sync"
	"time"

	"github.com/teulaert/locus/internal/peer"
)

// A manifest lists every document of a root, tombstones included, and every
// peer fetches it from every other peer at each interval. Unchanged, it is
// answered 304 against the ETag the fetcher kept from last time; changed, it
// is sent gzipped to a fetcher that asks for that. Builds that predate both
// send neither header and get the plain manifest as before.

// followerRefresh is how long a peer whose pair is led by the other side
// leaves the interval rounds with it to that side: after it, the follower
// runs one of its own, so what only its own rounds record (the peer round
// that tombstone pruning waits for) is not held back for long.
const followerRefresh = 5 * time.Minute

// manifestCache holds what the manifest exchange remembers between rounds.
type manifestCache struct {
	mu gosync.Mutex
	// served is the last manifest this peer served per root, with its
	// gzipped form, so an unchanged manifest is compressed only once.
	served map[string]servedManifest
	// fetched is the last manifest fetched per peer and root, answered
	// again when the peer says it has not changed.
	fetched map[string]fetchedManifest
	// askedBy is when each peer last fetched one of our manifests.
	askedBy map[string]time.Time
	// ranWith is when this peer last ran a clean round with each peer,
	// with the tags of its own manifests when that round began.
	ranWith map[string]ranRound
}

// ranRound is the last clean round with a peer: when it began, the tags of
// this peer's manifests then, and when the first clean round with those
// same tags began.
type ranRound struct {
	at, since time.Time
	tags      string
}

// settled reports whether a clean round began in a later second than the
// first that held these tags: the peer round it recorded then postdates
// every record in them, tombstones included, as pruning requires (it
// compares whole seconds, see store.PruneTombstones).
func (r ranRound) settled() bool { return r.at.Unix() > r.since.Unix() }

type servedManifest struct {
	etag string
	gz   []byte
}

type fetchedManifest struct {
	etag string
	m    *Manifest
}

func (c *manifestCache) init() {
	if c.served == nil {
		c.served = make(map[string]servedManifest)
		c.fetched = make(map[string]fetchedManifest)
		c.askedBy = make(map[string]time.Time)
		c.ranWith = make(map[string]ranRound)
	}
}

func (e *Engine) clock() time.Time {
	if e.now != nil {
		return e.now()
	}
	return time.Now()
}

// localManifest returns the manifest this peer serves for root, encoded,
// with its ETag.
func (e *Engine) localManifest(root string) (body []byte, etag string, err error) {
	entries, err := e.LocalManifest(root)
	if err != nil {
		return nil, "", err
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(Manifest{PeerID: e.Identity.PeerID, Root: root, RootName: e.NameOf(root), Entries: entries, SkipsHidden: !e.SyncsHidden(root)}); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), `"` + hex.EncodeToString(sum[:16]) + `"`, nil
}

// localTags returns the ETags of every root's manifest, joined; it changes
// whenever a record of any root does.
func (e *Engine) localTags() (string, error) {
	var tags strings.Builder
	for _, root := range e.Roots {
		_, etag, err := e.localManifest(root)
		if err != nil {
			return "", err
		}
		tags.WriteString(etag)
	}
	return tags.String(), nil
}

// serveManifest answers a peer's manifest request for root.
func (e *Engine) serveManifest(w http.ResponseWriter, r *http.Request, root, peerID string) {
	body, etag, err := e.localManifest(root)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	c := &e.manifests
	c.mu.Lock()
	c.init()
	c.askedBy[peerID] = e.clock()
	cached := c.served[root]
	c.mu.Unlock()

	w.Header().Set("ETag", etag)
	w.Header().Set("Vary", "Accept-Encoding")
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if !acceptsGzip(r.Header) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}
	gz := cached.gz
	if cached.etag != etag {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write(body)
		_ = zw.Close()
		gz = buf.Bytes()
		c.mu.Lock()
		c.served[root] = servedManifest{etag: etag, gz: gz}
		c.mu.Unlock()
	}
	w.Header().Set("Content-Encoding", "gzip")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(gz)
}

func acceptsGzip(h http.Header) bool {
	for _, v := range h.Values("Accept-Encoding") {
		for _, enc := range strings.Split(v, ",") {
			if name, _, _ := strings.Cut(strings.TrimSpace(enc), ";"); strings.EqualFold(name, "gzip") {
				return true
			}
		}
	}
	return false
}

// FetchManifest requests a peer's manifest for a root. A manifest that does
// not decode or validate is a *ManifestError.
func (e *Engine) FetchManifest(ctx context.Context, base, root string) (*Manifest, error) {
	return e.fetchManifest(ctx, base, "", root)
}

// fetchManifest is FetchManifest that, given the peer's ID, sends the ETag
// of the manifest last fetched from it and reuses that manifest when the
// peer answers it has not changed. The manifest returned is shared: callers
// must not modify it.
func (e *Engine) fetchManifest(ctx context.Context, base, peerID, root string) (*Manifest, error) {
	q := url.Values{"rootname": []string{e.NameOf(root)}}
	target := Target(base, "/v1/manifest", q)
	key := peerID + "\x00" + root
	c := &e.manifests
	c.mu.Lock()
	c.init()
	prev := c.fetched[key]
	c.mu.Unlock()
	extra := http.Header{"Accept-Encoding": {"gzip"}}
	if peerID != "" && prev.etag != "" {
		extra.Set("If-None-Match", prev.etag)
	}
	resp, err := e.sendWith(ctx, http.MethodGet, base, target, nil, emptyBodyHash, extra)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotModified && prev.m != nil {
		resp.Body.Close()
		return prev.m, nil
	}
	if resp.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			resp.Body.Close()
			return nil, &TransportError{Err: err}
		}
		resp.Body = struct {
			io.Reader
			io.Closer
		}{zr, resp.Body}
	}
	body, err := e.readBody(ctx, resp)
	if err != nil {
		return nil, err
	}
	m, err := DecodeManifest(body, e.NameOf(root))
	if err != nil {
		return nil, err
	}
	if etag := resp.Header.Get("ETag"); peerID != "" && etag != "" {
		c.mu.Lock()
		c.fetched[key] = fetchedManifest{etag: etag, m: m}
		c.mu.Unlock()
	}
	return m, nil
}

// noteRanWith records a clean round with peerID that began at at, after
// this peer's manifests were found to have tags.
func (e *Engine) noteRanWith(peerID string, at time.Time, tags string) {
	c := &e.manifests
	c.mu.Lock()
	c.init()
	r := ranRound{at: at, since: at, tags: tags}
	if prev := c.ranWith[peerID]; prev.tags == tags {
		r.since = prev.since
	}
	c.ranWith[peerID] = r
	c.mu.Unlock()
}

// Initiating returns the peers of an interval round this peer should start
// one with. A round with a peer pushes and pulls both ways, so one side of
// each pair running it is enough: the peer with the lower ID leads. The
// other leaves the rounds to it while nothing of its own is new to the
// pair: the leader fetches its manifests (within FollowWindow), and its
// last clean round with the leader began within followerRefresh with the
// manifests it has now and is settled. A change of its own, tombstones
// included, is followed by rounds of its own until one is settled, so the
// peer round that tombstone pruning waits for is recorded as promptly as
// before. A leader that stops coming,
// being down or unable to reach this peer, is followed no more. With
// FollowWindow zero every peer is initiated.
func (e *Engine) Initiating(peers []*peer.Entry) []*peer.Entry {
	if e.FollowWindow <= 0 {
		return peers
	}
	now := e.clock()
	c := &e.manifests
	c.mu.Lock()
	c.init()
	var candidates []bool
	some := false
	for _, p := range peers {
		ran := c.ranWith[p.PeerID]
		led := p.PeerID < e.Identity.PeerID &&
			now.Sub(c.askedBy[p.PeerID]) < e.FollowWindow &&
			now.Sub(ran.at) < followerRefresh
		candidates = append(candidates, led)
		some = some || led
	}
	c.mu.Unlock()
	if !some {
		return peers
	}
	tags, err := e.localTags()
	if err != nil {
		return peers
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := peers[:0:0]
	for i, p := range peers {
		if r := c.ranWith[p.PeerID]; !candidates[i] || r.tags != tags || !r.settled() {
			out = append(out, p)
		}
	}
	return out
}
