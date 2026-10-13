package update

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/teulaert/locus/internal/netguard"
)

// DefaultAPI is the GitHub API base releases are read from; update.url in
// the configuration replaces it (a mirror, or a test server).
const DefaultAPI = "https://api.github.com"

// Repo is the repository whose releases are installed.
const Repo = "teulaert/locus"

// Release is the part of a GitHub release the updater reads.
type Release struct {
	TagName     string    `json:"tag_name"`
	Name        string    `json:"name"`
	Draft       bool      `json:"draft"`
	Prerelease  bool      `json:"prerelease"`
	CreatedAt   time.Time `json:"created_at"`
	PublishedAt time.Time `json:"published_at"`
	Assets      []Asset   `json:"assets"`
}

// Asset is a file attached to a release.
type Asset struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	URL  string `json:"browser_download_url"`
}

// asset returns the release's asset called name.
func (r *Release) asset(name string) (*Asset, bool) {
	for i := range r.Assets {
		if r.Assets[i].Name == name {
			return &r.Assets[i], true
		}
	}
	return nil, false
}

// RateLimitError is GitHub refusing a request for exceeding a rate limit.
// Until is when it may be retried.
type RateLimitError struct {
	Status int
	Until  time.Time
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("GitHub rate limit (HTTP %d); retrying after %s", e.Status, e.Until.Format(time.RFC3339))
}

// Client reads releases from the GitHub API. It remembers the last answer
// and its ETag, so a poll that finds nothing new is a 304, which GitHub does
// not count against the 60 requests an hour it allows unauthenticated
// clients.
type Client struct {
	Base      string
	UserAgent string
	HTTP      *http.Client
	// Now is the clock rate-limit deadlines are computed from.
	Now func() time.Time

	mu     sync.Mutex
	etag   string
	cached []Release
}

// NewClient returns a client for base (DefaultAPI when empty).
func NewClient(base, userAgent string) *Client {
	if base == "" {
		base = DefaultAPI
	}
	return &Client{Base: strings.TrimRight(base, "/"), UserAgent: userAgent, HTTP: guardedHTTPClient(), Now: time.Now}
}

// guardedHTTPClient is an HTTP client that, under the test network guard
// (LOC-80), dials loopback test servers only: a test run can never reach
// GitHub, whatever a configuration says.
func guardedHTTPClient() *http.Client {
	d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Client{Transport: &http.Transport{
		Proxy: func(r *http.Request) (*url.URL, error) {
			// No proxy under the guard: a loopback proxy would carry a
			// test's requests to GitHub.
			if netguard.Active() {
				return nil, nil
			}
			return http.ProxyFromEnvironment(r)
		},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if err := netguard.CheckDial(addr); err != nil {
				return nil, err
			}
			return d.DialContext(ctx, network, addr)
		},
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: time.Minute,
		IdleConnTimeout:       90 * time.Second,
	}}
}

func (c *Client) newRequest(ctx context.Context, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	return req, nil
}

// Releases lists the newest releases, newest first.
func (c *Client) Releases(ctx context.Context) ([]Release, error) {
	req, err := c.newRequest(ctx, c.Base+"/repos/"+Repo+"/releases?per_page=30")
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	c.mu.Lock()
	if c.etag != "" && c.cached != nil {
		req.Header.Set("If-None-Match", c.etag)
	}
	c.mu.Unlock()
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotModified:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.cached, nil
	case resp.StatusCode == http.StatusOK:
	default:
		if rl := c.rateLimit(resp); rl != nil {
			return nil, rl
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("list releases: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var rels []Release
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&rels); err != nil {
		return nil, fmt.Errorf("list releases: %w", err)
	}
	sort.SliceStable(rels, func(i, j int) bool { return rels[i].CreatedAt.After(rels[j].CreatedAt) })
	c.mu.Lock()
	c.etag, c.cached = resp.Header.Get("ETag"), rels
	c.mu.Unlock()
	return rels, nil
}

// rateLimit interprets a refusal as a rate limit when GitHub says it is one:
// a Retry-After header (secondary limits), or a 403/429 with no requests
// remaining (X-RateLimit-Reset is when the window resets). 429 without
// either waits a minute.
func (c *Client) rateLimit(resp *http.Response) *RateLimitError {
	if resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusTooManyRequests {
		return nil
	}
	now := c.Now()
	if s := resp.Header.Get("Retry-After"); s != "" {
		if secs, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && secs >= 0 {
			return &RateLimitError{Status: resp.StatusCode, Until: now.Add(time.Duration(secs) * time.Second)}
		}
		if t, err := http.ParseTime(s); err == nil {
			return &RateLimitError{Status: resp.StatusCode, Until: t}
		}
	}
	if resp.Header.Get("X-RateLimit-Remaining") == "0" {
		until := now.Add(time.Hour)
		if reset, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil {
			until = time.Unix(reset, 0)
		}
		return &RateLimitError{Status: resp.StatusCode, Until: until}
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return &RateLimitError{Status: resp.StatusCode, Until: now.Add(time.Minute)}
	}
	return nil
}

// errTooLarge is a download that exceeded its size limit.
var errTooLarge = errors.New("download exceeds its expected size")

// Download writes the asset at url to w, refusing more than max bytes, and
// returns how many it wrote.
func (c *Client) Download(ctx context.Context, url string, max int64, w io.Writer) (int64, error) {
	req, err := c.newRequest(ctx, url)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/octet-stream")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if rl := c.rateLimit(resp); rl != nil {
			return 0, rl
		}
		return 0, fmt.Errorf("download %s: %s", url, resp.Status)
	}
	n, err := io.Copy(w, io.LimitReader(resp.Body, max+1))
	if err != nil {
		return n, fmt.Errorf("download %s: %w", url, err)
	}
	if n > max {
		return n, fmt.Errorf("download %s: %w (%d bytes)", url, errTooLarge, max)
	}
	return n, nil
}
