// Package gitremote contains helpers for talking to upstream git hosts.
//
// Use [Client] to probe or forward requests to an upstream repo using the
// [httpclient] package (SSRF-safe HTTP). When using the git CLI to fetch from a
// resolved URL, pass [Repo.GitFlags] to prevent unvalidated redirects.
package gitremote

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/http/cachecontrol"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/jonboulle/clockwork"
)

const (
	// maxRedirects is the maximum number of upstream redirects to follow.
	maxRedirects            = 10
	maxAuthorizationEntries = 100_000
	maxRedirectCacheEntries = 100_000
)

// HTTPError represents an HTTP error returned by an upstream git server.
type HTTPError struct {
	// StatusCode contains the upstream status code.
	StatusCode int
	// Header contains a subset of HTTP headers from the upstream server that
	// should be returned to clients.
	Header http.Header
}

// Error returns the upstream HTTP status as an error message.
func (e *HTTPError) Error() string {
	return fmt.Sprintf("upstream returned HTTP %d", e.StatusCode)
}

type redirectCacheEntry struct {
	repoURL   string
	expiresAt time.Time
}

// redirectTrackerContextKey carries redirect state from a resolution probe into
// the HTTP client's redirect callback.
type redirectTrackerContextKey struct{}

// redirectTracker records cacheability and the earliest expiration across all
// redirects followed by a single resolution probe.
type redirectTracker struct {
	clock      clockwork.Clock
	maxTTL     time.Duration
	expiresAt  time.Time
	cacheable  bool
	redirected bool
}

// observe records the earliest expiration across a redirect chain. If any hop
// cannot be reused without validation, the entire chain becomes uncacheable.
func (t *redirectTracker) observe(resp *http.Response) {
	t.redirected = true
	if !t.cacheable {
		return
	}
	now := t.clock.Now()
	freshness, specified, err := cachecontrol.FreshnessTTL(resp, now)
	permanent := resp.StatusCode == http.StatusMovedPermanently ||
		resp.StatusCode == http.StatusPermanentRedirect
	if err == nil && !specified && permanent {
		// A permanent redirect permits heuristic freshness, which begins when
		// the upstream generated the response rather than when we received it.
		age, ageErr := cachecontrol.ResponseAge(resp, now)
		if ageErr != nil {
			err = ageErr
		} else {
			freshness = max(t.maxTTL-age, 0)
			specified = true
		}
	}
	if err != nil || !specified || freshness <= 0 || t.maxTTL <= 0 {
		t.cacheable = false
		t.expiresAt = time.Time{}
		return
	}
	expiresAt := now.Add(min(freshness, t.maxTTL))
	if t.expiresAt.IsZero() || expiresAt.Before(t.expiresAt) {
		t.expiresAt = expiresAt
	}
}

// ClientOptions configures a Client.
type ClientOptions struct {
	// Clock controls cache expiration.
	Clock clockwork.Clock
	// AuthorizationCacheTTL controls how long successful credential checks may
	// be reused without contacting the upstream repository.
	AuthorizationCacheTTL time.Duration
	// RedirectCacheTTL limits how long resolved redirect targets are cached.
	RedirectCacheTTL time.Duration
	// AllowedPrivateIPNets may be contacted by the client.
	AllowedPrivateIPNets []*net.IPNet
	// InsecureHTTPHosts may be contacted without TLS.
	InsecureHTTPHosts []string
}

// Client validates and forwards requests to upstream repositories.
type Client struct {
	client                *http.Client
	clock                 clockwork.Clock
	allowedPrivateIPNets  []*net.IPNet
	insecureHTTPHosts     map[string]struct{}
	authorizationCache    lru.LRU[struct{}]
	authorizationCacheTTL time.Duration
	redirectCache         lru.LRU[redirectCacheEntry]
	redirectCacheTTL      time.Duration
}

// NewClient creates a client using opts.
func NewClient(opts ClientOptions) (*Client, error) {
	clock := opts.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}
	c := httpclient.New(opts.AllowedPrivateIPNets, "gitremote")
	authorizationCache, err := lru.New(&lru.Config[struct{}]{
		Name:       "gitmirror_authorization",
		Clock:      clock,
		TTL:        opts.AuthorizationCacheTTL,
		MaxSize:    maxAuthorizationEntries,
		SizeFn:     func(struct{}) int64 { return 1 },
		ThreadSafe: true,
	})
	if err != nil {
		return nil, fmt.Errorf("create authorization cache: %w", err)
	}
	redirectCache, err := lru.New(&lru.Config[redirectCacheEntry]{
		Name:          "gitmirror_redirect",
		Clock:         clock,
		TTL:           opts.RedirectCacheTTL,
		MaxSize:       maxRedirectCacheEntries,
		SizeFn:        func(redirectCacheEntry) int64 { return 1 },
		ThreadSafe:    true,
		UpdateInPlace: true,
	})
	if err != nil {
		return nil, fmt.Errorf("create redirect cache: %w", err)
	}
	hosts := make(map[string]struct{}, len(opts.InsecureHTTPHosts))
	for _, host := range opts.InsecureHTTPHosts {
		hosts[host] = struct{}{}
	}
	c.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if req.URL.Scheme == "http" {
			if _, ok := hosts[req.URL.Host]; !ok {
				return fmt.Errorf("HTTP redirect host %q is not allowlisted", req.URL.Host)
			}
		}
		if err := checkRedirect(req, via); err != nil {
			return err
		}
		if tracker, ok := req.Context().Value(redirectTrackerContextKey{}).(*redirectTracker); ok && req.Response != nil {
			tracker.observe(req.Response)
		}
		return nil
	}
	return &Client{
		client:                c,
		clock:                 clock,
		allowedPrivateIPNets:  opts.AllowedPrivateIPNets,
		insecureHTTPHosts:     hosts,
		authorizationCache:    authorizationCache,
		authorizationCacheTTL: opts.AuthorizationCacheTTL,
		redirectCache:         redirectCache,
		redirectCacheTTL:      opts.RedirectCacheTTL,
	}, nil
}

func (r *Client) upstreamURL(hostPath string) (*url.URL, error) {
	authority, path, _ := strings.Cut(hostPath, "/")
	scheme := "https"
	if _, ok := r.insecureHTTPHosts[authority]; ok {
		scheme = "http"
	}
	u, err := url.Parse(scheme + "://" + authority)
	if err != nil {
		return nil, fmt.Errorf("parse upstream authority: %w", err)
	}
	if u.Hostname() == "" || u.User != nil {
		return nil, fmt.Errorf("invalid upstream authority %q", authority)
	}
	if path != "" {
		u.Path = "/" + path
	}
	return u, nil
}

// Resolve validates credentials and redirects for repoHostPath using HTTPS, or
// HTTP when its authority is explicitly allowlisted. Fresh successful checks
// may be reused without contacting the upstream repository.
//
// Note: Go strips the Authorization header on cross-host redirects, so a
// repo that redirects to a different host will fail auth here.
// Git's libcurl transport applies the same restriction by default.
func (r *Client) Resolve(ctx context.Context, repoHostPath, authorization string) (*Repo, error) {
	requestedURL, err := r.upstreamURL(repoHostPath)
	if err != nil {
		return nil, err
	}
	repoURL := requestedURL.String()
	// Cache resolved targets by requested URL and Authorization value because an
	// upstream may redirect different users to different URLs.
	cacheKey := redirectCacheKey(repoURL, authorization)
	resolveURL := repoURL
	cachedEntry, cacheHit := r.redirectCache.Get(cacheKey)
	// A cached target skips known redirect hops, but resolve still probes it to
	// validate the credentials and endpoint.
	if cacheHit {
		if !r.clock.Now().Before(cachedEntry.expiresAt) {
			r.redirectCache.Remove(cacheKey)
			cacheHit = false
		} else {
			resolveURL = cachedEntry.repoURL
		}
	}
	candidateRepo, err := RestoreRepo(resolveURL)
	if err != nil {
		return nil, fmt.Errorf("normalize repo URL: %w", err)
	}
	// A redirect target identifies the candidate repository, but only a fresh
	// authorization entry permits returning it without another upstream probe.
	if r.authorizationCacheTTL > 0 {
		key := authorizationCacheKey(candidateRepo.String(), authorization)
		if _, ok := r.authorizationCache.Get(key); ok {
			return r.newResolvedRepo(ctx, candidateRepo.URL())
		}
	}
	finalURL, expiresAt, err := r.resolve(ctx, resolveURL, authorization)
	// If a cached target is stale, retry from the original URL so the upstream
	// can provide its current redirect target.
	if err != nil && cacheHit {
		r.redirectCache.Remove(cacheKey)
		cacheHit = false
		finalURL, expiresAt, err = r.resolve(ctx, repoURL, authorization)
	}
	if err != nil {
		return nil, err
	}
	repo, err := r.newResolvedRepo(ctx, finalURL)
	if err != nil {
		return nil, fmt.Errorf("resolve upstream repo: %w", err)
	}
	// Cache only successful checks, keyed by the final repository identity and
	// supplied credentials so redirects and credentials cannot alias entries.
	if r.authorizationCacheTTL > 0 {
		key := authorizationCacheKey(repo.String(), authorization)
		r.authorizationCache.Add(key, struct{}{})
	}
	if !expiresAt.IsZero() {
		if cacheHit && cachedEntry.expiresAt.Before(expiresAt) {
			expiresAt = cachedEntry.expiresAt
		}
		r.redirectCache.Add(cacheKey, redirectCacheEntry{repoURL: repo.String(), expiresAt: expiresAt})
	}
	return repo, nil
}

func (r *Client) newResolvedRepo(ctx context.Context, repoURL *url.URL) (*Repo, error) {
	repo, err := newRepo(repoURL)
	if err != nil {
		return nil, err
	}
	resolvedIPs, err := httpclient.ResolveHostIPs(
		ctx, repo.normalizedURL.Hostname(), r.allowedPrivateIPNets,
	)
	if err != nil {
		return nil, err
	}
	repo.resolvedIPs = resolvedIPs
	return repo, nil
}

// Forward sends req to upstreamHostPath using the client's transport and
// redirect policy. The caller must close the returned response body.
func (r *Client) Forward(req *http.Request, upstreamHostPath string) (*http.Response, error) {
	authority, escapedPath, _ := strings.Cut(upstreamHostPath, "/")
	u, err := r.upstreamURL(authority)
	if err != nil {
		return nil, err
	}
	path, err := url.PathUnescape(escapedPath)
	if err != nil {
		return nil, fmt.Errorf("unescape upstream path: %w", err)
	}
	if escapedPath != "" {
		u.Path = "/" + path
		u.RawPath = "/" + escapedPath
	}
	u.RawQuery = req.URL.RawQuery
	outbound := req.Clone(req.Context())
	outbound.URL = u
	outbound.Host = u.Host
	outbound.RequestURI = ""
	resp, err := r.client.Do(outbound)
	if err != nil {
		return nil, fmt.Errorf("forward upstream request: %w", err)
	}
	return resp, nil
}

func (r *Client) resolve(ctx context.Context, repoURL, authorization string) (*url.URL, time.Time, error) {
	u, err := url.Parse(repoURL)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("parse repo URL: %w", err)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if _, ok := r.insecureHTTPHosts[u.Host]; !ok {
			return nil, time.Time{}, fmt.Errorf("HTTP host %q is not allowlisted", u.Host)
		}
	default:
		return nil, time.Time{}, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	probeURL := *u
	probeURL.Path += "/info/refs"
	probeURL.RawQuery = "service=git-upload-pack"
	tracker := &redirectTracker{
		clock:     r.clock,
		maxTTL:    r.redirectCacheTTL,
		cacheable: true,
	}
	ctx = context.WithValue(ctx, redirectTrackerContextKey{}, tracker)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL.String(), nil)
	if err != nil {
		return nil, time.Time{}, err
	}
	// Protocol v2 (supported by all major providers) omits refs from the
	// initial response, reducing probe overhead. Older servers that don't
	// support v2 will ignore the header and return a normal v0 ref
	// advertisement.
	req.Header.Set("Git-Protocol", "version=2")
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("probe upstream: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, time.Time{}, &HTTPError{
			StatusCode: resp.StatusCode,
			Header: http.Header{
				// Preserve the auth challenge so Git can retry using its credential
				// helper.
				"WWW-Authenticate": resp.Header.Values("WWW-Authenticate"),
				// Preserve upstream retry timing for throttled or unavailable
				// requests.
				"Retry-After": resp.Header.Values("Retry-After"),
			},
		}
	}
	// resp.Request.URL is the final URL after redirects.
	final := *resp.Request.URL
	final.Path = strings.TrimSuffix(final.Path, "/info/refs")
	final.RawQuery = ""
	if !tracker.redirected || !tracker.cacheable {
		return &final, time.Time{}, nil
	}
	return &final, tracker.expiresAt, nil
}

func normalizeURL(repoURL *url.URL) (*url.URL, error) {
	normalized := *repoURL
	normalized.Scheme = strings.ToLower(normalized.Scheme)
	defaultPort := ""
	switch normalized.Scheme {
	case "https":
		defaultPort = "443"
	case "http":
		defaultPort = "80"
	default:
		return nil, fmt.Errorf("unsupported repo URL scheme %q", normalized.Scheme)
	}
	if normalized.Hostname() == "" {
		return nil, errors.New("missing repo URL host")
	}
	port := normalized.Port()
	if port == "" {
		port = defaultPort
	}
	normalized.Host = net.JoinHostPort(strings.ToLower(normalized.Hostname()), port)
	normalized.User = nil
	normalized.RawQuery = ""
	normalized.ForceQuery = false
	normalized.Fragment = ""
	normalized.RawFragment = ""
	normalized.RawPath = ""
	return &normalized, nil
}

func redirectCacheKey(repoURL, authorization string) string {
	digest := sha256.Sum256([]byte(repoURL + "\x00" + authorization))
	return fmt.Sprintf("%x", digest)
}

// authorizationCacheKey avoids retaining raw credentials in the cache key.
func authorizationCacheKey(repoURL, authorization string) string {
	credentialDigest := sha256.Sum256([]byte(authorization))
	key := repoURL + "\x00" + string(credentialDigest[:])
	digest := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", digest)
}

func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", len(via))
	}
	// A downgrade to http would send creds in cleartext.
	// TODO: allow downgrade for insecure_http_hosts?
	if req.URL.Scheme != "https" && via[0].URL.Scheme == "https" {
		return fmt.Errorf("refusing redirect from https to %s", req.URL.Scheme)
	}
	return nil
}

// Repo is an upstream repository after resolving and validating redirects.
type Repo struct {
	normalizedURL *url.URL
	resolvedIPs   []net.IP
}

func newRepo(repoURL *url.URL) (*Repo, error) {
	normalizedURL, err := normalizeURL(repoURL)
	if err != nil {
		return nil, err
	}
	return &Repo{normalizedURL: normalizedURL}, nil
}

// RestoreRepo reconstructs a repo from a previously resolved URL stored on
// disk. It does not perform network resolution.
func RestoreRepo(repoURL string) (*Repo, error) {
	parsedURL, err := url.Parse(repoURL)
	if err != nil {
		return nil, fmt.Errorf("parse repo URL: %w", err)
	}
	// XXX: re-normalize and verify it's the same?
	return newRepo(parsedURL)
}

// URL returns a copy of the resolved, normalized repository URL.
func (r *Repo) URL() *url.URL {
	u := *r.normalizedURL
	return &u
}

// String returns the resolved, normalized repository URL.
func (r *Repo) String() string {
	return r.normalizedURL.String()
}

// GitFlags returns `git -c` options that keep the git CLI within the validated
// network boundary.
func (r *Repo) GitFlags() ([]string, error) {
	flags := []string{
		"-c", "http.followRedirects=false",
		"-c", "http.curloptResolve=",
	}
	host := r.normalizedURL.Hostname()
	if net.ParseIP(host) != nil {
		return flags, nil
	}
	if len(r.resolvedIPs) == 0 {
		return nil, errors.New("repo URL has no validated IP addresses")
	}
	addresses := make([]string, 0, len(r.resolvedIPs))
	for _, ip := range r.resolvedIPs {
		address := ip.String()
		if ip.To4() == nil {
			address = "[" + address + "]"
		}
		addresses = append(addresses, address)
	}
	resolution := host + ":" + r.normalizedURL.Port() + ":" + strings.Join(addresses, ",")
	return append(flags, "-c", "http.curloptResolve="+resolution), nil
}
