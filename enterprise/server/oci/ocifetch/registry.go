package ocifetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
)

const (
	pullerLRUMaxEntries = 1000

	// public.ecr.aws rejects all blob HEAD requests with 401; see blobHeadFallbackRegistries.
	ecrPublicRegistry = "public.ecr.aws"
	// A single-byte range keeps the blob HEAD fallback GET cheap while still exposing
	// the full blob size in the response's Content-Range header.
	blobHeadFallbackRange = "bytes=0-0"
	// How much of a fallback response body to drain for connection reuse before closing.
	blobHeadFallbackDrainLimit = 1024
)

var (
	mirrors           = flag.Slice("executor.container_registry_mirrors", []interfaces.MirrorConfig{}, "")
	allowedPrivateIPs = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")

	// Restricting the fallback to hosts known to reject blob HEADs keeps genuine 401s
	// (e.g. expired credentials) from spawning an extra doomed GET per HEAD everywhere else.
	blobHeadFallbackRegistries = flag.Slice("ocifetcher.blob_head_fallback_registries", []string{ecrPublicRegistry}, "Registry hosts whose rejected blob HEAD requests are retried as single-byte ranged GETs.")

	// blobsPathRegexp matches blob pull paths, "/v2/<name>/blobs/<digest>" per
	// https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pulling-blobs.
	blobsPathRegexp = regexp.MustCompile(`^/v2/.+/blobs/[a-z0-9+._-]+:[a-zA-Z0-9=_-]+$`)
)

// RegistryUpstream fetches from container registries over HTTPS using
// go-containerregistry. It keeps one puller (and so one bearer token) per
// (repository, credentials), evicts and retries once on failure so expired
// tokens are refreshed, honours the configured registry mirrors and the
// private-IP allowlist, and refuses every request made with
// Options.BypassRegistry set.
type RegistryUpstream struct {
	httpClientName    string
	allowedPrivateIPs []*net.IPNet
	mirrors           []interfaces.MirrorConfig

	mu        sync.Mutex
	pullerLRU lru.LRU[*remote.Puller]
}

var _ Upstream = (*RegistryUpstream)(nil)

// NewRegistryUpstream returns a RegistryUpstream. httpClientName labels the
// HTTP client metrics (for example "oci_fetcher" on the app and "oci" on the
// executor). One instance per process is preferred so that there is one
// puller cache.
func NewRegistryUpstream(httpClientName string) (*RegistryUpstream, error) {
	allowed, err := ParseAllowedPrivateIPs()
	if err != nil {
		return nil, err
	}
	pullerLRU, err := lru.New[*remote.Puller](&lru.Config[*remote.Puller]{
		SizeFn:  func(_ *remote.Puller) int64 { return 1 },
		MaxSize: int64(pullerLRUMaxEntries),
	})
	if err != nil {
		return nil, status.InternalErrorf("error initializing puller cache: %s", err)
	}
	return &RegistryUpstream{
		httpClientName:    httpClientName,
		allowedPrivateIPs: allowed,
		mirrors:           Mirrors(),
		pullerLRU:         pullerLRU,
	}, nil
}

func (u *RegistryUpstream) Head(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, error) {
	if opts.BypassRegistry {
		return nil, bypassError("manifest metadata", ref)
	}
	return withPullerRetry(ctx, u, ref, creds, func(puller *remote.Puller) (*ctr.Descriptor, error) {
		return puller.Head(ctx, ref)
	})
}

func (u *RegistryUpstream) Manifest(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, []byte, error) {
	if opts.BypassRegistry {
		return nil, nil, bypassError("manifest", ref)
	}
	remoteDesc, err := withPullerRetry(ctx, u, ref, creds, func(puller *remote.Puller) (*remote.Descriptor, error) {
		return puller.Get(ctx, ref)
	})
	if err != nil {
		return nil, nil, err
	}
	desc := remoteDesc.Descriptor
	return &desc, remoteDesc.Manifest, nil
}

func (u *RegistryUpstream) BlobMetadata(ctx context.Context, ref ctrname.Digest, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, error) {
	if opts.BypassRegistry {
		return nil, bypassError("blob metadata", ref)
	}
	h, err := blobHash(ref)
	if err != nil {
		return nil, err
	}
	return withPullerRetry(ctx, u, ref, creds, func(puller *remote.Puller) (*ctr.Descriptor, error) {
		layer, err := puller.Layer(ctx, ref)
		if err != nil {
			return nil, err
		}
		size, err := layer.Size()
		if err != nil {
			return nil, err
		}
		mediaType, err := layer.MediaType()
		if err != nil {
			return nil, err
		}
		return &ctr.Descriptor{Digest: h, Size: size, MediaType: mediaType}, nil
	})
}

// Blob opens the blob. When opts.SizeBytes is set it is used as the size;
// otherwise the size is looked up (one HEAD). Size and media type are
// best-effort: if either is unavailable the blob is still returned with the
// corresponding descriptor field zero.
func (u *RegistryUpstream) Blob(ctx context.Context, ref ctrname.Digest, creds *rgpb.Credentials, opts Options) (io.ReadCloser, *ctr.Descriptor, error) {
	if opts.BypassRegistry {
		return nil, nil, bypassError("blob", ref)
	}
	h, err := blobHash(ref)
	if err != nil {
		return nil, nil, err
	}
	// All HTTP-triggering calls (Size, Compressed) must be inside the retry
	// scope so that token refresh covers them, not just the lazy Layer()
	// reference creation. Size is fetched before Compressed so that there is
	// no open ReadCloser to leak if it fails and triggers a retry.
	desc := &ctr.Descriptor{Digest: h, Size: opts.SizeBytes}
	rc, err := withPullerRetry(ctx, u, ref, creds, func(puller *remote.Puller) (io.ReadCloser, error) {
		layer, err := puller.Layer(ctx, ref)
		if err != nil {
			return nil, err
		}
		if mt, err := layer.MediaType(); err != nil {
			log.CtxWarningf(ctx, "Could not get media type for layer %s: %s", ref, err)
		} else {
			desc.MediaType = mt
		}
		if desc.Size == 0 {
			if sz, err := layer.Size(); err != nil {
				log.CtxWarningf(ctx, "Could not get size for layer %s: %s", ref, err)
			} else {
				desc.Size = sz
			}
		}
		return layer.Compressed()
	})
	if err != nil {
		return nil, nil, err
	}
	return rc, desc, nil
}

func bypassError(what string, ref ctrname.Reference) error {
	return status.NotFoundErrorf("bypassing registry, but %s for %q not found in cache", what, ref)
}

func (u *RegistryUpstream) remoteOptions(ctx context.Context, creds *rgpb.Credentials) []remote.Option {
	opts := []remote.Option{remote.WithContext(ctx)}
	if creds.GetUsername() != "" && creds.GetPassword() != "" {
		opts = append(opts, remote.WithAuth(&authn.Basic{
			Username: creds.GetUsername(),
			Password: creds.GetPassword(),
		}))
	}
	client := httpclient.New(u.allowedPrivateIPs, u.httpClientName)
	// The mirror transport sits inside the client, below the blob HEAD fallback transport, so
	// the fallback's allowlist check sees original registry hostnames rather than mirror
	// hostnames and its ranged GETs get the same mirror rewriting as any other request.
	if len(u.mirrors) > 0 {
		client.Transport = NewMirrorTransport(client.Transport, u.mirrors)
	}
	opts = append(opts, remote.WithTransport(NewBlobHeadFallbackTransport(client)))
	return opts
}

func (u *RegistryUpstream) getOrCreatePuller(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials) (*remote.Puller, error) {
	key := pullerKey(ref, creds)
	u.mu.Lock()
	defer u.mu.Unlock()
	if puller, ok := u.pullerLRU.Get(key); ok {
		return puller, nil
	}
	puller, err := remote.NewPuller(u.remoteOptions(ctx, creds)...)
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}
	u.pullerLRU.Add(key, puller)
	return puller, nil
}

func (u *RegistryUpstream) evictPuller(ref ctrname.Reference, creds *rgpb.Credentials) {
	u.mu.Lock()
	u.pullerLRU.Remove(pullerKey(ref, creds))
	u.mu.Unlock()
}

func pullerKey(ref ctrname.Reference, creds *rgpb.Credentials) string {
	return hash.Strings(
		ref.Context().RegistryStr(),
		ref.Context().RepositoryStr(),
		creds.GetUsername(),
		creds.GetPassword(),
	)
}

// withPullerRetry runs op with the cached puller for ref, evicting the
// puller and retrying once on failure so that expired bearer tokens are
// refreshed. Context errors are returned as-is and never retried.
func withPullerRetry[T any](ctx context.Context, u *RegistryUpstream, ref ctrname.Reference, creds *rgpb.Credentials, op func(puller *remote.Puller) (T, error)) (T, error) {
	var zero T
	puller, err := u.getOrCreatePuller(ctx, ref, creds)
	if err != nil {
		return zero, err
	}
	result, err := op(puller)
	if err == nil {
		return result, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return zero, err
	}
	u.evictPuller(ref, creds)
	puller, err = u.getOrCreatePuller(ctx, ref, creds)
	if err != nil {
		return zero, err
	}
	result, err = op(puller)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return zero, err
	}
	if err != nil {
		u.evictPuller(ref, creds)
		return zero, RemoteRegistryError(err, "could not fetch from remote registry")
	}
	return result, nil
}

// RemoteRegistryError converts an error from a remote registry request into a
// status error classified by HTTP status code when one is available, falling
// back to Unavailable otherwise. msg is a human-readable prefix describing the
// operation that failed.
func RemoteRegistryError(err error, msg string) error {
	var transportErr *transport.Error
	if !errors.As(err, &transportErr) {
		return status.UnavailableErrorf("%s: %s", msg, err)
	}
	return RegistryErrorFromHTTPStatusCode(
		transportErr.StatusCode,
		fmt.Sprintf("%s: remote registry HTTP status %d: %s", msg, transportErr.StatusCode, err),
	)
}

// RegistryErrorFromHTTPStatusCode returns a status error for a remote registry
// request failure when the HTTP status code is known.
func RegistryErrorFromHTTPStatusCode(httpStatusCode int, msg string) error {
	switch httpStatusCode {
	case http.StatusBadRequest:
		return status.InvalidArgumentError(msg)
	case http.StatusUnauthorized:
		return status.UnauthenticatedError(msg)
	case http.StatusForbidden:
		return status.PermissionDeniedError(msg)
	case http.StatusNotFound:
		return status.NotFoundError(msg)
	case http.StatusTooManyRequests:
		return status.ResourceExhaustedError(msg)
	}
	if httpStatusCode >= http.StatusBadRequest && httpStatusCode < http.StatusInternalServerError {
		return status.InvalidArgumentError(msg)
	}
	return status.UnavailableError(msg)
}

func ParseAllowedPrivateIPs() ([]*net.IPNet, error) {
	allowedPrivateIPNets := make([]*net.IPNet, 0, len(*allowedPrivateIPs))
	for _, r := range *allowedPrivateIPs {
		_, ipNet, err := net.ParseCIDR(r)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid value %q for executor.container_registry_allowed_private_ips flag: %s", r, err)
		}
		allowedPrivateIPNets = append(allowedPrivateIPNets, ipNet)
	}
	return allowedPrivateIPNets, nil
}

func Mirrors() []interfaces.MirrorConfig {
	return *mirrors
}

// NewBlobHeadFallbackTransport wraps client's transport to retry blob HEAD requests that
// registries in blobHeadFallbackRegistries reject with 401 or 405 as single-byte ranged GETs.
func NewBlobHeadFallbackTransport(client *http.Client) http.RoundTripper {
	return &blobHeadFallbackTransport{client: client}
}

var _ http.RoundTripper = (*blobHeadFallbackTransport)(nil)

type blobHeadFallbackTransport struct {
	client *http.Client
}

func (t *blobHeadFallbackTransport) RoundTrip(in *http.Request) (*http.Response, error) {
	resp, err := t.client.Transport.RoundTrip(in)
	if err != nil || in.Method != http.MethodHead || !blobHeadFallbackHost(in.URL.Hostname()) || !blobsPathRegexp.MatchString(in.URL.Path) {
		return resp, err
	}
	// If HEAD is not allowed (405) then fall back to a GET.
	// `public.ecr.aws` returns 401 instead of an explicit 405, so we fall back to GET in that case as well.
	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusMethodNotAllowed {
		return resp, nil
	}
	fallbackResp, fallbackErr := t.rangedGet(in)
	if fallbackErr != nil {
		log.CtxDebugf(in.Context(), "Ranged GET fallback for blob HEAD request that failed with HTTP status %d did not succeed either: %s", resp.StatusCode, fallbackErr)
		return resp, nil
	}
	resp.Body.Close()
	return fallbackResp, nil
}

// blobHeadFallbackHost reports whether hostname is on the blob HEAD fallback allowlist.
func blobHeadFallbackHost(hostname string) bool {
	for _, h := range *blobHeadFallbackRegistries {
		if strings.EqualFold(hostname, h) {
			return true
		}
	}
	return false
}

// rangedGet reissues a rejected blob HEAD request as a single-byte ranged GET and synthesizes
// an equivalent HEAD response, with the full blob size from Content-Range or Content-Length.
func (t *blobHeadFallbackTransport) rangedGet(headReq *http.Request) (*http.Response, error) {
	req := headReq.Clone(headReq.Context())
	req.Method = http.MethodGet
	req.Header.Set("Range", blobHeadFallbackRange)
	// client.Do follows redirects to blob CDNs, stripping Authorization on cross-host redirects.
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	size, err := blobSizeFromRangedGetResponse(resp)
	// Drain a range-honoring response (a single byte) so its connection can be reused; a body
	// left unread beyond this (a registry that ignored Range) is aborted by the close instead.
	io.Copy(io.Discard, io.LimitReader(resp.Body, blobHeadFallbackDrainLimit))
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	out := &http.Response{
		Status:        fmt.Sprintf("%d %s", http.StatusOK, http.StatusText(http.StatusOK)),
		StatusCode:    http.StatusOK,
		Proto:         resp.Proto,
		ProtoMajor:    resp.ProtoMajor,
		ProtoMinor:    resp.ProtoMinor,
		Header:        resp.Header.Clone(),
		Body:          http.NoBody,
		ContentLength: size,
		Request:       headReq,
	}
	out.Header.Del("Content-Range")
	// Content-Encoding describes the ranged GET's body, which the synthesized response doesn't
	// carry; drop it so the result reads like a real HEAD response.
	out.Header.Del("Content-Encoding")
	out.Header.Set("Content-Length", strconv.FormatInt(size, 10))
	return out, nil
}

// blobSizeFromRangedGetResponse extracts the full blob size from a single-byte ranged GET response.
func blobSizeFromRangedGetResponse(resp *http.Response) (int64, error) {
	switch resp.StatusCode {
	case http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
		// "Content-Range: bytes 0-0/123" (206), or "bytes */0" (416, zero-length blob).
		contentRange := resp.Header.Get("Content-Range")
		slash := strings.LastIndexByte(contentRange, '/')
		if slash < 0 {
			return 0, status.UnavailableErrorf("missing or malformed Content-Range header %q", contentRange)
		}
		if contentRange[slash+1:] == "*" {
			// RFC 9110 permits "bytes 0-0/*" for an unknown complete length.
			return 0, status.UnavailableErrorf("Content-Range header %q reports an unknown blob size", contentRange)
		}
		size, err := strconv.ParseInt(contentRange[slash+1:], 10, 64)
		if err != nil || size < 0 {
			return 0, status.UnavailableErrorf("malformed Content-Range header %q", contentRange)
		}
		return size, nil
	case http.StatusOK:
		if resp.ContentLength < 0 {
			return 0, status.UnavailableError("response reports an unknown content length")
		}
		return resp.ContentLength, nil
	}
	return 0, status.UnavailableErrorf("HTTP status %d", resp.StatusCode)
}

// NewMirrorTransport wraps an http.RoundTripper with registry mirror support.
// Requests matching a mirror's OriginalURL are rewritten to the MirrorURL,
// with automatic fallback to the original URL on failure.
func NewMirrorTransport(inner http.RoundTripper, mirrors []interfaces.MirrorConfig) http.RoundTripper {
	return &mirrorTransport{
		inner:   inner,
		mirrors: mirrors,
	}
}

// verify that mirrorTransport implements the RoundTripper interface.
var _ http.RoundTripper = (*mirrorTransport)(nil)

type mirrorTransport struct {
	inner   http.RoundTripper
	mirrors []interfaces.MirrorConfig
}

func (t *mirrorTransport) RoundTrip(in *http.Request) (out *http.Response, err error) {
	for _, mirror := range t.mirrors {
		if match, err := matchesMirror(mirror, in.URL); err == nil && match {
			mirroredRequest, err := rewriteToMirror(mirror, in)
			if err != nil {
				log.CtxErrorf(in.Context(), "error mirroring request: %s", err)
				continue
			}
			out, err := t.inner.RoundTrip(mirroredRequest)
			if err != nil {
				log.CtxErrorf(in.Context(), "mirror err: %s", err)
				continue
			}
			if out.StatusCode < http.StatusOK || out.StatusCode >= 300 {
				fallbackRequest, err := rewriteFallback(mirror, in)
				if err != nil {
					log.CtxErrorf(in.Context(), "error rewriting fallback request: %s", err)
					continue
				}
				return t.inner.RoundTrip(fallbackRequest)
			}
			return out, nil
		}
	}
	return t.inner.RoundTrip(in)
}

func matchesMirror(mc interfaces.MirrorConfig, u *url.URL) (bool, error) {
	originalURL, err := url.Parse(mc.OriginalURL)
	if err != nil {
		return false, status.InvalidArgumentErrorf("invalid mirror original URL %q: %s", mc.OriginalURL, err)
	}
	return originalURL.Host == u.Host, nil
}

func rewriteToMirror(mc interfaces.MirrorConfig, originalRequest *http.Request) (*http.Request, error) {
	mirrorURL, err := url.Parse(mc.MirrorURL)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid mirror URL %q: %s", mc.MirrorURL, err)
	}
	originalURL := originalRequest.URL.String()
	req := originalRequest.Clone(originalRequest.Context())
	req.URL.Scheme = mirrorURL.Scheme
	req.URL.Host = mirrorURL.Host
	// Set X-Forwarded-Host so the mirror knows which remote registry to make requests to.
	// ociregistry looks for this header and will default to forwarding requests to Docker Hub if not found.
	req.Header.Set("X-Forwarded-Host", originalRequest.URL.Host)
	log.CtxDebugf(originalRequest.Context(), "%q rewritten to %s", originalURL, req.URL.String())
	return req, nil
}

func rewriteFallback(mc interfaces.MirrorConfig, originalRequest *http.Request) (*http.Request, error) {
	originalURL, err := url.Parse(mc.OriginalURL)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid fallback URL %q: %s", mc.OriginalURL, err)
	}
	req := originalRequest.Clone(originalRequest.Context())
	req.URL.Scheme = originalURL.Scheme
	req.URL.Host = originalURL.Host
	log.CtxDebugf(originalRequest.Context(), "(fallback) %q rewritten to %s", originalURL, req.URL.String())
	return req, nil
}
