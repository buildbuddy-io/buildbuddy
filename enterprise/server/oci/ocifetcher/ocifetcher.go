// Package ocifetcher provides an OCIFetcherClient
// that fetches OCI blobs and manifests from remote registries.
//
// The client currently only implements FetchManifestMetadata.
// Once the client implements the entire OCIFetcherClient interface,
// all fetches of OCI blobs and manifests should go through this client
// in order to allow the corresponding OCIFetcherServer to deduplicate requests
// for the same blob or manifest.
//
// It is safe to construct multiple clients.
package ocifetcher

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	gcrname "github.com/google/go-containerregistry/pkg/name"
)

const pullerLRUMaxEntries = 1000

var (
	mirrors           = flag.Slice("executor.container_registry_mirrors", []interfaces.MirrorConfig{}, "")
	allowedPrivateIPs = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")
)

type pullerLRUEntry struct {
	puller *remote.Puller
}

type ociFetcherClient struct {
	allowedPrivateIPs []*net.IPNet
	mirrors           []interfaces.MirrorConfig

	// On the first call to Head, Get, or Layer, Pullers make a GET /v2/ request,
	// optionally followed by a POST request to an auth endpoint.
	// To avoid making these requests for already-authed {image, credentials} pairs,
	// we keep a small LRU cache of Pullers.
	mu        sync.Mutex
	pullerLRU *lru.LRU[*pullerLRUEntry]
}

// NewClient constructs a new OCI Fetcher client.
// For now, this client is "thick", meaning that it actually fetches blobs and manifests from remote OCI registries.
// Eventually, this functionality will live in an OCIFetcherServer and the client will simply make gRPC calls
// to that server.
// TODO(dan): Stop passing private IPs, mirror config to client once server owns the fetching logic.
// TODO(dan): Update this comment once server is implemented!
func NewClient(allowedPrivateIPs []*net.IPNet, mirrors []interfaces.MirrorConfig) (ofpb.OCIFetcherClient, error) {
	pullerLRU, err := lru.NewLRU[*pullerLRUEntry](&lru.Config[*pullerLRUEntry]{
		SizeFn:  func(_ *pullerLRUEntry) int64 { return 1 },
		MaxSize: int64(pullerLRUMaxEntries),
	})
	if err != nil {
		return nil, err
	}
	return &ociFetcherClient{
		allowedPrivateIPs: allowedPrivateIPs,
		mirrors:           mirrors,
		pullerLRU:         pullerLRU,
	}, nil
}

func RegisterClient(env *real_environment.RealEnv) error {
	allowedPrivateIPNets, err := ParseAllowedPrivateIPs()
	if err != nil {
		return err
	}
	client, err := NewClient(allowedPrivateIPNets, *mirrors)
	if err != nil {
		return err
	}
	env.SetOCIFetcherClient(client)
	return nil
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
		return false, err
	}
	return originalURL.Host == u.Host, nil
}

func rewriteToMirror(mc interfaces.MirrorConfig, originalRequest *http.Request) (*http.Request, error) {
	mirrorURL, err := url.Parse(mc.MirrorURL)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	req := originalRequest.Clone(originalRequest.Context())
	req.URL.Scheme = originalURL.Scheme
	req.URL.Host = originalURL.Host
	log.CtxDebugf(originalRequest.Context(), "(fallback) %q rewritten to %s", originalURL, req.URL.String())
	return req, nil
}

func (c *ociFetcherClient) getRemoteOpts(ctx context.Context, creds *rgpb.Credentials) []remote.Option {
	opts := []remote.Option{remote.WithContext(ctx)}

	if creds != nil && creds.GetUsername() != "" && creds.GetPassword() != "" {
		opts = append(opts, remote.WithAuth(&authn.Basic{
			Username: creds.GetUsername(),
			Password: creds.GetPassword(),
		}))
	}

	tr := httpclient.New(c.allowedPrivateIPs, "oci_fetcher").Transport

	if len(c.mirrors) > 0 {
		opts = append(opts, remote.WithTransport(NewMirrorTransport(tr, c.mirrors)))
	} else {
		opts = append(opts, remote.WithTransport(tr))
	}

	return opts
}

func pullerKey(ref gcrname.Reference, creds *rgpb.Credentials) string {
	if creds == nil {
		return hash.Strings(
			ref.Context().RegistryStr(),
			ref.Context().RepositoryStr(),
			"",
			"",
		)
	}
	return hash.Strings(
		ref.Context().RegistryStr(),
		ref.Context().RepositoryStr(),
		creds.GetUsername(),
		creds.GetPassword(),
	)
}

// getOrCreatePuller returns a cached Puller for the given image reference and credentials,
// or creates a new one if not found.
func (c *ociFetcherClient) getOrCreatePuller(ctx context.Context, imageRef gcrname.Reference, creds *rgpb.Credentials) (*remote.Puller, error) {
	key := pullerKey(imageRef, creds)

	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.pullerLRU.Get(key)

	if ok {
		return entry.puller, nil
	}

	remoteOpts := c.getRemoteOpts(ctx, creds)
	// As of go-containerregistry v0.20.3, NewPuller does not perform IO
	// and is safe to call with the mutex locked.
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, err
	}
	c.pullerLRU.Add(key, &pullerLRUEntry{puller: puller})

	return puller, nil
}

func (c *ociFetcherClient) evictPuller(imageRef gcrname.Reference, creds *rgpb.Credentials) {
	key := pullerKey(imageRef, creds)
	c.mu.Lock()
	c.pullerLRU.Remove(key)
	c.mu.Unlock()
}

// withPullerRetry handles the common pattern of executing an operation with a puller,
// evicting and retrying on failure due to expired tokens.
func withPullerRetry[T any](
	c *ociFetcherClient,
	ctx context.Context,
	ref gcrname.Reference,
	creds *rgpb.Credentials,
	op func(puller *remote.Puller) (T, error),
) (T, error) {
	var zero T

	puller, err := c.getOrCreatePuller(ctx, ref, creds)
	if err != nil {
		return zero, status.InternalErrorf("error creating puller: %s", err)
	}

	result, err := op(puller)
	if err == nil {
		return result, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return zero, err
	}

	// Pullers from the LRU may have expired Bearer tokens, so we evict and retry on most errors.
	c.evictPuller(ref, creds)
	puller, err = c.getOrCreatePuller(ctx, ref, creds)
	if err != nil {
		return zero, status.InternalErrorf("error creating puller: %s", err)
	}

	result, err = op(puller)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return zero, err
	}
	if err != nil {
		c.evictPuller(ref, creds)
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return zero, status.PermissionDeniedErrorf("not authorized to access resource: %s", err)
		}
		return zero, status.UnavailableErrorf("could not fetch from remote registry: %s", err)
	}

	return result, nil
}

// FetchManifestMetadata performs a HEAD request to get manifest metadata (digest, size, media type)
// without downloading the full manifest content.
func (c *ociFetcherClient) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestMetadataResponse, error) {
	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	desc, err := withPullerRetry(c, ctx, imageRef, req.GetCredentials(), func(puller *remote.Puller) (*v1.Descriptor, error) {
		return puller.Head(ctx, imageRef)
	})
	if err != nil {
		return nil, err
	}

	return &ofpb.FetchManifestMetadataResponse{
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
	}, nil
}

// FetchManifest fetches the full manifest content from a remote registry.
func (c *ociFetcherClient) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestResponse, error) {
	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	desc, err := withPullerRetry(c, ctx, imageRef, req.GetCredentials(), func(puller *remote.Puller) (*remote.Descriptor, error) {
		return puller.Get(ctx, imageRef)
	})
	if err != nil {
		return nil, err
	}

	return &ofpb.FetchManifestResponse{
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
		Manifest:  desc.Manifest,
	}, nil
}

func (c *ociFetcherClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	return nil, status.UnimplementedError("FetchBlobMetadata not implemented")
}

// FetchBlob streams blob content from a remote registry.
func (c *ociFetcherClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ofpb.FetchBlobResponse], error) {
	// Blob references must be digest references (e.g., repo@sha256:...).
	blobRef, err := gcrname.NewDigest(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid blob reference %q (must be a digest reference): %s", req.GetRef(), err)
	}

	layer, err := withPullerRetry(c, ctx, blobRef, req.GetCredentials(), func(puller *remote.Puller) (v1.Layer, error) {
		return puller.Layer(ctx, blobRef)
	})
	if err != nil {
		return nil, err
	}

	reader, err := layer.Compressed()
	if err != nil {
		return nil, status.UnavailableErrorf("could not get compressed layer: %s", err)
	}
	return &blobStreamClient{reader: reader}, nil
}

// blobStreamClient implements grpc.ServerStreamingClient for FetchBlob responses.
// Since fetching currently only happens on the client (executor) node,
// this struct exists to pass a ReadCloser through to ReadBlob.
type blobStreamClient struct {
	reader io.ReadCloser
}

func (c *blobStreamClient) Recv() (*ofpb.FetchBlobResponse, error) {
	return nil, status.UnimplementedError("Recv not implemented on blobStreamClient")
}

func (c *blobStreamClient) Header() (metadata.MD, error) {
	return nil, status.UnimplementedError("Header not implemented on blobStreamClient")
}

func (c *blobStreamClient) Trailer() metadata.MD {
	return nil
}

func (c *blobStreamClient) CloseSend() error {
	return status.UnimplementedError("CloseSend not implemented on blobStreamClient")
}

func (c *blobStreamClient) Context() context.Context {
	return context.TODO()
}

func (c *blobStreamClient) SendMsg(m any) error {
	return status.UnimplementedError("SendMsg not implemented on blobStreamClient")
}

func (c *blobStreamClient) RecvMsg(m any) error {
	return status.UnimplementedError("RecvMsg not implemented on blobStreamClient")
}

func ReadBlob(ctx context.Context, client ofpb.OCIFetcherClient, ref string, creds *rgpb.Credentials, bypassRegistry bool) (io.ReadCloser, error) {
	stream, err := client.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref:            ref,
		Credentials:    creds,
		BypassRegistry: bypassRegistry,
	})
	if err != nil {
		return nil, err
	}
	if bsc, ok := stream.(*blobStreamClient); ok {
		return bsc.reader, nil
	}
	return nil, status.FailedPreconditionError("ReadBlob only works with blobStreamClient implementations")
}
