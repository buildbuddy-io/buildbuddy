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
	"io"
	"net"
	"net/http"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	gcrname "github.com/google/go-containerregistry/pkg/name"
)

const (
	blobChunkSize = 32 * 1024 // 32KB streaming chunks
)

var (
	mirrors           = flag.Slice("executor.container_registry_mirrors", []interfaces.MirrorConfig{}, "")
	allowedPrivateIPs = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")
)

type ociFetcherClient struct {
	allowedPrivateIPs []*net.IPNet
	mirrors           []interfaces.MirrorConfig
}

// NewClient constructs a new OCI Fetcher client.
// For now, this client is "thick", meaning that it actually fetches blobs and manifests from remote OCI registries.
// Eventually, this functionality will live in an OCIFetcherServer and the client will simply make gRPC calls
// to that server.
// TODO(dan): Stop passing private IPs, mirror config to client once server owns the fetching logic.
// TODO(dan): Update this comment once server is implemented!
func NewClient(allowedPrivateIPs []*net.IPNet, mirrors []interfaces.MirrorConfig) ofpb.OCIFetcherClient {
	return &ociFetcherClient{
		allowedPrivateIPs: allowedPrivateIPs,
		mirrors:           mirrors,
	}
}

func RegisterClient(env *real_environment.RealEnv) error {
	allowedPrivateIPNets, err := ParseAllowedPrivateIPs()
	if err != nil {
		return err
	}
	client := NewClient(allowedPrivateIPNets, *mirrors)
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

// FetchManifestMetadata performs a HEAD request to get manifest metadata (digest, size, media type)
// without downloading the full manifest content.
func (c *ociFetcherClient) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestMetadataResponse, error) {
	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	remoteOpts := c.getRemoteOpts(ctx, req.GetCredentials())
	desc, err := remote.Head(imageRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to access image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not fetch manifest metadata from remote registry: %s", err)
	}

	return &ofpb.FetchManifestMetadataResponse{
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
	}, nil
}

func (c *ociFetcherClient) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestResponse, error) {
	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	remoteOpts := c.getRemoteOpts(ctx, req.GetCredentials())
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, status.InternalErrorf("failed to create puller: %s", err)
	}

	remoteDesc, err := puller.Get(ctx, imageRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to access image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not fetch manifest from remote registry: %s", err)
	}

	return &ofpb.FetchManifestResponse{
		Digest:    remoteDesc.Digest.String(),
		Size:      remoteDesc.Size,
		MediaType: string(remoteDesc.MediaType),
		Manifest:  remoteDesc.Manifest,
	}, nil
}

func (c *ociFetcherClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	blobRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid blob reference %q: %s", req.GetRef(), err)
	}

	remoteOpts := c.getRemoteOpts(ctx, req.GetCredentials())
	desc, err := remote.Head(blobRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to access blob: %s", err)
		}
		return nil, status.UnavailableErrorf("could not fetch blob metadata from remote registry: %s", err)
	}

	return &ofpb.FetchBlobMetadataResponse{
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
	}, nil
}

func (c *ociFetcherClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ofpb.FetchBlobResponse], error) {
	blobRef, err := gcrname.NewDigest(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid blob digest reference %q: %s", req.GetRef(), err)
	}

	remoteOpts := c.getRemoteOpts(ctx, req.GetCredentials())
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, status.InternalErrorf("failed to create puller: %s", err)
	}

	layer, err := puller.Layer(ctx, blobRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to access blob: %s", err)
		}
		return nil, status.UnavailableErrorf("could not fetch blob from remote registry: %s", err)
	}

	rc, err := layer.Compressed()
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to access blob: %s", err)
		}
		return nil, status.UnavailableErrorf("could not read blob content: %s", err)
	}

	return &fetchBlobStreamClient{
		ctx:    ctx,
		reader: rc,
		buf:    make([]byte, blobChunkSize),
	}, nil
}

type fetchBlobStreamClient struct {
	ctx    context.Context
	reader io.ReadCloser
	buf    []byte
}

var _ grpc.ServerStreamingClient[ofpb.FetchBlobResponse] = (*fetchBlobStreamClient)(nil)

func (s *fetchBlobStreamClient) Recv() (*ofpb.FetchBlobResponse, error) {
	n, err := s.reader.Read(s.buf)
	// Per io.Reader contract, n > 0 means data was read even if err is set.
	// Return the data first; EOF will surface on the next Recv() call.
	if n > 0 {
		return &ofpb.FetchBlobResponse{Data: s.buf[:n]}, nil
	}
	if err != nil {
		s.reader.Close()
		return nil, err
	}
	// n == 0 and err == nil is unusual but valid; treat as EOF
	s.reader.Close()
	return nil, io.EOF
}

func (s *fetchBlobStreamClient) Context() context.Context { return s.ctx }
func (s *fetchBlobStreamClient) CloseSend() error         { return s.reader.Close() }
func (s *fetchBlobStreamClient) Header() (metadata.MD, error) {
	return nil, nil
}
func (s *fetchBlobStreamClient) Trailer() metadata.MD { return nil }
func (s *fetchBlobStreamClient) RecvMsg(m any) error  { panic("not implemented") }
func (s *fetchBlobStreamClient) SendMsg(m any) error  { panic("not implemented") }

// ReadBlob provides a convenient io.ReadCloser interface for fetching blobs.
// Callers should prefer this over FetchBlob when they need an io.Reader.
func ReadBlob(ctx context.Context, client ofpb.OCIFetcherClient, ref string, creds *rgpb.Credentials) (io.ReadCloser, error) {
	stream, err := client.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref:         ref,
		Credentials: creds,
	})
	if err != nil {
		return nil, err
	}
	return &blobReader{stream: stream}, nil
}

type blobReader struct {
	stream grpc.ServerStreamingClient[ofpb.FetchBlobResponse]
	buf    []byte // leftover data from last Recv
}

func (r *blobReader) Read(p []byte) (n int, err error) {
	// Return buffered data first
	if len(r.buf) > 0 {
		n = copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}

	// Get more data from stream
	resp, err := r.stream.Recv()
	if err != nil {
		return 0, err
	}

	n = copy(p, resp.GetData())
	if n < len(resp.GetData()) {
		r.buf = resp.GetData()[n:]
	}
	return n, nil
}

func (r *blobReader) Close() error {
	return r.stream.CloseSend()
}
