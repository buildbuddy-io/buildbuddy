package fetcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	gcrname "github.com/google/go-containerregistry/pkg/name"
)

const (
	// blobChunkSize is the size of chunks sent in FetchBlob streaming responses.
	blobChunkSize = 1024 * 1024 // 1MB

	// maxPullerCacheEntries is the maximum number of Puller instances to cache.
	maxPullerCacheEntries = 100
)

var (
	mirrors           = flag.Slice("executor.container_registry_mirrors", []MirrorConfig{}, "")
	allowedPrivateIPs = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")
)

type MirrorConfig struct {
	OriginalURL string `yaml:"original_url" json:"original_url"`
	MirrorURL   string `yaml:"mirror_url" json:"mirror_url"`
}

func (mc MirrorConfig) matches(u *url.URL) (bool, error) {
	originalURL, err := url.Parse(mc.OriginalURL)
	if err != nil {
		return false, err
	}
	match := originalURL.Host == u.Host
	return match, nil
}

func (mc MirrorConfig) rewriteRequest(originalRequest *http.Request) (*http.Request, error) {
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
	log.Debugf("%q rewritten to %s", originalURL, req.URL.String())
	return req, nil
}

func (mc MirrorConfig) rewriteFallbackRequest(originalRequest *http.Request) (*http.Request, error) {
	originalURL, err := url.Parse(mc.OriginalURL)
	if err != nil {
		return nil, err
	}
	req := originalRequest.Clone(originalRequest.Context())
	req.URL.Scheme = originalURL.Scheme
	req.URL.Host = originalURL.Host
	log.Debugf("(fallback) %q rewritten to %s", originalURL, req.URL.String())
	return req, nil
}

// verify that mirrorTransport implements the RoundTripper interface.
var _ http.RoundTripper = (*mirrorTransport)(nil)

type mirrorTransport struct {
	inner   http.RoundTripper
	mirrors []MirrorConfig
}

func newMirrorTransport(inner http.RoundTripper, mirrors []MirrorConfig) http.RoundTripper {
	return &mirrorTransport{
		inner:   inner,
		mirrors: mirrors,
	}
}

func (t *mirrorTransport) RoundTrip(in *http.Request) (out *http.Response, err error) {
	for _, mirror := range t.mirrors {
		if match, err := mirror.matches(in.URL); err == nil && match {
			mirroredRequest, err := mirror.rewriteRequest(in)
			if err != nil {
				log.Errorf("error mirroring request: %s", err)
				continue
			}
			out, err := t.inner.RoundTrip(mirroredRequest)
			if err != nil {
				log.Errorf("mirror err: %s", err)
				continue
			}
			if out.StatusCode < http.StatusOK || out.StatusCode >= 300 {
				fallbackRequest, err := mirror.rewriteFallbackRequest(in)
				if err != nil {
					log.Errorf("error rewriting fallback request: %s", err)
					continue
				}
				return t.inner.RoundTrip(fallbackRequest)
			}
			return out, nil // Return successful mirror response
		}
	}
	return t.inner.RoundTrip(in)
}

// LocalOCIFetcherClient implements ofpb.OCIFetcherClient by directly
// accessing OCI registries using go-containerregistry.
type LocalOCIFetcherClient struct {
	allowedPrivateIPs []*net.IPNet

	pullerCacheMu sync.Mutex
	pullerCache   *lru.LRU[*remote.Puller]
}

// Verify that LocalOCIFetcherClient implements OCIFetcherClient interface.
var _ ofpb.OCIFetcherClient = (*LocalOCIFetcherClient)(nil)

func New() (*LocalOCIFetcherClient, error) {
	allowedPrivateIPNets := make([]*net.IPNet, 0, len(*allowedPrivateIPs))
	for _, r := range *allowedPrivateIPs {
		_, ipNet, err := net.ParseCIDR(r)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid value %q for executor.container_registry_allowed_private_ips flag: %s", r, err)
		}
		allowedPrivateIPNets = append(allowedPrivateIPNets, ipNet)
	}

	pullerCache, err := lru.NewLRU(&lru.Config[*remote.Puller]{
		MaxSize: maxPullerCacheEntries,
		SizeFn:  func(*remote.Puller) int64 { return 1 },
	})
	if err != nil {
		return nil, status.InternalErrorf("failed to create puller cache: %s", err)
	}

	return &LocalOCIFetcherClient{
		allowedPrivateIPs: allowedPrivateIPNets,
		pullerCache:       pullerCache,
	}, nil
}

func Register(env *real_environment.RealEnv) error {
	f, err := New()
	if err != nil {
		return err
	}
	env.SetOCIFetcherClient(f)
	return nil
}

func (f *LocalOCIFetcherClient) getRemoteOpts(ctx context.Context, username, password string) []remote.Option {
	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
	}
	if username != "" && password != "" {
		remoteOpts = append(remoteOpts, remote.WithAuth(&authn.Basic{
			Username: username,
			Password: password,
		}))
	}

	tr := httpclient.New(f.allowedPrivateIPs, "oci").Transport
	if len(*mirrors) > 0 {
		remoteOpts = append(remoteOpts, remote.WithTransport(newMirrorTransport(tr, *mirrors)))
	} else {
		remoteOpts = append(remoteOpts, remote.WithTransport(tr))
	}
	return remoteOpts
}

// pullerCacheKey generates a cache key for a Puller based on the registry,
// repository, and credentials. The password is hashed for security.
func pullerCacheKey(registry, repository, username, password string) string {
	// Hash the password so we don't store it in plain text in memory
	h := sha256.Sum256([]byte(password))
	passwordHash := hex.EncodeToString(h[:8]) // Use first 8 bytes for brevity
	return registry + "/" + repository + ":" + username + ":" + passwordHash
}

// getPuller retrieves a cached Puller or creates a new one if not found.
func (f *LocalOCIFetcherClient) getPuller(ctx context.Context, ref gcrname.Reference, username, password string) (*remote.Puller, error) {
	registry := ref.Context().RegistryStr()
	repository := ref.Context().RepositoryStr()
	key := pullerCacheKey(registry, repository, username, password)

	f.pullerCacheMu.Lock()
	if puller, ok := f.pullerCache.Get(key); ok {
		f.pullerCacheMu.Unlock()
		return puller, nil
	}
	f.pullerCacheMu.Unlock()

	// Create a new Puller
	remoteOpts := f.getRemoteOpts(ctx, username, password)
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}

	// Cache it
	f.pullerCacheMu.Lock()
	f.pullerCache.Add(key, puller)
	f.pullerCacheMu.Unlock()

	return puller, nil
}

func (f *LocalOCIFetcherClient) CanAccess(ctx context.Context, req *ofpb.CanAccessRequest, opts ...grpc.CallOption) (*ofpb.CanAccessResponse, error) {
	imageRef, err := gcrname.ParseReference(req.GetReference())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetReference(), err)
	}

	creds := req.GetCredentials()
	remoteOpts := f.getRemoteOpts(ctx, creds.GetUsername(), creds.GetPassword())

	_, err = remote.Head(imageRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return &ofpb.CanAccessResponse{CanAccess: false}, nil
		}
		return nil, status.UnavailableErrorf("could not access registry: %s", err)
	}
	return &ofpb.CanAccessResponse{CanAccess: true}, nil
}

func (f *LocalOCIFetcherClient) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestResponse, error) {
	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	creds := req.GetCredentials()
	puller, err := f.getPuller(ctx, imageRef, creds.GetUsername(), creds.GetPassword())
	if err != nil {
		return nil, err
	}

	desc, err := puller.Get(ctx, imageRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to retrieve image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	return &ofpb.FetchManifestResponse{
		Manifest:  desc.Manifest,
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
	}, nil
}

func (f *LocalOCIFetcherClient) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestMetadataResponse, error) {
	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	creds := req.GetCredentials()
	puller, err := f.getPuller(ctx, imageRef, creds.GetUsername(), creds.GetPassword())
	if err != nil {
		return nil, err
	}

	desc, err := puller.Head(ctx, imageRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to access manifest metadata: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest metadata from remote: %s", err)
	}

	return &ofpb.FetchManifestMetadataResponse{
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
	}, nil
}

func (f *LocalOCIFetcherClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ofpb.FetchBlobResponse], error) {
	blobRef, err := gcrname.NewDigest(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid blob reference %q (must be a digest reference): %s", req.GetRef(), err)
	}

	creds := req.GetCredentials()
	puller, err := f.getPuller(ctx, blobRef, creds.GetUsername(), creds.GetPassword())
	if err != nil {
		return nil, err
	}

	layer, err := puller.Layer(ctx, blobRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to access blob: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve blob from remote: %s", err)
	}

	rc, err := layer.Compressed()
	if err != nil {
		return nil, status.UnavailableErrorf("could not get compressed blob: %s", err)
	}

	return &localBlobStream{
		ctx:    ctx,
		reader: rc,
	}, nil
}

func (f *LocalOCIFetcherClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	blobRef, err := gcrname.NewDigest(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid blob reference %q (must be a digest reference): %s", req.GetRef(), err)
	}

	creds := req.GetCredentials()
	puller, err := f.getPuller(ctx, blobRef, creds.GetUsername(), creds.GetPassword())
	if err != nil {
		return nil, err
	}

	layer, err := puller.Layer(ctx, blobRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to access blob metadata: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve blob metadata from remote: %s", err)
	}

	size, err := layer.Size()
	if err != nil {
		return nil, status.UnavailableErrorf("could not get blob size: %s", err)
	}

	mediaType, err := layer.MediaType()
	if err != nil {
		return nil, status.UnavailableErrorf("could not get blob media type: %s", err)
	}

	return &ofpb.FetchBlobMetadataResponse{
		SizeBytes: size,
		MediaType: string(mediaType),
	}, nil
}

// localBlobStream implements grpc.ServerStreamingClient[ofpb.FetchBlobResponse]
// for streaming blob data from an io.ReadCloser.
type localBlobStream struct {
	ctx    context.Context
	reader io.ReadCloser
}

var _ grpc.ServerStreamingClient[ofpb.FetchBlobResponse] = (*localBlobStream)(nil)

func (s *localBlobStream) Recv() (*ofpb.FetchBlobResponse, error) {
	buf := make([]byte, blobChunkSize)
	n, err := s.reader.Read(buf)

	// Per io.Reader contract, Read can return data along with io.EOF.
	// Return data first if we got any.
	if n > 0 {
		return &ofpb.FetchBlobResponse{Data: buf[:n]}, nil
	}

	// No data, handle the error
	if err != nil {
		s.reader.Close()
		return nil, err // includes io.EOF
	}

	// n == 0 and err == nil (unusual but valid per io.Reader contract)
	return &ofpb.FetchBlobResponse{Data: nil}, nil
}

func (s *localBlobStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *localBlobStream) Trailer() metadata.MD {
	return nil
}

func (s *localBlobStream) CloseSend() error {
	return nil
}

func (s *localBlobStream) Context() context.Context {
	return s.ctx
}

func (s *localBlobStream) SendMsg(m any) error {
	return status.UnimplementedError("SendMsg not supported on local blob stream")
}

func (s *localBlobStream) RecvMsg(m any) error {
	resp, err := s.Recv()
	if err != nil {
		return err
	}
	// Copy the response into m
	if msg, ok := m.(*ofpb.FetchBlobResponse); ok {
		msg.Data = resp.Data
		return nil
	}
	return status.InvalidArgumentError("RecvMsg called with wrong type")
}
