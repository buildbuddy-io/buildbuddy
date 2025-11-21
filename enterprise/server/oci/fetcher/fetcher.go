package fetcher

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/authn"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	ocifetcherpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

const (
	// Maximum chunk size for streaming blob responses (1MB)
	maxBlobChunkSize = 1024 * 1024

	// Maximum number of cached pullers
	pullerCacheMaxEntries = 1024
	// Duration for which cached pullers are valid
	pullerCacheTTL = 15 * time.Minute
)

var (
	allowedPrivateIPs = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")
	mirrors           = flag.Slice("executor.container_registry_mirrors", []MirrorConfig{}, "")
)

// cachedPuller wraps a remote.Puller with an expiration time for caching
type cachedPuller struct {
	puller    *remote.Puller
	expiresAt time.Time
}

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
	//Set X-Forwarded-Host so the mirror knows which remote registry to make requests to.
	//ociregistry looks for this header and will default to forwarding requests to Docker Hub if not found.
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

type OCIFetcherServer struct {
	env               environment.Env
	allowedPrivateIPs []*net.IPNet
	mirrors           []MirrorConfig

	// Puller cache to reuse authenticated transports
	pullerCache   *lru.LRU[*cachedPuller]
	pullerCacheMu sync.Mutex
}

func Register(env *real_environment.RealEnv) error {
	allowedPrivateIPNets := make([]*net.IPNet, 0, len(*allowedPrivateIPs))
	for _, r := range *allowedPrivateIPs {
		_, ipNet, err := net.ParseCIDR(r)
		if err != nil {
			return status.InvalidArgumentErrorf("invalid value %q for executor.container_registry_allowed_private_ips flag: %s", r, err)
		}
		allowedPrivateIPNets = append(allowedPrivateIPNets, ipNet)
	}

	pullerCache, err := lru.NewLRU[*cachedPuller](&lru.Config[*cachedPuller]{
		SizeFn:  func(_ *cachedPuller) int64 { return 1 },
		MaxSize: pullerCacheMaxEntries,
	})
	if err != nil {
		return status.InternalErrorf("failed to create puller cache: %s", err)
	}

	server := &OCIFetcherServer{
		env:               env,
		allowedPrivateIPs: allowedPrivateIPNets,
		mirrors:           *mirrors,
		pullerCache:       pullerCache,
	}
	env.SetOCIFetcherServer(server)
	return nil
}

func (s *OCIFetcherServer) getRemoteOpts(ctx context.Context, credentials *rgpb.Credentials) []remote.Option {
	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
	}

	if credentials != nil && credentials.GetUsername() != "" {
		remoteOpts = append(remoteOpts, remote.WithAuth(&authn.Basic{
			Username: credentials.GetUsername(),
			Password: credentials.GetPassword(),
		}))
	}

	tr := httpclient.New(s.allowedPrivateIPs, "oci_fetcher").Transport
	if len(s.mirrors) > 0 {
		tr = newMirrorTransport(tr, s.mirrors)
	}
	remoteOpts = append(remoteOpts, remote.WithTransport(tr))

	return remoteOpts
}

// getOrCreatePuller returns a cached puller for the given image reference and credentials,
// or creates a new one if not cached or expired.
func (s *OCIFetcherServer) getOrCreatePuller(ctx context.Context, imageRef gcrname.Reference, credentials *rgpb.Credentials) (*remote.Puller, error) {
	registry := imageRef.Context().RegistryStr()
	repository := imageRef.Context().RepositoryStr()
	username := ""
	password := ""
	if credentials != nil {
		username = credentials.GetUsername()
		password = credentials.GetPassword()
	}

	// Create cache key from registry, repository, and credentials
	cacheKey := hash.Strings(registry, repository, username, password)

	s.pullerCacheMu.Lock()
	cached, ok := s.pullerCache.Get(cacheKey)
	s.pullerCacheMu.Unlock()

	// Check if cached puller is still valid
	if ok && cached != nil && time.Now().Before(cached.expiresAt) {
		return cached.puller, nil
	}

	// Build remote options without context (context is passed to puller methods)
	remoteOpts := []remote.Option{}

	if credentials != nil && credentials.GetUsername() != "" {
		remoteOpts = append(remoteOpts, remote.WithAuth(&authn.Basic{
			Username: credentials.GetUsername(),
			Password: credentials.GetPassword(),
		}))
	}

	tr := httpclient.New(s.allowedPrivateIPs, "oci_fetcher").Transport
	if len(s.mirrors) > 0 {
		tr = newMirrorTransport(tr, s.mirrors)
	}
	remoteOpts = append(remoteOpts, remote.WithTransport(tr))

	// Create new puller
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, err
	}

	// Cache the puller with expiration
	entry := &cachedPuller{
		puller:    puller,
		expiresAt: time.Now().Add(pullerCacheTTL),
	}

	s.pullerCacheMu.Lock()
	s.pullerCache.Add(cacheKey, entry)
	s.pullerCacheMu.Unlock()

	return puller, nil
}

// invalidatePullerCache removes a puller from the cache
func (s *OCIFetcherServer) invalidatePullerCache(imageRef gcrname.Reference, credentials *rgpb.Credentials) {
	registry := imageRef.Context().RegistryStr()
	repository := imageRef.Context().RepositoryStr()
	username := ""
	password := ""
	if credentials != nil {
		username = credentials.GetUsername()
		password = credentials.GetPassword()
	}

	cacheKey := hash.Strings(registry, repository, username, password)

	s.pullerCacheMu.Lock()
	s.pullerCache.Remove(cacheKey)
	s.pullerCacheMu.Unlock()
}

func (s *OCIFetcherServer) CanAccess(ctx context.Context, req *ocifetcherpb.CanAccessRequest) (*ocifetcherpb.CanAccessResponse, error) {
	if req.GetReference() == "" {
		return nil, status.InvalidArgumentError("reference is required")
	}

	imageRef, err := gcrname.ParseReference(req.GetReference())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetReference(), err)
	}

	log.CtxInfof(ctx, "Checking access to registry %q", imageRef.Context().RegistryStr())

	remoteOpts := s.getRemoteOpts(ctx, req.GetCredentials())
	_, err = remote.Head(imageRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return &ocifetcherpb.CanAccessResponse{CanAccess: false}, nil
		}
		return nil, status.UnavailableErrorf("could not check access to remote registry: %s", err)
	}

	return &ocifetcherpb.CanAccessResponse{CanAccess: true}, nil
}

func (s *OCIFetcherServer) FetchManifest(ctx context.Context, req *ocifetcherpb.FetchManifestRequest) (*ocifetcherpb.FetchManifestResponse, error) {
	if req.GetRef() == "" {
		return nil, status.InvalidArgumentError("ref is required")
	}

	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	log.CtxInfof(ctx, "Fetching manifest for %q", imageRef)

	puller, err := s.getOrCreatePuller(ctx, imageRef, req.GetCredentials())
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}

	remoteDesc, err := puller.Get(ctx, imageRef)
	if err != nil {
		// Invalidate cache on error
		s.invalidatePullerCache(imageRef, req.GetCredentials())
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to retrieve image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	manifestBytes, err := remoteDesc.RawManifest()
	if err != nil {
		return nil, status.InternalErrorf("could not read manifest: %s", err)
	}

	return &ocifetcherpb.FetchManifestResponse{
		Manifest: manifestBytes,
	}, nil
}

func (s *OCIFetcherServer) FetchManifestMetadata(ctx context.Context, req *ocifetcherpb.FetchManifestMetadataRequest) (*ocifetcherpb.FetchManifestMetadataResponse, error) {
	if req.GetRef() == "" {
		return nil, status.InvalidArgumentError("ref is required")
	}

	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	log.CtxInfof(ctx, "Fetching manifest metadata for %q", imageRef)

	puller, err := s.getOrCreatePuller(ctx, imageRef, req.GetCredentials())
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}

	desc, err := puller.Head(ctx, imageRef)
	if err != nil {
		// Invalidate cache on error
		s.invalidatePullerCache(imageRef, req.GetCredentials())
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to access image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not get manifest metadata from remote: %s", err)
	}

	return &ocifetcherpb.FetchManifestMetadataResponse{
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
	}, nil
}

func (s *OCIFetcherServer) FetchBlob(req *ocifetcherpb.FetchBlobRequest, stream ocifetcherpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

	if req.GetRef() == "" {
		return status.InvalidArgumentError("ref is required")
	}

	ref, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return status.InvalidArgumentErrorf("invalid blob reference %q: %s", req.GetRef(), err)
	}

	blobRef, ok := ref.(gcrname.Digest)
	if !ok {
		return status.InvalidArgumentErrorf("blob reference must be a digest, got %q", req.GetRef())
	}

	log.CtxInfof(ctx, "Fetching blob for %q", blobRef)

	puller, err := s.getOrCreatePuller(ctx, blobRef, req.GetCredentials())
	if err != nil {
		return status.InternalErrorf("error creating puller: %s", err)
	}

	// For blobs, we need to fetch them as layers
	layer, err := puller.Layer(ctx, blobRef)
	if err != nil {
		// Invalidate cache on error
		s.invalidatePullerCache(blobRef, req.GetCredentials())
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return status.PermissionDeniedErrorf("not authorized to retrieve blob: %s", err)
		}
		return status.UnavailableErrorf("could not retrieve blob from remote: %s", err)
	}

	// Get the compressed layer data
	rc, err := layer.Compressed()
	if err != nil {
		return status.InternalErrorf("could not get compressed layer: %s", err)
	}
	defer rc.Close()

	// Stream the blob data in chunks
	buf := make([]byte, maxBlobChunkSize)
	for {
		n, err := rc.Read(buf)
		if err != nil && err != io.EOF {
			return status.InternalErrorf("error reading blob data: %s", err)
		}
		if n == 0 {
			break
		}

		resp := &ocifetcherpb.FetchBlobResponse{
			Data: buf[:n],
		}
		if err := stream.Send(resp); err != nil {
			return status.InternalErrorf("error streaming blob data: %s", err)
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

func (s *OCIFetcherServer) FetchBlobMetadata(ctx context.Context, req *ocifetcherpb.FetchBlobMetadataRequest) (*ocifetcherpb.FetchBlobMetadataResponse, error) {
	if req.GetRef() == "" {
		return nil, status.InvalidArgumentError("ref is required")
	}

	ref, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid blob reference %q: %s", req.GetRef(), err)
	}

	blobRef, ok := ref.(gcrname.Digest)
	if !ok {
		return nil, status.InvalidArgumentErrorf("blob reference must be a digest, got %q", req.GetRef())
	}

	log.CtxInfof(ctx, "Fetching blob metadata for %q", blobRef)

	puller, err := s.getOrCreatePuller(ctx, blobRef, req.GetCredentials())
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}

	// Fetch the layer to get metadata
	layer, err := puller.Layer(ctx, blobRef)
	if err != nil {
		// Invalidate cache on error
		s.invalidatePullerCache(blobRef, req.GetCredentials())
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to retrieve blob: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve blob from remote: %s", err)
	}

	size, err := layer.Size()
	if err != nil {
		return nil, status.InternalErrorf("could not get blob size: %s", err)
	}

	mediaType, err := layer.MediaType()
	if err != nil {
		return nil, status.InternalErrorf("could not get blob media type: %s", err)
	}

	return &ocifetcherpb.FetchBlobMetadataResponse{
		SizeBytes: size,
		MediaType: string(mediaType),
	}, nil
}
