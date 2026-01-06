// Package ocifetcher provides an OCIFetcherServer
// that fetches OCI blobs and manifests from remote registries.
package ocifetcher

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	blobChunkSize       = 256 * 1000 // 256 KB to match cachetools buffer size
	pullerLRUMaxEntries = 1000
)

var (
	enabled           = flag.Bool("ocifetcher.enabled", false, "Whether to enable the OCI fetcher service.")
	mirrors           = flag.Slice("executor.container_registry_mirrors", []interfaces.MirrorConfig{}, "")
	allowedPrivateIPs = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")

	blobBufPool = bytebufferpool.VariableSize(blobChunkSize)
)

// blobFetchKey identifies a blob fetch for deduplication.
// Includes registry, repository, digest, and credentials hash.
type blobFetchKey struct {
	registry   string
	repository string
	digest     string
	credsHash  string
}

func blobDedupeKey(repo gcrname.Repository, h gcr.Hash, creds *rgpb.Credentials) blobFetchKey {
	credsHash := ""
	if creds != nil {
		credsHash = hash.Strings(creds.GetUsername(), creds.GetPassword())
	}
	return blobFetchKey{
		registry:   repo.RegistryStr(),
		repository: repo.RepositoryStr(),
		digest:     h.String(),
		credsHash:  credsHash,
	}
}

type ociFetcherServer struct {
	allowedPrivateIPs []*net.IPNet
	mirrors           []interfaces.MirrorConfig

	bsClient bspb.ByteStreamClient
	acClient repb.ActionCacheClient

	mu        sync.Mutex
	pullerLRU *lru.LRU[*pullerLRUEntry]

	// blobFetchGroup deduplicates concurrent blob fetch requests.
	// Only one request fetches from upstream and writes to cache;
	// other requests wait and then read from cache.
	blobFetchGroup singleflight.Group[blobFetchKey, struct{}]
}

// NewServer constructs an OCIFetcherServer that
// fetches OCI blobs and manifests from remote registries.
//
// bsClient and acClient are required for blob caching.
//
// It is preferred to construct only one server, so that there is only
// one Puller cache per process.
func NewServer(bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient) (ofpb.OCIFetcherServer, error) {
	if bsClient == nil {
		return nil, status.FailedPreconditionError("OCIFetcherServer requires a non-nil byte stream client")
	}
	if acClient == nil {
		return nil, status.FailedPreconditionError("OCIFetcherServer requires a non-nil action cache client")
	}
	allowedPrivateIPs, err := ParseAllowedPrivateIPs()
	if err != nil {
		return nil, err
	}
	pullerLRU, err := lru.NewLRU[*pullerLRUEntry](&lru.Config[*pullerLRUEntry]{
		SizeFn:  func(_ *pullerLRUEntry) int64 { return 1 },
		MaxSize: int64(pullerLRUMaxEntries),
	})
	if err != nil {
		return nil, status.InternalErrorf("error initializing puller cache: %s", err)
	}
	return &ociFetcherServer{
		allowedPrivateIPs: allowedPrivateIPs,
		mirrors:           Mirrors(),
		bsClient:          bsClient,
		acClient:          acClient,
		pullerLRU:         pullerLRU,
	}, nil
}

func RegisterServer(env *real_environment.RealEnv) error {
	if !*enabled {
		return nil
	}
	server, err := NewServer(env.GetByteStreamClient(), env.GetActionCacheClient())
	if err != nil {
		return err
	}
	env.SetOCIFetcherServer(server)
	return nil
}

// FetchBlob streams an OCI blob from the byte stream server if present.
// Otherwise it streams the blob from the upstream remote registry, writing
// to the byte stream server at the same time.
//
// Requests may have a bypass_registry flag set.
// Server admins can bypass the registry: the blob will be streamed from the byte stream
// server if present, and FetchBlob will not fall back to the remote registry.
func (s *ociFetcherServer) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

	if err := authorizeBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return err
	}

	blobRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return status.InvalidArgumentErrorf("invalid blob reference %q: %s", req.GetRef(), err)
	}

	digestRef, ok := blobRef.(gcrname.Digest)
	if !ok {
		return status.InvalidArgumentErrorf("blob reference must be a digest reference, got %q", req.GetRef())
	}

	repo := digestRef.Context()
	hash, err := gcr.NewHash(digestRef.DigestStr())
	if err != nil {
		return status.InvalidArgumentErrorf("invalid digest format %q: %s", digestRef.DigestStr(), err)
	}

	err = s.fetchBlobFromCache(ctx, stream, repo, hash)
	if err == nil {
		return nil
	}
	if !status.IsNotFoundError(err) {
		// It is possible this error occurred while writing to the stream.
		// Since we do not know the state of the stream, it is not safe
		// to write bytes to the stream past this point.
		log.CtxWarningf(ctx, "Error fetching blob from cache: %s", err)
		return err
	}

	if req.GetBypassRegistry() {
		return status.NotFoundErrorf("bypassing registry, but blob %q not found in cache", blobRef)
	}

	// Singleflight: leader streams from upstream to response while caching,
	// followers wait and then read from cache.
	key := blobDedupeKey(repo, hash, req.GetCredentials())
	var isLeader bool
	_, _, err = s.blobFetchGroup.Do(ctx, key, func(ctx context.Context) (struct{}, error) {
		isLeader = true
		return struct{}{}, s.fetchBlobFromRemoteWriteToCacheAndResponse(ctx, digestRef, repo, hash, req.GetCredentials(), stream)
	})
	if err != nil {
		return err
	}

	if isLeader {
		// We ran the callback - already streamed to our response.
		return nil
	}

	// We were a follower - read from cache.
	return s.fetchBlobFromCache(ctx, stream, repo, hash)
}

// FetchBlobMetadata returns OCI blob metadata (size, media type).
// It will first read this metadata from the action cache, falling back
// to the upstream remote registry.
//
// Requests may have a bypass_registry flag set.
// Server admins can bypass the registry: the metadata will be served from the action cache
// if present. If not present, FetchBlobMetadata will not fall back to the remote registry.
func (s *ociFetcherServer) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	if err := authorizeBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return nil, err
	}

	blobRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid blob reference %q: %s", req.GetRef(), err)
	}

	digestRef, ok := blobRef.(gcrname.Digest)
	if !ok {
		return nil, status.InvalidArgumentErrorf("blob reference must be a digest reference (e.g., repo@sha256:...), got %q", req.GetRef())
	}

	repo := digestRef.Context()
	hash, err := gcr.NewHash(digestRef.DigestStr())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid digest format %q: %s", digestRef.DigestStr(), err)
	}

	metadata, err := ocicache.FetchBlobMetadataFromCache(ctx, s.bsClient, s.acClient, repo, hash)
	if err == nil {
		return &ofpb.FetchBlobMetadataResponse{
			Size:      metadata.GetContentLength(),
			MediaType: metadata.GetContentType(),
		}, nil
	}
	if !status.IsNotFoundError(err) {
		log.CtxWarningf(ctx, "Error fetching blob metadata from cache: %s", err)
	}

	if req.GetBypassRegistry() {
		return nil, status.NotFoundErrorf("bypassing registry, but blob metadata for %q not found in cache", blobRef)
	}

	layer, err := withPullerRetry(ctx, s, blobRef, req.GetCredentials(), func(puller *remote.Puller) (gcr.Layer, error) {
		return puller.Layer(ctx, digestRef)
	})
	if err != nil {
		return nil, err
	}

	size, sizeErr := layer.Size()
	if sizeErr != nil {
		return nil, wrapError(sizeErr, "error getting layer size")
	}
	mediaType, mtErr := layer.MediaType()
	if mtErr != nil {
		return nil, wrapError(mtErr, "error getting layer media type")
	}
	return &ofpb.FetchBlobMetadataResponse{
		Size:      size,
		MediaType: string(mediaType),
	}, nil
}

// FetchManifest returns an OCI manifest from the action cache if present,
// falling back to the remote registry if not present.
// FetchManifest will write the manifest contents to the action cache
// after reading from the remote registry.
//
// Requests may have a bypass_registry flag set.
// Server admins can bypass the registry: the manifest will be served from the action cache
// if present. If not present, FetchManifest will not fall back to the remote registry.
func (s *ociFetcherServer) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	if err := authorizeBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return nil, err
	}

	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	var hash gcr.Hash
	if digestRef, ok := imageRef.(gcrname.Digest); ok {
		hash, err = gcr.NewHash(digestRef.DigestStr())
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid digest format %q: %s", digestRef.DigestStr(), err)
		}
	} else {
		if req.GetBypassRegistry() {
			return nil, status.NotFoundErrorf("bypassing registry, but cannot resolve tag ref %q from cache", imageRef)
		}

		desc, err := withPullerRetry(ctx, s, imageRef, req.GetCredentials(), func(puller *remote.Puller) (*gcr.Descriptor, error) {
			return puller.Head(ctx, imageRef)
		})
		if err != nil {
			return nil, err
		}
		hash, err = gcr.NewHash(desc.Digest.String())
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid resolved digest %q: %s", desc.Digest.String(), err)
		}
	}

	repo := imageRef.Context()
	cached, err := ocicache.FetchManifestFromAC(ctx, s.acClient, repo, hash, imageRef)
	if err == nil {
		return &ofpb.FetchManifestResponse{
			Digest:    hash.String(),
			Size:      int64(len(cached.GetRaw())),
			MediaType: cached.GetContentType(),
			Manifest:  cached.GetRaw(),
		}, nil
	}
	if !status.IsNotFoundError(err) {
		log.CtxWarningf(ctx, "Error fetching manifest from cache: %s", err)
	}

	if req.GetBypassRegistry() {
		return nil, status.NotFoundErrorf("bypassing registry, but manifest for %q not found in cache", imageRef)
	}

	remoteDesc, err := withPullerRetry(ctx, s, imageRef, req.GetCredentials(), func(puller *remote.Puller) (*remote.Descriptor, error) {
		return puller.Get(ctx, imageRef)
	})
	if err != nil {
		return nil, err
	}

	if err := ocicache.WriteManifestToAC(ctx, remoteDesc.Manifest, s.acClient, repo, hash, string(remoteDesc.MediaType), imageRef); err != nil {
		log.CtxWarningf(ctx, "Error writing manifest to cache: %s", err)
	}

	return &ofpb.FetchManifestResponse{
		Digest:    remoteDesc.Digest.String(),
		Size:      remoteDesc.Size,
		MediaType: string(remoteDesc.MediaType),
		Manifest:  remoteDesc.Manifest,
	}, nil
}

// FetchManifestMetadata fetches metadata (digest, size, media type) for an OCI manifest
// from a remote registry.
//
// FetchManifestMetadata does not read from or write to the action cache or byte stream server.
// Callers may rely on FetchManifestMetadata returning successfully as an indication
// that the input credentials grant access to the OCI image in the remote registry.
// Bypassing the registry is not possible. Requests that set the bypass_registry flag
// will fail with an error.
func (s *ociFetcherServer) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	if err := checkBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return nil, err
	}
	imageRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", req.GetRef(), err)
	}

	desc, err := withPullerRetry(ctx, s, imageRef, req.GetCredentials(), func(puller *remote.Puller) (*gcr.Descriptor, error) {
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

func (s *ociFetcherServer) getRemoteOpts(ctx context.Context, creds *rgpb.Credentials) []remote.Option {
	opts := []remote.Option{remote.WithContext(ctx)}

	if creds != nil && creds.GetUsername() != "" && creds.GetPassword() != "" {
		opts = append(opts, remote.WithAuth(&authn.Basic{
			Username: creds.GetUsername(),
			Password: creds.GetPassword(),
		}))
	}

	tr := httpclient.New(s.allowedPrivateIPs, "oci_fetcher").Transport

	if len(s.mirrors) > 0 {
		opts = append(opts, remote.WithTransport(NewMirrorTransport(tr, s.mirrors)))
	} else {
		opts = append(opts, remote.WithTransport(tr))
	}

	return opts
}

func (s *ociFetcherServer) getOrCreatePuller(ctx context.Context, imageRef gcrname.Reference, creds *rgpb.Credentials) (*remote.Puller, error) {
	key := pullerKey(imageRef, creds)

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.pullerLRU.Get(key)

	if ok {
		return entry.puller, nil
	}

	remoteOpts := s.getRemoteOpts(ctx, creds)
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}
	s.pullerLRU.Add(key, &pullerLRUEntry{puller: puller})

	return puller, nil
}

func (s *ociFetcherServer) evictPuller(imageRef gcrname.Reference, creds *rgpb.Credentials) {
	key := pullerKey(imageRef, creds)
	s.mu.Lock()
	s.pullerLRU.Remove(key)
	s.mu.Unlock()
}

// fetchBlobFromCache attempts to fetch a blob from the cache and streams it directly to the gRPC response.
// Returns nil if successful, NotFoundError if not in cache, or another error on failure.
func (s *ociFetcherServer) fetchBlobFromCache(ctx context.Context, stream ofpb.OCIFetcher_FetchBlobServer, repo gcrname.Repository, hash gcr.Hash) error {
	metadata, err := ocicache.FetchBlobMetadataFromCache(ctx, s.bsClient, s.acClient, repo, hash)
	if err != nil {
		return err
	}
	w := &grpcStreamWriter{stream: stream}
	return ocicache.FetchBlobFromCache(ctx, w, s.bsClient, hash, metadata.GetContentLength())
}

// fetchBlobFromRemoteWriteToCacheAndResponse fetches a blob from the upstream registry, streams it to the
// response, and writes it to the cache simultaneously using read-through caching.
func (s *ociFetcherServer) fetchBlobFromRemoteWriteToCacheAndResponse(ctx context.Context, digestRef gcrname.Digest, repo gcrname.Repository, hash gcr.Hash, creds *rgpb.Credentials, stream ofpb.OCIFetcher_FetchBlobServer) error {
	// Double-check cache to handle race between initial check and singleflight entry.
	if err := s.fetchBlobFromCache(ctx, stream, repo, hash); err == nil {
		return nil
	} else if !status.IsNotFoundError(err) {
		return err
	}

	// Fetch from upstream
	layer, err := withPullerRetry(ctx, s, digestRef, creds, func(puller *remote.Puller) (gcr.Layer, error) {
		return puller.Layer(ctx, digestRef)
	})
	if err != nil {
		return err
	}

	rc, err := layer.Compressed()
	if err != nil {
		return wrapError(err, "error getting compressed layer")
	}
	// Note: rc is closed by cachedRC.Close() in the happy path,
	// or by defer rc.Close() in the early-return paths below.

	mediaType, err := layer.MediaType()
	if err != nil {
		defer rc.Close()
		log.CtxWarningf(ctx, "Could not get media type for layer: %s", err)
		return status.InternalErrorf("could not cache blob: %s", err)
	}

	size, err := layer.Size()
	if err != nil {
		defer rc.Close()
		log.CtxWarningf(ctx, "Could not get size for layer: %s", err)
		return status.InternalErrorf("could not cache blob: %s", err)
	}

	// Read-through cacher: streams to response AND writes to cache
	cachedRC, err := ocicache.NewBlobReadThroughCacher(ctx, rc, s.bsClient, s.acClient, repo, hash, string(mediaType), size)
	if err != nil {
		defer rc.Close()
		log.CtxWarningf(ctx, "Error creating read-through cacher: %s", err)
		return status.InternalErrorf("could not cache blob: %s", err)
	}
	defer cachedRC.Close()

	return s.streamBlob(cachedRC, stream)
}

// streamBlob reads from rc and streams the data to the gRPC stream in chunks.
func (s *ociFetcherServer) streamBlob(rc io.Reader, stream ofpb.OCIFetcher_FetchBlobServer) error {
	buf := blobBufPool.Get(blobChunkSize)
	defer blobBufPool.Put(buf)
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&ofpb.FetchBlobResponse{Data: buf[:n]}); sendErr != nil {
				return status.WrapError(sendErr, "send")
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.InternalErrorf("error reading blob: %s", err)
		}
	}
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

// withPullerRetry handles the common pattern of executing an operation with a puller,
// evicting and retrying on failure due to expired tokens.
func withPullerRetry[T any](
	ctx context.Context,
	s *ociFetcherServer,
	ref gcrname.Reference,
	creds *rgpb.Credentials,
	op func(puller *remote.Puller) (T, error),
) (T, error) {
	var zero T

	puller, err := s.getOrCreatePuller(ctx, ref, creds)
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

	// Pullers from the LRU may have expired Bearer tokens, so we evict and retry on most errors.
	s.evictPuller(ref, creds)
	puller, err = s.getOrCreatePuller(ctx, ref, creds)
	if err != nil {
		return zero, err
	}

	result, err = op(puller)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return zero, err
	}
	if err != nil {
		s.evictPuller(ref, creds)
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return zero, status.UnauthenticatedErrorf("not authorized to access resource: %s", err)
		}
		return zero, status.UnavailableErrorf("could not fetch from remote registry: %s", err)
	}

	return result, nil
}

// authorizeBypassRegistry checks if bypass_registry is enabled and if so,
// verifies the caller has server admin permissions. Returns an error if
// bypass_registry is true but the caller is not a server admin.
func authorizeBypassRegistry(ctx context.Context, bypassRegistry bool) error {
	if !bypassRegistry {
		return nil
	}
	if err := claims.AuthorizeServerAdmin(ctx); err != nil {
		return status.PermissionDeniedErrorf("not authorized to bypass registry: %s", err)
	}
	return nil
}

// checkBypassRegistry is used by FetchManifestMetadata which does not support
// bypass_registry at all (it always needs registry access for credential validation).
func checkBypassRegistry(ctx context.Context, bypassRegistry bool) error {
	if !bypassRegistry {
		return nil
	}
	if err := claims.AuthorizeServerAdmin(ctx); err != nil {
		return status.PermissionDeniedErrorf("authorize bypass_registry: %s", err)
	}
	return status.NotFoundError("bypass_registry is not yet supported")
}

// wrapError wraps an error, converting 401 Unauthorized to Unauthenticated.
func wrapError(err error, message string) error {
	if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
		return status.UnauthenticatedErrorf("not authorized to access resource: %s", err)
	}
	return status.InternalErrorf("%s: %s", message, err)
}

type grpcStreamWriter struct {
	stream ofpb.OCIFetcher_FetchBlobServer
}

func (w *grpcStreamWriter) Write(p []byte) (int, error) {
	if err := w.stream.Send(&ofpb.FetchBlobResponse{Data: p}); err != nil {
		return 0, status.WrapError(err, "send")
	}
	return len(p), nil
}

type pullerLRUEntry struct {
	puller *remote.Puller
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
