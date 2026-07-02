// Package ocifetcher provides an OCIFetcherServer
// that fetches OCI blobs and manifests from remote registries.
package ocifetcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocicache"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
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
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	blobChunkSize       = 256 * 1000 // 256 KB to match cachetools buffer size
	pullerLRUMaxEntries = 1000

	accessProofCacheTTL        = 15 * time.Minute
	accessProofCacheMaxEntries = 1000
)

var (
	enabled           = flag.Bool("ocifetcher.enabled", false, "Whether to enable the OCI fetcher service.")
	mirrors           = flag.Slice("executor.container_registry_mirrors", []interfaces.MirrorConfig{}, "")
	allowedPrivateIPs = flag.Slice("executor.container_registry_allowed_private_ips", []string{}, "Allowed private IP ranges for container registries. Private IPs are disallowed by default.")

	blobBufPool = bytebufferpool.VariableSize(blobChunkSize)
)

type ociFetcherServer struct {
	allowedPrivateIPs []*net.IPNet
	mirrors           []interfaces.MirrorConfig

	bsClient bspb.ByteStreamClient
	acClient repb.ActionCacheClient

	mu        sync.Mutex
	pullerLRU lru.LRU[*pullerLRUEntry]

	// accessProofCache records repository+credentials pairs for which a
	// registry access check recently succeeded. Pull authorization is
	// repository-scoped, so one success lets every manifest and blob in the
	// repo skip the registry for those credentials until the entry expires.
	accessProofCache lru.LRU[struct{}]

	// blobFetchGroup deduplicates concurrent blob fetch requests.
	// Only one request fetches from upstream and writes to cache;
	// other requests wait and then read from cache using the returned
	// content length.
	blobFetchGroup singleflight.Group[ocicache.BlobFetchKey, int64]
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
	pullerLRU, err := lru.New[*pullerLRUEntry](&lru.Config[*pullerLRUEntry]{
		SizeFn:  func(_ *pullerLRUEntry) int64 { return 1 },
		MaxSize: int64(pullerLRUMaxEntries),
	})
	if err != nil {
		return nil, status.InternalErrorf("error initializing puller cache: %s", err)
	}
	accessProofCache, err := lru.New[struct{}](&lru.Config[struct{}]{
		SizeFn:     func(_ struct{}) int64 { return 1 },
		MaxSize:    int64(accessProofCacheMaxEntries),
		TTL:        accessProofCacheTTL,
		ThreadSafe: true,
	})
	if err != nil {
		return nil, status.InternalErrorf("error initializing access proof cache: %s", err)
	}
	return &ociFetcherServer{
		allowedPrivateIPs: allowedPrivateIPs,
		mirrors:           Mirrors(),
		bsClient:          bsClient,
		acClient:          acClient,
		pullerLRU:         pullerLRU,
		accessProofCache:  accessProofCache,
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
// FetchBlob guarantees there will be at most one request to the upstream remote registry
// open at a time.
// Requests that arrive while the blob is being fetched from an upstream remote registry
// will wait until that fetch completes.
//
// Requests may have a bypass_registry flag set.
// Server admins can bypass the registry: the blob will be streamed from the byte stream
// server if present, and FetchBlob will not fall back to the remote registry.
func (s *ociFetcherServer) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

	if err := validateBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return err
	}
	digestRef, hash, err := parseBlobDigestRef(req.GetRef())
	if err != nil {
		return err
	}

	if req.GetBypassRegistry() {
		err := s.streamBlobFromCache(ctx, stream, digestRef.Context(), hash)
		if err == nil {
			return nil
		}
		if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Error fetching blob from cache: %s", err)
			return err
		}
		return status.NotFoundErrorf("bypassing registry, but blob %q not found in cache", digestRef)
	}
	return s.dedupedFetchBlob(ctx, stream, digestRef, hash, req.GetManifestRef(), req.GetCredentials())
}

// FetchBlobMetadata returns OCI blob metadata (size, media type).
// It will first read this metadata from the action cache, falling back
// to the upstream remote registry.
//
// Requests may have a bypass_registry flag set.
// Server admins can bypass the registry: the metadata will be served from the action cache
// if present. If not present, FetchBlobMetadata will not fall back to the remote registry.
func (s *ociFetcherServer) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	if err := validateBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return nil, err
	}
	digestRef, hash, err := parseBlobDigestRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	repo := digestRef.Context()

	accessKey := repoAccessKey(repo, req.GetCredentials())
	proven := req.GetBypassRegistry() || s.accessProofCache.Contains(accessKey)
	// A manifest ref hint proves access via a manifest metadata HEAD, and its
	// descriptor for the blob can serve the metadata itself. Both matter for
	// registries that reject HEAD requests on blob endpoints (e.g.
	// public.ecr.aws).
	var manifestDesc *ctr.Descriptor
	if !proven && req.GetManifestRef() != "" {
		desc, err := s.proveBlobAccessViaManifest(ctx, digestRef, hash, req.GetManifestRef(), req.GetCredentials())
		if err == nil {
			proven = true
			manifestDesc = desc
		} else if status.IsUnauthenticatedError(err) || status.IsPermissionDeniedError(err) {
			return nil, err
		} else {
			log.CtxDebugf(ctx, "Could not prove access to blob %q via manifest ref %q, falling back to blob metadata: %s", digestRef, req.GetManifestRef(), err)
		}
	}
	if proven {
		if resp, err := s.fetchBlobMetadataFromCache(ctx, digestRef, hash); err == nil {
			return resp, nil
		} else if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Error fetching blob metadata from cache: %s", err)
		}
		if req.GetBypassRegistry() {
			return nil, status.NotFoundErrorf("bypassing registry, but blob metadata for %q not found in cache", digestRef)
		}
		if manifestDesc != nil {
			return &ofpb.FetchBlobMetadataResponse{
				Size:      manifestDesc.Size,
				MediaType: string(manifestDesc.MediaType),
			}, nil
		}
	}

	resp, err := s.fetchBlobMetadataFromRemote(ctx, digestRef, req.GetCredentials())
	if err != nil {
		return nil, err
	}
	s.accessProofCache.Add(accessKey, struct{}{})
	return resp, nil
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
	if err := validateBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return nil, err
	}
	imageRef, err := parseManifestRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	hash, err := s.resolveManifestDigest(ctx, imageRef, req.GetCredentials(), req.GetBypassRegistry())
	if err != nil {
		return nil, err
	}
	if resp, err := s.fetchManifestFromCache(ctx, imageRef, hash); err == nil {
		return resp, nil
	} else if !status.IsNotFoundError(err) {
		log.CtxWarningf(ctx, "Error fetching manifest from cache: %s", err)
	}
	if req.GetBypassRegistry() {
		return nil, status.NotFoundErrorf("bypassing registry, but manifest for %q not found in cache", imageRef)
	}
	return s.fetchManifestFromRemoteWriteToCache(ctx, imageRef, hash, req.GetCredentials())
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
	if err := validateUnsupportedBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return nil, err
	}
	imageRef, err := parseManifestRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	resp, err := s.fetchManifestMetadataFromRemote(ctx, imageRef, req.GetCredentials())
	if err != nil {
		return nil, err
	}
	s.accessProofCache.Add(repoAccessKey(imageRef.Context(), req.GetCredentials()), struct{}{})
	return resp, nil
}

// validateBypassRegistry checks if bypass_registry is enabled and if so,
// verifies the caller has server admin permissions. Returns an error if
// bypass_registry is true but the caller is not a server admin.
func validateBypassRegistry(ctx context.Context, bypassRegistry bool) error {
	if !bypassRegistry {
		return nil
	}
	if err := claims.AuthorizeServerAdmin(ctx); err != nil {
		return status.PermissionDeniedErrorf("not authorized to bypass registry: %s", err)
	}
	return nil
}

func parseBlobDigestRef(ref string) (ctrname.Digest, ctr.Hash, error) {
	blobRef, err := ctrname.ParseReference(ref)
	if err != nil {
		return ctrname.Digest{}, ctr.Hash{}, status.InvalidArgumentErrorf("invalid blob reference %q: %s", ref, err)
	}
	digestRef, ok := blobRef.(ctrname.Digest)
	if !ok {
		return ctrname.Digest{}, ctr.Hash{}, status.InvalidArgumentErrorf("blob reference must be a digest reference (e.g., repo@sha256:...), got %q", ref)
	}
	hash, err := ctr.NewHash(digestRef.DigestStr())
	if err != nil {
		return ctrname.Digest{}, ctr.Hash{}, status.InvalidArgumentErrorf("invalid digest format %q: %s", digestRef.DigestStr(), err)
	}
	return digestRef, hash, nil
}

func (s *ociFetcherServer) streamBlobFromCache(ctx context.Context, stream ofpb.OCIFetcher_FetchBlobServer, repo ctrname.Repository, hash ctr.Hash) error {
	metadata, err := ocicache.FetchBlobMetadataFromCache(ctx, s.bsClient, s.acClient, repo, hash)
	if err != nil {
		return err
	}
	return ocicache.FetchBlobFromCache(ctx, &grpcStreamWriter{stream: stream}, s.bsClient, hash, metadata.GetContentLength())
}

func (s *ociFetcherServer) dedupedFetchBlob(ctx context.Context, stream ofpb.OCIFetcher_FetchBlobServer, digestRef ctrname.Digest, hash ctr.Hash, manifestRef string, creds *rgpb.Credentials) error {
	start := time.Now()
	repo := digestRef.Context()
	key := ocicache.NewBlobFetchKey(repo, hash, creds)
	isLeader := false
	contentLength, _, err := s.blobFetchGroup.Do(ctx, key, func(ctx context.Context) (int64, error) {
		isLeader = true

		// Cached metadata proves the content-addressed blob exists; we only need
		// to prove the caller may access the repo before serving it from cache.
		metadata, err := ocicache.FetchBlobMetadataFromCache(ctx, s.bsClient, s.acClient, repo, hash)
		if err == nil {
			if err := s.proveBlobAccessWithManifestHint(ctx, digestRef, hash, manifestRef, creds); err != nil {
				return 0, err
			}
			contentLength := metadata.GetContentLength()
			cacheWriter := &grpcStreamWriter{stream: stream}
			err := ocicache.FetchBlobFromCache(ctx, cacheWriter, s.bsClient, hash, contentLength)
			if err == nil {
				return contentLength, nil
			}
			// Only recover if no bytes were streamed. Once bytes have been sent,
			// falling back would corrupt the response by replaying from offset 0.
			if cacheWriter.bytesWritten > 0 {
				return 0, err
			}
			log.CtxWarningf(ctx, "Blob metadata was cached but reading blob %q from cache failed before streaming bytes; falling back to registry: %s", digestRef, err)
		} else if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Error looking up blob metadata in cache; falling back to registry: %s", err)
		}

		// Cache miss: fetch from the registry, which proves access and writes
		// the blob through to the cache.
		size, err := s.fetchBlobFromRemoteWriteToCacheAndResponse(ctx, digestRef, repo, hash, manifestRef, creds, stream)
		if err == nil {
			s.accessProofCache.Add(repoAccessKey(repo, creds), struct{}{})
		}
		return size, err
	})

	if isLeader {
		recordFetchBlobMetrics(metrics.OCIFetcherRoleLeader, err, time.Since(start))
		return err
	}
	if err != nil {
		recordFetchBlobMetrics(metrics.OCIFetcherRoleWaiter, err, time.Since(start))
		return err
	}
	err = ocicache.FetchBlobFromCache(ctx, &grpcStreamWriter{stream: stream}, s.bsClient, hash, contentLength)
	recordFetchBlobMetrics(metrics.OCIFetcherRoleWaiter, err, time.Since(start))
	return err
}

func recordFetchBlobMetrics(role string, err error, duration time.Duration) {
	statusLabel := metrics.OCIFetcherStatusOK
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || status.IsDeadlineExceededError(err) {
			statusLabel = metrics.OCIFetcherStatusTimeout
		} else if errors.Is(err, context.Canceled) || status.IsCanceledError(err) {
			statusLabel = metrics.OCIFetcherStatusCanceled
		} else {
			statusLabel = metrics.OCIFetcherStatusError
		}
	}
	metrics.OCIFetcherRequestCount.WithLabelValues(metrics.OCIFetcherMethodFetchBlob, role, statusLabel).Inc()
	metrics.OCIFetcherRequestDurationUsec.WithLabelValues(metrics.OCIFetcherMethodFetchBlob, role).Observe(float64(duration.Microseconds()))
}

// fetchBlobFromRemoteWriteToCacheAndResponse fetches a blob from the upstream
// registry, streams it to the response, and writes it to the cache
// simultaneously using read-through caching.
// It returns the content length of the blob (0 if metadata was unavailable).
func (s *ociFetcherServer) fetchBlobFromRemoteWriteToCacheAndResponse(ctx context.Context, digestRef ctrname.Digest, repo ctrname.Repository, hash ctr.Hash, manifestRef string, creds *rgpb.Credentials, stream ofpb.OCIFetcher_FetchBlobServer) (int64, error) {
	// All HTTP-triggering calls (Compressed, MediaType, Size) must be
	// inside the retry scope so that token refresh covers them, not just
	// the lazy Layer() reference creation.
	//
	// MediaType and Size are fetched before Compressed so that there is
	// no open ReadCloser to leak if they fail and trigger a retry.
	// They are best-effort: failures are logged but don't prevent
	// streaming the blob data. Caching is skipped when metadata is
	// unavailable.
	var mediaType string
	var size int64
	rc, err := withPullerRetry(ctx, s, digestRef, creds, func(puller *remote.Puller) (io.ReadCloser, error) {
		layer, err := puller.Layer(ctx, digestRef)
		if err != nil {
			return nil, err
		}
		// Best-effort metadata for read-through caching.
		if mt, err := layer.MediaType(); err != nil {
			log.CtxWarningf(ctx, "Could not get media type for layer: %s", err)
		} else {
			mediaType = string(mt)
		}
		if sz, err := layer.Size(); err == nil {
			size = sz
		} else if _, desc, descErr := s.verifiedBlobDescriptorFromCachedManifest(ctx, digestRef, hash, manifestRef); descErr == nil {
			// Some registries (e.g. public.ecr.aws) reject the HEAD requests
			// layer.Size() makes on blob endpoints. The digest-verified cached
			// manifest is an authoritative source for the blob's descriptor.
			size = desc.Size
			mediaType = string(desc.MediaType)
		} else {
			log.CtxWarningf(ctx, "Could not get size for layer: HEAD: %s; cached manifest: %s", err, descErr)
		}
		return layer.Compressed()
	})
	if err != nil {
		return 0, err
	}

	// streamAndClose streams from reader and closes it when done.
	streamAndClose := func(r io.ReadCloser) error {
		defer r.Close()
		return s.streamBlob(r, stream)
	}

	// Skip caching when metadata is unavailable.
	if mediaType == "" || size == 0 {
		return 0, streamAndClose(rc)
	}

	// cachedRC wraps rc and takes ownership (closes it).
	cachedRC, err := ocicache.NewBlobReadThroughCacher(ctx, rc, s.bsClient, s.acClient, repo, hash, mediaType, size)
	if err != nil {
		log.CtxWarningf(ctx, "Error creating read-through cacher: %s", err)
		return size, streamAndClose(rc)
	}
	return size, streamAndClose(cachedRC)
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

func (s *ociFetcherServer) fetchBlobMetadataFromCache(ctx context.Context, digestRef ctrname.Digest, hash ctr.Hash) (*ofpb.FetchBlobMetadataResponse, error) {
	metadata, err := ocicache.FetchBlobMetadataFromCache(ctx, s.bsClient, s.acClient, digestRef.Context(), hash)
	if err != nil {
		return nil, err
	}
	return &ofpb.FetchBlobMetadataResponse{
		Size:      metadata.GetContentLength(),
		MediaType: metadata.GetContentType(),
	}, nil
}

func (s *ociFetcherServer) fetchBlobMetadataFromRemote(ctx context.Context, digestRef ctrname.Digest, creds *rgpb.Credentials) (*ofpb.FetchBlobMetadataResponse, error) {
	return withPullerRetry(ctx, s, digestRef, creds, func(puller *remote.Puller) (*ofpb.FetchBlobMetadataResponse, error) {
		layer, err := puller.Layer(ctx, digestRef)
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
		return &ofpb.FetchBlobMetadataResponse{
			Size:      size,
			MediaType: string(mediaType),
		}, nil
	})
}

func parseManifestRef(ref string) (ctrname.Reference, error) {
	imageRef, err := ctrname.ParseReference(ref)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", ref, err)
	}
	return imageRef, nil
}

func (s *ociFetcherServer) resolveManifestDigest(ctx context.Context, imageRef ctrname.Reference, creds *rgpb.Credentials, bypassRegistry bool) (ctr.Hash, error) {
	if digestRef, ok := imageRef.(ctrname.Digest); ok {
		hash, err := ctr.NewHash(digestRef.DigestStr())
		if err != nil {
			return ctr.Hash{}, status.InvalidArgumentErrorf("invalid digest format %q: %s", digestRef.DigestStr(), err)
		}
		if !bypassRegistry {
			if err := s.proveManifestAccess(ctx, imageRef, creds); err != nil {
				return ctr.Hash{}, err
			}
		}
		return hash, nil
	}
	if bypassRegistry {
		return ctr.Hash{}, status.NotFoundErrorf("bypassing registry, but cannot resolve tag ref %q from cache", imageRef)
	}
	return s.resolveTagToDigest(ctx, imageRef, creds)
}

// proveManifestAccess checks that the caller's credentials allow accessing
// the given manifest ref in the remote registry.
func (s *ociFetcherServer) proveManifestAccess(ctx context.Context, imageRef ctrname.Reference, creds *rgpb.Credentials) error {
	repo := imageRef.Context()
	if s.accessProofCache.Contains(repoAccessKey(repo, creds)) {
		return nil
	}
	if _, err := withPullerRetry(ctx, s, imageRef, creds, func(puller *remote.Puller) (*ctr.Descriptor, error) {
		return puller.Head(ctx, imageRef)
	}); err != nil {
		return err
	}
	s.accessProofCache.Add(repoAccessKey(repo, creds), struct{}{})
	return nil
}

// proveBlobAccess checks that the caller's credentials allow accessing the
// blob's repository in the remote registry (via a metadata HEAD).
func (s *ociFetcherServer) proveBlobAccess(ctx context.Context, digestRef ctrname.Digest, creds *rgpb.Credentials) error {
	repo := digestRef.Context()
	if s.accessProofCache.Contains(repoAccessKey(repo, creds)) {
		return nil
	}
	if _, err := s.fetchBlobMetadataFromRemote(ctx, digestRef, creds); err != nil {
		return err
	}
	s.accessProofCache.Add(repoAccessKey(repo, creds), struct{}{})
	return nil
}

// proveBlobAccessWithManifestHint checks that the caller's credentials allow
// accessing the blob's repository in the remote registry. When the caller
// provided a manifest ref hint, access is proven via a manifest metadata HEAD
// instead of a blob metadata HEAD, which works on registries that reject HEAD
// requests on blob endpoints (e.g. public.ecr.aws). If the hint is unusable
// (e.g. the manifest is not cached, or does not reference the blob), access
// is proven via the blob directly.
func (s *ociFetcherServer) proveBlobAccessWithManifestHint(ctx context.Context, digestRef ctrname.Digest, hash ctr.Hash, manifestRef string, creds *rgpb.Credentials) error {
	repo := digestRef.Context()
	if s.accessProofCache.Contains(repoAccessKey(repo, creds)) {
		return nil
	}
	if manifestRef != "" {
		_, err := s.proveBlobAccessViaManifest(ctx, digestRef, hash, manifestRef, creds)
		if err == nil {
			return nil
		}
		// The registry denied the caller access to a manifest in the blob's
		// repository; a blob metadata request would be denied the same way.
		if status.IsUnauthenticatedError(err) || status.IsPermissionDeniedError(err) {
			return err
		}
		log.CtxDebugf(ctx, "Could not prove access to blob %q via manifest ref %q, falling back to blob metadata: %s", digestRef, manifestRef, err)
	}
	return s.proveBlobAccess(ctx, digestRef, creds)
}

// proveBlobAccessViaManifest checks that the caller's credentials allow
// accessing the manifest identified by manifestRef, after verifying that the
// manifest actually references the blob. Since registry pull authorization is
// repository-scoped, access to the manifest proves access to the blob.
// On success it returns the manifest's descriptor for the blob.
func (s *ociFetcherServer) proveBlobAccessViaManifest(ctx context.Context, digestRef ctrname.Digest, hash ctr.Hash, manifestRef string, creds *rgpb.Credentials) (*ctr.Descriptor, error) {
	mRef, desc, err := s.verifiedBlobDescriptorFromCachedManifest(ctx, digestRef, hash, manifestRef)
	if err != nil {
		return nil, err
	}
	if err := s.proveManifestAccess(ctx, mRef, creds); err != nil {
		return nil, err
	}
	return desc, nil
}

// verifiedBlobDescriptorFromCachedManifest returns the blob's descriptor from
// the cached manifest identified by manifestRef. The manifest ref is supplied
// by the caller and cannot be trusted, so it must be a digest reference in
// the blob's repository, the cached manifest's contents must match that
// digest, and the manifest must reference the blob as its config or one of
// its layers. The descriptor of a manifest that passes these checks is
// authoritative for the blob, but proves nothing about registry access:
// callers granting access to the blob's contents must still prove access to
// the manifest against the remote registry.
func (s *ociFetcherServer) verifiedBlobDescriptorFromCachedManifest(ctx context.Context, digestRef ctrname.Digest, hash ctr.Hash, manifestRef string) (ctrname.Digest, *ctr.Descriptor, error) {
	if manifestRef == "" {
		return ctrname.Digest{}, nil, status.NotFoundError("no manifest ref provided")
	}
	repo := digestRef.Context()
	ref, err := ctrname.ParseReference(manifestRef)
	if err != nil {
		return ctrname.Digest{}, nil, status.InvalidArgumentErrorf("invalid manifest ref %q: %s", manifestRef, err)
	}
	mRef, ok := ref.(ctrname.Digest)
	if !ok {
		return ctrname.Digest{}, nil, status.InvalidArgumentErrorf("manifest ref must be a digest reference (e.g., repo@sha256:...), got %q", manifestRef)
	}
	if mRef.Context().Name() != repo.Name() {
		return ctrname.Digest{}, nil, status.InvalidArgumentErrorf("manifest ref %q is not in blob repository %q", manifestRef, repo.Name())
	}
	mHash, err := ctr.NewHash(mRef.DigestStr())
	if err != nil {
		return ctrname.Digest{}, nil, status.InvalidArgumentErrorf("invalid manifest digest %q: %s", mRef.DigestStr(), err)
	}
	cached, err := ocicache.FetchManifestFromAC(ctx, s.acClient, repo, mHash, mRef)
	if err != nil {
		return ctrname.Digest{}, nil, err
	}
	contentHash, _, err := ctr.SHA256(bytes.NewReader(cached.GetRaw()))
	if err != nil {
		return ctrname.Digest{}, nil, err
	}
	if mHash.Algorithm != "sha256" || contentHash.Hex != mHash.Hex {
		return ctrname.Digest{}, nil, status.FailedPreconditionErrorf("cached manifest contents do not match digest %q", mRef.DigestStr())
	}
	manifest, err := ctr.ParseManifest(bytes.NewReader(cached.GetRaw()))
	if err != nil {
		return ctrname.Digest{}, nil, status.InvalidArgumentErrorf("could not parse cached manifest %q: %s", manifestRef, err)
	}
	if manifest.Config.Digest == hash {
		return mRef, &manifest.Config, nil
	}
	for _, desc := range manifest.Layers {
		if desc.Digest == hash {
			return mRef, &desc, nil
		}
	}
	return ctrname.Digest{}, nil, status.NotFoundErrorf("manifest %q does not reference blob %q", manifestRef, digestRef)
}

func (s *ociFetcherServer) resolveTagToDigest(ctx context.Context, imageRef ctrname.Reference, creds *rgpb.Credentials) (ctr.Hash, error) {
	desc, err := withPullerRetry(ctx, s, imageRef, creds, func(puller *remote.Puller) (*ctr.Descriptor, error) {
		return puller.Head(ctx, imageRef)
	})
	if err != nil {
		return ctr.Hash{}, err
	}
	s.accessProofCache.Add(repoAccessKey(imageRef.Context(), creds), struct{}{})
	hash, err := ctr.NewHash(desc.Digest.String())
	if err != nil {
		return ctr.Hash{}, status.InvalidArgumentErrorf("invalid resolved digest %q: %s", desc.Digest.String(), err)
	}
	return hash, nil
}

func (s *ociFetcherServer) fetchManifestFromCache(ctx context.Context, imageRef ctrname.Reference, hash ctr.Hash) (*ofpb.FetchManifestResponse, error) {
	cached, err := ocicache.FetchManifestFromAC(ctx, s.acClient, imageRef.Context(), hash, imageRef)
	if err != nil {
		return nil, err
	}
	return &ofpb.FetchManifestResponse{
		Digest:    hash.String(),
		Size:      int64(len(cached.GetRaw())),
		MediaType: cached.GetContentType(),
		Manifest:  cached.GetRaw(),
	}, nil
}

func (s *ociFetcherServer) fetchManifestFromRemoteWriteToCache(ctx context.Context, imageRef ctrname.Reference, hash ctr.Hash, creds *rgpb.Credentials) (*ofpb.FetchManifestResponse, error) {
	remoteDesc, err := withPullerRetry(ctx, s, imageRef, creds, func(puller *remote.Puller) (*remote.Descriptor, error) {
		return puller.Get(ctx, imageRef)
	})
	if err != nil {
		return nil, err
	}
	if err := ocicache.WriteManifestToAC(ctx, remoteDesc.Manifest, s.acClient, imageRef.Context(), hash, string(remoteDesc.MediaType), imageRef); err != nil {
		log.CtxWarningf(ctx, "Error writing manifest to cache: %s", err)
	}
	return &ofpb.FetchManifestResponse{
		Digest:    remoteDesc.Digest.String(),
		Size:      remoteDesc.Size,
		MediaType: string(remoteDesc.MediaType),
		Manifest:  remoteDesc.Manifest,
	}, nil
}

// validateUnsupportedBypassRegistry is used by FetchManifestMetadata which does not support
// bypass_registry at all (it always needs registry access for credential validation).
func validateUnsupportedBypassRegistry(ctx context.Context, bypassRegistry bool) error {
	if !bypassRegistry {
		return nil
	}
	if err := claims.AuthorizeServerAdmin(ctx); err != nil {
		return status.PermissionDeniedErrorf("authorize bypass_registry: %s", err)
	}
	return status.NotFoundError("bypass_registry is not yet supported")
}

func (s *ociFetcherServer) fetchManifestMetadataFromRemote(ctx context.Context, imageRef ctrname.Reference, creds *rgpb.Credentials) (*ofpb.FetchManifestMetadataResponse, error) {
	desc, err := withPullerRetry(ctx, s, imageRef, creds, func(puller *remote.Puller) (*ctr.Descriptor, error) {
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

func (s *ociFetcherServer) getOrCreatePuller(ctx context.Context, imageRef ctrname.Reference, creds *rgpb.Credentials) (*remote.Puller, error) {
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

func (s *ociFetcherServer) evictPuller(imageRef ctrname.Reference, creds *rgpb.Credentials) {
	key := pullerKey(imageRef, creds)
	s.mu.Lock()
	s.pullerLRU.Remove(key)
	s.mu.Unlock()
}

// repoAccessKey returns the access-proof cache key for the given repository and
// credentials. Registry pull authorization is repository-scoped, so the key is
// deliberately not specific to any one manifest or blob digest.
func repoAccessKey(repo ctrname.Repository, creds *rgpb.Credentials) string {
	if creds == nil {
		return hash.Strings(repo.Name(), "", "")
	}
	return hash.Strings(repo.Name(), creds.GetUsername(), creds.GetPassword())
}

func pullerKey(ref ctrname.Reference, creds *rgpb.Credentials) string {
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
	ref ctrname.Reference,
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

type grpcStreamWriter struct {
	stream       ofpb.OCIFetcher_FetchBlobServer
	bytesWritten int64
}

func (w *grpcStreamWriter) Write(p []byte) (int, error) {
	if err := w.stream.Send(&ofpb.FetchBlobResponse{Data: p}); err != nil {
		return 0, status.WrapError(err, "send")
	}
	w.bytesWritten += int64(len(p))
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
