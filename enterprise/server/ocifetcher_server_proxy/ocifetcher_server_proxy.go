// Package ocifetcher_server_proxy provides an OCIFetcherServer
// implementation for the cache proxy.
//
// For groups in the cache_proxy.oci_fetch_from_registry experiment, the proxy
// fetches from remote registries itself, caching in its local cache, and
// only falls back to the upstream OCIFetcher service (the apps) if the
// registry can't be reached or rejects the proxy.
//
// Otherwise it passes requests through to the apps. For FetchBlob, it checks
// the local byte stream cache before forwarding to the upstream. On upstream
// fetch, the blob is written to the local byte stream cache for future
// requests.
package ocifetcher_server_proxy

import (
	"context"
	"io"
	"net"
	"sync/atomic"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocicache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/prometheus/client_golang/prometheus"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	cacheDigestFunction = repb.DigestFunction_SHA256

	fetchFromRegistryExperiment = "cache_proxy.oci_fetch_from_registry"
)

type OCIFetcherServerProxy struct {
	remote        ofpb.OCIFetcherClient
	localBSClient bspb.ByteStreamClient

	// registryFetcher fetches from remote registries itself, caching in the
	// proxy's local cache. It's nil if the proxy has no local action cache
	// client.
	registryFetcher ofpb.OCIFetcherServer
	efp             interfaces.ExperimentFlagProvider

	// fetchGroup deduplicates concurrent FetchBlob requests for the same
	// blob and credentials. The leader fetches from the upstream (apps)
	// and writes to local BSS; waiters block until the leader finishes,
	// then all callers stream from local BSS.
	fetchGroup singleflight.Group[ocicache.BlobFetchKey, struct{}]
}

func Register(env *real_environment.RealEnv) error {
	proxy, err := New(env)
	if err != nil {
		return status.InternalErrorf("Error initializing OCIFetcherServerProxy: %s", err)
	}
	env.SetOCIFetcherServer(proxy)
	return nil
}

func New(env environment.Env) (*OCIFetcherServerProxy, error) {
	if env.GetOCIFetcherClient() == nil {
		return nil, status.FailedPreconditionError("An OCIFetcherClient is required to enable the OCIFetcherServerProxy")
	}
	if env.GetLocalByteStreamClient() == nil {
		return nil, status.FailedPreconditionError("A LocalByteStreamClient is required to enable the OCIFetcherServerProxy")
	}
	proxy := &OCIFetcherServerProxy{
		remote:        env.GetOCIFetcherClient(),
		localBSClient: env.GetLocalByteStreamClient(),
		efp:           env.GetExperimentFlagProvider(),
	}
	if localACClient := env.GetLocalActionCacheClient(); localACClient != nil {
		registryFetcher, err := ocifetcher.NewServer(env.GetLocalByteStreamClient(), localACClient)
		if err != nil {
			return nil, err
		}
		proxy.registryFetcher = registryFetcher
	}
	return proxy, nil
}

// fetchFromRegistry returns whether the proxy should fetch from the remote
// registry itself, rather than through the apps.
//
// Requests that bypass the registry always go to the apps, which check that
// the caller is a server admin.
func (s *OCIFetcherServerProxy) fetchFromRegistry(ctx context.Context, bypassRegistry bool) bool {
	return s.registryFetcher != nil && !bypassRegistry && s.efp != nil && s.efp.Boolean(ctx, fetchFromRegistryExperiment, false)
}

// shouldFallBackToApps returns whether a request that failed when the proxy
// fetched from the registry itself should be retried through the apps. That's
// the case when the registry couldn't be reached, or rejected or rate-limited
// the proxy, which might be because it only accepts requests from the apps.
func shouldFallBackToApps(err error) bool {
	return status.IsUnavailableError(err) ||
		status.IsUnauthenticatedError(err) ||
		status.IsPermissionDeniedError(err) ||
		status.IsResourceExhaustedError(err)
}

func recordFallbackToApps(ctx context.Context, method, ref string, err error) {
	registry := registryLabel(ref)
	group := groupID(ctx)
	log.CtxWarningf(ctx, "OCI fetcher proxy: %s for %q from registry %q failed for group %q, falling back to the apps: %s", method, ref, registry, group, err)
	metrics.OCIFetcherProxyFallbackCount.With(prometheus.Labels{
		metrics.OCIFetcherMethodLabel:    method,
		metrics.GroupID:                  group,
		metrics.ImageFetchRegistryLabel:  registry,
		metrics.StatusHumanReadableLabel: status.MetricsLabel(err),
	}).Inc()
}

// registryLabel returns the eTLD+1 of the registry in ref, for use as a
// metric label.
func registryLabel(ref string) string {
	parsed, err := ctrname.ParseReference(ref)
	if err != nil {
		return "[UNKNOWN]"
	}
	host := parsed.Context().RegistryStr()
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return httpclient.HostLabel(host)
}

func groupID(ctx context.Context) string {
	if c, err := claims.ClaimsFromContext(ctx); err == nil {
		return c.GroupID
	}
	return interfaces.AuthAnonymousUser
}

func (s *OCIFetcherServerProxy) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	if s.fetchFromRegistry(ctx, req.GetBypassRegistry()) {
		resp, err := s.registryFetcher.FetchManifest(ctx, req)
		if !shouldFallBackToApps(err) {
			return resp, err
		}
		recordFallbackToApps(ctx, "FetchManifest", req.GetRef(), err)
	}
	return s.remote.FetchManifest(ctx, req)
}

func (s *OCIFetcherServerProxy) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	if s.fetchFromRegistry(ctx, req.GetBypassRegistry()) {
		resp, err := s.registryFetcher.FetchManifestMetadata(ctx, req)
		if !shouldFallBackToApps(err) {
			return resp, err
		}
		recordFallbackToApps(ctx, "FetchManifestMetadata", req.GetRef(), err)
	}
	return s.remote.FetchManifestMetadata(ctx, req)
}

func (s *OCIFetcherServerProxy) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	if s.fetchFromRegistry(ctx, req.GetBypassRegistry()) {
		resp, err := s.registryFetcher.FetchBlobMetadata(ctx, req)
		if !shouldFallBackToApps(err) {
			return resp, err
		}
		recordFallbackToApps(ctx, "FetchBlobMetadata", req.GetRef(), err)
	}
	return s.remote.FetchBlobMetadata(ctx, req)
}

func (s *OCIFetcherServerProxy) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

	if s.fetchFromRegistry(ctx, req.GetBypassRegistry()) {
		counter := &countingFetchBlobStream{OCIFetcher_FetchBlobServer: stream}
		err := s.registryFetcher.FetchBlob(req, counter)
		// Once bytes have been sent, falling back would replay the blob
		// from the start. Before that, also fall back on Internal errors,
		// which is how the fetcher reports failing to read the blob from
		// the registry.
		if counter.bytesSent.Load() > 0 || !(shouldFallBackToApps(err) || status.IsInternalError(err)) {
			return err
		}
		recordFallbackToApps(ctx, "FetchBlob", req.GetRef(), err)
	}

	digestRef, hash, err := parseBlobDigestRef(req.GetRef())
	if err != nil {
		return err
	}

	size, err := s.fetchBlobMetadataSize(ctx, req)
	if err != nil {
		return err
	}

	// Also check FailedPrecondition: cachetools.GetBlob wraps NotFound cache
	// misses as FailedPrecondition via MissingDigestError.
	if err := fetchBlobFromLocalBS(ctx, s.localBSClient, hash, size, &grpcStreamWriter{stream: stream}); err == nil {
		return nil // local cache hit
	} else if !status.IsNotFoundError(err) && !status.IsFailedPreconditionError(err) {
		return err
	}

	return s.dedupedFetchBlob(ctx, stream, digestRef, hash, size, req)
}

func parseBlobDigestRef(ref string) (ctrname.Digest, ctr.Hash, error) {
	blobRef, err := ctrname.ParseReference(ref)
	if err != nil {
		return ctrname.Digest{}, ctr.Hash{}, status.InvalidArgumentErrorf("invalid blob reference %q: %s", ref, err)
	}
	digestRef, ok := blobRef.(ctrname.Digest)
	if !ok {
		return ctrname.Digest{}, ctr.Hash{}, status.InvalidArgumentErrorf("blob reference must be a digest reference, got %q", ref)
	}
	hash, err := ctr.NewHash(digestRef.DigestStr())
	if err != nil {
		return ctrname.Digest{}, ctr.Hash{}, status.InvalidArgumentErrorf("invalid blob digest in reference %q: %s", ref, err)
	}
	return digestRef, hash, nil
}

func (s *OCIFetcherServerProxy) fetchBlobMetadataSize(ctx context.Context, req *ofpb.FetchBlobRequest) (int64, error) {
	metaResp, err := s.remote.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
		Ref:            req.GetRef(),
		Credentials:    req.GetCredentials(),
		BypassRegistry: req.GetBypassRegistry(),
	})
	if err != nil {
		return 0, err
	}
	return metaResp.GetSize(), nil
}

func (s *OCIFetcherServerProxy) dedupedFetchBlob(ctx context.Context, stream ofpb.OCIFetcher_FetchBlobServer, digestRef ctrname.Digest, hash ctr.Hash, size int64, req *ofpb.FetchBlobRequest) error {
	// Deduplicate concurrent upstream fetches for the same blob+creds.
	// The leader fetches from upstream and writes to local BSS.
	// After the singleflight completes, all callers stream from local BSS.
	key := ocicache.NewBlobFetchKey(digestRef.Context(), hash, req.GetCredentials())
	isLeader := false
	_, _, err := s.fetchGroup.Do(ctx, key, func(ctx context.Context) (struct{}, error) {
		isLeader = true
		return struct{}{}, s.fetchBlobFromUpstreamToLocalBS(ctx, req, hash, size)
	})
	if err != nil {
		return err
	}

	if isLeader {
		log.CtxInfof(ctx, "FetchBlob singleflight leader for %s, streaming from local BS", hash.Hex)
	} else {
		log.CtxInfof(ctx, "FetchBlob singleflight waiter for %s, streaming from local BS", hash.Hex)
	}

	// Stream the blob from local BSS to the caller.
	return fetchBlobFromLocalBS(ctx, s.localBSClient, hash, size, &grpcStreamWriter{stream: stream})
}

// fetchBlobFromUpstreamToLocalBS fetches a blob from the upstream OCIFetcher
// and writes it to the local byte stream cache. It does not stream to any
// caller; callers read from local BSS after this completes.
func (s *OCIFetcherServerProxy) fetchBlobFromUpstreamToLocalBS(ctx context.Context, req *ofpb.FetchBlobRequest, hash ctr.Hash, size int64) error {
	remoteStream, err := s.remote.FetchBlob(ctx, req)
	if err != nil {
		return err
	}

	cacheWriter, err := newLocalBSWriter(ctx, s.localBSClient, hash, size)
	if err != nil {
		return err
	}
	defer cacheWriter.Close()

	for {
		resp, err := remoteStream.Recv()
		if err == io.EOF {
			return cacheWriter.Commit()
		}
		if err != nil {
			return err
		}

		_, writeErr := cacheWriter.Write(resp.GetData())
		if writeErr != nil {
			if status.IsAlreadyExistsError(writeErr) {
				// Blob was cached by another writer; we're done.
				return nil
			}
			return writeErr
		}
	}
}

// fetchBlobFromLocalBS reads a blob from the local byte stream cache.
func fetchBlobFromLocalBS(ctx context.Context, bsClient bspb.ByteStreamClient, hash ctr.Hash, size int64, w io.Writer) error {
	blobDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: size,
	}
	rn := digest.NewCASResourceName(blobDigest, "", cacheDigestFunction)
	rn.SetCompressor(repb.Compressor_ZSTD)
	return cachetools.GetBlob(ctx, bsClient, rn, w)
}

// newLocalBSWriter creates a cache writer for writing a blob to local BS.
func newLocalBSWriter(ctx context.Context, bsClient bspb.ByteStreamClient, hash ctr.Hash, size int64) (*cachetools.UploadWriter, error) {
	blobDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: size,
	}
	rn := digest.NewCASResourceName(blobDigest, "", cacheDigestFunction)
	rn.SetCompressor(repb.Compressor_ZSTD)
	return cachetools.NewUploadWriter(ctx, bsClient, rn)
}

// countingFetchBlobStream counts the bytes sent through a FetchBlob stream.
// The fetcher may still be sending from another goroutine after FetchBlob
// returns (if the request was canceled while it was deduplicated with
// another one), so the count is atomic.
type countingFetchBlobStream struct {
	ofpb.OCIFetcher_FetchBlobServer
	bytesSent atomic.Int64
}

func (s *countingFetchBlobStream) Send(resp *ofpb.FetchBlobResponse) error {
	if err := s.OCIFetcher_FetchBlobServer.Send(resp); err != nil {
		return err
	}
	s.bytesSent.Add(int64(len(resp.GetData())))
	return nil
}

type grpcStreamWriter struct {
	stream ofpb.OCIFetcher_FetchBlobServer
}

func (w *grpcStreamWriter) Write(p []byte) (int, error) {
	if err := w.stream.Send(&ofpb.FetchBlobResponse{Data: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}
