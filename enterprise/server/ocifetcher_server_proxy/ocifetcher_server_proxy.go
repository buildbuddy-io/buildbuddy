// Package ocifetcher_server_proxy provides an OCIFetcherServer
// implementation that passes through requests to an upstream
// OCIFetcher service (in this case, the apps).
//
// For FetchBlob, the proxy checks the local byte stream cache before
// forwarding to the upstream. On upstream fetch, the blob is written
// to the local byte stream cache for future requests.
//
// When cache_proxy.oci_fetcher.use_local_fetcher is enabled, requests are
// instead served by a local OCI fetcher that caches through this proxy.
package ocifetcher_server_proxy

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocicache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const cacheDigestFunction = repb.DigestFunction_SHA256

var useLocalFetcher = expflag.Bool("cache_proxy.oci_fetcher.use_local_fetcher", false, "If true, serve OCIFetcher requests with a local OCI fetcher that reads and writes through this proxy's own ByteStream and ActionCache servers, instead of forwarding them to the apps.")

type OCIFetcherServerProxy struct {
	remote        ofpb.OCIFetcherClient
	localBSClient bspb.ByteStreamClient
	// fetchGroup deduplicates concurrent FetchBlob requests for the same
	// blob and credentials. The leader fetches from the upstream (apps)
	// and writes to local BSS; waiters block until the leader finishes,
	// then all callers stream from local BSS.
	fetchGroup singleflight.Group[ocicache.BlobFetchKey, struct{}]

	// fetcher is a local OCI fetcher whose cache clients loop back to this
	// proxy's own public gRPC servers.
	fetcher ofpb.OCIFetcherServer
}

// Register creates the OCIFetcherServerProxy and sets it as env's
// OCIFetcherServer. bsClient and acClient are the cache clients used by the
// local OCI fetcher; they should point at this proxy's own gRPC servers.
func Register(env *real_environment.RealEnv, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient) error {
	// Reuse one instance across ports so they share one singleflight group.
	if env.GetOCIFetcherServer() != nil {
		return nil
	}
	proxy, err := New(env)
	if err != nil {
		return status.InternalErrorf("Error initializing OCIFetcherServerProxy: %s", err)
	}
	fetcher, err := ocifetcher.NewServer(bsClient, acClient)
	if err != nil {
		return status.InternalErrorf("Error initializing OCIFetcherServerProxy local fetcher: %s", err)
	}
	proxy.fetcher = fetcher
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
	return &OCIFetcherServerProxy{
		remote:        env.GetOCIFetcherClient(),
		localBSClient: env.GetLocalByteStreamClient(),
	}, nil
}

// useFetcher returns whether a request should be served by the local fetcher.
// Requests that bypass the registry always take the forwarding path.
func (s *OCIFetcherServerProxy) useFetcher(ctx context.Context, bypassRegistry bool) bool {
	return !bypassRegistry && useLocalFetcher.Get(ctx)
}

func recordRequest(method, path string, err error) {
	metrics.CacheProxyOCIFetcherRequestCount.WithLabelValues(method, path, status.MetricsLabel(err)).Inc()
}

func (s *OCIFetcherServerProxy) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	method := metrics.OCIFetcherMethodFetchManifest
	if s.useFetcher(ctx, req.GetBypassRegistry()) {
		resp, err := s.fetcher.FetchManifest(ctx, req)
		recordRequest(method, metrics.OCIFetcherPathFetcher, err)
		return resp, err
	}
	resp, err := s.remote.FetchManifest(ctx, req)
	recordRequest(method, metrics.OCIFetcherPathLegacy, err)
	return resp, err
}

func (s *OCIFetcherServerProxy) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	method := metrics.OCIFetcherMethodFetchManifestMetadata
	if s.useFetcher(ctx, req.GetBypassRegistry()) {
		resp, err := s.fetcher.FetchManifestMetadata(ctx, req)
		recordRequest(method, metrics.OCIFetcherPathFetcher, err)
		return resp, err
	}
	resp, err := s.remote.FetchManifestMetadata(ctx, req)
	recordRequest(method, metrics.OCIFetcherPathLegacy, err)
	return resp, err
}

func (s *OCIFetcherServerProxy) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	method := metrics.OCIFetcherMethodFetchBlobMetadata
	if s.useFetcher(ctx, req.GetBypassRegistry()) {
		resp, err := s.fetcher.FetchBlobMetadata(ctx, req)
		recordRequest(method, metrics.OCIFetcherPathFetcher, err)
		return resp, err
	}
	resp, err := s.remote.FetchBlobMetadata(ctx, req)
	recordRequest(method, metrics.OCIFetcherPathLegacy, err)
	return resp, err
}

func (s *OCIFetcherServerProxy) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()
	method := metrics.OCIFetcherMethodFetchBlob
	if s.useFetcher(ctx, req.GetBypassRegistry()) {
		err := s.fetcher.FetchBlob(req, stream)
		recordRequest(method, metrics.OCIFetcherPathFetcher, err)
		return err
	}
	err := s.legacyFetchBlob(req, stream)
	recordRequest(method, metrics.OCIFetcherPathLegacy, err)
	return err
}

// legacyFetchBlob serves FetchBlob from the local byte stream cache, falling
// back to the upstream (apps) OCIFetcher.
func (s *OCIFetcherServerProxy) legacyFetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

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

type grpcStreamWriter struct {
	stream ofpb.OCIFetcher_FetchBlobServer
}

func (w *grpcStreamWriter) Write(p []byte) (int, error) {
	if err := w.stream.Send(&ofpb.FetchBlobResponse{Data: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}
