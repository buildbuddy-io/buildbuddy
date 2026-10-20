// Package ocifetcher_server_proxy provides an OCIFetcherServer
// implementation that passes through requests to an upstream
// OCIFetcher service (in this case, the apps).
//
// For FetchBlob, the proxy checks the local byte stream cache before
// forwarding to the upstream. On upstream fetch, the blob is written
// to the local byte stream cache for future requests.
//
// When the cache_proxy.oci_fetcher.use_local_fetcher experiment is enabled,
// requests are instead served by a local OCI fetcher whose ByteStream and
// ActionCache clients loop back to this proxy's own gRPC servers, so cached
// manifests and blobs are read through and written through the proxy caches.
package ocifetcher_server_proxy

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocicache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"google.golang.org/grpc"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const cacheDigestFunction = repb.DigestFunction_SHA256

var useLocalFetcher = expflag.Bool("cache_proxy.oci_fetcher.use_local_fetcher", false, "If true, serve OCIFetcher requests with a local OCI fetcher that reads and writes through this proxy's own ByteStream and ActionCache servers, instead of forwarding them to the apps. Failed registry requests are retried on the forwarding path.")

type OCIFetcherServerProxy struct {
	remote        ofpb.OCIFetcherClient
	localBSClient bspb.ByteStreamClient
	// fetchGroup deduplicates concurrent FetchBlob requests for the same
	// blob and credentials. The leader fetches from the upstream (apps)
	// and writes to local BSS; waiters block until the leader finishes,
	// then all callers stream from local BSS.
	fetchGroup singleflight.Group[ocicache.BlobFetchKey, struct{}]

	// fetcher is a local OCI fetcher whose cache clients loop back to this
	// proxy's own public gRPC servers. It is nil if it couldn't be built.
	fetcher ofpb.OCIFetcherServer
}

func Register(env *real_environment.RealEnv) error {
	proxy, err := New(env)
	if err != nil {
		return status.InternalErrorf("Error initializing OCIFetcherServerProxy: %s", err)
	}
	proxy.initLocalFetcher(env, fmt.Sprintf("grpc://localhost:%d", grpc_server.GRPCPort()))
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

// initLocalFetcher builds the local OCI fetcher, with ByteStream and
// ActionCache clients connected to target. On failure it logs a warning and
// leaves the fetcher unset, so all requests take the forwarding path.
func (s *OCIFetcherServerProxy) initLocalFetcher(env environment.Env, target string, opts ...grpc.DialOption) {
	conn, err := grpc_client.DialInternalWithoutPooling(env, target, opts...)
	if err != nil {
		log.Warningf("OCIFetcherServerProxy: could not dial %s, local OCI fetcher disabled: %s", target, err)
		return
	}
	fetcher, err := ocifetcher.NewServer(bspb.NewByteStreamClient(conn), repb.NewActionCacheClient(conn))
	if err != nil {
		log.Warningf("OCIFetcherServerProxy: could not create local OCI fetcher, local OCI fetcher disabled: %s", err)
		conn.Close()
		return
	}
	s.fetcher = fetcher
}

// useFetcher returns whether a request should be served by the local fetcher.
// Requests that bypass the registry always take the forwarding path.
func (s *OCIFetcherServerProxy) useFetcher(ctx context.Context, bypassRegistry bool) bool {
	return s.fetcher != nil && !bypassRegistry && useLocalFetcher.Get(ctx)
}

// shouldFallBack returns whether a request that failed on the local fetcher
// with err should be retried on the forwarding path.
func shouldFallBack(ctx context.Context, err error) bool {
	switch {
	case status.IsUnavailableError(err),
		status.IsUnauthenticatedError(err),
		status.IsPermissionDeniedError(err),
		status.IsResourceExhaustedError(err):
		return true
	case status.IsDeadlineExceededError(err):
		return ctx.Err() == nil
	}
	return false
}

func recordRequest(method, path string, err error, start time.Time) {
	statusLabel := status.MetricsLabel(err)
	metrics.CacheProxyOCIFetcherRequestCount.WithLabelValues(method, path, statusLabel).Inc()
	metrics.CacheProxyOCIFetcherRequestDurationUsec.WithLabelValues(method, path, statusLabel).Observe(float64(time.Since(start).Microseconds()))
}

func recordFallback(ctx context.Context, method string, err error) {
	log.CtxInfof(ctx, "OCIFetcherServerProxy: local fetcher %s failed, falling back to remote: %s", method, err)
	metrics.CacheProxyOCIFetcherFallbackCount.WithLabelValues(method, status.MetricsLabel(err)).Inc()
}

// serveUnary serves a unary request with the local fetcher or the forwarding
// (legacy) path, falling back to the legacy path on registry errors.
func serveUnary[T any](ctx context.Context, s *OCIFetcherServerProxy, method string, bypassRegistry bool, fetcher, legacy func(context.Context) (T, error)) (T, error) {
	if s.useFetcher(ctx, bypassRegistry) {
		start := time.Now()
		resp, err := fetcher(ctx)
		recordRequest(method, metrics.OCIFetcherPathFetcher, err, start)
		if err == nil || !shouldFallBack(ctx, err) {
			return resp, err
		}
		recordFallback(ctx, method, err)
	}
	start := time.Now()
	resp, err := legacy(ctx)
	recordRequest(method, metrics.OCIFetcherPathLegacy, err, start)
	return resp, err
}

func (s *OCIFetcherServerProxy) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	return serveUnary(ctx, s, metrics.OCIFetcherMethodFetchManifest, req.GetBypassRegistry(),
		func(ctx context.Context) (*ofpb.FetchManifestResponse, error) {
			return s.fetcher.FetchManifest(ctx, req)
		},
		func(ctx context.Context) (*ofpb.FetchManifestResponse, error) {
			return s.remote.FetchManifest(ctx, req)
		})
}

func (s *OCIFetcherServerProxy) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	return serveUnary(ctx, s, metrics.OCIFetcherMethodFetchManifestMetadata, req.GetBypassRegistry(),
		func(ctx context.Context) (*ofpb.FetchManifestMetadataResponse, error) {
			return s.fetcher.FetchManifestMetadata(ctx, req)
		},
		func(ctx context.Context) (*ofpb.FetchManifestMetadataResponse, error) {
			return s.remote.FetchManifestMetadata(ctx, req)
		})
}

func (s *OCIFetcherServerProxy) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	return serveUnary(ctx, s, metrics.OCIFetcherMethodFetchBlobMetadata, req.GetBypassRegistry(),
		func(ctx context.Context) (*ofpb.FetchBlobMetadataResponse, error) {
			return s.fetcher.FetchBlobMetadata(ctx, req)
		},
		func(ctx context.Context) (*ofpb.FetchBlobMetadataResponse, error) {
			return s.remote.FetchBlobMetadata(ctx, req)
		})
}

// sendCountingStream counts Send attempts on a FetchBlob stream, so that a
// failed local fetch is only retried if nothing was sent to the caller.
type sendCountingStream struct {
	ofpb.OCIFetcher_FetchBlobServer
	sends atomic.Int64
}

func (s *sendCountingStream) Send(resp *ofpb.FetchBlobResponse) error {
	s.sends.Add(1)
	return s.OCIFetcher_FetchBlobServer.Send(resp)
}

func (s *OCIFetcherServerProxy) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()
	method := metrics.OCIFetcherMethodFetchBlob
	if s.useFetcher(ctx, req.GetBypassRegistry()) {
		start := time.Now()
		countingStream := &sendCountingStream{OCIFetcher_FetchBlobServer: stream}
		err := s.fetcher.FetchBlob(req, countingStream)
		recordRequest(method, metrics.OCIFetcherPathFetcher, err, start)
		if err == nil || countingStream.sends.Load() > 0 || !shouldFallBack(ctx, err) {
			return err
		}
		recordFallback(ctx, method, err)
	}
	start := time.Now()
	err := s.legacyFetchBlob(req, stream)
	recordRequest(method, metrics.OCIFetcherPathLegacy, err, start)
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
