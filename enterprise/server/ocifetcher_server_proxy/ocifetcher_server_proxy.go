// Package ocifetcher_server_proxy provides an OCIFetcherServer
// implementation that passes through requests to an upstream
// OCIFetcher service (in this case, the apps).
//
// For FetchBlob, the proxy checks the local byte stream cache before
// forwarding to the upstream. On upstream fetch, the blob is written
// to the local byte stream cache for future requests.
package ocifetcher_server_proxy

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/protobuf/proto"
)

var requireTrustedClient = flag.Bool("ocifetcher_server_proxy.require_trusted_client", false, "If true, the OCIFetcher proxy on the cache proxy rejects requests from clients that do not present an executor client identity. Requires a ClientIdentityService to be configured.")

const cacheDigestFunction = repb.DigestFunction_SHA256

type OCIFetcherServerProxy struct {
	remote        ofpb.OCIFetcherClient
	localBSClient bspb.ByteStreamClient
	// clientIdentityService is optional. When present, it is used to
	// gate trust of client-supplied size hints in FetchBlobRequest, and
	// (when --ocifetcher_server_proxy.require_trusted_client is set)
	// to reject requests from untrusted clients altogether.
	clientIdentityService interfaces.ClientIdentityService
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
	if *requireTrustedClient && env.GetClientIdentityService() == nil {
		return nil, status.FailedPreconditionError("OCIFetcherServerProxy requires a ClientIdentityService when --ocifetcher_server_proxy.require_trusted_client is set")
	}
	return &OCIFetcherServerProxy{
		remote:                env.GetOCIFetcherClient(),
		localBSClient:         env.GetLocalByteStreamClient(),
		clientIdentityService: env.GetClientIdentityService(),
	}, nil
}

// isTrustedClient reports whether the calling client's identity is one we
// will trust to provide accurate size hints in FetchBlobRequest. Only
// executors (and other cache proxies, for proxy-to-proxy hops) talk
// directly to this service in production. When no ClientIdentityService
// is configured, all callers are considered trusted.
func (s *OCIFetcherServerProxy) isTrustedClient(ctx context.Context) bool {
	if s.clientIdentityService == nil {
		return true
	}
	id, err := s.clientIdentityService.IdentityFromContext(ctx)
	if err != nil || id == nil {
		return false
	}
	return id.Client == interfaces.ClientIdentityExecutor ||
		id.Client == interfaces.ClientIdentityCacheProxy ||
		id.Client == interfaces.ClientIdentityWorkflow
}

func (s *OCIFetcherServerProxy) authorizeTrustedClient(ctx context.Context) error {
	if !*requireTrustedClient {
		return nil
	}
	if !s.isTrustedClient(ctx) {
		return status.PermissionDeniedError("OCIFetcher proxy is restricted to trusted internal clients")
	}
	return nil
}

func (s *OCIFetcherServerProxy) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	if err := s.authorizeTrustedClient(ctx); err != nil {
		return nil, err
	}
	return s.remote.FetchManifest(ctx, req)
}

func (s *OCIFetcherServerProxy) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	if err := s.authorizeTrustedClient(ctx); err != nil {
		return nil, err
	}
	return s.remote.FetchManifestMetadata(ctx, req)
}

func (s *OCIFetcherServerProxy) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	if err := s.authorizeTrustedClient(ctx); err != nil {
		return nil, err
	}
	return s.remote.FetchBlobMetadata(ctx, req)
}

func (s *OCIFetcherServerProxy) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

	if err := s.authorizeTrustedClient(ctx); err != nil {
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
	hash, err := gcr.NewHash(digestRef.DigestStr())
	if err != nil {
		return status.InvalidArgumentErrorf("invalid blob digest in reference %q: %s", req.GetRef(), err)
	}

	if req.GetSize() < 0 {
		return status.InvalidArgumentErrorf("invalid size %d in FetchBlobRequest", req.GetSize())
	}

	// Strip the client-supplied size hint when the caller isn't a
	// trusted internal client. A wrong size would not poison the local
	// BS cache (the bytestream server validates size+digest match the
	// streamed bytes), but it would let an untrusted caller force
	// failed cache writes, breaking caching for that {repo, hash}.
	// Also strip the hint from the request we forward upstream so the
	// apps don't trust it either.
	if !s.isTrustedClient(ctx) && (req.GetSize() != 0 || req.GetMediaType() != "") {
		log.CtxWarningf(ctx, "Ignoring size/media_type hints from untrusted client for %s", req.GetRef())
		req = cloneFetchBlobRequest(req)
		req.Size = 0
		req.MediaType = ""
	}

	// The local byte stream cache is keyed by {hash, size}; we need a size
	// to read or write it. Prefer the size supplied by the client (from
	// the referencing manifest's descriptor) and fall back to asking the
	// upstream OCIFetcher only when no hint was provided.
	size := req.GetSize()
	if size == 0 {
		metaResp, err := s.remote.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
			Ref:            req.GetRef(),
			Credentials:    req.GetCredentials(),
			BypassRegistry: req.GetBypassRegistry(),
		})
		if err != nil {
			return err
		}
		size = metaResp.GetSize()
	}

	// Fast path: serve from local BS cache.
	err = fetchBlobFromLocalBS(ctx, s.localBSClient, hash, size, &grpcStreamWriter{stream: stream})
	if err == nil {
		return nil // local cache hit
	}
	// Also check FailedPrecondition: cachetools.GetBlob wraps NotFound
	// cache misses as FailedPrecondition via MissingDigestError.
	if !status.IsNotFoundError(err) && !status.IsFailedPreconditionError(err) {
		return err
	}

	// Deduplicate concurrent upstream fetches for the same blob+creds.
	// The leader fetches from upstream and writes to local BSS.
	// After the singleflight completes, all callers stream from local BSS.
	key := ocicache.NewBlobFetchKey(digestRef.Context(), hash, req.GetCredentials())
	isLeader := false
	_, _, err = s.fetchGroup.Do(ctx, key, func(ctx context.Context) (struct{}, error) {
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
func (s *OCIFetcherServerProxy) fetchBlobFromUpstreamToLocalBS(ctx context.Context, req *ofpb.FetchBlobRequest, hash gcr.Hash, size int64) error {
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
func fetchBlobFromLocalBS(ctx context.Context, bsClient bspb.ByteStreamClient, hash gcr.Hash, size int64, w io.Writer) error {
	blobDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: size,
	}
	rn := digest.NewCASResourceName(blobDigest, "", cacheDigestFunction)
	rn.SetCompressor(repb.Compressor_ZSTD)
	return cachetools.GetBlob(ctx, bsClient, rn, w)
}

// newLocalBSWriter creates a cache writer for writing a blob to local BS.
func newLocalBSWriter(ctx context.Context, bsClient bspb.ByteStreamClient, hash gcr.Hash, size int64) (*cachetools.UploadWriter, error) {
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

// cloneFetchBlobRequest makes a shallow copy of a FetchBlobRequest. The
// proto type contains internal generated state we don't want to mutate on
// the caller-owned request value.
func cloneFetchBlobRequest(req *ofpb.FetchBlobRequest) *ofpb.FetchBlobRequest {
	if req == nil {
		return nil
	}
	return proto.Clone(req).(*ofpb.FetchBlobRequest)
}
