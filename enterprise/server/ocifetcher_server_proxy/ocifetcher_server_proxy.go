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
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const cacheDigestFunction = repb.DigestFunction_SHA256

type OCIFetcherServerProxy struct {
	remote        ofpb.OCIFetcherClient
	localBSClient bspb.ByteStreamClient
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
	return &OCIFetcherServerProxy{
		remote:        env.GetOCIFetcherClient(),
		localBSClient: env.GetLocalByteStreamClient(),
	}, nil
}

func (s *OCIFetcherServerProxy) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	return s.remote.FetchManifest(ctx, req)
}

func (s *OCIFetcherServerProxy) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	return s.remote.FetchManifestMetadata(ctx, req)
}

func (s *OCIFetcherServerProxy) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	return s.remote.FetchBlobMetadata(ctx, req)
}

func (s *OCIFetcherServerProxy) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

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

	// Fast path: serve from local BS cache. The local cache lookup is
	// hash-based, so the size in the resource name does not affect the
	// lookup result; use a placeholder.
	err = fetchBlobFromLocalBS(ctx, s.localBSClient, hash, localBSReadPlaceholderSize, &grpcStreamWriter{stream: stream})
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
		return struct{}{}, s.fetchBlobFromUpstreamToLocalBS(ctx, req, hash)
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
	return fetchBlobFromLocalBS(ctx, s.localBSClient, hash, localBSReadPlaceholderSize, &grpcStreamWriter{stream: stream})
}

// fetchBlobFromUpstreamToLocalBS fetches a blob from the upstream OCIFetcher
// and writes it to the local byte stream cache. It does not stream to any
// caller; callers read from local BSS after this completes.
//
// The blob size is read from the first FetchBlobResponse message rather than
// being fetched in a separate FetchBlobMetadata RPC. This avoids an extra
// upstream round trip (which today translates into a registry HEAD against
// the OCI registry).
func (s *OCIFetcherServerProxy) fetchBlobFromUpstreamToLocalBS(ctx context.Context, req *ofpb.FetchBlobRequest, hash gcr.Hash) error {
	remoteStream, err := s.remote.FetchBlob(ctx, req)
	if err != nil {
		return err
	}

	// Read the first message to learn the blob size before opening the
	// local-BS upload writer (which needs the size up front).
	first, err := remoteStream.Recv()
	if err != nil {
		if err == io.EOF {
			return status.UnavailableError("upstream FetchBlob returned no data and no size")
		}
		return err
	}
	size := first.GetSize()
	if size <= 0 {
		return status.UnavailableErrorf("upstream FetchBlob did not include blob size on first message")
	}

	cacheWriter, err := newLocalBSWriter(ctx, s.localBSClient, hash, size)
	if err != nil {
		return err
	}
	defer cacheWriter.Close()

	writeData := func(data []byte) error {
		_, writeErr := cacheWriter.Write(data)
		if writeErr != nil {
			if status.IsAlreadyExistsError(writeErr) {
				// Blob was cached by another writer; we're done.
				return io.EOF
			}
		}
		return writeErr
	}

	if data := first.GetData(); len(data) > 0 {
		if err := writeData(data); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}

	for {
		resp, err := remoteStream.Recv()
		if err == io.EOF {
			return cacheWriter.Commit()
		}
		if err != nil {
			return err
		}
		if err := writeData(resp.GetData()); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// localBSReadPlaceholderSize is used in the size component of the local-BS
// resource name when reading. The local cache lookup is hash-based, so the
// size in the resource name does not affect the lookup; we just need a
// non-empty value to satisfy URL parsing and avoid the empty-blob fast path.
const localBSReadPlaceholderSize = 1

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
