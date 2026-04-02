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
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

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
		return nil, fmt.Errorf("An OCIFetcherClient is required to enable the OCIFetcherServerProxy")
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

	// If no local BS client is configured, fall back to simple pass-through.
	if s.localBSClient == nil {
		return s.fetchBlobPassthrough(ctx, req, stream)
	}

	// Parse the ref to extract repo and digest hash.
	hash, err := parseBlobDigest(req.GetRef())
	if err != nil {
		// If we can't parse, fall back to pass-through.
		log.CtxWarningf(ctx, "Could not parse blob ref for local cache lookup: %s", err)
		return s.fetchBlobPassthrough(ctx, req, stream)
	}

	// Get blob size from upstream metadata (needed for both local cache
	// read and write).
	metaResp, err := s.remote.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
		Ref:            req.GetRef(),
		Credentials:    req.GetCredentials(),
		BypassRegistry: req.GetBypassRegistry(),
	})
	if err != nil {
		return err
	}

	// Try local BS cache.
	err = fetchBlobFromLocalBS(ctx, s.localBSClient, hash, metaResp.GetSize(), &grpcStreamWriter{stream: stream})
	if err == nil {
		return nil // local cache hit
	}
	if !status.IsNotFoundError(err) {
		log.CtxWarningf(ctx, "Error reading blob from local cache: %s", err)
	}

	// Fetch from upstream and tee to local BS cache.
	return s.fetchBlobFromUpstreamAndCache(ctx, req, stream, hash, metaResp.GetSize())
}

// fetchBlobPassthrough relays a FetchBlob stream from upstream without
// local caching.
func (s *OCIFetcherServerProxy) fetchBlobPassthrough(ctx context.Context, req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	remoteStream, err := s.remote.FetchBlob(ctx, req)
	if err != nil {
		return err
	}
	for {
		resp, err := remoteStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// fetchBlobFromUpstreamAndCache streams a blob from the upstream OCIFetcher,
// sends each chunk to the caller, and writes the blob to local BS cache.
func (s *OCIFetcherServerProxy) fetchBlobFromUpstreamAndCache(ctx context.Context, req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer, hash gcr.Hash, size int64) error {
	remoteStream, err := s.remote.FetchBlob(ctx, req)
	if err != nil {
		return err
	}

	// Set up local cache writer.
	cacheWriter, cacheErr := newLocalBSWriter(ctx, s.localBSClient, hash, size)
	if cacheErr != nil {
		log.CtxWarningf(ctx, "Could not create local cache writer: %s", cacheErr)
	}
	defer func() {
		if cacheWriter != nil {
			cacheWriter.Close()
		}
	}()

	for {
		resp, err := remoteStream.Recv()
		if err == io.EOF {
			if cacheWriter != nil {
				if commitErr := cacheWriter.Commit(); commitErr != nil {
					log.CtxWarningf(ctx, "Error committing blob to local cache: %s", commitErr)
				}
			}
			return nil
		}
		if err != nil {
			return err
		}

		// Write to local cache (best-effort).
		if cacheWriter != nil {
			if _, writeErr := cacheWriter.Write(resp.GetData()); writeErr != nil {
				if !status.IsAlreadyExistsError(writeErr) {
					log.CtxWarningf(ctx, "Error writing blob to local cache: %s", writeErr)
				}
				cacheWriter.Close()
				cacheWriter = nil
			}
		}

		// Send to caller.
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// parseBlobDigest extracts the OCI digest hash from a blob reference string
// like "gcr.io/repo@sha256:abc123".
func parseBlobDigest(ref string) (gcr.Hash, error) {
	blobRef, err := gcrname.ParseReference(ref)
	if err != nil {
		return gcr.Hash{}, err
	}
	digestRef, ok := blobRef.(gcrname.Digest)
	if !ok {
		return gcr.Hash{}, fmt.Errorf("blob reference must be a digest reference, got %q", ref)
	}
	return gcr.NewHash(digestRef.DigestStr())
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
