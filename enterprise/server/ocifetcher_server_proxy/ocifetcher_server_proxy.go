// Package ocifetcher_server_proxy provides an OCIFetcherServer
// implementation that passes through requests to an upstream
// OCIFetcher service (in this case, the apps).
//
// Eventually, this proxy service will
//   - serve blobs from the local byte stream server and
//   - singleflight requests for the same blob or manifest,
// reducing the amount of network traffic to the public internet
// and byte stream servers in the cache proxies and apps
// when multiple executors fetch the same uncached OCI image simultaneously.
package ocifetcher_server_proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

const (
	blobOutputFilePath         = "_bb_ociregistry_blob_"
	actionResultInstanceName   = interfaces.OCIImageInstanceNamePrefix
	cacheDigestFunction        = repb.DigestFunction_SHA256
)

type OCIFetcherServerProxy struct {
	local         interfaces.ByteStreamServer
	localACServer repb.ActionCacheServer
	remote        ofpb.OCIFetcherClient
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
	local := env.GetLocalByteStreamServer()
	if local == nil {
		return nil, fmt.Errorf("A local ByteStreamServer is required to enable the OCIFetcherServerProxy")
	}
	localACServer := env.GetLocalActionCacheServer()
	if localACServer == nil {
		return nil, fmt.Errorf("A local ActionCacheServer is required to enable the OCIFetcherServerProxy")
	}
	return &OCIFetcherServerProxy{
		local:         local,
		localACServer: localACServer,
		remote:        env.GetOCIFetcherClient(),
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

	// Parse the digest and repo from the ref
	hash, repo, err := parseOCIBlobRef(req.GetRef())
	if err != nil {
		log.CtxDebugf(ctx, "Could not parse OCI blob ref %q: %v", req.GetRef(), err)
		return s.fetchBlobFromRemote(ctx, req, stream)
	}

	// Try to get blob size from local action cache
	size, err := s.getBlobSizeFromLocalAC(ctx, repo, hash)
	if err == nil && size > 0 {
		// Local metadata exists - read from local (fail if local read fails)
		return s.fetchBlobFromLocal(ctx, hash, size, stream)
	}

	// No local metadata, fall back to remote
	return s.fetchBlobFromRemote(ctx, req, stream)
}

// fetchBlobToByteStreamAdapter adapts an OCIFetcher_FetchBlobServer to a
// bspb.ByteStream_ReadServer for use with local.ReadCASResource.
type fetchBlobToByteStreamAdapter struct {
	grpc.ServerStream
	fetchBlobStream ofpb.OCIFetcher_FetchBlobServer
}

func (a *fetchBlobToByteStreamAdapter) Send(resp *bspb.ReadResponse) error {
	return a.fetchBlobStream.Send(&ofpb.FetchBlobResponse{Data: resp.GetData()})
}

func (a *fetchBlobToByteStreamAdapter) Context() context.Context {
	return a.fetchBlobStream.Context()
}

// parseOCIBlobRef parses an OCI blob reference and returns the digest hash and repository.
// The ref format is like "gcr.io/org/repo@sha256:abc123..."
func parseOCIBlobRef(ref string) (gcr.Hash, gcrname.Repository, error) {
	blobRef, err := gcrname.ParseReference(ref)
	if err != nil {
		return gcr.Hash{}, gcrname.Repository{}, status.InvalidArgumentErrorf("invalid blob reference %q: %s", ref, err)
	}

	digestRef, ok := blobRef.(gcrname.Digest)
	if !ok {
		return gcr.Hash{}, gcrname.Repository{}, status.InvalidArgumentErrorf("blob reference must be a digest reference, got %q", ref)
	}

	gcrHash, err := gcr.NewHash(digestRef.DigestStr())
	if err != nil {
		return gcr.Hash{}, gcrname.Repository{}, status.InvalidArgumentErrorf("invalid digest format %q: %s", digestRef.DigestStr(), err)
	}

	return gcrHash, digestRef.Context(), nil
}

// getBlobSizeFromLocalAC looks up the blob size from the local action cache.
// This mirrors the key computation in ocicache.FetchBlobMetadataFromCache.
func (s *OCIFetcherServerProxy) getBlobSizeFromLocalAC(ctx context.Context, repo gcrname.Repository, hash gcr.Hash) (int64, error) {
	arKey := &ocipb.OCIActionResultKey{
		Registry:      repo.RegistryStr(),
		Repository:    repo.RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return 0, err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		return 0, err
	}

	req := &repb.GetActionResultRequest{
		InstanceName:   actionResultInstanceName,
		ActionDigest:   arDigest,
		DigestFunction: cacheDigestFunction,
	}
	ar, err := s.localACServer.GetActionResult(ctx, req)
	if err != nil {
		return 0, err
	}

	// Extract blob size from output files
	for _, outputFile := range ar.GetOutputFiles() {
		if outputFile.GetPath() == blobOutputFilePath {
			return outputFile.GetDigest().GetSizeBytes(), nil
		}
	}
	return 0, status.NotFoundErrorf("blob size not found in action result for %s", repo)
}

// fetchBlobFromLocal reads the blob from the local ByteStream server.
func (s *OCIFetcherServerProxy) fetchBlobFromLocal(ctx context.Context, hash gcr.Hash, sizeBytes int64, stream ofpb.OCIFetcher_FetchBlobServer) error {
	d := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: sizeBytes,
	}
	rn := digest.NewCASResourceName(d, "", cacheDigestFunction)

	adapter := &fetchBlobToByteStreamAdapter{
		fetchBlobStream: stream,
	}
	return s.local.ReadCASResource(ctx, rn, 0, 0, adapter)
}

// fetchBlobFromRemote fetches the blob from the remote OCIFetcher service.
func (s *OCIFetcherServerProxy) fetchBlobFromRemote(ctx context.Context, req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
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
