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
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

const (
	cacheDigestFunction = repb.DigestFunction_SHA256
)

type OCIFetcherServerProxy struct {
	local  interfaces.ByteStreamServer
	remote ofpb.OCIFetcherClient
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
	return &OCIFetcherServerProxy{
		local:  local,
		remote: env.GetOCIFetcherClient(),
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

	metadataResp, err := s.remote.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
		Ref:            req.GetRef(),
		Credentials:    req.GetCredentials(),
		BypassRegistry: req.GetBypassRegistry(),
	})
	if err != nil {
		return s.fetchBlobFromRemote(ctx, req, stream)
	}

	blobRef, err := gcrname.ParseReference(req.GetRef())
	if err != nil {
		return s.fetchBlobFromRemote(ctx, req, stream)
	}
	digestRef, ok := blobRef.(gcrname.Digest)
	if !ok {
		return s.fetchBlobFromRemote(ctx, req, stream)
	}
	hash, err := gcr.NewHash(digestRef.DigestStr())
	if err != nil {
		return s.fetchBlobFromRemote(ctx, req, stream)
	}

	if err := s.fetchBlobFromLocal(ctx, hash, metadataResp.GetSize(), stream); err != nil {
		return s.fetchBlobFromRemote(ctx, req, stream)
	}
	return nil
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

// fetchBlobFromLocal streams the blob from the local ByteStream server.
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
