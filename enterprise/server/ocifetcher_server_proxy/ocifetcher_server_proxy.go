// Package ocifetcher_server_proxy provides an OCIFetcherServer
// implementation that passes through requests to an upstream
// OCIFetcher service (in this case, the apps).
//
// Eventually, this proxy service will
//   - serve blobs from the local byte stream server and
//   - singleflight requests for the same blob or manifest,
//
// reducing the amount of network traffic to the public internet
// and byte stream servers in the cache proxies and apps
// when multiple executors fetch the same uncached OCI image simultaneously.
package ocifetcher_server_proxy

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
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

// FetchBlob streams the blob from the local byte stream server if present,
// falling back to the remote if not.
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

// fetchBlobStreamWriter is an io.Writer that sends decompressed data to a
// FetchBlob stream.
type fetchBlobStreamWriter struct {
	stream ofpb.OCIFetcher_FetchBlobServer
}

func (w *fetchBlobStreamWriter) Write(p []byte) (int, error) {
	if err := w.stream.Send(&ofpb.FetchBlobResponse{Data: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}

// decompressingByteStreamAdapter adapts ByteStream read responses to a
// FetchBlob stream, decompressing ZSTD data before sending.
type decompressingByteStreamAdapter struct {
	grpc.ServerStream
	ctx          context.Context
	decompressor io.WriteCloser
}

func (a *decompressingByteStreamAdapter) Send(resp *bspb.ReadResponse) error {
	_, err := a.decompressor.Write(resp.GetData())
	return err
}

func (a *decompressingByteStreamAdapter) Context() context.Context {
	return a.ctx
}

// fetchBlobFromLocal streams the blob from the local ByteStream server,
// decompressing ZSTD data before sending to the FetchBlob stream.
func (s *OCIFetcherServerProxy) fetchBlobFromLocal(ctx context.Context, hash gcr.Hash, sizeBytes int64, stream ofpb.OCIFetcher_FetchBlobServer) error {
	rn := ocicache.NewBlobCASResourceName(hash, sizeBytes)

	streamWriter := &fetchBlobStreamWriter{stream: stream}
	decompressor, err := compression.NewZstdDecompressor(streamWriter)
	if err != nil {
		return err
	}
	defer decompressor.Close()

	adapter := &decompressingByteStreamAdapter{
		ctx:          ctx,
		decompressor: decompressor,
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
