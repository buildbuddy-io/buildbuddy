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
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type OCIFetcherServerProxy struct {
	remote        ofpb.OCIFetcherClient
	localBSServer interfaces.ByteStreamServer
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
	if env.GetLocalByteStreamServer() == nil {
		return nil, status.FailedPreconditionError("A local byte stream server is required to enable the OCIFetcherServerProxy")
	}
	return &OCIFetcherServerProxy{
		remote:        env.GetOCIFetcherClient(),
		localBSServer: env.GetLocalByteStreamServer(),
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

func parseDigestRef(ref string) (gcrname.Repository, gcr.Hash, error) {
	blobRef, err := gcrname.ParseReference(ref)
	if err != nil {
		return gcrname.Repository{}, gcr.Hash{}, err
	}
	digestRef, ok := blobRef.(gcrname.Digest)
	if !ok {
		return gcrname.Repository{}, gcr.Hash{}, status.InvalidArgumentErrorf("ref %q must be a digest ref", ref)
	}
	hash, err := gcr.NewHash(digestRef.DigestStr())
	if err != nil {
		return gcrname.Repository{}, gcr.Hash{}, err
	}
	return digestRef.Context(), hash, nil
}

// blobCacheWriteStream adapts the OCI fetcher stream to write to the local
// ByteStream server while also sending responses to the client.
// It implements bspb.ByteStream_WriteServer.
type blobCacheWriteStream struct {
	bspb.ByteStream_WriteServer // Embed for interface compliance

	ctx          context.Context
	remoteStream ofpb.OCIFetcher_FetchBlobClient
	clientStream ofpb.OCIFetcher_FetchBlobServer
	uploadRN     string

	writeOffset int64
	remoteErr   error
	clientErr   error
	finished    bool
}

func (s *blobCacheWriteStream) Recv() (*bspb.WriteRequest, error) {
	resp, err := s.remoteStream.Recv()
	if err != nil {
		s.remoteErr = err
		if err == io.EOF {
			return &bspb.WriteRequest{
				ResourceName: s.uploadRN,
				WriteOffset:  s.writeOffset,
				FinishWrite:  true,
			}, nil
		}
		return nil, err
	}

	if err := s.clientStream.Send(resp); err != nil {
		s.clientErr = err
		return nil, err
	}

	data := resp.GetData()
	req := &bspb.WriteRequest{
		ResourceName: s.uploadRN,
		Data:         data,
		WriteOffset:  s.writeOffset,
		FinishWrite:  false,
	}
	s.writeOffset += int64(len(data))
	return req, nil
}

func (s *blobCacheWriteStream) SendAndClose(*bspb.WriteResponse) error {
	s.finished = true
	return nil
}

func (s *blobCacheWriteStream) Context() context.Context { return s.ctx }

func (s *OCIFetcherServerProxy) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

	_, hash, err := parseDigestRef(req.GetRef())
	if err != nil {
		log.CtxWarningf(ctx, "Failed to parse ref %q, skipping cache: %v", req.GetRef(), err)
		return s.fetchBlobPassthrough(req, stream)
	}

	metaResp, err := s.remote.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
		Ref:            req.GetRef(),
		Credentials:    req.GetCredentials(),
		BypassRegistry: req.GetBypassRegistry(),
	})
	if err != nil {
		log.CtxWarningf(ctx, "Failed to get blob metadata, skipping cache: %v", err)
		return s.fetchBlobPassthrough(req, stream)
	}

	d := &repb.Digest{Hash: hash.Hex, SizeBytes: metaResp.GetSize()}
	rn := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)
	uploadRN := rn.NewUploadString()

	remoteStream, err := s.remote.FetchBlob(ctx, req)
	if err != nil {
		return err
	}

	cacheStream := &blobCacheWriteStream{
		ctx:          ctx,
		remoteStream: remoteStream,
		clientStream: stream,
		uploadRN:     uploadRN,
	}

	localErr := s.localBSServer.Write(cacheStream)
	if localErr != nil {
		log.CtxWarningf(ctx, "Error writing to local cache: %v", localErr)
	}

	if cacheStream.clientErr != nil {
		return cacheStream.clientErr
	}
	if cacheStream.remoteErr != nil && cacheStream.remoteErr != io.EOF {
		return cacheStream.remoteErr
	}

	return nil
}

func (s *OCIFetcherServerProxy) fetchBlobPassthrough(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

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
