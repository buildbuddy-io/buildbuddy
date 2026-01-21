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

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
)

type OCIFetcherServerProxy struct {
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
	return &OCIFetcherServerProxy{
		remote: env.GetOCIFetcherClient(),
	}, nil
}

func hasCredentials(username string) bool {
	return username != ""
}

func (s *OCIFetcherServerProxy) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchManifest entry ref=%q bypassRegistry=%v hasCredentials=%v", req.GetRef(), req.GetBypassRegistry(), hasCredentials(req.GetCredentials().GetUsername()))
	resp, err := s.remote.FetchManifest(ctx, req)
	if err != nil {
		log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchManifest failed ref=%q err=%v", req.GetRef(), err)
		return nil, err
	}
	log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchManifest success ref=%q digest=%q size=%d mediaType=%q", req.GetRef(), resp.GetDigest(), resp.GetSize(), resp.GetMediaType())
	return resp, nil
}

func (s *OCIFetcherServerProxy) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchManifestMetadata entry ref=%q bypassRegistry=%v hasCredentials=%v", req.GetRef(), req.GetBypassRegistry(), hasCredentials(req.GetCredentials().GetUsername()))
	resp, err := s.remote.FetchManifestMetadata(ctx, req)
	if err != nil {
		log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchManifestMetadata failed ref=%q err=%v", req.GetRef(), err)
		return nil, err
	}
	log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchManifestMetadata success ref=%q digest=%q size=%d mediaType=%q", req.GetRef(), resp.GetDigest(), resp.GetSize(), resp.GetMediaType())
	return resp, nil
}

func (s *OCIFetcherServerProxy) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchBlobMetadata entry ref=%q bypassRegistry=%v hasCredentials=%v", req.GetRef(), req.GetBypassRegistry(), hasCredentials(req.GetCredentials().GetUsername()))
	resp, err := s.remote.FetchBlobMetadata(ctx, req)
	if err != nil {
		log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchBlobMetadata failed ref=%q err=%v", req.GetRef(), err)
		return nil, err
	}
	log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchBlobMetadata success ref=%q size=%d mediaType=%q", req.GetRef(), resp.GetSize(), resp.GetMediaType())
	return resp, nil
}

func (s *OCIFetcherServerProxy) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()

	log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchBlob entry ref=%q bypassRegistry=%v hasCredentials=%v", req.GetRef(), req.GetBypassRegistry(), hasCredentials(req.GetCredentials().GetUsername()))
	remoteStream, err := s.remote.FetchBlob(ctx, req)
	if err != nil {
		log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchBlob failed ref=%q err=%v", req.GetRef(), err)
		return err
	}

	var bytesForwarded int64
	for {
		resp, err := remoteStream.Recv()
		if err == io.EOF {
			log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchBlob success ref=%q bytesForwarded=%d", req.GetRef(), bytesForwarded)
			return nil
		}
		if err != nil {
			log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchBlob failed ref=%q bytesForwarded=%d err=%v", req.GetRef(), bytesForwarded, err)
			return err
		}
		bytesForwarded += int64(len(resp.GetData()))
		if err := stream.Send(resp); err != nil {
			log.CtxDebugf(ctx, "[OCIFetcher] Proxy: FetchBlob failed ref=%q bytesForwarded=%d err=%v", req.GetRef(), bytesForwarded, err)
			return err
		}
	}
}
