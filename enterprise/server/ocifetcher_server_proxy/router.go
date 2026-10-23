package ocifetcher_server_proxy

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var useLocalFetcher = expflag.Bool("cache_proxy.oci_fetcher.use_local_fetcher", false, "If true, serve OCIFetcher requests with a local OCI fetcher that reads and writes through this proxy's own ByteStream and ActionCache servers, instead of forwarding them to the apps.")

// router serves each OCIFetcher request with either the legacy
// OCIFetcherServerProxy, which forwards to the apps, or a local OCI fetcher,
// depending on cache_proxy.oci_fetcher.use_local_fetcher.
type router struct {
	legacy  ofpb.OCIFetcherServer
	fetcher ofpb.OCIFetcherServer
}

// RegisterWithLocalFetcher sets env's OCIFetcherServer to a router between an
// OCIFetcherServerProxy and a local OCI fetcher. bsClient and acClient are the
// local fetcher's cache clients; they should point at this proxy's own gRPC
// servers.
func RegisterWithLocalFetcher(env *real_environment.RealEnv, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient) error {
	legacy, err := New(env)
	if err != nil {
		return status.InternalErrorf("Error initializing OCIFetcherServerProxy: %s", err)
	}
	fetcher, err := ocifetcher.NewServer(bsClient, acClient)
	if err != nil {
		return status.InternalErrorf("Error initializing OCIFetcherServerProxy local fetcher: %s", err)
	}
	env.SetOCIFetcherServer(&router{legacy: legacy, fetcher: fetcher})
	return nil
}

// pick returns the server that should handle a request, and its path label.
// Requests that bypass the registry always go to the legacy server.
func (r *router) pick(ctx context.Context, bypassRegistry bool) (ofpb.OCIFetcherServer, string) {
	if !bypassRegistry && useLocalFetcher.Get(ctx) {
		return r.fetcher, metrics.OCIFetcherPathFetcher
	}
	return r.legacy, metrics.OCIFetcherPathLegacy
}

func recordRequest(method, path string, err error) {
	metrics.CacheProxyOCIFetcherRequestCount.WithLabelValues(method, path, status.MetricsLabel(err)).Inc()
}

func (r *router) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	s, path := r.pick(ctx, req.GetBypassRegistry())
	resp, err := s.FetchManifest(ctx, req)
	recordRequest(metrics.OCIFetcherMethodFetchManifest, path, err)
	return resp, err
}

func (r *router) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	s, path := r.pick(ctx, req.GetBypassRegistry())
	resp, err := s.FetchManifestMetadata(ctx, req)
	recordRequest(metrics.OCIFetcherMethodFetchManifestMetadata, path, err)
	return resp, err
}

func (r *router) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	s, path := r.pick(ctx, req.GetBypassRegistry())
	resp, err := s.FetchBlobMetadata(ctx, req)
	recordRequest(metrics.OCIFetcherMethodFetchBlobMetadata, path, err)
	return resp, err
}

func (r *router) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	s, path := r.pick(stream.Context(), req.GetBypassRegistry())
	err := s.FetchBlob(req, stream)
	recordRequest(metrics.OCIFetcherMethodFetchBlob, path, err)
	return err
}
