package ocifetcher_server_proxy

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
)

// fakeOCIFetcherServer counts calls and returns err from every method.
type fakeOCIFetcherServer struct {
	calls int
	err   error
}

func (f *fakeOCIFetcherServer) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	f.calls++
	return &ofpb.FetchManifestResponse{}, f.err
}

func (f *fakeOCIFetcherServer) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	f.calls++
	return &ofpb.FetchManifestMetadataResponse{}, f.err
}

func (f *fakeOCIFetcherServer) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	f.calls++
	return &ofpb.FetchBlobMetadataResponse{}, f.err
}

func (f *fakeOCIFetcherServer) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	f.calls++
	return f.err
}

type fakeFetchBlobServer struct {
	ofpb.OCIFetcher_FetchBlobServer
	ctx context.Context
}

func (s *fakeFetchBlobServer) Context() context.Context {
	return s.ctx
}

func TestRouter(t *testing.T) {
	methods := []struct {
		name string
		call func(ctx context.Context, r *router, bypassRegistry bool) error
	}{
		{
			name: metrics.OCIFetcherMethodFetchManifest,
			call: func(ctx context.Context, r *router, bypassRegistry bool) error {
				_, err := r.FetchManifest(ctx, &ofpb.FetchManifestRequest{BypassRegistry: bypassRegistry})
				return err
			},
		},
		{
			name: metrics.OCIFetcherMethodFetchManifestMetadata,
			call: func(ctx context.Context, r *router, bypassRegistry bool) error {
				_, err := r.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{BypassRegistry: bypassRegistry})
				return err
			},
		},
		{
			name: metrics.OCIFetcherMethodFetchBlobMetadata,
			call: func(ctx context.Context, r *router, bypassRegistry bool) error {
				_, err := r.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{BypassRegistry: bypassRegistry})
				return err
			},
		},
		{
			name: metrics.OCIFetcherMethodFetchBlob,
			call: func(ctx context.Context, r *router, bypassRegistry bool) error {
				return r.FetchBlob(&ofpb.FetchBlobRequest{BypassRegistry: bypassRegistry}, &fakeFetchBlobServer{ctx: ctx})
			},
		},
	}
	for _, tc := range []struct {
		name           string
		flag           bool
		bypassRegistry bool
		fetcherErr     error
		wantFetcher    bool
	}{
		{name: "FlagOff"},
		{name: "BypassRegistry", flag: true, bypassRegistry: true},
		{name: "FlagOn", flag: true, wantFetcher: true},
		{name: "FlagOnFetcherError", flag: true, fetcherErr: status.UnavailableError("local fetcher unavailable"), wantFetcher: true},
	} {
		for _, m := range methods {
			t.Run(tc.name+"/"+m.name, func(t *testing.T) {
				flags.Set(t, "cache_proxy.oci_fetcher.use_local_fetcher", tc.flag)
				legacy := &fakeOCIFetcherServer{}
				fetcher := &fakeOCIFetcherServer{err: tc.fetcherErr}
				r := &router{legacy: legacy, fetcher: fetcher}

				wantPath, otherPath := metrics.OCIFetcherPathLegacy, metrics.OCIFetcherPathFetcher
				if tc.wantFetcher {
					wantPath, otherPath = otherPath, wantPath
				}
				wantStatus := status.MetricsLabel(tc.fetcherErr)
				count := func(path string) float64 {
					return testmetrics.CounterValueForLabels(t, metrics.CacheProxyOCIFetcherRequestCount, prometheus.Labels{
						metrics.OCIFetcherMethodLabel:    m.name,
						metrics.OCIFetcherPathLabel:      path,
						metrics.StatusHumanReadableLabel: wantStatus,
					})
				}
				wantBefore, otherBefore := count(wantPath), count(otherPath)

				err := m.call(context.Background(), r, tc.bypassRegistry)

				require.Equal(t, tc.fetcherErr, err)
				if tc.wantFetcher {
					require.Equal(t, 1, fetcher.calls)
					require.Equal(t, 0, legacy.calls)
				} else {
					require.Equal(t, 0, fetcher.calls)
					require.Equal(t, 1, legacy.calls)
				}
				require.Equal(t, wantBefore+1, count(wantPath))
				require.Equal(t, otherBefore, count(otherPath))
			})
		}
	}
}
