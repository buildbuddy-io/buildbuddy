package usagetracker

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	gstatus "google.golang.org/grpc/status"
)

// fakeAPIClient returns a fixed response and records the request.
type fakeAPIClient struct {
	rfspb.ApiClient

	rsp *rfpb.SyncProposeResponse
	err error
	req *rfpb.SyncProposeRequest
}

func (c *fakeAPIClient) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest, opts ...grpc.CallOption) (*rfpb.SyncProposeResponse, error) {
	c.req = req
	return c.rsp, c.err
}

func deleteUnion(err error) *rfpb.ResponseUnion {
	s, _ := gstatus.FromError(err)
	return &rfpb.ResponseUnion{
		Status: s.Proto(),
		Value:  &rfpb.ResponseUnion_Delete{Delete: &rfpb.DeleteResponse{}},
	}
}

func notFoundUnion() *rfpb.ResponseUnion {
	return &rfpb.ResponseUnion{
		Value: &rfpb.ResponseUnion_Delete{Delete: &rfpb.DeleteResponse{NotFound: true}},
	}
}

func testCandidates(n int) ([]*evictionCandidate, []*evictionKeyMeta) {
	candidates := make([]*evictionCandidate, n)
	keyMetas := make([]*evictionKeyMeta, n)
	for i := range n {
		candidates[i] = &evictionCandidate{
			keyBytes: []byte{byte('a' + i)},
			atime:    time.UnixMicro(int64(1000 + i)),
		}
		keyMetas[i] = sender.NewKeyMeta(candidates[i].keyBytes, candidates[i])
	}
	return candidates, keyMetas
}

func TestDeleteBatch(t *testing.T) {
	pu := &partitionUsage{}
	mismatch := status.FailedPreconditionError("Atime mismatch")

	for _, tc := range []struct {
		name          string
		rsp           *rfpb.BatchCmdResponse
		wantEvicted   []int
		wantNotFound  []int
		wantNumFailed int
	}{
		{
			name: "all deleted",
			rsp: &rfpb.BatchCmdResponse{Union: []*rfpb.ResponseUnion{
				deleteUnion(nil), deleteUnion(nil), deleteUnion(nil),
			}},
			wantEvicted: []int{0, 1, 2},
		},
		{
			// Keep successful deletes when another key fails.
			name: "partial failure",
			rsp: &rfpb.BatchCmdResponse{Union: []*rfpb.ResponseUnion{
				deleteUnion(nil), deleteUnion(mismatch), deleteUnion(nil),
			}},
			wantEvicted:   []int{0, 2},
			wantNumFailed: 1,
		},
		{
			// A record that was already gone is not an eviction.
			name: "not found",
			rsp: &rfpb.BatchCmdResponse{Union: []*rfpb.ResponseUnion{
				deleteUnion(nil), notFoundUnion(), deleteUnion(nil),
			}},
			wantEvicted:  []int{0, 2},
			wantNotFound: []int{1},
		},
		{
			name: "batch status error",
			rsp: &rfpb.BatchCmdResponse{
				Union:  []*rfpb.ResponseUnion{{}, {}, {}},
				Status: gstatus.Convert(mismatch).Proto(),
			},
			wantNumFailed: 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidates, keyMetas := testCandidates(3)
			c := &fakeAPIClient{rsp: &rfpb.SyncProposeResponse{Batch: tc.rsp}}

			res, err := pu.deleteBatch(context.Background(), c, &rfpb.Header{}, keyMetas)
			require.NoError(t, err)

			var wantEvicted, wantNotFound []*evictionCandidate
			for _, i := range tc.wantEvicted {
				wantEvicted = append(wantEvicted, candidates[i])
			}
			for _, i := range tc.wantNotFound {
				wantNotFound = append(wantNotFound, candidates[i])
			}
			require.Equal(t, wantEvicted, res.evicted)
			require.Equal(t, wantNotFound, res.notFound)
			require.Equal(t, tc.wantNumFailed, res.numFailed)
			if tc.wantNumFailed > 0 {
				require.True(t, status.IsFailedPreconditionError(res.lastErr), "lastErr: %v", res.lastErr)
			}

			// Deletes must match the candidates' atimes.
			reqs := c.req.GetBatch().GetUnion()
			require.Len(t, reqs, len(candidates))
			for i, r := range reqs {
				require.Equal(t, candidates[i].keyBytes, r.GetDelete().GetKey())
				require.Equal(t, candidates[i].atime.UnixMicro(), r.GetDelete().GetMatchAtime())
			}
		})
	}
}

func TestDeleteBatchRPCError(t *testing.T) {
	pu := &partitionUsage{}
	_, keyMetas := testCandidates(2)
	c := &fakeAPIClient{err: status.UnavailableError("replica down")}

	_, err := pu.deleteBatch(context.Background(), c, &rfpb.Header{}, keyMetas)
	require.True(t, status.IsUnavailableError(err), "err: %v", err)
}

func testMetricSet() metricSet {
	return metricSet{
		cacheEvictionAgeMsec:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "age"}),
		cacheLastEvictionAgeUsec: prometheus.NewGauge(prometheus.GaugeOpts{Name: "last_age"}),
		cacheNumEvictions:        prometheus.NewCounter(prometheus.CounterOpts{Name: "num"}),
		cacheBytesEvicted:        prometheus.NewCounter(prometheus.CounterOpts{Name: "bytes"}),
	}
}

func TestUpdateEvictionMetricsScalesRemoteSizes(t *testing.T) {
	for _, tc := range []struct {
		name           string
		localSizeBytes int64
		wantRemote     int64
	}{
		// Evicting 10% locally shrinks the remote estimate by 10%.
		{name: "local size known", localSizeBytes: 1000, wantRemote: 450},
		// Without a local estimate, leave the remote estimate unchanged.
		{name: "local size unknown", localSizeBytes: 0, wantRemote: 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pu := &partitionUsage{
				sizeBytes: tc.localSizeBytes,
				nodes:     map[string]*nodePartitionUsage{"remote": {sizeBytes: 500}},
				metrics:   testMetricSet(),
			}
			candidates := []*evictionCandidate{{sizeBytes: 100, atime: time.Now()}}

			require.NoError(t, pu.updateEvictionMetrics(candidates))
			require.Equal(t, tc.wantRemote, pu.nodes["remote"].sizeBytes)
		})
	}
}
