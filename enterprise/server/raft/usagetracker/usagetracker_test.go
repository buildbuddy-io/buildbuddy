package usagetracker

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/approxlru"
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

func testSamples(n int) ([]*approxlru.Sample[*evictionKey], []*evictionKeyMeta) {
	samples := make([]*approxlru.Sample[*evictionKey], n)
	keys := make([]*evictionKeyMeta, n)
	for i := range n {
		samples[i] = &approxlru.Sample[*evictionKey]{
			Key:       &evictionKey{bytes: []byte{byte('a' + i)}},
			Timestamp: time.UnixMicro(int64(1000 + i)),
		}
		keys[i] = &evictionKeyMeta{Key: samples[i].Key.bytes, Meta: samples[i]}
	}
	return samples, keys
}

func TestDeleteBatch(t *testing.T) {
	pu := &partitionUsage{}
	mismatch := status.FailedPreconditionError("Atime mismatch")

	for _, tc := range []struct {
		name          string
		rsp           *rfpb.BatchCmdResponse
		wantEvicted   []int
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
			name: "batch status error",
			rsp: &rfpb.BatchCmdResponse{
				Union:  []*rfpb.ResponseUnion{{}, {}, {}},
				Status: gstatus.Convert(mismatch).Proto(),
			},
			wantNumFailed: 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			samples, keys := testSamples(3)
			c := &fakeAPIClient{rsp: &rfpb.SyncProposeResponse{Batch: tc.rsp}}

			res, err := pu.deleteBatch(context.Background(), c, &rfpb.Header{}, keys)
			require.NoError(t, err)

			var wantEvicted []*approxlru.Sample[*evictionKey]
			for _, i := range tc.wantEvicted {
				wantEvicted = append(wantEvicted, samples[i])
			}
			require.Equal(t, wantEvicted, res.evicted)
			require.Equal(t, tc.wantNumFailed, res.numFailed)
			if tc.wantNumFailed > 0 {
				require.True(t, status.IsFailedPreconditionError(res.lastErr), "lastErr: %v", res.lastErr)
			}

			// Deletes must match the sampled atimes.
			reqs := c.req.GetBatch().GetUnion()
			require.Len(t, reqs, len(samples))
			for i, r := range reqs {
				require.Equal(t, samples[i].Key.bytes, r.GetDelete().GetKey())
				require.Equal(t, samples[i].Timestamp.UnixMicro(), r.GetDelete().GetMatchAtime())
			}
		})
	}
}

func TestDeleteBatchRPCError(t *testing.T) {
	pu := &partitionUsage{}
	_, keys := testSamples(2)
	c := &fakeAPIClient{err: status.UnavailableError("replica down")}

	_, err := pu.deleteBatch(context.Background(), c, &rfpb.Header{}, keys)
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
			samples := []*approxlru.Sample[*evictionKey]{{SizeBytes: 100, Timestamp: time.Now()}}

			require.NoError(t, pu.updateEvictionMetrics(samples))
			require.Equal(t, tc.wantRemote, pu.nodes["remote"].sizeBytes)
		})
	}
}
