package atime_updater

import (
	"context"
	"flag"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

var (
	atimeUpdaterEnqueueChanSize = flag.Int("cache_proxy.remote_atime_queue_size", 1000*1000, "The size of the channel to use for buffering enqueued atime updates.")

	// The maximum gRPC message size is 4MB. Each digest proto is
	// 64 bytes (hash) + 8 bytes (size) + 2 bytes (tags) = 74 bytes. The
	// remaining fields in FindMissingBlobsRequest should be small (1kb?), so
	// we can conceivably fit 4MB / 74bytes = 14.1k of these. Cap it at 10k for
	// now and we can increase it (or increase the update frequency) if we drop
	// a lot of updates. Note this also affects memory usage of the proxy!
	atimeUpdaterMaxDigestsPerUpdate = flag.Int("cache_proxy.remote_atime_max_digests_per_update", 10*1000, "The maximum number of blob digests to send in a single request for updating blob access times in the remote cache.")
	atimeUpdaterMaxUpdatesPerGroup  = flag.Int("cache_proxy.remote_atime_max_updates_per_group", 50, "The maximum number of FindMissingBlobRequests to accumulate per group for updating blob access times in the remote cache.")
	atimeUpdaterMaxDigestsPerGroup  = flag.Int("cache_proxy.remote_atime_max_digests_per_group", 200*1000, "The maximum number of blob digests to enqueue atime updates for, per group, across all instance names / hash fucntions.")
	atimeUpdaterBatchUpdateInterval = flag.Duration("cache_proxy.remote_atime_update_interval", 10*time.Second, "The time interval to wait between sending access time updates to the remote cache.")
)

func Register(env *real_environment.RealEnv) error {
	updater, err := new(env)
	if err != nil {
		return err
	}
	updater.Start(env.GetHealthChecker())
	env.SetAtimeUpdater(updater)
	return nil
}

func new(env *real_environment.RealEnv) (batch_operator.BatchDigestOperator, error) {
	handleBatch := func(ctx context.Context, groupID string, u *batch_operator.DigestBatch) error {
		req := &repb.FindMissingBlobsRequest{
			InstanceName:   u.InstanceName,
			BlobDigests:    make([]*repb.Digest, len(u.Digests)),
			DigestFunction: u.DigestFunction,
		}
		i := 0
		for d := range u.Digests {
			req.BlobDigests[i] = d.ToDigest()
			i++
		}

		_, err := env.GetContentAddressableStorageClient().FindMissingBlobs(ctx, req)
		metrics.RemoteAtimeUpdatesSent.WithLabelValues(
			groupID,
			gstatus.Code(err).String(),
		).Inc()
		return err
	}
	operator, err := batch_operator.New(env, "atime-updater", handleBatch, batch_operator.BatchDigestOperatorConfig{
		QueueSize:          *atimeUpdaterEnqueueChanSize,
		BatchInterval:      *atimeUpdaterBatchUpdateInterval,
		MaxDigestsPerGroup: *atimeUpdaterMaxDigestsPerGroup,
		MaxDigestsPerBatch: *atimeUpdaterMaxDigestsPerUpdate,
		MaxBatchesPerGroup: *atimeUpdaterMaxUpdatesPerGroup,
	})
	if err != nil {
		return nil, err
	}
	return operator, nil
}
