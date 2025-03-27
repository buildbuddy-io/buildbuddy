package hit_tracker_client

import (
	"context"
	"flag"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	hitpb "github.com/buildbuddy-io/buildbuddy/proto/hit_tracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var (
	remoteHitTrackerTarget  = flag.String("cache.hit_tracker.target", "", "The gRPC target of the remote cache-hit-tracker.")
	remoteHitTrackerTimeout = flag.Duration("cache.hit_tracker.rpc_timeout", 5*time.Second, "The timout to use for gRPC requests to the remote cache-hit-tracker.")
)

func Register(env *real_environment.RealEnv) error {
	conn, err := grpc_client.DialInternal(env, *remoteHitTrackerTarget)
	if err != nil {
		return err
	}
	env.SetHitTrackerFactory(newHitTrackerClient(conn))
	return nil
}

func newHitTrackerClient(conn grpc.ClientConnInterface) *HitTrackerFactory {
	return &HitTrackerFactory{client: hitpb.NewHitTrackerServiceClient(conn)}
}

type HitTrackerFactory struct {
	env    environment.Env
	client hitpb.HitTrackerServiceClient
}

func (h HitTrackerFactory) NewACHitTracker(ctx context.Context, invocationID string, requestMetadata *repb.RequestMetadata) interfaces.HitTracker {
	return &HitTracker{}
}

func (h HitTrackerFactory) NewCASHitTracker(ctx context.Context, invocationID string, requestMetadata *repb.RequestMetadata) interfaces.HitTracker {
	return &HitTracker{}
}

// For Action Cache hit-tracking, explicitly ignore everything. These cache
// artifacts should always be served from the authoritative cache which will
// take care of hit-tracking.
type NoOpHitTracker struct {
}

func (h *NoOpHitTracker) SetExecutedActionMetadata(md *repb.ExecutedActionMetadata) {
}

func (h *NoOpHitTracker) TrackMiss(d *repb.Digest) error {
	return nil
}

func (h *NoOpHitTracker) TrackEmptyHit() error {
	return nil
}

func (h *NoOpHitTracker) TrackDownload(d *repb.Digest) interfaces.TransferTimer {
	return &NoOpTransferTimer{}
}

func (h *NoOpHitTracker) TrackUpload(d *repb.Digest) interfaces.TransferTimer {
	return &NoOpTransferTimer{}
}

type NoOpTransferTimer struct {
}

func (t *NoOpTransferTimer) CloseWithBytesTransferred(bytesTransferredCache, bytesTransferredClient int64, compressor repb.Compressor_Value, serverLabel string) error {
	return nil
}

func (t *NoOpTransferTimer) Record(bytesTransferred int64, duration time.Duration, compressor repb.Compressor_Value) error {
	return nil
}

// For CAS hit-tracking, use a hit-tracker that sends information about local
// cache hits to the RPC service at the configured backend.
type HitTrackerClient struct {
	ctx          context.Context
	client       hitpb.HitTrackerServiceClient
	invocationID string
}

// This should only be used for Action Cache hit tracking.
func (h *HitTrackerClient) SetExecutedActionMetadata(md *repb.ExecutedActionMetadata) {
	log.Warning("Unexpected call to SetExecutedActionMetadata")
}

// Misses hit the backing cache, so no need to report on hit-tracking.
func (h *HitTrackerClient) TrackMiss(d *repb.Digest) error {
	return nil
}

// TODO(iain): implement some client-side batching here.
func sendAsync(ctx context.Context, client hitpb.HitTrackerServiceClient, req *hitpb.TrackRequest) {
	go func() {
		ctx, cancel := context.WithTimeout(ctx, *remoteHitTrackerTimeout)
		_, err := client.Track(ctx, req)
		cancel()
		if err != nil {
			log.Infof("Error sending hit_tracker.Track RPC: %v", err)
		}
	}()
}

func (h *HitTrackerClient) TrackEmptyHit() error {
	req := hitpb.TrackRequest{
		Hits: []*hitpb.CacheHits{
			&hitpb.CacheHits{
				InvocationId: h.invocationID,
				EmptyHits:    1,
			},
		},
	}
	sendAsync(h.ctx, h.client, &req)
	return nil
}

type TransferTimer struct {
	ctx          context.Context
	invocationID string
	digest       *repb.Digest
	start        time.Time
	client       hitpb.HitTrackerServiceClient
}

func (t *TransferTimer) CloseWithBytesTransferred(bytesTransferredCache, bytesTransferredClient int64, compressor repb.Compressor_Value, serverLabel string) error {
	download := &hitpb.Download{
		Resource: &rspb.ResourceName{
			Digest:    t.digest,
			CacheType: rspb.CacheType_CAS,
		},
		SizeBytes: bytesTransferredClient,
		Duration:  durationpb.New(time.Since(t.start)),
	}
	req := hitpb.TrackRequest{
		Hits: []*hitpb.CacheHits{
			&hitpb.CacheHits{
				InvocationId: t.invocationID,
				Downloads:    []*hitpb.Download{download},
			},
		},
	}
	sendAsync(t.ctx, t.client, &req)
	return nil
}

func (t *TransferTimer) Record(bytesTransferred int64, duration time.Duration, compressor repb.Compressor_Value) error {
	return status.InternalError("Unxpected call to hit_tracker_client.Record()")
}

func (h *HitTrackerClient) TrackDownload(digest *repb.Digest) interfaces.TransferTimer {
	return &TransferTimer{
		ctx:          h.ctx,
		invocationID: h.invocationID,
		digest:       digest,
		start:        time.Now(),
		client:       h.client,
	}
}

// Writes hit the backing cache, so no need to report on hit-tracking.
func (h *HitTrackerClient) TrackUpload(digest *repb.Digest) interfaces.TransferTimer {
	return &NoOpTransferTimer{}
}
