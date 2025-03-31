package hit_tracker_client

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type HitTrackerFactory struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) {
	env.SetHitTrackerFactory(HitTrackerFactory{})
}

// TODO(iain): hook this up with a to-be-added RPC service in the app.
type HitTracker struct {
}

func (h HitTrackerFactory) NewACHitTracker(ctx context.Context, invocationID string) interfaces.HitTracker {
	return &HitTracker{}
}

func (h HitTrackerFactory) NewCASHitTracker(ctx context.Context, invocationID string) interfaces.HitTracker {
	return &HitTracker{}
}

func (h *HitTracker) SetExecutedActionMetadata(md *repb.ExecutedActionMetadata) {
}

func (h *HitTracker) TrackMiss(d *repb.Digest) error {
	return nil
}

func (h *HitTracker) TrackEmptyHit() error {
	return nil
}

type TransferTimer struct {
}

func (t *TransferTimer) CloseWithBytesTransferred(bytesTransferredCache, bytesTransferredClient int64, compressor repb.Compressor_Value, serverLabel string) error {
	return nil
}

func (t *TransferTimer) Record(bytesTransferred int64, duration time.Duration, compressor repb.Compressor_Value) error {
	return nil
}

func (h *HitTracker) TrackDownload(d *repb.Digest) interfaces.TransferTimer {
	return &TransferTimer{}
}

func (h *HitTracker) TrackUpload(d *repb.Digest) interfaces.TransferTimer {
	return &TransferTimer{}
}
