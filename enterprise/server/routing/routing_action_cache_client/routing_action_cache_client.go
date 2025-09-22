package routing_action_cache_client

import (
	"context"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	migrationACQueueSize       = flag.Int("cache_proxy.migration.ac_queue_size", 1_000_000, "The length of the holding queue from which we create AC migration batches (shared across all group IDs).")
	migrationACDigestsPerBatch = flag.Int("cache_proxy.migration.ac_digests_per_batch", 100, "The number of digests to flush out to the AC migration operator in a single batch, per (group ID, instance name) tuple.")
	migrationACBatchesPerGroup = flag.Int("cache_proxy.migration.ac_batches_per_group", 10_000, "The number of batches to hold for the AC migration operator, per (group ID, instance name) tuple.")
	migrationACDigestsPerGroup = flag.Int("cache_proxy.migration.ac_digests_per_group", 30_000, "The number of digests that we're willing to hold in batches per Group ID (across all instance names).")
	migrationACBatchInterval   = flag.Duration("cache_proxy.migration.ac_batch_interval", 10*time.Millisecond, "The time interval to wait between flushing AC batches, one batch (group ID, instance name) tuple per flush.")
)

type RoutingACClient struct {
	router interfaces.CacheRoutingService
	copyOp batch_operator.BatchDigestOperator
	readOp batch_operator.BatchDigestOperator
}

func handleCopy(ctx context.Context, groupID string, b *batch_operator.DigestBatch, routingService interfaces.CacheRoutingService) error {
	primary, secondary, err := routingService.GetACClients(ctx)
	if err != nil {
		return err
	}
	for _, d := range b.Digests {
		r, err := primary.GetActionResult(ctx, &repb.GetActionResultRequest{
			InstanceName:   b.InstanceName,
			DigestFunction: b.DigestFunction,
			ActionDigest:   d,
		})
		if err != nil {
			log.Infof("Failed to read AC digest %s when syncing: %s", d.GetHash(), err)
			return err
		}
		_, err = secondary.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
			InstanceName:   b.InstanceName,
			DigestFunction: b.DigestFunction,
			ActionDigest:   d,
			ActionResult:   r,
		})
		if err != nil {
			log.Infof("Failed to write AC digest %s when syncing: %s", d.GetHash(), err)
			return err
		}
	}
	return nil
}

func handleRead(ctx context.Context, groupID string, b *batch_operator.DigestBatch, routingService interfaces.CacheRoutingService) error {
	// Performing this read implicitly validates the full contents of the ActionResult, assuming it exists.
	_, secondary, err := routingService.GetACClients(ctx)
	if err != nil {
		return err
	}
	for _, d := range b.Digests {
		_, err = secondary.GetActionResult(ctx, &repb.GetActionResultRequest{
			InstanceName:   b.InstanceName,
			DigestFunction: b.DigestFunction,
			ActionDigest:   d,
		})
		if err != nil {
			log.Infof("Failed to read AC digest %s when validating: %s", d.GetHash(), err)
			return err
		}
	}
	return nil
}

func New(env environment.Env) (repb.ActionCacheClient, error) {
	routingService := env.GetCacheRoutingService()

	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	copyOp, err := batch_operator.New(
		env,
		"action_cache_copier",
		func(ctx context.Context, groupID string, u *batch_operator.DigestBatch) error {
			return handleCopy(ctx, groupID, u, routingService)
		},
		batch_operator.BatchDigestOperatorConfig{
			QueueSize:          *migrationACQueueSize,
			BatchInterval:      *migrationACBatchInterval,
			MaxDigestsPerGroup: *migrationACDigestsPerGroup,
			MaxDigestsPerBatch: *migrationACDigestsPerBatch,
			MaxBatchesPerGroup: *migrationACBatchesPerGroup,
		})
	if err != nil {
		return nil, err
	}
	readOp, err := batch_operator.New(
		env,
		"action_cache_reader",
		func(ctx context.Context, groupID string, u *batch_operator.DigestBatch) error {
			return handleRead(ctx, groupID, u, routingService)
		},
		batch_operator.BatchDigestOperatorConfig{
			QueueSize:          *migrationACQueueSize,
			BatchInterval:      *migrationACBatchInterval,
			MaxDigestsPerGroup: *migrationACDigestsPerGroup,
			MaxDigestsPerBatch: *migrationACDigestsPerBatch,
			MaxBatchesPerGroup: *migrationACBatchesPerGroup,
		})
	if err != nil {
		return nil, err
	}

	return &RoutingACClient{
		router: routingService,
		copyOp: copyOp,
		readOp: readOp,
	}, nil
}

func (r *RoutingACClient) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	primaryClient, _, err := r.router.GetACClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	rsp, err := primaryClient.GetActionResult(ctx, req, opts...)
	if err != nil {
		return rsp, err
	}

	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return rsp, nil
	}
	singleRandValue := rand.Float32()
	if singleRandValue < c.GetBackgroundReadFraction() {
		r.readOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	} else if (singleRandValue) < c.GetBackgroundCopyFraction() {
		r.copyOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	}
	return rsp, nil
}

func (r *RoutingACClient) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	primaryClient, _, err := r.router.GetACClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	rsp, err := primaryClient.UpdateActionResult(ctx, req, opts...)
	if err != nil {
		return rsp, err
	}

	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return rsp, nil
	}
	singleRandValue := rand.Float32()
	if (singleRandValue) < c.GetBackgroundCopyFraction() {
		r.copyOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	}
	return rsp, nil
}
