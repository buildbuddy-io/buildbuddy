package routing_action_cache_client

import (
	"context"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/migration_operators"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	acMigrationOpTimeout = flag.Duration("cache_proxy.migration.ac_timeout", 60*time.Second, "Timeout for AC operations that happen during cache migrations.")
)

type RoutingACClient struct {
	router       interfaces.CacheRoutingService
	copyOp       batch_operator.DigestOperator
	readOp       batch_operator.DigestOperator
	readVerifyOp batch_operator.DigestOperator
}

func New(env environment.Env) (repb.ActionCacheClient, error) {
	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, status.FailedPreconditionError("An authenticator is required to route and migrate between caches.")
	}

	bytestreamCopyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.ByteStreamCopy(ctx, env.GetCacheRoutingService(), groupID, b)
	}
	bsCopyOp := batch_operator.NewImmediateDigestOperator(authenticator, "ac-bytestream-copy", bytestreamCopyClosure, *acMigrationOpTimeout)

	casCopyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.CASBatchCopy(ctx, env.GetCacheRoutingService(), groupID, b)
	}
	casCopyOp := batch_operator.NewImmediateDigestOperator(authenticator, "ac-cas-copy", casCopyClosure, *acMigrationOpTimeout)

	routedCopyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.RoutedCopy(ctx, groupID, casCopyOp, bsCopyOp, b)
	}
	routedCopyOp := batch_operator.NewImmediateDigestOperator(authenticator, "ac-routed-copy", routedCopyClosure, *acMigrationOpTimeout)

	acCopyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.ACCopy(ctx, env.GetCacheRoutingService(), routedCopyOp, groupID, b)
	}
	copyOp := batch_operator.NewImmediateDigestOperator(authenticator, "ac-copy", acCopyClosure, *acMigrationOpTimeout)

	acReadVerifyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.ACReadAndVerify(ctx, env.GetCacheRoutingService(), groupID, b)
	}

	readOp := batch_operator.NewImmediateDigestOperator(authenticator, "ac-read", acReadVerifyClosure, *acMigrationOpTimeout)
	readVerifyOp := batch_operator.NewImmediateDigestOperator(authenticator, "ac-read-verify", acReadVerifyClosure, *acMigrationOpTimeout)

	return &RoutingACClient{
		router:       routingService,
		copyOp:       copyOp,
		readOp:       readOp,
		readVerifyOp: readVerifyOp,
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
	singleRandValue := rand.Float64()
	if singleRandValue < c.GetBackgroundReadVerifyFraction() {
		r.readVerifyOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	} else if singleRandValue < c.GetBackgroundReadFraction() {
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
	singleRandValue := rand.Float64()
	if (singleRandValue) < c.GetBackgroundCopyFraction() {
		r.copyOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	}
	return rsp, nil
}
