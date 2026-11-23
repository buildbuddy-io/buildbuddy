package action_merger_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/action_merger"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type fakeScheduler struct {
	interfaces.SchedulerService
}

func (*fakeScheduler) ExistsTask(ctx context.Context, taskID string) (bool, error) {
	return true, nil
}

func setup(t *testing.T, hashChar string) (context.Context, redis.UniversalClient, *digest.CASResourceName) {
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), nullauth.NewNullAuthenticator(true))
	require.NoError(t, err)
	rdb := testredis.Start(t).Client()
	rn := digest.NewCASResourceName(&repb.Digest{Hash: strings.Repeat(hashChar, 64), SizeBytes: 1}, "", repb.DigestFunction_SHA256)
	return ctx, rdb, rn
}

func getOrCreate(ctx context.Context, rdb redis.UniversalClient, rn *digest.CASResourceName, priority int32) (string, action_merger.DispatchAction) {
	return action_merger.GetOrCreateExecutionID(ctx, rdb, &fakeScheduler{}, rn, false /*=doNotCache*/, priority)
}

func TestPriority_HigherPriorityDoesNotMergeIntoQueuedLowerPriority(t *testing.T) {
	ctx, rdb, rn := setup(t, "a")

	lowID, op := getOrCreate(ctx, rdb, rn, 10)
	require.Equal(t, action_merger.New, op)

	// Merge into the low priority execution so we can check that its merge
	// state is preserved.
	id, op := getOrCreate(ctx, rdb, rn, 10)
	require.Equal(t, action_merger.Merge, op)
	require.Equal(t, lowID, id)
	require.NoError(t, action_merger.RecordMergedExecution(ctx, rdb, rn, "" /*=groupIdForMetrics*/))

	highID, op := getOrCreate(ctx, rdb, rn, 0)
	require.Equal(t, action_merger.New, op)
	require.NotEqual(t, lowID, highID)

	// The higher priority execution doesn't record any action-merging state,
	// so the queued execution remains canonical and keeps its merge state.
	merged, err := action_merger.CheckMerged(ctx, rdb, rn)
	require.NoError(t, err)
	require.True(t, merged)
	id, op = getOrCreate(ctx, rdb, rn, 10)
	require.Equal(t, action_merger.Merge, op)
	require.Equal(t, lowID, id)

	// Claiming or completing the higher priority execution is a no-op.
	require.NoError(t, action_merger.RecordClaimedExecution(ctx, rdb, highID, time.Minute))
	require.NoError(t, action_merger.DeletePendingExecution(ctx, rdb, highID))
	id, op = getOrCreate(ctx, rdb, rn, 10)
	require.Equal(t, action_merger.Merge, op)
	require.Equal(t, lowID, id)
}

func TestPriority_EqualOrLowerPriorityMerges(t *testing.T) {
	ctx, rdb, rn := setup(t, "b")

	firstID, op := getOrCreate(ctx, rdb, rn, 0)
	require.Equal(t, action_merger.New, op)

	id, op := getOrCreate(ctx, rdb, rn, 0)
	require.Equal(t, action_merger.Merge, op)
	require.Equal(t, firstID, id)

	id, op = getOrCreate(ctx, rdb, rn, 5)
	require.Equal(t, action_merger.Merge, op)
	require.Equal(t, firstID, id)
}

func TestPriority_HigherPriorityMergesIntoClaimedExecution(t *testing.T) {
	ctx, rdb, rn := setup(t, "c")

	lowID, op := getOrCreate(ctx, rdb, rn, 10)
	require.Equal(t, action_merger.New, op)
	require.NoError(t, action_merger.RecordClaimedExecution(ctx, rdb, lowID, time.Minute))

	id, op := getOrCreate(ctx, rdb, rn, 0)
	require.Equal(t, action_merger.Merge, op)
	require.Equal(t, lowID, id)
}
