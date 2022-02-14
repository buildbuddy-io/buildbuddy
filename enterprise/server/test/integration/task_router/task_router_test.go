package task_router_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_router"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// Executor IDs for use in test cases. Should be in the range [0 ... 99]
	executorID1 = "7"
	executorID2 = "42"
)

func TestTaskRouter_RankNodes_Workflows_ReturnsMultipleRunnersThatExecutedWorkflow(t *testing.T) {
	// Mark a routable workflow task complete by executor 1.

	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	cmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
				{Name: "workflow-id", Value: "WF1"},
			},
		},
	}
	instanceName := "test-instance"

	router.MarkComplete(ctx, cmd, instanceName, executorID1)

	nodes := sequentiallyNumberedNodes(100)

	// Task should now be routed to executor 1.

	ranked := router.RankNodes(ctx, cmd, instanceName, nodes)

	require.ElementsMatch(t, nodes, ranked)
	require.Equal(t, executorID1, ranked[0].GetExecutorID())

	// Mark the same task complete by executor 2 as well.

	router.MarkComplete(ctx, cmd, instanceName, executorID2)

	// Task should now be routed to executor 2 then 1 in order, since executor 2
	// ran the task more recently, and we memorize several recent executors for
	// workflow tasks.

	ranked = router.RankNodes(ctx, cmd, instanceName, nodes)

	require.ElementsMatch(t, nodes, ranked)
	require.Equal(t, executorID2, ranked[0].GetExecutorID())
	require.Equal(t, executorID1, ranked[1].GetExecutorID())
}

func TestTaskRouter_RankNodes_DefaultNodeLimit_ReturnsOnlyLatestNodeMarkedComplete(t *testing.T) {
	// Mark a routable workflow task complete by executor 1.

	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	cmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
			},
		},
	}
	instanceName := "test-instance"

	router.MarkComplete(ctx, cmd, instanceName, executorID1)

	nodes := sequentiallyNumberedNodes(100)

	// Task should now be routed to executor 1.

	ranked := router.RankNodes(ctx, cmd, instanceName, nodes)

	require.ElementsMatch(t, nodes, ranked)
	require.Equal(t, executorID1, ranked[0].GetExecutorID())

	// Mark the same task complete by executor 2 as well.

	router.MarkComplete(ctx, cmd, instanceName, executorID2)

	ranked = router.RankNodes(ctx, cmd, instanceName, nodes)

	// Task should now be routed to executor 2, but executor 1 should be ranked
	// randomly, since we only store up to 1 recent executor for non-workflow
	// tasks.

	require.ElementsMatch(t, nodes, ranked)
	require.Equal(t, executorID2, ranked[0].GetExecutorID())

	requireNotAlwaysRanked(1, executorID1, t, router, ctx, cmd, instanceName)
}

func TestTaskRouter_RankNodes_JustShufflesIfCommandIsNotAvailable(t *testing.T) {
	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	nodes := sequentiallyNumberedNodes(100)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	instanceName := ""

	ranked := router.RankNodes(ctx, nil /*=cmd*/, instanceName, nodes)

	requireReordered(t, nodes, ranked)
}

func TestTaskRouter_MarkComplete_DoesNotAffectNonRecyclableTasks(t *testing.T) {
	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	cmd := &repb.Command{}
	instanceName := "test-instance"

	router.MarkComplete(ctx, cmd, instanceName, executorID1)

	requireNotAlwaysRanked(0, executorID1, t, router, ctx, cmd, instanceName)
}

func TestTaskRouter_MarkComplete_DoesNotAffectOtherGroups(t *testing.T) {
	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	ctx1 := withAuthUser(t, context.Background(), env, "US1")
	cmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
			},
		},
	}
	instanceName := "test-instance"

	router.MarkComplete(ctx1, cmd, instanceName, executorID1)

	ctx2 := withAuthUser(t, context.Background(), env, "US2")

	requireNotAlwaysRanked(0, executorID1, t, router, ctx2, cmd, instanceName)
}

func TestTaskRouter_MarkComplete_DoesNotAffectOtherRemoteInstances(t *testing.T) {
	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	cmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
			},
		},
	}
	instanceName1 := "test-instance"

	router.MarkComplete(ctx, cmd, instanceName1, executorID1)

	instanceName2 := "another-test-instance"

	requireNotAlwaysRanked(0, executorID1, t, router, ctx, cmd, instanceName2)
}

// requireNotAlwaysRanked requires that the task router does not
// deterministically assign the given rank to the given executor ID.
func requireNotAlwaysRanked(rank int, executorID string, t *testing.T, router interfaces.TaskRouter, ctx context.Context, cmd *repb.Command, instanceName string) {
	nodes := sequentiallyNumberedNodes(100)
	nTrials := 10
	for i := 0; i < nTrials; i++ {
		ranked := router.RankNodes(ctx, cmd, instanceName, nodes)

		require.Equal(t, len(nodes), len(ranked))
		if ranked[rank].GetExecutorID() != executorID {
			return
		}
	}

	require.FailNowf(
		t,
		"executor rank is not randomized",
		"task router unexpectedly ranked executor #%s as rank %d in all %d trials, each with %d executors",
		executorID, rank, nTrials, len(nodes),
	)
}

func requireReordered(t *testing.T, nodes []interfaces.ExecutionNode, ranked []interfaces.ExecutionNode) {
	require.ElementsMatch(t, nodes, ranked)

	for i := range nodes {
		if nodes[i] != ranked[i] {
			return
		}
	}
	require.FailNow(t, "nodes were not reordered")
}

func newTaskRouter(t *testing.T, env environment.Env) interfaces.TaskRouter {
	router, err := task_router.New(env)
	require.NoError(t, err)
	return router
}

func newTestEnv(t *testing.T) environment.Env {
	rand.Seed(time.Now().UnixNano())

	redisTarget := testredis.Start(t).Target
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{
		RedisTarget: redisTarget,
	})
	userMap := testauth.TestUsers("US1", "GR1", "US2", "GR2")
	env.SetAuthenticator(testauth.NewTestAuthenticator(userMap))
	return env
}

func withAuthUser(t *testing.T, ctx context.Context, env environment.Env, userID string) context.Context {
	a := env.GetAuthenticator().(*testauth.TestAuthenticator)
	ctx, err := a.WithAuthenticatedUser(ctx, userID)
	require.NoError(t, err)
	return ctx
}

func sequentiallyNumberedNodes(n int) []interfaces.ExecutionNode {
	nodes := make([]interfaces.ExecutionNode, 0, n)
	for i := 0; i < n; i++ {
		nodes = append(nodes, &testNode{i})
	}
	return nodes
}

type testNode struct{ id int }

func (n *testNode) GetExecutorID() string { return fmt.Sprintf("%d", n.id) }
