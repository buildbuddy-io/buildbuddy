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
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Executor host IDs for use in test cases.
// These match the host IDs returned by sequentiallyNumberedNodes.
const (
	executorHostID1 = "host-7"
	executorHostID2 = "host-42"
)

func TestTaskRouter_RankNodes_Workflows_ReturnsLatestRunnerThatExecutedWorkflow(t *testing.T) {
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
		Arguments: []string{"./buildbuddy_ci_runner"},
	}
	instanceName := "test-instance"

	router.MarkComplete(ctx, cmd, instanceName, executorHostID1)

	nodes := sequentiallyNumberedNodes(100)

	// Task should now be routed to executor 1.

	ranked := router.RankNodes(ctx, cmd, instanceName, nodes)

	requireSameExecutionNodes(t, nodes, ranked)
	require.Equal(t, executorHostID1, ranked[0].GetExecutionNode().GetExecutorHostID())

	// The remaining nodes should be shuffled.
	requireNonSequential(t, ranked[1:])

	// Mark the same task complete by executor 2 as well.

	router.MarkComplete(ctx, cmd, instanceName, executorHostID2)

	// Task should now be routed to executor 2, since executor 2 ran the task
	// more recently.

	ranked = router.RankNodes(ctx, cmd, instanceName, nodes)

	requireSameExecutionNodes(t, nodes, ranked)
	require.Equal(t, executorHostID2, ranked[0].GetExecutionNode().GetExecutorHostID())
	requireNonSequential(t, ranked[2:])
}

func TestTaskRouter_RankNodes_DefaultNodeLimit_ReturnsOnlyLatestNodeMarkedComplete(t *testing.T) {
	// Mark a routable task complete by executor 1.

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

	router.MarkComplete(ctx, cmd, instanceName, executorHostID1)

	nodes := sequentiallyNumberedNodes(100)

	// Task should now be routed to executor 1.

	ranked := router.RankNodes(ctx, cmd, instanceName, nodes)

	requireSameExecutionNodes(t, nodes, ranked)
	require.Equal(t, executorHostID1, ranked[0].GetExecutionNode().GetExecutorHostID())
	requireNonSequential(t, ranked[1:])

	// Mark the same task complete by executor 2 as well.

	router.MarkComplete(ctx, cmd, instanceName, executorHostID2)

	ranked = router.RankNodes(ctx, cmd, instanceName, nodes)

	// Task should now be routed to executor 2, but executor 1 should be ranked
	// randomly, since we only store up to 1 recent executor for non-workflow
	// tasks.

	requireSameExecutionNodes(t, nodes, ranked)
	require.Equal(t, executorHostID2, ranked[0].GetExecutionNode().GetExecutorHostID())
	require.True(t, ranked[0].IsPreferred())

	requireNotAlwaysRanked(1, executorHostID1, t, router, ctx, cmd, instanceName)
	requireNonSequential(t, ranked[1:])

	for i := 1; i < 100; i++ {
		require.False(t, ranked[i].IsPreferred())
	}
}

func TestTaskRouter_RankNodes_RoutesByHostID(t *testing.T) {
	// Mark a routable task complete by executor 1.

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

	router.MarkComplete(ctx, cmd, instanceName, executorHostID1)

	nodes := sequentiallyNumberedNodes(100)

	// Task should now be routed to executor 1.

	ranked := router.RankNodes(ctx, cmd, instanceName, nodes)

	requireSameExecutionNodes(t, nodes, ranked)
	require.Equal(t, executorHostID1, ranked[0].GetExecutionNode().GetExecutorHostID())
	requireNonSequential(t, ranked[1:])

	// Simulate executor 1 restarting by updating its executor ID but keeping
	// the host ID the same.

	idx := slices.IndexFunc(nodes, func(n interfaces.ExecutionNode) bool {
		return n.GetExecutorHostID() == executorHostID1
	})
	require.NotEqual(t, -1, idx)
	nodes[idx].(*testNode).executorID = "new-executor-id"

	// Task should now be routed to the new node on this restarted host.

	ranked = router.RankNodes(ctx, cmd, instanceName, nodes)

	requireSameExecutionNodes(t, nodes, ranked)
	require.Equal(t, executorHostID1, ranked[0].GetExecutionNode().GetExecutorHostID())
	requireNonSequential(t, ranked[1:])
}

func TestTaskRouter_RankNodes_AffinityRouting(t *testing.T) {
	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	firstCmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "affinity-routing", Value: "true"},
			},
		},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "foo", Value: "bar"},
		},
		Arguments:   []string{"gcc", "-c", "dbg", "foo.c"},
		OutputPaths: []string{"/bazel-out/foo.a"},
	}
	instanceName := "test-instance"

	// No executor should be preferred.
	nodes := sequentiallyNumberedNodes(100)
	ranked := router.RankNodes(ctx, firstCmd, instanceName, nodes)
	requireNotAlwaysRanked(0, executorHostID1, t, router, ctx, firstCmd, instanceName)
	requireNonSequential(t, ranked)
	requireNonePreferred(t, ranked)

	// Mark the task as complete by executor 1.
	router.MarkComplete(ctx, firstCmd, instanceName, executorHostID1)

	secondCmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "affinity-routing", Value: "true"},
			},
		},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "foo", Value: "baz"},
		},
		Arguments:   []string{"gcc", "-c", "opt", "foo.c"},
		OutputPaths: []string{"/bazel-out/foo.a"},
	}

	// Task should now be routed to executor 1.
	ranked = router.RankNodes(ctx, secondCmd, instanceName, nodes)

	requireSameExecutionNodes(t, nodes, ranked)
	require.Equal(t, executorHostID1, ranked[0].GetExecutionNode().GetExecutorHostID())
	require.True(t, ranked[0].IsPreferred())
	requireNonSequential(t, ranked[1:])
	requireNonePreferred(t, ranked[1:])

	// Mark the task complete by executor 2 as well.
	router.MarkComplete(ctx, secondCmd, instanceName, executorHostID2)

	// If the first output is specified as an OutputFile rather than an
	// OutputPath, the routing should still consider this.
	thirdCmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "affinity-routing", Value: "true"},
			},
		},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "foo", Value: "qux"},
		},
		Arguments:   []string{"gcc", "-c", "opt", "foo.c"},
		OutputFiles: []string{"/bazel-out/foo.a"},
	}

	ranked = router.RankNodes(ctx, thirdCmd, instanceName, nodes)

	// Task should now be routed to executor 2, with executor 1 ranked randomly
	requireSameExecutionNodes(t, nodes, ranked)
	require.Equal(t, executorHostID2, ranked[0].GetExecutionNode().GetExecutorHostID())
	require.True(t, ranked[0].IsPreferred())
	requireNonSequential(t, ranked[1:])
	requireNonePreferred(t, ranked[1:])

	requireNotAlwaysRanked(1, executorHostID1, t, router, ctx, thirdCmd, instanceName)

	// Verify that tasks with a different first output are routed randomly.
	fourthCmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "affinity-routing", Value: "true"},
			},
		},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "foo", Value: "bar"},
		},
		Arguments:   []string{"gcc", "-c", "dbg", "foo.c"},
		OutputPaths: []string{"/bazel-out/bar.a"},
	}
	requireNotAlwaysRanked(0, executorHostID2, t, router, ctx, fourthCmd, instanceName)
}

func TestTaskRouter_RankNodes_AffinityRoutingNoOutputs(t *testing.T) {
	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	cmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "affinity-routing", Value: "true"},
			},
		},
	}
	instanceName := "test-instance"

	router.MarkComplete(ctx, cmd, instanceName, executorHostID1)

	nodes := sequentiallyNumberedNodes(100)

	// No nodes should be preferred as there are no outputs to route using.
	ranked := router.RankNodes(ctx, cmd, instanceName, nodes)
	requireSameExecutionNodes(t, nodes, ranked)
	requireNonSequential(t, ranked)
	requireNotAlwaysRanked(0, executorHostID1, t, router, ctx, cmd, instanceName)
}

func TestTaskRouter_RankNodes_AffinityRoutingDisabled(t *testing.T) {
	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	flags.Set(t, "executor.affinity_routing_enabled", false)
	cmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "affinity-routing", Value: "true"},
			},
		},
		Arguments:   []string{"gcc", "-c", "dbg", "foo.c"},
		OutputPaths: []string{"/bazel-out/foo.a"},
	}
	instanceName := "test-instance"

	router.MarkComplete(ctx, cmd, instanceName, executorHostID1)

	nodes := sequentiallyNumberedNodes(100)

	// No nodes should be preferred as affinity routing is disabled.
	ranked := router.RankNodes(ctx, cmd, instanceName, nodes)
	requireSameExecutionNodes(t, nodes, ranked)
	requireNonSequential(t, ranked)
	requireNotAlwaysRanked(0, executorHostID1, t, router, ctx, cmd, instanceName)
}

func TestTaskRouter_RankNodes_RunnerRecyclingTakesPrecedence(t *testing.T) {
	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	oaCmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
				{Name: "affinity-routing", Value: "true"},
			},
		},
		OutputPaths: []string{"/bazel-out/foo.a"},
	}
	instanceName := "test-instance"

	router.MarkComplete(ctx, oaCmd, instanceName, executorHostID1)

	rrCmd := &repb.Command{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
				{Name: "affinity-routing", Value: "true"},
			},
		},
	}

	router.MarkComplete(ctx, rrCmd, instanceName, executorHostID2)

	nodes := sequentiallyNumberedNodes(100)

	// Task should be routed to executor 2, because the runner recycling
	// routing should take priority
	ranked := router.RankNodes(ctx, oaCmd, instanceName, nodes)

	requireSameExecutionNodes(t, nodes, ranked)
	require.Equal(t, executorHostID2, ranked[0].GetExecutionNode().GetExecutorHostID())
	requireNonSequential(t, ranked[1:])
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

	router.MarkComplete(ctx, cmd, instanceName, executorHostID1)

	requireNotAlwaysRanked(0, executorHostID1, t, router, ctx, cmd, instanceName)
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

	router.MarkComplete(ctx1, cmd, instanceName, executorHostID1)

	ctx2 := withAuthUser(t, context.Background(), env, "US2")

	requireNotAlwaysRanked(0, executorHostID1, t, router, ctx2, cmd, instanceName)
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

	router.MarkComplete(ctx, cmd, instanceName1, executorHostID1)

	instanceName2 := "another-test-instance"

	requireNotAlwaysRanked(0, executorHostID1, t, router, ctx, cmd, instanceName2)
}

func TestTaskRouter_WorkflowGitRefRouting(t *testing.T) {
	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	nodes := sequentiallyNumberedNodes(100)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	instanceName := ""

	// Mark executor1 as having completed a workflow run on the "main" branch.
	mainBranchCmd := &repb.Command{
		Platform: &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "recycle-runner", Value: "true"},
			{Name: "workflow-id", Value: "WF123"},
		}},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GIT_BRANCH", Value: "main"},
		},
		Arguments: []string{"./buildbuddy_ci_runner"},
	}
	router.MarkComplete(ctx, mainBranchCmd, instanceName, executorHostID1)

	// executor1 should now be the preferred executor when running this workflow
	// on the main branch.
	ranked := router.RankNodes(ctx, mainBranchCmd, instanceName, nodes)
	require.Equal(t, executorHostID1, ranked[0].GetExecutionNode().GetExecutorHostID())

	// executor1 should not necessarily be preferred when running this workflow
	// on a different branch.
	prBranchCmd := &repb.Command{
		Platform: &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "recycle-runner", Value: "true"},
			{Name: "workflow-id", Value: "WF123"},
		}},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GIT_BRANCH", Value: "my-cool-pr"},
		},
		Arguments: []string{"./buildbuddy_ci_runner"},
	}
	requireNotAlwaysRanked(0, executorHostID1, t, router, ctx, prBranchCmd, instanceName)
}

func TestTaskRouter_WorkflowGitRefRouting_DefaultRef(t *testing.T) {
	flags.Set(t, "remote_execution.workflow_default_branch_routing_enabled", true)
	env := newTestEnv(t)
	router := newTaskRouter(t, env)
	nodes := sequentiallyNumberedNodes(100)
	ctx := withAuthUser(t, context.Background(), env, "US1")
	instanceName := ""

	// Mark executor1 as having completed a workflow run on the "main" branch.
	mainBranchCmd := &repb.Command{
		Platform: &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "recycle-runner", Value: "true"},
			{Name: "workflow-id", Value: "WF123"},
		}},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GIT_BRANCH", Value: "main"},
		},
		Arguments: []string{"./buildbuddy_ci_runner"},
	}
	router.MarkComplete(ctx, mainBranchCmd, instanceName, executorHostID1)

	// Even though this workflow is running on a different branch, executor1
	// should be preferred because it ran the workflow on a matching
	// default branch.
	prBranchCmd := &repb.Command{
		Platform: &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "recycle-runner", Value: "true"},
			{Name: "workflow-id", Value: "WF123"},
		}},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GIT_BRANCH", Value: "my-cool-pr"},
			{Name: "GIT_REPO_DEFAULT_BRANCH", Value: "main"},
		},
		Arguments: []string{"./buildbuddy_ci_runner"},
	}
	ranked := router.RankNodes(ctx, prBranchCmd, instanceName, nodes)
	require.Equal(t, executorHostID1, ranked[0].GetExecutionNode().GetExecutorHostID())
	// Mark executor1 as having completed a workflow run on the pr branch.
	router.MarkComplete(ctx, prBranchCmd, instanceName, executorHostID1)

	// Simulate executor2 running a workflow on the "main" branch.
	router.MarkComplete(ctx, mainBranchCmd, instanceName, executorHostID2)

	// The router should prioritize routing a workflow for the pr branch to the
	// executor that last ran the pr branch, not the one that last ran the default branch.
	ranked = router.RankNodes(ctx, prBranchCmd, instanceName, nodes)
	require.Equal(t, executorHostID1, ranked[0].GetExecutionNode().GetExecutorHostID())
}

func requireNonePreferred(t *testing.T, rankedNodes []interfaces.RankedExecutionNode) {
	for i := 1; i < len(rankedNodes); i++ {
		require.False(t, rankedNodes[i].IsPreferred())
	}
}

func requireSameExecutionNodes(t *testing.T, nodes []interfaces.ExecutionNode, ranked []interfaces.RankedExecutionNode) {
	rankedNodes := make([]interfaces.ExecutionNode, len(ranked))
	for i, rankedNode := range ranked {
		rankedNodes[i] = rankedNode.GetExecutionNode()
	}
	require.ElementsMatch(t, nodes, rankedNodes)
}

// requireNotAlwaysRanked requires that the task router does not
// deterministically assign the given rank to the given executor ID.
func requireNotAlwaysRanked(rank int, executorID string, t *testing.T, router interfaces.TaskRouter, ctx context.Context, cmd *repb.Command, instanceName string) {
	nodes := sequentiallyNumberedNodes(100)
	nTrials := 10
	for i := 0; i < nTrials; i++ {
		ranked := router.RankNodes(ctx, cmd, instanceName, nodes)

		require.Equal(t, len(nodes), len(ranked))
		if ranked[rank].GetExecutionNode().GetExecutorHostID() != executorID {
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

func requireReordered(t *testing.T, nodes []interfaces.ExecutionNode, ranked []interfaces.RankedExecutionNode) {
	requireSameExecutionNodes(t, nodes, ranked)

	for i := range nodes {
		if nodes[i].GetExecutorHostID() != ranked[i].GetExecutionNode().GetExecutorHostID() {
			return
		}
	}
	require.FailNow(t, "nodes were not reordered")
}

func requireNonSequential(t *testing.T, nodes []interfaces.RankedExecutionNode) {
	if len(nodes) <= 1 {
		require.FailNow(t, "slice too short to test for sequential order")
	}
	prev := nodes[0].GetExecutionNode().(*testNode).index
	for i := 1; i < len(nodes); i++ {
		cur := nodes[i].GetExecutionNode().(*testNode).index
		if cur < prev {
			return
		}
		prev = cur
	}
	require.FailNow(t, "nodes were in sequential order")
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
		nodes = append(nodes, &testNode{
			index:          i,
			executorID:     fmt.Sprintf("executor-%d", i),
			executorHostID: fmt.Sprintf("host-%d", i),
		})
	}
	return nodes
}

type testNode struct {
	index          int
	executorID     string
	executorHostID string
}

func (n *testNode) GetExecutorID() string {
	return n.executorID
}

func (n *testNode) GetExecutorHostID() string {
	return n.executorHostID
}
