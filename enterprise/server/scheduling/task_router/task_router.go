package task_router

import (
	"context"
	"flag"
	"math/rand"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	affinityRoutingEnabled      = flag.Bool("executor.affinity_routing_enabled", true, "Enables affinity routing, which attempts to route actions to the executor that most recently ran that action.")
	defaultBranchRoutingEnabled = flag.Bool("executor.workflow_default_branch_routing_enabled", false, "Enables default branch routing for workflows. When routing a workflow action, if there are no executors that ran that action for the same git branch, try to route it to an executor that ran the action for the same default branch.")
)

const (
	day = 24 * time.Hour

	// The TTL for each node list in the routing table.
	routingPropsKeyTTL = 7 * day

	// The default max number of preferred nodes that are returned by the task
	// router for routable tasks. This is intentionally less than the number of
	// probes per task (for load balancing purposes).
	defaultPreferredNodeLimit = 1
	// The preferred node limit for workflows.
	// This is set higher than the default limit since we strongly prefer
	// workflow tasks to hit a node with a warm bazel workspace, but it is
	// set less than the number of probes so that we can autoscale the workflow
	// executor pool effectively.
	workflowsPreferredNodeLimit = 1
)

type taskRouter struct {
	env environment.Env
	rdb redis.UniversalClient

	// The RoutingStrategies available to use for task routing. The routing
	// strategy used is the first one that matches the input request from this
	// ordered slice.
	strategies []Router
}

func Register(env *real_environment.RealEnv) error {
	if env.GetRemoteExecutionRedisClient() == nil {
		return nil
	}
	// Task router uses the remote execution redis client.
	taskRouter, err := New(env)
	if err != nil {
		return status.InternalErrorf("Failed to create server: %s", err)
	}
	env.SetTaskRouter(taskRouter)
	return nil
}

func New(env environment.Env) (interfaces.TaskRouter, error) {
	rdb := env.GetRemoteExecutionRedisClient()
	if rdb == nil {
		return nil, status.FailedPreconditionError("Redis is required for task router")
	}
	strategies := []Router{runnerRecycler{}, affinityRouter{}}
	return &taskRouter{
		env:        env,
		rdb:        rdb,
		strategies: strategies,
	}, nil
}

type rankedExecutionNode struct {
	node      interfaces.ExecutionNode
	preferred bool
}

func (n rankedExecutionNode) GetExecutionNode() interfaces.ExecutionNode {
	return n.node
}

func (n rankedExecutionNode) IsPreferred() bool {
	return n.preferred
}

func nonePreferred(nodes []interfaces.ExecutionNode) []interfaces.RankedExecutionNode {
	rankedNodes := make([]interfaces.RankedExecutionNode, len(nodes))
	for i, node := range nodes {
		rankedNodes[i] = rankedExecutionNode{node: node}
	}
	return rankedNodes
}

// RankNodes returns the input nodes ordered by their affinity to the given
// routing properties.
func (tr *taskRouter) RankNodes(ctx context.Context, cmd *repb.Command, remoteInstanceName string, nodes []interfaces.ExecutionNode) []interfaces.RankedExecutionNode {
	nodes = copyNodes(nodes)

	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	params := getRoutingParams(ctx, tr.env, cmd, remoteInstanceName)
	strategy := tr.selectRouter(params)
	if strategy == nil {
		return nonePreferred(nodes)
	}

	preferredNodeLimit, routingKeys, err := strategy.RoutingInfo(params)
	if err != nil {
		log.Errorf("Failed to compute routing info: %s", err)
		return nonePreferred(nodes)
	}
	if preferredNodeLimit == 0 {
		return nonePreferred(nodes)
	}

	nodeByID := map[string]interfaces.ExecutionNode{}
	for _, node := range nodes {
		nodeByID[node.GetExecutorID()] = node
	}
	preferredSet := map[string]struct{}{}
	ranked := make([]interfaces.RankedExecutionNode, 0, len(nodes))

	// Routing keys should be prioritized in the order they were returned
	for _, routingKey := range routingKeys {
		preferredNodeIDs, err := tr.rdb.LRange(ctx, routingKey, 0, -1).Result()
		if err != nil {
			log.Errorf("Failed to rank nodes: redis LRANGE failed: %s", err)
			return nonePreferred(nodes)
		}

		log.Debugf("Preferred executor IDs for %q: %v", routingKey, preferredNodeIDs)

		// Place all preferred nodes first.
		// For each routing key, preferred nodes should be prioritized in the order
		// they appear in the Redis list.
		for _, id := range preferredNodeIDs[:min(preferredNodeLimit, len(preferredNodeIDs))] {
			node := nodeByID[id]
			if node == nil {
				continue
			}
			if _, ok := preferredSet[node.GetExecutorID()]; ok {
				continue
			}
			preferredSet[id] = struct{}{}
			ranked = append(ranked, rankedExecutionNode{node: node, preferred: true})
		}
	}

	// Randomly shuffle non-preferred nodes at the end of the ranking.
	for _, node := range nodes {
		if _, ok := preferredSet[node.GetExecutorID()]; ok {
			continue
		}
		ranked = append(ranked, rankedExecutionNode{node: node})
	}

	return ranked
}

// MarkComplete updates the routing table after a task is completed, so that
// future tasks with those properties are more likely to be fulfilled by the
// given node.
func (tr *taskRouter) MarkComplete(ctx context.Context, cmd *repb.Command, remoteInstanceName, executorID string) {
	params := getRoutingParams(ctx, tr.env, cmd, remoteInstanceName)
	strategy := tr.selectRouter(params)
	if strategy == nil {
		return
	}
	preferredNodeLimit, routingKeys, err := strategy.RoutingInfo(params)
	if err != nil {
		log.Errorf("Failed to compute routing info: %s", err)
		return
	} else if len(routingKeys) < 1 {
		log.Errorf("No routing keys were generated")
		return
	}

	if preferredNodeLimit == 0 {
		return
	}

	// Routing keys are ranked in order of priority. We only update the
	// routing table for the highest priority key.
	routingKey := routingKeys[0]

	pipe := tr.rdb.TxPipeline()
	// Push the node to the head of the list (but first remove it if already
	// present to avoid dupes), trim to max length to prevent it from growing
	// too large, and renew the TTL.
	pipe.LRem(ctx, routingKey, 1, executorID)
	pipe.LPush(ctx, routingKey, executorID)
	pipe.LTrim(ctx, routingKey, 0, int64(preferredNodeLimit)-1)
	pipe.Expire(ctx, routingKey, routingPropsKeyTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Errorf("Failed to mark task complete: redis pipeline failed: %s", err)
		return
	}

	log.Debugf("Preferred executor %q added to %q", executorID, routingKey)
}

// Contains the parameters required to make a routing decision.
type routingParams struct {
	cmd                *repb.Command
	remoteInstanceName string
	groupID            string
}

func getRoutingParams(ctx context.Context, env environment.Env, cmd *repb.Command, remoteInstanceName string) routingParams {
	groupID := interfaces.AuthAnonymousUser
	if u, err := env.GetAuthenticator().AuthenticatedUser(ctx); err == nil {
		groupID = u.GetGroupID()
	}
	return routingParams{cmd: cmd, remoteInstanceName: remoteInstanceName, groupID: groupID}
}

// Selects and returns a Router to use, or nil if none applies.
func (tr taskRouter) selectRouter(params routingParams) Router {
	for _, strategy := range tr.strategies {
		if strategy.Applies(params) {
			return strategy
		}
	}
	return nil
}

func copyNodes(nodes []interfaces.ExecutionNode) []interfaces.ExecutionNode {
	out := make([]interfaces.ExecutionNode, len(nodes))
	copy(out, nodes)
	return out
}

// A Router encapsulates a strategy for performing task routing based on a set
// of provided routingParams. Currently this interface is somewhat coupled with
// the Redis reading and writing logic in the task_router, but that could be
// uncoupled in the future when other routing strategies that aren't
// Redis-list-based are added.
type Router interface {
	// Returns true if this router applies to the given routing parameters,
	// false otherwise. Note: Applies() must be deterministic.
	Applies(params routingParams) bool

	// Returns the routing info (preferredNodeLimit and routingKeys) for the
	// provided routing parameters. The preferredNodeLimit is the number of
	// preferred executor nodes that should be used. The routing keys are
	// Redis keys where the list of preferred executor nodes are stored. Keys
	// are sorted in order of most preferred to least preferred. That order
	// should be preserved when ranking nodes.
	RoutingInfo(params routingParams) (int, []string, error)
}

// The runnerRecycler is a router that attempts to "recycle" warm execution
// nodes when possible.
type runnerRecycler struct{}

func (runnerRecycler) Applies(params routingParams) bool {
	return platform.IsTrue(platform.FindValue(params.cmd.GetPlatform(), platform.RecycleRunnerPropertyName))
}

func (runnerRecycler) preferredNodeLimit(params routingParams) int {
	if isWorkflow(params.cmd) {
		return workflowsPreferredNodeLimit
	}
	return defaultPreferredNodeLimit
}

func (runnerRecycler) routingKeys(params routingParams) ([]string, error) {
	parts := []string{"task_route", params.groupID}
	keys := make([]string, 0)

	if params.remoteInstanceName != "" {
		parts = append(parts, params.remoteInstanceName)
	}

	p := params.cmd.GetPlatform()
	if p == nil {
		p = &repb.Platform{}
	}
	b, err := proto.Marshal(p)
	if err != nil {
		return nil, status.InternalErrorf("failed to marshal Command: %s", err)
	}
	parts = append(parts, hash.Bytes(b))

	// For workflow tasks, route using git branch name so that when re-running the
	// workflow multiple times using the same branch, the runs are more likely
	// to hit an executor with a warmer snapshot cache.
	if platform.IsCICommand(params.cmd) {
		envVarNames := []string{"GIT_BRANCH"}
		if *defaultBranchRoutingEnabled {
			envVarNames = append(envVarNames, "GIT_BASE_BRANCH", "GIT_REPO_DEFAULT_BRANCH")
		}
		for _, routingEnvVar := range envVarNames {
			for _, envVar := range params.cmd.EnvironmentVariables {
				if envVar.GetName() == routingEnvVar {
					branch := envVar.GetValue()
					keys = append(keys, strings.Join(append(parts, hash.String(branch)), "/"))
				}
			}
		}
	}

	if len(keys) == 0 {
		keys = append(keys, strings.Join(parts, "/"))
	}

	return keys, nil
}

func (s runnerRecycler) RoutingInfo(params routingParams) (int, []string, error) {
	nodeLimit := s.preferredNodeLimit(params)
	keys, err := s.routingKeys(params)
	return nodeLimit, keys, err
}

func isWorkflow(cmd *repb.Command) bool {
	return platform.FindValue(cmd.GetPlatform(), platform.WorkflowIDPropertyName) != ""
}

// affinityRouter generates Redis routing keys based on:
//   - remoteInstanceName
//   - groupID
//   - platform properties
//   - and the name of the first action output
//
// Because only a single action can generate a given output in Bazel, this key
// uniquely identifies an action and is stable even if the action's inputs
// change. The intent of using this routing key is to route successive actions
// whose inputs have changed to nodes which previously executed that action to
// increase the local-cache hitrate, as it's likely that for large actions most
// of the input tree is unchanged.
type affinityRouter struct{}

func (affinityRouter) Applies(params routingParams) bool {
	return *affinityRoutingEnabled && getFirstOutput(params.cmd) != ""
}

func (affinityRouter) preferredNodeLimit(params routingParams) int {
	return defaultPreferredNodeLimit
}

func (affinityRouter) routingKey(params routingParams) (string, error) {
	parts := []string{"task_route", params.groupID}

	if params.remoteInstanceName != "" {
		parts = append(parts, params.remoteInstanceName)
	}

	platform := params.cmd.GetPlatform()
	if platform == nil {
		platform = &repb.Platform{}
	}
	b, err := proto.Marshal(platform)
	if err != nil {
		return "", status.InternalErrorf("failed to marshal Command: %s", err)
	}
	parts = append(parts, hash.Bytes(b))

	// Add the first output as the final part of the routing key. This should
	// uniquely identify a bazel action and is an attempt to route actions to
	// executor nodes that are warmed up (with inputs and OCI images) for this
	// action.
	firstOutput := getFirstOutput(params.cmd)
	if firstOutput == "" {
		return "", status.InternalError("routing key requested for action with no outputs")
	}
	parts = append(parts, hash.String(firstOutput))

	return strings.Join(parts, "/"), nil
}

func (s affinityRouter) RoutingInfo(params routingParams) (int, []string, error) {
	nodeLimit := s.preferredNodeLimit(params)
	key, err := s.routingKey(params)
	return nodeLimit, []string{key}, err
}

func getFirstOutput(cmd *repb.Command) string {
	if cmd == nil {
		return ""
	}
	if len(cmd.OutputPaths) > 0 && cmd.OutputPaths[0] != "" {
		return cmd.OutputPaths[0]
	} else if len(cmd.OutputFiles) > 0 && cmd.OutputFiles[0] != "" {
		return cmd.OutputFiles[0]
	} else if len(cmd.OutputDirectories) > 0 && cmd.OutputDirectories[0] != "" {
		return cmd.OutputDirectories[0]
	}
	return ""
}
