package task_router

import (
	"context"
	"flag"
	"math/rand"
	"sort"
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
	defaultBranchRoutingEnabled = flag.Bool("remote_execution.workflow_default_branch_routing_enabled", false, "Enables default branch routing for workflows. When routing a workflow action, if there are no executors that ran that action for the same git branch, try to route it to an executor that ran the action for the same default branch.")
)

const (
	day = 24 * time.Hour

	// The TTL for each node list in the routing table.
	routingPropsKeyTTL = 7 * day

	// The default max number of preferred nodes that are returned by the task
	// router for routable tasks. This is intentionally less than the number of
	// probes per task (for load balancing purposes).
	defaultPreferredNodeLimit = 1
	// The preferred node limit for ci_runner tasks.
	// This is set higher than the default limit since we strongly prefer
	// these tasks to hit a node with a warm bazel workspace, but it is
	// set less than the number of probes so that we can autoscale the workflow
	// executor pool effectively.
	ciRunnerPreferredNodeLimit = 1
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
	strategies := []Router{ciRunnerRouter{}, affinityRouter{}}
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
	return dedupe(rankedNodes)
}

func dedupe(nodes []interfaces.RankedExecutionNode) []interfaces.RankedExecutionNode {
	seen := make(map[string]struct{}, len(nodes))
	deduped := make([]interfaces.RankedExecutionNode, 0, len(nodes)/2)
	for _, node := range nodes {
		if _, ok := seen[node.GetExecutionNode().GetExecutorId()]; ok {
			continue
		}
		seen[node.GetExecutionNode().GetExecutorId()] = struct{}{}
		deduped = append(deduped, node)
	}
	return deduped
}

// weightedResample resamples the slice `nodes` by weighting each node by its
// assignable CPU. This means that larger nodes have an opportunity to appear
// more often than smaller nodes, increasing their chance of being hit.
//
// To ensure fairness and diversity (mostly for tests with small number of
// nodes):
//   - the returned number of nodes is (maxSize / minSize) times greater than
//     the original number of nodes
//   - all original nodes will be returned at least once
func weightedResample(nodes []interfaces.ExecutionNode) []interfaces.ExecutionNode {
	if len(nodes) <= 1 {
		return nodes
	}
	largest := float64(0)
	smallest := float64(1e6)

	unsampledOriginalNodes := make(map[interfaces.ExecutionNode]struct{}, len(nodes))
	cumulativeSum := make([]float64, len(nodes))
	for i := 0; i < len(nodes); i++ {
		cpu := float64(nodes[i].GetAssignableMilliCpu())
		cumulativeSum[i] = cpu
		if i > 0 {
			cumulativeSum[i] += cumulativeSum[i-1]
		}
		if cpu > largest {
			largest = cpu
		}
		if cpu < smallest {
			smallest = cpu
		}
		unsampledOriginalNodes[nodes[i]] = struct{}{}
	}

	samples := make([]interfaces.ExecutionNode, len(nodes)*int(largest/smallest))
	for i := 0; i < len(samples); i++ {
		val := rand.Float64() * cumulativeSum[len(cumulativeSum)-1]
		n := sort.Search(len(cumulativeSum), func(i int) bool { return cumulativeSum[i] > val })
		sampledNode := nodes[n]
		samples[i] = sampledNode
		delete(unsampledOriginalNodes, sampledNode)
	}

	// Ensure that any of the original nodeset that was not sampled gets
	// added. This ensures that all nodes are represented, so that node
	// specific task routing ("route to this one node") still works.
	for unsampledNode := range unsampledOriginalNodes {
		samples = append(samples, unsampledNode)
	}
	return samples
}

// RankNodes returns the input nodes ordered by their affinity to the given
// routing properties.
func (tr *taskRouter) RankNodes(ctx context.Context, action *repb.Action, cmd *repb.Command, remoteInstanceName string, nodes []interfaces.ExecutionNode) []interfaces.RankedExecutionNode {
	nodes = copyNodes(nodes)

	// Resample nodes by CPU weight so that nodes with more resources
	// are more likely to get more tasks.
	nodes = weightedResample(nodes)

	params := getRoutingParams(ctx, tr.env, action, cmd, remoteInstanceName)
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

	// Note: if multiple executors live on the same host, the last one in the
	// list wins. For now, this is fine because we don't recommend running
	// multiple executors on the same host. If this use case arises then we
	// could instead route to the last executor to run the task (by exeucutor
	// ID), or if that executor doesn't exist then fall back to an arbitrary
	// executor on the same host.
	nodeByHostID := map[string]interfaces.ExecutionNode{}
	for _, node := range nodes {
		nodeByHostID[node.GetExecutorHostId()] = node
	}

	// Executor IDs of the nodes we've added to the front of the list so far.
	rankedNodeSet := map[string]struct{}{}
	ranked := make([]interfaces.RankedExecutionNode, 0, len(nodes))

	// Routing keys should be prioritized in the order they were returned
	for _, routingKey := range routingKeys {
		preferredHostIDs, err := tr.rdb.LRange(ctx, routingKey, 0, -1).Result()
		if err != nil {
			log.Errorf("Failed to rank nodes: redis LRANGE failed: %s", err)
			return nonePreferred(nodes)
		}

		log.Debugf("Preferred executor host IDs for %q: %v", routingKey, preferredHostIDs)

		// Place all preferred nodes first.
		// For each routing key, preferred nodes should be prioritized in the order
		// they appear in the Redis list.
		for _, hostID := range preferredHostIDs[:min(preferredNodeLimit, len(preferredHostIDs))] {
			node := nodeByHostID[hostID]
			if node == nil {
				continue
			}
			if _, ok := rankedNodeSet[node.GetExecutorId()]; ok {
				continue
			}
			rankedNodeSet[node.GetExecutorId()] = struct{}{}
			ranked = append(ranked, rankedExecutionNode{node: node, preferred: true})
		}
	}

	// Randomly shuffle non-preferred nodes at the end of the ranking.
	for _, node := range nodes {
		if _, ok := rankedNodeSet[node.GetExecutorId()]; ok {
			continue
		}
		ranked = append(ranked, rankedExecutionNode{node: node})
		rankedNodeSet[node.GetExecutorId()] = struct{}{}
	}

	return ranked
}

// MarkSucceeded updates the routing table after a task is completed, so that
// future tasks with those properties are more likely to be fulfilled by the
// given node.
func (tr *taskRouter) MarkSucceeded(ctx context.Context, action *repb.Action, cmd *repb.Command, remoteInstanceName, executorHostID string) {
	params := getRoutingParams(ctx, tr.env, action, cmd, remoteInstanceName)
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
	pipe.LRem(ctx, routingKey, 1, executorHostID)
	pipe.LPush(ctx, routingKey, executorHostID)
	pipe.LTrim(ctx, routingKey, 0, int64(preferredNodeLimit)-1)
	pipe.Expire(ctx, routingKey, routingPropsKeyTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Errorf("Failed to mark task complete: redis pipeline failed: %s", err)
		return
	}

	log.Debugf("Preferred executor host ID %q added to %q", executorHostID, routingKey)
}

// MarkFailed clears the routing table after a task fails. This makes it so
// that subsequent executions will run on random execution nodes.
func (tr *taskRouter) MarkFailed(ctx context.Context, action *repb.Action, cmd *repb.Command, remoteInstanceName, executorHostID string) {
	params := getRoutingParams(ctx, tr.env, action, cmd, remoteInstanceName)
	strategy := tr.selectRouter(params)
	if strategy == nil {
		return
	}
	_, routingKeys, err := strategy.RoutingInfo(params)
	if err != nil {
		log.CtxErrorf(ctx, "Failed to compute routing info: %s", err)
		return
	}
	for _, routingKey := range routingKeys {
		// Note: -1 means remove all occurrences.
		if res := tr.rdb.LRem(ctx, routingKey, -1, executorHostID); res.Err() != nil {
			log.CtxErrorf(ctx, "Error removing task routing state from redis: %s", res.Err())
		} else {
			log.CtxDebugf(ctx, "Removed %s from routing key %s", executorHostID, routingKey)
		}
	}
}

// Contains the parameters required to make a routing decision.
type routingParams struct {
	cmd                *repb.Command
	platform           *repb.Platform
	remoteInstanceName string
	groupID            string
}

func getRoutingParams(ctx context.Context, env environment.Env, action *repb.Action, cmd *repb.Command, remoteInstanceName string) routingParams {
	groupID := interfaces.AuthAnonymousUser
	if u, err := env.GetAuthenticator().AuthenticatedUser(ctx); err == nil {
		groupID = u.GetGroupID()
	}
	return routingParams{
		cmd:                cmd,
		platform:           platform.GetProto(action, cmd),
		remoteInstanceName: remoteInstanceName,
		groupID:            groupID}
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

// The ciRunnerRouter routes ci_runner tasks according to git branch
// information.
type ciRunnerRouter struct{}

func (ciRunnerRouter) Applies(params routingParams) bool {
	return platform.IsCICommand(params.cmd, params.platform) && platform.IsTrue(platform.FindValue(params.platform, platform.RecycleRunnerPropertyName))
}

func (ciRunnerRouter) preferredNodeLimit(_ routingParams) int {
	return ciRunnerPreferredNodeLimit
}

func (ciRunnerRouter) routingKeys(params routingParams) ([]string, error) {
	parts := []string{"task_route", params.groupID}
	keys := make([]string, 0)

	if params.remoteInstanceName != "" {
		parts = append(parts, params.remoteInstanceName)
	}

	b, err := proto.Marshal(params.platform)
	if err != nil {
		return nil, status.InternalErrorf("failed to marshal Command: %s", err)
	}
	parts = append(parts, hash.Bytes(b))

	// For workflow tasks, route using git branch name so that when re-running the
	// workflow multiple times using the same branch, the runs are more likely
	// to hit an executor with a warmer snapshot cache.
	if platform.IsCICommand(params.cmd, params.platform) {
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

func (s ciRunnerRouter) RoutingInfo(params routingParams) (int, []string, error) {
	nodeLimit := s.preferredNodeLimit(params)
	keys, err := s.routingKeys(params)
	return nodeLimit, keys, err
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

func (affinityRouter) preferredNodeLimit(_ routingParams) int {
	return defaultPreferredNodeLimit
}

func (affinityRouter) routingKey(params routingParams) (string, error) {
	parts := []string{"task_route", params.groupID}

	if params.remoteInstanceName != "" {
		parts = append(parts, params.remoteInstanceName)
	}

	b, err := proto.Marshal(params.platform)
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
