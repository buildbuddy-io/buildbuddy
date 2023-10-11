package task_router

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	workflowsPreferredNodeLimit = 2
)

type taskRouter struct {
	env environment.Env
	rdb redis.UniversalClient

	// The ordered list of routingStrategies to use for task routing.
	strategies []routingStrategy
}

func Register(env environment.Env) error {
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
	strategies := []routingStrategy{runnerRecyclingRoutingStrategy{}}
	return &taskRouter{
		env:        env,
		rdb:        rdb,
		strategies: strategies,
	}, nil
}

// RankNodes returns the input nodes ordered by their affinity to the given
// routing properties.
func (tr *taskRouter) RankNodes(ctx context.Context, cmd *repb.Command, remoteInstanceName string, nodes []interfaces.ExecutionNode) []interfaces.ExecutionNode {
	nodes = copyNodes(nodes)

	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	params := getRoutingParams(ctx, tr.env, cmd, remoteInstanceName)
	strategy := tr.selectRoutingStrategy(params)
	if strategy == nil {
		log.Info("no routing strategy applied to the routing params")
		return nodes
	}

	preferredNodeLimit, routingKey, err := strategy.routingInfo(params)
	if err != nil {
		log.Errorf("Failed to compute routing info: %s", err)
		return nodes
	}
	if preferredNodeLimit == 0 {
		return nodes
	}

	preferredNodeIDs, err := tr.rdb.LRange(ctx, routingKey, 0, -1).Result()
	if err != nil {
		log.Errorf("Failed to rank nodes: redis LRANGE failed: %s", err)
		return nodes
	}

	nodeByID := map[string]interfaces.ExecutionNode{}
	for _, node := range nodes {
		nodeByID[node.GetExecutorID()] = node
	}
	preferredSet := map[string]struct{}{}
	ranked := make([]interfaces.ExecutionNode, 0, len(nodes))

	log.Debugf("Preferred executor IDs for %q: %v", routingKey, preferredNodeIDs)

	// Place all preferred nodes first (in the order that they appear in the Redis
	// list), then the remaining ones in shuffled order.
	for _, id := range preferredNodeIDs[:minInt(preferredNodeLimit, len(preferredNodeIDs))] {
		node := nodeByID[id]
		if node == nil {
			continue
		}
		preferredSet[id] = struct{}{}
		ranked = append(ranked, node)
	}
	for _, node := range nodes {
		if _, ok := preferredSet[node.GetExecutorID()]; ok {
			continue
		}
		ranked = append(ranked, node)
	}

	return ranked
}

// MarkComplete updates the routing table after a task is completed, so that
// future tasks with those properties are more likely to be fulfilled by the
// given node.
func (tr *taskRouter) MarkComplete(ctx context.Context, cmd *repb.Command, remoteInstanceName, executorID string) {
	params := getRoutingParams(ctx, tr.env, cmd, remoteInstanceName)
	strategy := tr.selectRoutingStrategy(params)
	if strategy == nil {
		log.Info("no routing strategy applied to the routing params")
		return
	}
	preferredNodeLimit, routingKey, err := strategy.routingInfo(params)
	if err != nil {
		log.Errorf("Failed to compute routing info: %s", err)
		return
	}
	if preferredNodeLimit == 0 {
		return
	}

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
	if u, err := perms.AuthenticatedUser(ctx, env); err == nil {
		groupID = u.GetGroupID()
	}
	return routingParams{cmd: cmd, remoteInstanceName: remoteInstanceName, groupID: groupID}
}

// Selects and returns a routingStrategy to use, or nil if none applies.
func (tr taskRouter) selectRoutingStrategy(params routingParams) routingStrategy {
	for _, strategy := range tr.strategies {
		if strategy.applies(params) {
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// A routingStrategy encapsulates a strategy for performing task routing based
// on a set of provided routingParams. Currently this interface is somewhat
// coupled with the Redis reading and writing logic in the task_router, but
// that could be uncoupled in the future when other routing strategies that
// arent Redis-list-based are added.
type routingStrategy interface {

	// Returns true if this routing strategy applies to the given routing
	// parameters, false otherwise. Note: applies() must be deterministic.
	applies(params routingParams) bool

	// Returns the routing info (preferredNodeLimit and routingKey) for the
	// provided routing parameters. The preferredNodeLimit is the number of
	// preferred executor nodes that should be used, and the routing Key is the
	// Redis key where the list of preferred executor nodes are stored.
	routingInfo(params routingParams) (int, string, error)
}

// The runnerRecyclingRoutingStrategy is a routing strategy that attempts to
// "recycle" warm execution nodes when possible.
type runnerRecyclingRoutingStrategy struct {
}

func (runnerRecyclingRoutingStrategy) applies(params routingParams) bool {
	return platform.IsTrue(platform.FindValue(params.cmd.GetPlatform(), platform.RecycleRunnerPropertyName))
}

func (runnerRecyclingRoutingStrategy) preferredNodeLimit(params routingParams) int {
	workflowID := platform.FindValue(params.cmd.GetPlatform(), platform.WorkflowIDPropertyName)
	if workflowID != "" {
		return workflowsPreferredNodeLimit
	}
	return defaultPreferredNodeLimit
}

func (runnerRecyclingRoutingStrategy) routingKey(params routingParams) (string, error) {
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
	parts = append(parts, fmt.Sprintf("%x", sha256.Sum256(b)))

	return strings.Join(parts, "/"), nil
}

func (s runnerRecyclingRoutingStrategy) routingInfo(params routingParams) (int, string, error) {
	nodeLimit := s.preferredNodeLimit(params)
	key, err := s.routingKey(params)
	return nodeLimit, key, err
}
