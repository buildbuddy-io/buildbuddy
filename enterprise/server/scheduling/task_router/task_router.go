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
	return &taskRouter{
		env: env,
		rdb: rdb,
	}, nil
}

// RankNodes returns the input nodes ordered by their affinity to the given
// routing properties.
func (tr *taskRouter) RankNodes(ctx context.Context, cmd *repb.Command, remoteInstanceName string, nodes []interfaces.ExecutionNode) []interfaces.ExecutionNode {
	nodes = copyNodes(nodes)

	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	if cmd == nil {
		return nodes
	}
	preferredNodeLimit := getPreferredNodeLimit(cmd)
	if preferredNodeLimit == 0 {
		return nodes
	}

	key, err := tr.routingKey(ctx, cmd, remoteInstanceName)
	if err != nil {
		log.Errorf("Failed to compute routing key: %s", err)
		return nodes
	}
	preferredNodeIDs, err := tr.rdb.LRange(ctx, key, 0, -1).Result()
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

	log.Debugf("Preferred executor IDs for %q: %v", key, preferredNodeIDs)

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
	nodeListMaxLength := getPreferredNodeLimit(cmd)
	if nodeListMaxLength == 0 {
		return
	}
	key, err := tr.routingKey(ctx, cmd, remoteInstanceName)
	if err != nil {
		log.Errorf("Failed to compute routing key: %s", err)
		return
	}

	pipe := tr.rdb.TxPipeline()
	// Push the node to the head of the list (but first remove it if already
	// present to avoid dupes), trim to max length to prevent it from growing
	// too large, and renew the TTL.
	pipe.LRem(ctx, key, 1, executorID)
	pipe.LPush(ctx, key, executorID)
	pipe.LTrim(ctx, key, 0, int64(nodeListMaxLength)-1)
	pipe.Expire(ctx, key, routingPropsKeyTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Errorf("Failed to mark task complete: redis pipeline failed: %s", err)
		return
	}

	log.Debugf("Preferred executor %q added to %q", executorID, key)
}

// getPreferredNodeLimit returns the max number of nodes that should be stored
// in the preferred executors list for each task key, as well as the max number
// of preferred nodes that should be returned by RankNodes.
func getPreferredNodeLimit(cmd *repb.Command) int {
	isRunnerRecyclingEnabled := platform.IsTrue(platform.FindValue(cmd.GetPlatform(), platform.RecycleRunnerPropertyName))
	if !isRunnerRecyclingEnabled {
		return 0
	}
	workflowID := platform.FindValue(cmd.GetPlatform(), platform.WorkflowIDPropertyName)
	if workflowID != "" {
		return workflowsPreferredNodeLimit
	}
	return defaultPreferredNodeLimit
}

func (tr *taskRouter) routingKey(ctx context.Context, cmd *repb.Command, remoteInstanceName string) (string, error) {
	parts := []string{"task_route"}

	if u, err := perms.AuthenticatedUser(ctx, tr.env); err == nil {
		parts = append(parts, u.GetGroupID())
	} else {
		parts = append(parts, interfaces.AuthAnonymousUser)
	}

	if remoteInstanceName != "" {
		parts = append(parts, remoteInstanceName)
	}

	platform := cmd.GetPlatform()
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
