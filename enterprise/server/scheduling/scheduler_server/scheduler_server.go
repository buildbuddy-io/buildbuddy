package scheduler_server

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	remote_execution_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config"
	scheduler_server_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server/config"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	tpb "github.com/buildbuddy-io/buildbuddy/proto/trace"
)

var (
	defaultPoolName              = flag.String("remote_execution.default_pool_name", "", "The default executor pool to use if one is not specified.")
	sharedExecutorPoolGroupID    = flag.String("remote_execution.shared_executor_pool_group_id", "", "Group ID that owns the shared executor pool.")
	requireExecutorAuthorization = flag.Bool("remote_execution.require_executor_authorization", false, "If true, executors connecting to this server must provide a valid executor API key.")
	removeStaleExecutors         = flag.Bool("remote_execution.remove_stale_executors", false, "If true, executors are removed if they are not heard from for a prolonged amount of time.")
)

const (
	leaseInterval    = 10 * time.Second
	leaseGracePeriod = 10 * time.Second

	// This number controls how many reservations the scheduler will
	// enqueue (across executor nodes) for each task. Typically this
	// is 2 -- we've increased it to 3 because each executor will
	// work on tasks "as they fit" into available RAM, and this gives
	// us a slightly higher chance of a task being completed at the
	// cost of slightly more network traffic / queueing latency.
	probesPerTask = 3

	// How stale to allow the set of execution backends to be. If a new
	// request to enqueue work is recieved and the set of execution nodes
	// was fetched more than this duration ago, they will be re-fetched.
	maxAllowedExecutionNodesStaleness = 10 * time.Second

	// An executor is removed if it does not refresh its registration within
	// this amount of time.
	executorMaxRegistrationStaleness = 10 * time.Minute

	// The maximum number of times a task may be re-enqueued.
	maxTaskAttemptCount = 5

	// Number of unclaimed tasks to try to assign to a node that newly joined.
	tasksToEnqueueOnJoin = 20

	// Maximum task TTL in Redis.
	taskTTL = 24 * time.Hour

	// Names of task fields in Redis task hash.
	redisTaskProtoField       = "taskProto"
	redisTaskMetadataField    = "schedulingMetadataProto"
	redisTaskQueuedAtUsec     = "queuedAtUsec"
	redisTaskAttempCountField = "attemptCount"
	redisTaskClaimedField     = "claimed"

	// Maximum number of unclaimed task IDs we track per pool.
	maxUnclaimedTasksTracked = 10_000
	// TTL for sets used to track unclaimed tasks in Redis. TTL is extended when new tasks are added.
	unclaimedTaskSetTTL = 1 * time.Hour
	// Unclaimed tasks older than this are removed from the unclaimed tasks list.
	unclaimedTaskMaxAge = 2 * time.Hour

	unusedSchedulerClientExpiration    = 5 * time.Minute
	unusedSchedulerClientCheckInterval = 1 * time.Minute

	// Timeout when making EnqueueTaskReservation RPC to a different scheduler.
	schedulerEnqueueTaskReservationTimeout      = 3 * time.Second
	schedulerEnqueueTaskReservationFailureSleep = 1 * time.Second
	// Timeout when sending EnqueueTaskReservation request to a connected executor.
	executorEnqueueTaskReservationTimeout = 100 * time.Millisecond

	removeExecutorCleanupTimeout = 15 * time.Second

	// How often we revalidate credentials for an open registration stream.
	checkRegistrationCredentialsInterval = 5 * time.Minute

	// Platform property value corresponding with the darwin (Mac) operating system.
	darwinOperatingSystemName = "darwin"
)

var (
	queueWaitTimeMs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "queue_wait_time_ms",
		Help:    "WorkQueue wait time [milliseconds]",
		Buckets: prometheus.ExponentialBuckets(1, 2, 20),
	})
	// Claim field is set only if task exists & claim field is not present.
	// Return values:
	//  - 0 claim failed for unknown reason (shouldn't happen)
	//  - 1 claim successful
	//  - 10 task doesn't exist
	//  - 11 task already claimed
	redisAcquireClaim = redis.NewScript(`
		-- Task not found
		if redis.call("exists", KEYS[1]) == 0 then
			return 10
		end
	
		-- Task already claimed.
		if redis.call("hexists", KEYS[1], "claimed") == 1 then
			return 11
		end

		return redis.call("hset", KEYS[1], "claimed", "1") 
		`)
	// Claim field is removed only if it's present.
	redisReleaseClaim = redis.NewScript(`
		if redis.call("hget", KEYS[1], "claimed") == "1" then 
			return redis.call("hdel", KEYS[1], "claimed")
		else 
			return 0 
		end`)
	// Task deleted if claim field is present.
	redisDeleteClaimedTask = redis.NewScript(`
		if redis.call("hget", KEYS[1], "claimed") == "1" then 
			return redis.call("del", KEYS[1]) 
		else 
			return 0 
		end`)
)

func init() {
	prometheus.MustRegister(queueWaitTimeMs)
}

// enqueueTaskReservationRequest represents a request to be sent via the executor work stream and a channel for the
// reply once one is received via the stream.
type enqueueTaskReservationRequest struct {
	proto    *scpb.EnqueueTaskReservationRequest
	response chan<- *scpb.EnqueueTaskReservationResponse
}

type executorHandle struct {
	env                  environment.Env
	scheduler            *SchedulerServer
	requireAuthorization bool
	stream               scpb.Scheduler_RegisterAndStreamWorkServer
	groupID              string

	registrationMu sync.Mutex
	registration   *scpb.ExecutionNode

	mu       sync.RWMutex
	requests chan enqueueTaskReservationRequest
	replies  map[string]chan<- *scpb.EnqueueTaskReservationResponse
}

func newExecutorHandle(env environment.Env, scheduler *SchedulerServer, requireAuthorization bool, stream scpb.Scheduler_RegisterAndStreamWorkServer) *executorHandle {
	h := &executorHandle{
		env:                  env,
		scheduler:            scheduler,
		requireAuthorization: requireAuthorization,
		stream:               stream,
		requests:             make(chan enqueueTaskReservationRequest, 10),
		replies:              make(map[string]chan<- *scpb.EnqueueTaskReservationResponse),
	}
	h.startTaskReservationStreamer()
	return h
}

func (h *executorHandle) GroupID() string {
	return h.groupID
}

func (h *executorHandle) authorize(ctx context.Context) (string, error) {
	if !h.requireAuthorization {
		return "", nil
	}

	auth := h.env.GetAuthenticator()
	if auth == nil {
		return "", status.FailedPreconditionError("executor authorization required, but authenticator is not set")
	}
	// We intentionally use AuthenticateGRPCRequest instead of AuthenticatedUser to ensure that we refresh the
	// credentials to handle the case where the API key is deleted (or capabilities are updated) after the stream was
	// created.
	user, err := auth.AuthenticateGRPCRequest(ctx)
	if err != nil {
		return "", err
	}
	if !user.HasCapability(akpb.ApiKey_REGISTER_EXECUTOR_CAPABILITY) {
		return "", status.PermissionDeniedError("API key is missing executor registration capability")
	}
	return user.GetGroupID(), nil
}

func (h *executorHandle) getRegistration() *scpb.ExecutionNode {
	h.registrationMu.Lock()
	defer h.registrationMu.Unlock()
	return h.registration
}

func (h *executorHandle) setRegistration(r *scpb.ExecutionNode) {
	h.registrationMu.Lock()
	defer h.registrationMu.Unlock()
	h.registration = r
}

func (h *executorHandle) Serve(ctx context.Context) error {
	groupID, err := h.authorize(ctx)
	if err != nil {
		return err
	}
	h.groupID = groupID

	removeConnectedExecutor := func() {
		registration := h.getRegistration()
		if registration == nil {
			return
		}
		h.scheduler.RemoveConnectedExecutor(ctx, h, registration)
		h.setRegistration(nil)
	}
	defer removeConnectedExecutor()

	requestChan := make(chan *scpb.RegisterAndStreamWorkRequest, 1)
	errChan := make(chan error)
	go func() {
		for {
			req, err := h.stream.Recv()
			if err == io.EOF {
				close(requestChan)
				break
			}
			if err != nil {
				errChan <- err
				break
			}
			requestChan <- req
		}
	}()

	checkCredentialsTicker := time.NewTicker(checkRegistrationCredentialsInterval)
	defer checkCredentialsTicker.Stop()

	executorID := "unknown"
	for {
		select {
		case <-h.scheduler.shuttingDown:
			return status.CanceledError("server is shutting down")
		case err := <-errChan:
			return err
		case req, ok := <-requestChan:
			if !ok {
				return nil
			}
			if req.GetRegisterExecutorRequest() != nil {
				registration := req.GetRegisterExecutorRequest().GetNode()
				if err := h.scheduler.AddConnectedExecutor(ctx, h, registration); err != nil {
					return err
				}
				h.setRegistration(registration)
				executorID = registration.GetExecutorId()
			} else if req.GetEnqueueTaskReservationResponse() != nil {
				h.handleTaskReservationResponse(req.GetEnqueueTaskReservationResponse())
			} else if req.GetShuttingDownRequest() != nil {
				log.CtxInfof(ctx, "Executor %q is going away, re-enqueueing %d task reservations", executorID, len(req.GetShuttingDownRequest().GetTaskId()))
				// Remove the executor first so that we don't try to send any work its way.
				removeConnectedExecutor()
				for _, taskID := range req.GetShuttingDownRequest().GetTaskId() {
					if err := h.scheduler.reEnqueueTask(ctx, taskID, 1 /*=numReplicas*/, "executor shutting down"); err != nil {
						log.CtxWarningf(ctx, "Could not re-enqueue task reservation for executor %q going down: %s", executorID, err)
					}
				}
			} else {
				out, _ := prototext.Marshal(req)
				log.CtxWarningf(ctx, "Invalid message from executor:\n%q", string(out))
				return status.InternalErrorf("message from executor did not contain any data")
			}
		case <-checkCredentialsTicker.C:
			if _, err := h.authorize(ctx); err != nil {
				if status.IsPermissionDeniedError(err) || status.IsUnauthenticatedError(err) {
					return err
				}
				log.CtxWarningf(ctx, "could not revalidate executor registration: %h", err)
			}
		}
	}
}

func (h *executorHandle) handleTaskReservationResponse(response *scpb.EnqueueTaskReservationResponse) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := h.replies[response.GetTaskId()]
	if ch == nil {
		log.CtxWarningf(h.stream.Context(), "Got task reservation response for unknown task %q", response.GetTaskId())
		return
	}

	// Reply channel is buffered so it's okay to write while holding lock.
	ch <- response
	close(ch)
	delete(h.replies, response.GetTaskId())
}

func (h *executorHandle) EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error) {
	// EnqueueTaskReservation may be called multiple times and OpenTelemetry doesn't have clear documentation as to
	// whether it's safe to call Inject using a carrier that already has metadata so we clone the proto to be defensive.
	// We also clone to avoid mutating the proto in applyMeasuredTaskSize below.
	req, ok := proto.Clone(req).(*scpb.EnqueueTaskReservationRequest)
	if !ok {
		log.CtxErrorf(ctx, "could not clone reservation request")
		return nil, status.InternalError("could not clone reservation request")
	}
	tracing.InjectProtoTraceMetadata(ctx, req.GetTraceMetadata(), func(m *tpb.Metadata) { req.TraceMetadata = m })

	// Just before enqueueing, resize the task to match the measured task size,
	// up to the executor's limits. This late-resizing approach ensures that we
	// assign tasks to executors independently of any measured task sizes, to
	// avoid issues where a task can't be rescheduled due to its measured size
	// exceeding executor limits.
	h.applyMeasuredTaskSize(req)

	timeout := time.NewTimer(executorEnqueueTaskReservationTimeout)
	rspCh := make(chan *scpb.EnqueueTaskReservationResponse, 1)
	select {
	case h.requests <- enqueueTaskReservationRequest{proto: req, response: rspCh}:
	case <-ctx.Done():
		return nil, status.CanceledErrorf("could not enqueue task reservation %q", req.GetTaskId())
	case <-timeout.C:
		log.CtxWarningf(ctx, "Could not enqueue task reservation %q on to work stream within timeout", req.GetTaskId())
		return nil, status.DeadlineExceededErrorf("could not enqueue task reservation %q on to stream", req.GetTaskId())
	}
	if !timeout.Stop() {
		<-timeout.C
	}

	select {
	case <-ctx.Done():
		return nil, status.CanceledErrorf("could not enqueue task reservation %q", req.GetTaskId())
	case rsp := <-rspCh:
		return rsp, nil
	}
}

func (h *executorHandle) applyMeasuredTaskSize(req *scpb.EnqueueTaskReservationRequest) {
	registration := h.getRegistration()
	if registration == nil {
		return
	}

	size := req.GetTaskSize()
	measuredSize := req.GetSchedulingMetadata().GetMeasuredTaskSize()
	if measuredSize.GetEstimatedMemoryBytes() != 0 {
		size.EstimatedMemoryBytes = measuredSize.GetEstimatedMemoryBytes()
		executorMem := int64(float64(registration.GetAssignableMemoryBytes()) * tasksize.MaxResourceCapacityRatio)
		if size.EstimatedMemoryBytes > executorMem {
			size.EstimatedMemoryBytes = executorMem
		}
	}
	if measuredSize.GetEstimatedMilliCpu() != 0 {
		size.EstimatedMilliCpu = measuredSize.GetEstimatedMilliCpu()
		executorMilliCPU := int64(float64(registration.GetAssignableMilliCpu()) * tasksize.MaxResourceCapacityRatio)
		if size.EstimatedMilliCpu > executorMilliCPU {
			size.EstimatedMilliCpu = executorMilliCPU
		}
	}

	req.TaskSize = size
	if req.SchedulingMetadata != nil {
		req.SchedulingMetadata.TaskSize = size
	}
}

func (h *executorHandle) startTaskReservationStreamer() {
	go func() {
		for {
			select {
			case req := <-h.requests:
				msg := scpb.RegisterAndStreamWorkResponse{EnqueueTaskReservationRequest: req.proto}
				h.mu.Lock()
				h.replies[req.proto.GetTaskId()] = req.response
				h.mu.Unlock()
				if err := h.stream.Send(&msg); err != nil {
					log.CtxWarningf(h.stream.Context(), "Error sending task reservation response: %s", err)
					return
				}
			case <-h.stream.Context().Done():
				return
			}
		}
	}()
}

type executionNode struct {
	executorID            string
	assignableMemoryBytes int64
	assignableMilliCpu    int64
	// Optional host:port of the scheduler to which the executor is connected. Only set for executors connecting using
	// the "task streaming" API.
	schedulerHostPort string
	// Optional handle for locally connected executor that can be used to enqueue task reservations.
	handle *executorHandle
}

func (en *executionNode) CanFit(size *scpb.TaskSize) bool {
	return int64(float64(en.assignableMemoryBytes)*tasksize.MaxResourceCapacityRatio) >= size.GetEstimatedMemoryBytes() &&
		int64(float64(en.assignableMilliCpu)*tasksize.MaxResourceCapacityRatio) >= size.GetEstimatedMilliCpu()
}

func (en *executionNode) String() string {
	if en.handle != nil {
		return fmt.Sprintf("connected executor(%s)", en.executorID)
	}
	return fmt.Sprintf("executor(%s) @ scheduler(%s)", en.executorID, en.schedulerHostPort)
}

func (en *executionNode) GetExecutorID() string {
	return en.executorID
}

func nodesThatFit(nodes []*executionNode, taskSize *scpb.TaskSize) []*executionNode {
	var out []*executionNode
	for _, node := range nodes {
		if node.CanFit(taskSize) {
			out = append(out, node)
		}
	}
	return out
}

// TODO(bduffany): Expand interfaces.ExecutionNode interface to include all
// executionNode functionality, then use interfaces.ExecutionNode everywhere.

func toNodeInterfaces(nodes []*executionNode) []interfaces.ExecutionNode {
	out := make([]interfaces.ExecutionNode, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, node)
	}
	return out
}

func fromNodeInterfaces(nodes []interfaces.ExecutionNode) ([]*executionNode, error) {
	out := make([]*executionNode, 0, len(nodes))
	for _, node := range nodes {
		en, ok := node.(*executionNode)
		if !ok {
			return nil, status.InternalError("failed to convert executionNode to interface; this should never happen")
		}
		out = append(out, en)
	}
	return out, nil
}

type nodePoolKey struct {
	groupID string
	os      string
	arch    string
	pool    string
}

func (k *nodePoolKey) redisKeySuffix() string {
	key := ""
	if k.groupID != "" {
		key += k.groupID + "-"
	}
	return key + fmt.Sprintf("%s-%s-%s", k.os, k.arch, k.pool)
}

func (k *nodePoolKey) redisPoolKey() string {
	return "executorPool/" + k.redisKeySuffix()
}

func (k *nodePoolKey) redisUnclaimedTasksKey() string {
	return "unclaimedTasks/" + k.redisKeySuffix()
}

type nodePool struct {
	env       environment.Env
	rdb       redis.UniversalClient
	mu        sync.Mutex
	lastFetch time.Time
	nodes     []*executionNode
	key       nodePoolKey
	// Executors that are currently connected to this instance of the scheduler server.
	connectedExecutors []*executionNode
}

func newNodePool(env environment.Env, key nodePoolKey) *nodePool {
	np := &nodePool{
		env: env,
		key: key,
		rdb: env.GetRemoteExecutionRedisClient(),
	}
	return np
}

func (np *nodePool) fetchExecutionNodes(ctx context.Context) ([]*executionNode, error) {
	redisExecutors, err := np.rdb.HGetAll(ctx, np.key.redisPoolKey()).Result()
	if err != nil {
		return nil, err
	}
	var executors []*executionNode
	for id, data := range redisExecutors {
		node := &scpb.RegisteredExecutionNode{}
		err := proto.Unmarshal([]byte(data), node)
		if err != nil {
			return nil, err
		}

		if *removeStaleExecutors && time.Since(node.GetLastPingTime().AsTime()) > executorMaxRegistrationStaleness {
			log.Infof("Removing stale executor %q from pool %+v", id, np.key)
			if err := np.rdb.HDel(ctx, np.key.redisPoolKey(), id).Err(); err != nil {
				log.Warningf("could not remove stale executor: %s", err)
			}
			continue
		}

		executors = append(executors, &executionNode{
			executorID:            id,
			schedulerHostPort:     node.GetSchedulerHostPort(),
			assignableMemoryBytes: node.GetRegistration().GetAssignableMemoryBytes(),
			assignableMilliCpu:    node.GetRegistration().GetAssignableMilliCpu(),
		})
	}

	return executors, nil
}

// GetNodes returns the execution nodes in this node pool, optionally filtering
// to just the nodes that are directly connected to this server instance.
func (np *nodePool) GetNodes(connectedOnly bool) []*executionNode {
	np.mu.Lock()
	defer np.mu.Unlock()

	if connectedOnly {
		return np.connectedExecutors
	}
	return np.nodes
}

func (np *nodePool) RefreshNodes(ctx context.Context) error {
	np.mu.Lock()
	defer np.mu.Unlock()
	if np.lastFetch.Unix() > time.Now().Add(-1*maxAllowedExecutionNodesStaleness).Unix() && len(np.nodes) > 0 {
		return nil
	}
	nodes, err := np.fetchExecutionNodes(ctx)
	if err != nil {
		return err
	}
	np.nodes = nodes
	np.lastFetch = time.Now()
	return nil
}

func (np *nodePool) NodeCount(ctx context.Context, taskSize *scpb.TaskSize) (int, error) {
	if err := np.RefreshNodes(ctx); err != nil {
		return 0, err
	}
	np.mu.Lock()
	defer np.mu.Unlock()

	if len(np.nodes) == 0 {
		return 0, status.UnavailableErrorf("No registered executors in pool %q with os %q with arch %q.", np.key.pool, np.key.os, np.key.arch)
	}

	fitCount := 0
	for _, node := range np.nodes {
		if node.CanFit(taskSize) {
			fitCount++
		}
	}

	if fitCount == 0 {
		return 0, status.UnavailableErrorf("No registered executors in pool %q with os %q with arch %q can fit a task with %d milli-cpu and %d bytes of memory.", np.key.pool, np.key.os, np.key.arch, taskSize.GetEstimatedMilliCpu(), taskSize.GetEstimatedMemoryBytes())
	}

	return fitCount, nil
}

func (np *nodePool) AddConnectedExecutor(id string, mem int64, cpu int64, handle *executorHandle) bool {
	np.mu.Lock()
	defer np.mu.Unlock()
	for _, e := range np.connectedExecutors {
		if e.executorID == id {
			return false
		}
	}
	np.connectedExecutors = append(np.connectedExecutors, &executionNode{
		executorID:            id,
		handle:                handle,
		assignableMemoryBytes: mem,
		assignableMilliCpu:    cpu,
	})
	return true
}

func (np *nodePool) RemoveConnectedExecutor(id string) bool {
	np.mu.Lock()
	defer np.mu.Unlock()
	for i, e := range np.connectedExecutors {
		if e.executorID == id {
			nodes := make([]*executionNode, 0, len(np.connectedExecutors)-1)
			nodes = append(nodes, np.connectedExecutors[:i]...)
			nodes = append(nodes, np.connectedExecutors[i+1:]...)
			np.connectedExecutors = nodes
			return true
		}
	}
	return false
}

func (np *nodePool) FindConnectedExecutorByID(executorID string) *executionNode {
	if executorID == "" {
		return nil
	}
	np.mu.Lock()
	defer np.mu.Unlock()
	for _, node := range np.connectedExecutors {
		if node.GetExecutorID() == executorID {
			return node
		}
	}
	return nil
}

func (np *nodePool) AddUnclaimedTask(ctx context.Context, taskID string) error {
	key := np.key.redisUnclaimedTasksKey()
	m := &redis.Z{
		Member: taskID,
		Score:  float64(time.Now().Unix()),
	}
	err := np.rdb.ZAdd(ctx, key, m).Err()
	if err != nil {
		return err
	}
	err = np.rdb.Expire(ctx, key, unclaimedTaskSetTTL).Err()
	if err != nil {
		return err
	}

	// Trim the set if necessary.
	// The next 2 commands are not atomic but it's okay if the list length is not exactly what we want.
	n, err := np.rdb.ZCard(ctx, key).Result()
	if err != nil {
		return err
	}
	if n > maxUnclaimedTasksTracked {
		// Trim the oldest tasks. We use the task insertion timestamp as the score so the oldest task is at rank 0, next
		// oldest is at rank 1 and so on. We subtract 1 because the indexes are inclusive.
		if err := np.rdb.ZRemRangeByRank(ctx, key, 0, n-maxUnclaimedTasksTracked-1).Err(); err != nil {
			log.CtxWarningf(ctx, "Error trimming unclaimed tasks: %s", err)
		}
	}

	// Also trim any stale tasks from the set. The data is stored in score order so this is a cheap operation.
	cutoff := time.Now().Add(-unclaimedTaskMaxAge).Unix()
	if err := np.rdb.ZRemRangeByScore(ctx, key, "0", strconv.FormatInt(cutoff, 10)).Err(); err != nil {
		log.CtxWarningf(ctx, "Error deleting old unclaimed tasks: %s", err)
	}

	return nil
}

func (np *nodePool) RemoveUnclaimedTask(ctx context.Context, taskID string) error {
	return np.rdb.ZRem(ctx, np.key.redisUnclaimedTasksKey(), taskID).Err()
}

func (np *nodePool) SampleUnclaimedTasks(ctx context.Context, n int) ([]string, error) {
	// Get all task IDs. Redis >=6.2 has a ZRANDMEMBER command, but we don't want to assume that onprem customers have a
	// relatively new Redis version.
	unclaimed, err := np.rdb.ZRange(ctx, np.key.redisUnclaimedTasksKey(), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	// Random sample (without replacement) up to `count` tasks from the
	// returned results.
	rand.Shuffle(len(unclaimed), func(i, j int) {
		unclaimed[i], unclaimed[j] = unclaimed[j], unclaimed[i]
	})
	return unclaimed[:minInt(n, len(unclaimed))], nil
}

type persistedTask struct {
	taskID          string
	metadata        *scpb.SchedulingMetadata
	serializedTask  []byte
	queuedTimestamp time.Time
	attemptCount    int64
}

type schedulerClient struct {
	// either localServer or rpc* fields will be populated depending on whether the destination is local or remote.

	localServer *SchedulerServer
	rpcClient   scpb.SchedulerClient
	rpcConn     *grpc.ClientConn

	lastAccess time.Time
}

func (c *schedulerClient) EnqueueTaskReservation(ctx context.Context, request *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error) {
	if c.localServer != nil {
		return c.localServer.EnqueueTaskReservation(ctx, request)
	}
	return c.rpcClient.EnqueueTaskReservation(ctx, request)
}

type schedulerClientCache struct {
	mu      sync.Mutex
	clients map[string]schedulerClient
	// Address of this app instance. If the destination address matches the address of this instance, we call into
	// the local scheduler server instance directly instead of using RPCs.
	localServerHostPort string
	localServer         *SchedulerServer
}

func newSchedulerClientCache(localServerHostPort string, localServer *SchedulerServer) *schedulerClientCache {
	cache := &schedulerClientCache{
		clients:             make(map[string]schedulerClient),
		localServerHostPort: localServerHostPort,
		localServer:         localServer,
	}
	cache.startExpirer()
	return cache
}

func (c *schedulerClientCache) startExpirer() {
	go func() {
		for {
			c.mu.Lock()
			for addr, client := range c.clients {
				if time.Now().Sub(client.lastAccess) > unusedSchedulerClientExpiration {
					if client.rpcConn != nil {
						_ = client.rpcConn.Close()
					}
					delete(c.clients, addr)
				}
			}
			c.mu.Unlock()
			time.Sleep(unusedSchedulerClientCheckInterval)
		}
	}()
}

func (c *schedulerClientCache) get(hostPort string) (schedulerClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	client, ok := c.clients[hostPort]
	if !ok {
		log.Infof("Creating new scheduler client for %q", hostPort)
		if hostPort == c.localServerHostPort {
			client = schedulerClient{localServer: c.localServer}
		} else {
			// This is non-blocking so it's OK to hold the lock.
			conn, err := grpc_client.DialTarget("grpc://" + hostPort)
			if err != nil {
				return schedulerClient{}, status.UnavailableErrorf("could not dial scheduler: %s", err)
			}
			client = schedulerClient{rpcClient: scpb.NewSchedulerClient(conn), rpcConn: conn}
		}
		c.clients[hostPort] = client
	}
	client.lastAccess = time.Now()
	return client, nil
}

// Options for overriding server behavior needed for testing.
type Options struct {
	LocalPortOverride            int32
	RequireExecutorAuthorization bool
}

type SchedulerServer struct {
	env                  environment.Env
	rdb                  redis.UniversalClient
	taskRouter           interfaces.TaskRouter
	schedulerClientCache *schedulerClientCache
	shuttingDown         <-chan struct{}
	// host:port at which this scheduler can be reached
	ownHostPort string

	// If enabled, users may register their own executors.
	// When enabled, the executor group ID becomes part of the executor key.
	enableUserOwnedExecutors bool
	// Force darwin executions to use executors owned by user.
	forceUserOwnedDarwinExecutors bool
	// If enabled, executors will be required to present an API key with appropriate capabilities in order to register.
	requireExecutorAuthorization bool

	enableRedisAvailabilityMonitoring bool

	mu    sync.RWMutex
	pools map[nodePoolKey]*nodePool
}

func Register(env environment.Env) error {
	if !remote_execution_config.RemoteExecutionEnabled() {
		return nil
	}
	schedulerServer, err := NewSchedulerServer(env)
	if err != nil {
		return status.InternalErrorf("Error configuring scheduler server: %v", err)
	}
	env.SetSchedulerService(schedulerServer)
	return nil
}

func NewSchedulerServer(env environment.Env) (*SchedulerServer, error) {
	return NewSchedulerServerWithOptions(env, &Options{})
}

func NewSchedulerServerWithOptions(env environment.Env, options *Options) (*SchedulerServer, error) {
	if env.GetRemoteExecutionRedisClient() == nil {
		return nil, status.FailedPreconditionErrorf("Redis is required for remote execution")
	}

	shuttingDown := make(chan struct{})
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		close(shuttingDown)
		return nil
	})

	taskRouter := env.GetTaskRouter()
	if taskRouter == nil {
		return nil, status.FailedPreconditionError("Missing task router in env")
	}

	ownHostname, err := resources.GetMyHostname()
	if err != nil {
		return nil, status.UnknownErrorf("Could not determine own hostname: %s", err)
	}
	ownPort := options.LocalPortOverride
	if ownPort == 0 {
		ownPort, err = resources.GetMyPort()
		if err != nil {
			return nil, status.UnknownErrorf("Could not determine own port: %s", err)
		}
	}

	s := &SchedulerServer{
		env:                               env,
		pools:                             make(map[nodePoolKey]*nodePool),
		rdb:                               env.GetRemoteExecutionRedisClient(),
		taskRouter:                        taskRouter,
		shuttingDown:                      shuttingDown,
		enableUserOwnedExecutors:          remote_execution_config.RemoteExecutionEnabled() && scheduler_server_config.UserOwnedExecutorsEnabled(),
		forceUserOwnedDarwinExecutors:     remote_execution_config.RemoteExecutionEnabled() && scheduler_server_config.ForceUserOwnedDarwinExecutors(),
		requireExecutorAuthorization:      options.RequireExecutorAuthorization || (remote_execution_config.RemoteExecutionEnabled() && *requireExecutorAuthorization),
		enableRedisAvailabilityMonitoring: remote_execution_config.RemoteExecutionEnabled() && env.GetRemoteExecutionService().RedisAvailabilityMonitoringEnabled(),
		ownHostPort:                       fmt.Sprintf("%s:%d", ownHostname, ownPort),
	}
	s.schedulerClientCache = newSchedulerClientCache(s.ownHostPort, s)
	return s, nil
}

func (s *SchedulerServer) GetGroupIDAndDefaultPoolForUser(ctx context.Context, os string, useSelfHosted bool) (string, string, error) {
	if !s.enableUserOwnedExecutors {
		return "", *defaultPoolName, nil
	}
	user, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		if s.env.GetAuthenticator().AnonymousUsageEnabled() {
			if s.forceUserOwnedDarwinExecutors && os == darwinOperatingSystemName {
				return "", "", status.FailedPreconditionErrorf("Darwin remote build execution is not enabled for anonymous requests.")
			}
			if useSelfHosted {
				return "", "", status.FailedPreconditionErrorf("Self-hosted executors not enabled for anonymous requests.")
			}
			return *sharedExecutorPoolGroupID, *defaultPoolName, nil
		}
		return "", "", err
	}
	if user.GetUseGroupOwnedExecutors() {
		return user.GetGroupID(), "", nil
	}
	if s.forceUserOwnedDarwinExecutors && os == darwinOperatingSystemName {
		return user.GetGroupID(), "", nil
	}
	if useSelfHosted {
		return user.GetGroupID(), "", nil
	}
	return *sharedExecutorPoolGroupID, *defaultPoolName, nil
}

func (s *SchedulerServer) checkPreconditions(node *scpb.ExecutionNode) error {
	if node.GetHost() == "" || node.GetPort() == 0 {
		return status.FailedPreconditionErrorf("Cannot register node with empty host/port: %s:%d", node.GetHost(), node.GetPort())
	}

	return nil
}

func (s *SchedulerServer) RemoveConnectedExecutor(ctx context.Context, handle *executorHandle, node *scpb.ExecutionNode) {
	nodePoolKey := nodePoolKey{os: node.GetOs(), arch: node.GetArch(), pool: node.GetPool()}
	if s.enableUserOwnedExecutors {
		nodePoolKey.groupID = handle.GroupID()
	}
	pool, ok := s.getPool(nodePoolKey)
	if ok {
		if !pool.RemoveConnectedExecutor(node.GetExecutorId()) {
			log.CtxWarningf(ctx, "Executor %q not in pool %+v", node.GetExecutorId(), nodePoolKey)
		}
	} else {
		log.CtxWarningf(ctx, "Tried to remove executor %q for unknown pool %+v", node.GetExecutorId(), nodePoolKey)
	}

	// Don't use the stream context since we want to do cleanup when stream context is cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), removeExecutorCleanupTimeout)
	defer cancel()
	addr := fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort())
	if err := s.deleteNode(ctx, node, nodePoolKey); err != nil {
		log.CtxWarningf(ctx, "Scheduler: could not unregister node %q: %s", addr, err)
		return
	}
	log.CtxInfof(ctx, "Scheduler: unregistered worker node: %q", addr)
}

func (s *SchedulerServer) deleteNode(ctx context.Context, node *scpb.ExecutionNode, poolKey nodePoolKey) error {
	if err := s.checkPreconditions(node); err != nil {
		return err
	}
	return s.rdb.HDel(ctx, poolKey.redisPoolKey(), node.GetExecutorId()).Err()
}

func (s *SchedulerServer) AddConnectedExecutor(ctx context.Context, handle *executorHandle, node *scpb.ExecutionNode) error {
	poolKey := nodePoolKey{os: node.GetOs(), arch: node.GetArch(), pool: node.GetPool()}
	if s.enableUserOwnedExecutors {
		poolKey.groupID = handle.GroupID()
	}

	err := s.insertOrUpdateNode(ctx, handle, node, poolKey)
	if err != nil {
		return err
	}

	pool := s.getOrCreatePool(poolKey)
	newExecutor := pool.AddConnectedExecutor(node.GetExecutorId(), node.GetAssignableMemoryBytes(), node.GetAssignableMilliCpu(), handle)
	if !newExecutor {
		return nil
	}
	addr := fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort())
	log.CtxInfof(ctx, "Scheduler: registered executor %q (host ID %q, addr %q) for pool %+v", node.GetExecutorId(), node.GetExecutorHostId(), addr, poolKey)

	go func() {
		if err := s.assignWorkToNode(ctx, handle, poolKey); err != nil {
			log.CtxWarningf(ctx, "Failed to assign work to new node: %s", err.Error())
		}
	}()
	return nil

}

func (s *SchedulerServer) redisKeyForExecutorPools(groupID string) string {
	key := "executorPools/"
	if s.enableUserOwnedExecutors {
		key += groupID
	}
	return key
}

func (s *SchedulerServer) insertOrUpdateNode(ctx context.Context, executorHandle *executorHandle, node *scpb.ExecutionNode, poolKey nodePoolKey) error {
	if err := s.checkPreconditions(node); err != nil {
		return err
	}

	permissions := 0
	if s.requireExecutorAuthorization {
		permissions = perms.GROUP_WRITE | perms.GROUP_READ
	} else {
		permissions = perms.OTHERS_READ
	}
	acl := perms.ToACLProto(nil /* userID= */, executorHandle.GroupID(), permissions)

	groupID := executorHandle.GroupID()

	r := &scpb.RegisteredExecutionNode{
		Registration:      node,
		SchedulerHostPort: s.ownHostPort,
		GroupId:           groupID,
		Acl:               acl,
		LastPingTime:      timestamppb.Now(),
	}
	b, err := proto.Marshal(r)
	if err != nil {
		return err
	}

	pipe := s.rdb.TxPipeline()
	poolRedisKey := poolKey.redisPoolKey()
	pipe.HSet(ctx, poolRedisKey, node.GetExecutorId(), b)
	pipe.SAdd(ctx, s.redisKeyForExecutorPools(groupID), poolRedisKey)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *SchedulerServer) RegisterAndStreamWork(stream scpb.Scheduler_RegisterAndStreamWorkServer) error {
	handle := newExecutorHandle(s.env, s, s.requireExecutorAuthorization, stream)
	return handle.Serve(stream.Context())
}

func (s *SchedulerServer) assignWorkToNode(ctx context.Context, handle *executorHandle, nodePoolKey nodePoolKey) error {
	tasks, err := s.sampleUnclaimedTasks(ctx, tasksToEnqueueOnJoin, nodePoolKey)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}

	var reqs []*scpb.EnqueueTaskReservationRequest
	for _, task := range tasks {
		req := &scpb.EnqueueTaskReservationRequest{
			TaskId:             task.taskID,
			TaskSize:           task.metadata.GetTaskSize(),
			SchedulingMetadata: task.metadata,
		}
		reqs = append(reqs, req)
	}

	for _, req := range reqs {
		if _, err = handle.EnqueueTaskReservation(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

func (s *SchedulerServer) redisKeyForTask(taskID string) string {
	if s.enableRedisAvailabilityMonitoring {
		// Use the taskID as the input to the Redis consistent hash function so that task information and pubsub
		// channels end up on the same Redis shard.
		return fmt.Sprintf("task/{%s}", taskID)
	} else {
		return fmt.Sprintf("task/%s", taskID)
	}
}

func (s *SchedulerServer) insertTask(ctx context.Context, taskID string, metadata *scpb.SchedulingMetadata, serializedTask []byte) error {
	serializedMetadata, err := proto.Marshal(metadata)
	if err != nil {
		return status.InternalErrorf("unable to serialize scheduling metadata: %v", err)
	}

	props := map[string]interface{}{
		redisTaskProtoField:       serializedTask,
		redisTaskMetadataField:    serializedMetadata,
		redisTaskQueuedAtUsec:     time.Now().UnixMicro(),
		redisTaskAttempCountField: 0,
	}
	c, err := s.rdb.HSet(ctx, s.redisKeyForTask(taskID), props).Result()
	if err != nil {
		return err
	}
	if c == 0 {
		return status.AlreadyExistsErrorf("task %s already exists", taskID)
	}
	ok, err := s.rdb.Expire(ctx, s.redisKeyForTask(taskID), taskTTL).Result()
	if err != nil {
		return err
	}
	if !ok {
		return status.DataLossErrorf("task %s disappeared before we could set TTL", taskID)
	}
	return nil
}

func (s *SchedulerServer) deleteTask(ctx context.Context, taskID string) (bool, error) {
	key := s.redisKeyForTask(taskID)
	n, err := s.rdb.Del(ctx, key).Result()
	return n == 1, err
}

func (s *SchedulerServer) deleteClaimedTask(ctx context.Context, taskID string) error {
	// The script will return 1 if the task is claimed & has been deleted.
	r, err := redisDeleteClaimedTask.Run(ctx, s.rdb, []string{s.redisKeyForTask(taskID)}).Result()
	if err != nil {
		return err
	}
	if c, ok := r.(int64); !ok || c != 1 {
		return status.NotFoundErrorf("unable to delete claimed task %s", taskID)
	}
	return nil
}

func (s *SchedulerServer) unclaimTask(ctx context.Context, taskID string) error {
	// The script will return 1 if the task is claimed & claim has been released.
	r, err := redisReleaseClaim.Run(ctx, s.rdb, []string{s.redisKeyForTask(taskID)}).Result()
	if err != nil {
		return err
	}
	if c, ok := r.(int64); !ok || c != 1 {
		return status.NotFoundErrorf("unable to release task claim for task %s", taskID)
	}
	return nil
}

func (s *SchedulerServer) claimTask(ctx context.Context, taskID string, claimTime time.Time) error {
	r, err := redisAcquireClaim.Run(ctx, s.rdb, []string{s.redisKeyForTask(taskID)}).Result()
	if err != nil {
		return err
	}

	c, ok := r.(int64)
	if !ok {
		return status.FailedPreconditionErrorf("unexpected result from claim attempt: %v", r)
	}

	switch c {
	case 1:
		// Success
		break
	case 10:
		return status.NotFoundError("task does not exist")
	case 11:
		return status.NotFoundError("task already claimed")
	default:
		return status.UnknownErrorf("unknown error %d", c)
	}

	err = s.rdb.HIncrBy(ctx, s.redisKeyForTask(taskID), redisTaskAttempCountField, 1).Err()
	if err != nil {
		return err
	}

	return nil
}

func (s *SchedulerServer) readTasks(ctx context.Context, taskIDs []string) ([]*persistedTask, error) {
	var tasks []*persistedTask

	for _, taskID := range taskIDs {
		task, err := s.readTask(ctx, taskID)
		if err != nil {
			if !status.IsNotFoundError(err) {
				log.CtxErrorf(ctx, "error reading task from redis: %v", err)
			}
			continue
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (s *SchedulerServer) getPool(key nodePoolKey) (*nodePool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodePool, ok := s.pools[key]
	return nodePool, ok
}

func (s *SchedulerServer) getOrCreatePool(key nodePoolKey) *nodePool {
	s.mu.RLock()
	nodePool, ok := s.pools[key]
	s.mu.RUnlock()
	if ok {
		return nodePool
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	nodePool, ok = s.pools[key]
	if ok {
		return nodePool
	}
	nodePool = newNodePool(s.env, key)
	s.pools[key] = nodePool
	return nodePool
}

func (s *SchedulerServer) sampleUnclaimedTasks(ctx context.Context, count int, nodePoolKey nodePoolKey) ([]*persistedTask, error) {
	nodePool, ok := s.getPool(nodePoolKey)
	if !ok {
		return nil, nil
	}
	taskIDs, err := nodePool.SampleUnclaimedTasks(ctx, count)
	if err != nil {
		return nil, err
	}
	tasks, err := s.readTasks(ctx, taskIDs)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (s *SchedulerServer) readTask(ctx context.Context, taskID string) (*persistedTask, error) {
	if s.rdb == nil {
		return nil, status.FailedPreconditionError("redis client not set")
	}

	fields := []string{
		redisTaskProtoField,
		redisTaskMetadataField,
		redisTaskQueuedAtUsec,
		redisTaskAttempCountField,
	}
	key := s.redisKeyForTask(taskID)
	vals, err := s.rdb.HMGet(ctx, key, fields...).Result()
	if err != nil {
		return nil, status.InternalErrorf("could not read task from redis: %v", err)
	}
	if len(vals) != len(fields) {
		return nil, status.FailedPreconditionErrorf("unexpected # of returned values in redis response: %+v", vals)
	}
	if vals[0] == nil {
		return nil, status.NotFoundErrorf("task %q not found", taskID)
	}

	// Task field.
	taskString, ok := vals[0].(string)
	if !ok {
		return nil, status.InvalidArgumentErrorf("unexpected type %T for task", vals[0])
	}
	serializedTask := []byte(taskString)

	// Scheduling metadata field.
	metadataString, ok := vals[1].(string)
	if !ok {
		return nil, status.InvalidArgumentErrorf("unexpected type %T for metadata", vals[1])
	}
	metadata := &scpb.SchedulingMetadata{}
	err = proto.Unmarshal([]byte(metadataString), metadata)
	if err != nil {
		return nil, status.InternalErrorf("could not deserialize metadata proto: %v", err)
	}

	// Queued At Timestamp field.
	queuedAtUsecString, ok := vals[2].(string)
	if !ok {
		return nil, status.InvalidArgumentErrorf("unexpected type %T for queued at timestamp", vals[2])
	}
	queuedAtUsec, err := strconv.ParseInt(queuedAtUsecString, 10, 64)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not parse queued at timestamp %q: %v", queuedAtUsecString, err)
	}

	// Attempt count field.
	attemptCountStr, ok := vals[3].(string)
	if !ok {
		return nil, status.InvalidArgumentErrorf("unexpected type %T for attempt count", vals[3])
	}
	attemptCount, err := strconv.ParseInt(attemptCountStr, 10, 64)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not parse attempt count %q: %v", attemptCountStr, attemptCount)
	}

	return &persistedTask{
		taskID:          taskID,
		metadata:        metadata,
		serializedTask:  serializedTask,
		queuedTimestamp: time.UnixMicro(queuedAtUsec),
		attemptCount:    attemptCount,
	}, nil
}

// TODO(vadim): we should verify that the executor is authorized to read the task
func (s *SchedulerServer) LeaseTask(stream scpb.Scheduler_LeaseTaskServer) error {
	ctx := stream.Context()
	lastCheckin := time.Now()
	claimed := false
	taskID := ""

	// TODO(vadim): remove after executor ID in lease request is rolled out
	executorID := "unknown"
	if p, ok := peer.FromContext(ctx); ok {
		executorID = p.Addr.String()
	}

	// If we've exited our event loop and the task is still claimed, then
	// the worker did not finish properly and we should re-enqueue it.
	defer func() {
		if !claimed {
			return
		}
		log.CtxWarningf(ctx, "LeaseTask %q exited event-loop with task still claimed. Will ReEnqueue!", taskID)
		ctx, cancel := background.ExtendContextForFinalization(ctx, 3*time.Second)
		defer cancel()
		if _, err := s.ReEnqueueTask(ctx, &scpb.ReEnqueueTaskRequest{TaskId: taskID}); err != nil {
			log.CtxErrorf(ctx, "LeaseTask %q tried to re-enqueue task but failed with err: %s", taskID, err.Error())
		} // Success case will be logged by ReEnqueueTask flow.
	}()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.CtxWarningf(ctx, "LeaseTask %q got EOF: %s", taskID, err)
			break
		}
		if err != nil {
			log.CtxWarningf(ctx, "LeaseTask %q recv with err: %s", taskID, err)
			break
		}
		if req.GetTaskId() == "" || taskID != "" && req.GetTaskId() != taskID {
			// We don't re-enqueue in this case which is a bummer but
			// makes the code significantly simpler. Also, don't do this!
			return status.InvalidArgumentError("TaskId must be set and the same value for all requests")
		}
		if req.GetExecutorId() != "" {
			executorID = req.GetExecutorId()
		}
		taskID = req.GetTaskId()
		if time.Since(lastCheckin) > (leaseInterval + leaseGracePeriod) {
			log.CtxWarningf(ctx, "LeaseTask %q client went away after %s", taskID, time.Since(lastCheckin))
			break
		}
		rsp := &scpb.LeaseTaskResponse{
			LeaseDurationSeconds: int64(leaseInterval.Seconds()),
		}
		if !claimed {
			log.CtxInfof(ctx, "LeaseTask %q claim attempt from executor %q", taskID, executorID)
			err = s.claimTask(ctx, taskID, time.Now())
			if err != nil {
				return err
			}
			claimed = true
			task, err := s.readTask(ctx, req.GetTaskId())
			if err != nil {
				log.CtxErrorf(ctx, "LeaseTask %q error reading task %s", taskID, err.Error())
				return err
			}

			log.CtxInfof(ctx, "LeaseTask task %q successfully claimed by executor %q", taskID, executorID)

			key := nodePoolKey{
				os:      task.metadata.GetOs(),
				arch:    task.metadata.GetArch(),
				pool:    task.metadata.GetPool(),
				groupID: task.metadata.GetExecutorGroupId(),
			}
			nodePool, ok := s.getPool(key)
			if ok {
				err := nodePool.RemoveUnclaimedTask(ctx, taskID)
				if err != nil {
					log.CtxWarningf(ctx, "Could not remove task from unclaimed list: %s", err)
				}
			}

			// Prometheus: observe queue wait time.
			ageInMillis := time.Since(task.queuedTimestamp).Milliseconds()
			queueWaitTimeMs.Observe(float64(ageInMillis))
			rsp.SerializedTask = task.serializedTask
		} else {
			if _, err := s.readTask(ctx, req.GetTaskId()); status.IsNotFoundError(err) {
				// No point re-enqueuing.
				claimed = false
				return status.NotFoundErrorf("task %q disappeared, possibly cancelled", req.GetTaskId())
			}
		}

		done := req.GetFinalize() || req.GetRelease()

		if req.GetFinalize() && claimed {
			// Finalize deletes the task (and implicitly releases the lease).
			// It implies that no further work will/can be attempted for this task.

			err := s.deleteClaimedTask(ctx, taskID)
			if err == nil {
				claimed = false
				log.CtxInfof(ctx, "LeaseTask task %q successfully finalized by %q", taskID, executorID)
			} else {
				log.CtxWarningf(ctx, "Could not delete claimed task %q: %s", taskID, err)
			}
		} else if req.GetRelease() && claimed {
			// Release removes the claim on the task without deleting the task.

			err := s.unclaimTask(ctx, taskID)
			if err == nil {
				claimed = false
				log.CtxInfof(ctx, "LeaseTask task %q successfully released by %q", taskID, executorID)
			} else {
				log.CtxWarningf(ctx, "Could not release lease for task %q: %s", taskID, err)
			}
		}

		rsp.ClosedCleanly = !claimed
		lastCheckin = time.Now()
		if err := stream.Send(rsp); err != nil {
			return err
		}
		if done {
			break
		}
	}

	return nil
}

func minInt(i, j int) int {
	if i < j {
		return i
	}
	return j
}

type enqueueTaskReservationOpts struct {
	numReplicas int
	maxAttempts int
	// This option determines whether tasks should be scheduled only on executors connected to this scheduler.
	// If false, this scheduler will make RPCs to other schedulers to have them enqueue tasks on their connected
	// executors.
	scheduleOnConnectedExecutors bool
}

func (s *SchedulerServer) enqueueTaskReservations(ctx context.Context, enqueueRequest *scpb.EnqueueTaskReservationRequest, serializedTask []byte, opts enqueueTaskReservationOpts) error {
	os := enqueueRequest.GetSchedulingMetadata().GetOs()
	arch := enqueueRequest.GetSchedulingMetadata().GetArch()
	pool := enqueueRequest.GetSchedulingMetadata().GetPool()
	groupID := enqueueRequest.GetSchedulingMetadata().GetExecutorGroupId()

	key := nodePoolKey{os: os, arch: arch, pool: pool, groupID: groupID}

	log.CtxInfof(ctx, "Enqueue task reservations for task %q with pool key %+v.", enqueueRequest.GetTaskId(), key)

	nodeBalancer := s.getOrCreatePool(key)
	nodeCount, err := nodeBalancer.NodeCount(ctx, enqueueRequest.GetTaskSize())
	if err != nil {
		return err
	}

	// We only want to add the unclaimed task once on the "master" scheduler.
	// scheduleOnConnectedExecutors implies that we are enqueuing task reservations on behalf of another scheduler.
	if !opts.scheduleOnConnectedExecutors {
		err = nodeBalancer.AddUnclaimedTask(ctx, enqueueRequest.GetTaskId())
		if err != nil {
			log.CtxWarningf(ctx, "Could not add task to unclaimed task list: %s", err)
		}
	}

	probeCount := minInt(opts.numReplicas, nodeCount)
	probesSent := 0

	startTime := time.Now()
	var successfulReservations []string
	defer func() {
		log.CtxInfof(ctx, "Enqueue task reservations for task %q took %s. Reservations: [%s]",
			enqueueRequest.GetTaskId(), time.Now().Sub(startTime), strings.Join(successfulReservations, ", "))
	}()

	cmd, remoteInstanceName, err := extractRoutingProps(serializedTask)
	if err != nil {
		return err
	}

	// Note: preferredNode may be nil if the executor ID isn't specified or if
	// the executor is no longer connected.
	preferredNode := nodeBalancer.FindConnectedExecutorByID(enqueueRequest.GetExecutorId())

	attempts := 0
	var nodes []*executionNode
	sampleIndex := 0
	for probesSent < probeCount {
		attempts++
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			break
		}
		if opts.maxAttempts > 0 && attempts > opts.maxAttempts {
			return status.ResourceExhaustedErrorf("could not enqueue task reservation to executor")
		}
		if attempts > 100 {
			log.CtxWarningf(ctx, "Attempted to send probe %d times for task %q with pool key %+v. This should not happen.", attempts, enqueueRequest.GetTaskId(), key)
		}
		if sampleIndex == 0 {
			if preferredNode != nil {
				nodes = []*executionNode{preferredNode}
				// Unset the preferred node so that we fall back to random sampling
				// (in subsequent loop iterations) if the preferred node probe fails.
				preferredNode = nil
			} else {
				nodes = nodeBalancer.GetNodes(opts.scheduleOnConnectedExecutors)
				if len(nodes) == 0 {
					return status.UnavailableErrorf("No registered executors in pool %q with os %q with arch %q.", pool, os, arch)
				}
				nodes = nodesThatFit(nodes, enqueueRequest.GetTaskSize())
				if len(nodes) == 0 {
					return status.UnavailableErrorf(
						"No registered executors in pool %q with os %q with arch %q can fit a task with %d milli-cpu and %d bytes of memory.",
						pool, os, arch,
						enqueueRequest.GetTaskSize().GetEstimatedMilliCpu(),
						enqueueRequest.GetTaskSize().GetEstimatedMemoryBytes())
				}
				rankedNodes := s.taskRouter.RankNodes(ctx, cmd, remoteInstanceName, toNodeInterfaces(nodes))
				nodes, err = fromNodeInterfaces(rankedNodes)
				if err != nil {
					return err
				}
			}
		}
		if sampleIndex >= len(nodes) {
			return status.FailedPreconditionErrorf("sampleIndex %d >= %d", sampleIndex, len(nodes))
		}
		node := nodes[sampleIndex]
		sampleIndex = (sampleIndex + 1) % len(nodes)
		// Set the executor ID in case the node is owned by another scheduler, so
		// that the scheduler can prefer this node for the probe.
		enqueueRequest.ExecutorId = node.GetExecutorID()

		enqueueStart := time.Now()
		if opts.scheduleOnConnectedExecutors {
			if node.handle == nil {
				log.CtxErrorf(ctx, "nil handle for a local executor %q", node.GetExecutorID())
				continue
			}
			_, err := node.handle.EnqueueTaskReservation(ctx, enqueueRequest)
			if err != nil {
				continue
			}
		} else {
			if node.schedulerHostPort == "" {
				log.CtxErrorf(ctx, "node %q has no scheduler host:port set", node.GetExecutorID())
				continue
			}

			schedulerClient, err := s.schedulerClientCache.get(node.schedulerHostPort)
			if err != nil {
				log.CtxWarningf(ctx, "Could not get SchedulerClient for %q: %s", node.schedulerHostPort, err)
				continue
			}
			rpcCtx, cancel := context.WithTimeout(ctx, schedulerEnqueueTaskReservationTimeout)
			_, err = schedulerClient.EnqueueTaskReservation(rpcCtx, enqueueRequest)
			cancel()
			if err != nil {
				log.CtxWarningf(ctx, "EnqueueTaskReservation to %q failed: %s", node.schedulerHostPort, err)
				time.Sleep(schedulerEnqueueTaskReservationFailureSleep)
				continue
			}
		}
		successfulReservations = append(successfulReservations, fmt.Sprintf("%s [%s]", node.String(), time.Now().Sub(enqueueStart).String()))
		probesSent++
	}
	return nil
}

func (s *SchedulerServer) ScheduleTask(ctx context.Context, req *scpb.ScheduleTaskRequest) (*scpb.ScheduleTaskResponse, error) {
	if req.GetTaskId() == "" {
		return nil, status.FailedPreconditionError("A task_id is required")
	}
	if req.Metadata == nil {
		return nil, status.FailedPreconditionError("Scheduling metadata is required")
	}
	if req.Metadata.TaskSize == nil {
		return nil, status.FailedPreconditionError("A task_size is required")
	}
	if len(req.GetSerializedTask()) == 0 {
		return nil, status.FailedPreconditionError("Serialized task is required")
	}
	taskID := req.GetTaskId()
	metadata := req.GetMetadata()
	if err := s.insertTask(ctx, taskID, metadata, req.GetSerializedTask()); err != nil {
		return nil, err
	}
	enqueueRequest := &scpb.EnqueueTaskReservationRequest{
		TaskId:             taskID,
		TaskSize:           req.GetMetadata().GetTaskSize(),
		SchedulingMetadata: metadata,
	}

	opts := enqueueTaskReservationOpts{
		numReplicas:                  probesPerTask,
		scheduleOnConnectedExecutors: false,
	}
	if err := s.enqueueTaskReservations(ctx, enqueueRequest, req.GetSerializedTask(), opts); err != nil {
		return nil, err
	}
	return &scpb.ScheduleTaskResponse{}, nil
}

func (s *SchedulerServer) CancelTask(ctx context.Context, taskID string) (bool, error) {
	return s.deleteTask(ctx, taskID)
}

func (s *SchedulerServer) ExistsTask(ctx context.Context, taskID string) (bool, error) {
	key := s.redisKeyForTask(taskID)
	n, err := s.rdb.Exists(ctx, key).Result()
	return n == 1, err
}

func (s *SchedulerServer) EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error) {
	// TODO(vadim): verify user is authorized to use executor pool

	opts := enqueueTaskReservationOpts{
		numReplicas:                  1,
		maxAttempts:                  10,
		scheduleOnConnectedExecutors: true,
	}
	if err := s.enqueueTaskReservations(ctx, req, nil /*=serializedTask*/, opts); err != nil {
		return nil, err
	}
	return &scpb.EnqueueTaskReservationResponse{}, nil
}

func (s *SchedulerServer) reEnqueueTask(ctx context.Context, taskID string, numReplicas int, reason string) error {
	if taskID == "" {
		return status.FailedPreconditionError("A task_id is required")
	}
	task, err := s.readTask(ctx, taskID)
	if err != nil {
		return err
	}
	if task.attemptCount >= maxTaskAttemptCount {
		if _, err := s.deleteTask(ctx, taskID); err != nil {
			return err
		}
		msg := fmt.Sprintf("Task %q already attempted %d times.", taskID, task.attemptCount)
		if reason != "" {
			msg += " Last failure: " + reason
		}
		if err := s.env.GetRemoteExecutionService().MarkExecutionFailed(ctx, taskID, status.InternalError(msg)); err != nil {
			log.CtxWarningf(ctx, "Could not mark execution failed for task %q: %s", taskID, err)
		}
		return status.ResourceExhaustedErrorf(msg)
	}
	_ = s.unclaimTask(ctx, taskID) // ignore error -- it's fine if it's already unclaimed.
	log.CtxDebugf(ctx, "ReEnqueueTask RPC for task %q", taskID)
	enqueueRequest := &scpb.EnqueueTaskReservationRequest{
		TaskId:             taskID,
		TaskSize:           task.metadata.GetTaskSize(),
		SchedulingMetadata: task.metadata,
	}
	opts := enqueueTaskReservationOpts{
		numReplicas:                  numReplicas,
		scheduleOnConnectedExecutors: false,
	}
	if err := s.enqueueTaskReservations(ctx, enqueueRequest, task.serializedTask, opts); err != nil {
		// Unavailable indicates that it's impossible to schedule the task (no executors in pool).
		if status.IsUnavailableError(err) {
			if markErr := s.env.GetRemoteExecutionService().MarkExecutionFailed(ctx, taskID, err); markErr != nil {
				log.CtxWarningf(ctx, "Could not mark execution failed for task %q: %s", taskID, markErr)
			}
		}
		return err
	}
	log.CtxDebugf(ctx, "ReEnqueueTask succeeded for task %q", taskID)
	return nil
}

func (s *SchedulerServer) ReEnqueueTask(ctx context.Context, req *scpb.ReEnqueueTaskRequest) (*scpb.ReEnqueueTaskResponse, error) {
	if err := s.reEnqueueTask(ctx, req.GetTaskId(), probesPerTask, req.GetReason()); err != nil {
		log.CtxErrorf(ctx, "ReEnqueueTask failed for task %q: %s", req.GetTaskId(), err)
		return nil, err
	}
	return &scpb.ReEnqueueTaskResponse{}, nil
}

func (s *SchedulerServer) getExecutionNodesFromRedis(ctx context.Context, groupID string) ([]*scpb.ExecutionNode, error) {
	user, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return nil, err
	}

	poolSetKey := s.redisKeyForExecutorPools(groupID)
	poolKeys, err := s.rdb.SMembers(ctx, poolSetKey).Result()
	if err != nil {
		return nil, err
	}
	var executionNodes []*scpb.ExecutionNode
	for _, k := range poolKeys {
		executors, err := s.rdb.HGetAll(ctx, k).Result()
		if err != nil {
			return nil, err
		}
		for _, data := range executors {
			registeredNode := &scpb.RegisteredExecutionNode{}
			if err := proto.Unmarshal([]byte(data), registeredNode); err != nil {
				return nil, err
			}

			err := perms.AuthorizeRead(&user, registeredNode.GetAcl())
			if err != nil {
				continue
			}
			executionNodes = append(executionNodes, registeredNode.GetRegistration())
		}
	}
	return executionNodes, nil
}

func (s *SchedulerServer) GetExecutionNodes(ctx context.Context, req *scpb.GetExecutionNodesRequest) (*scpb.GetExecutionNodesResponse, error) {
	groupID := req.GetRequestContext().GetGroupId()
	if groupID == "" {
		return nil, status.InvalidArgumentError("group not specified")
	}

	// If executor auth is not enabled, executors do not belong to any group.
	if !s.requireExecutorAuthorization {
		groupID = ""
	}

	executionNodes, err := s.getExecutionNodesFromRedis(ctx, groupID)
	if err != nil {
		return nil, err
	}

	userOwnedExecutorsEnabled := s.enableUserOwnedExecutors
	// Don't report user owned executors as being enabled for the shared executor group ID (i.e. the BuildBuddy group)
	if userOwnedExecutorsEnabled && groupID == *sharedExecutorPoolGroupID {
		userOwnedExecutorsEnabled = false
	}

	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return nil, err
	}
	g, err := s.env.GetUserDB().GetGroupByID(ctx, u.GetGroupID())
	if err != nil {
		return nil, err
	}
	useGroupOwnedExecutors := g.UseGroupOwnedExecutors != nil && *g.UseGroupOwnedExecutors

	executors := make([]*scpb.GetExecutionNodesResponse_Executor, len(executionNodes))
	for i, node := range executionNodes {
		isDarwinExecutor := strings.EqualFold(node.Os, platform.DarwinOperatingSystemName)
		executors[i] = &scpb.GetExecutionNodesResponse_Executor{
			Node: node,
			IsDefault: !s.requireExecutorAuthorization ||
				groupID == *sharedExecutorPoolGroupID ||
				(s.enableUserOwnedExecutors &&
					(useGroupOwnedExecutors || (s.forceUserOwnedDarwinExecutors && isDarwinExecutor))),
		}
	}

	return &scpb.GetExecutionNodesResponse{
		Executor:                    executors,
		UserOwnedExecutorsSupported: userOwnedExecutorsEnabled,
	}, nil
}

// extractRoutingProps deserializes the given task and returns the properties
// needed to route the task (command and remote instance name).
func extractRoutingProps(serializedTask []byte) (*repb.Command, string, error) {
	if serializedTask == nil {
		return nil, "", nil
	}
	task := &repb.ExecutionTask{}
	if err := proto.Unmarshal(serializedTask, task); err != nil {
		return nil, "", status.InternalErrorf("failed to unmarshal ExecutionTask: %s", err)
	}
	return task.GetCommand(), task.GetExecuteRequest().GetInstanceName(), nil
}
