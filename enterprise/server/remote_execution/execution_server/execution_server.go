package execution_server

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pubsub"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	remote_execution_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	gstatus "google.golang.org/grpc/status"
)

const (
	// TTL for keys used to track pending executions for action merging.
	pendingExecutionTTL = 8 * time.Hour
)

var (
	enableRedisAvailabilityMonitoring = flag.Bool("remote_execution.enable_redis_availability_monitoring", false, "If enabled, the execution server will detect if Redis has lost state and will ask Bazel to retry executions.")
	enableActionMerging               = flag.Bool("remote_execution.enable_action_merging", true, "If enabled, identical actions being executed concurrently are merged into a single execution.")
)

func fillExecutionFromSummary(summary *espb.ExecutionSummary, execution *tables.Execution) {
	// IOStats
	execution.FileDownloadCount = summary.GetIoStats().GetFileDownloadCount()
	execution.FileDownloadSizeBytes = summary.GetIoStats().GetFileDownloadSizeBytes()
	execution.FileDownloadDurationUsec = summary.GetIoStats().GetFileDownloadDurationUsec()
	execution.FileUploadCount = summary.GetIoStats().GetFileUploadCount()
	execution.FileUploadSizeBytes = summary.GetIoStats().GetFileUploadSizeBytes()
	execution.FileUploadDurationUsec = summary.GetIoStats().GetFileUploadDurationUsec()
	// ExecutedActionMetadata
	execution.Worker = summary.GetExecutedActionMetadata().GetWorker()
	execution.QueuedTimestampUsec = summary.GetExecutedActionMetadata().GetQueuedTimestamp().AsTime().UnixMicro()
	execution.WorkerStartTimestampUsec = summary.GetExecutedActionMetadata().GetWorkerStartTimestamp().AsTime().UnixMicro()
	execution.WorkerCompletedTimestampUsec = summary.GetExecutedActionMetadata().GetWorkerCompletedTimestamp().AsTime().UnixMicro()
	execution.InputFetchStartTimestampUsec = summary.GetExecutedActionMetadata().GetInputFetchStartTimestamp().AsTime().UnixMicro()
	execution.InputFetchCompletedTimestampUsec = summary.GetExecutedActionMetadata().GetInputFetchCompletedTimestamp().AsTime().UnixMicro()
	execution.ExecutionStartTimestampUsec = summary.GetExecutedActionMetadata().GetExecutionStartTimestamp().AsTime().UnixMicro()
	execution.ExecutionCompletedTimestampUsec = summary.GetExecutedActionMetadata().GetExecutionCompletedTimestamp().AsTime().UnixMicro()
	execution.OutputUploadStartTimestampUsec = summary.GetExecutedActionMetadata().GetOutputUploadStartTimestamp().AsTime().UnixMicro()
	execution.OutputUploadCompletedTimestampUsec = summary.GetExecutedActionMetadata().GetOutputUploadCompletedTimestamp().AsTime().UnixMicro()
}

func generateCommandSnippet(command *repb.Command) string {
	const targetLength = 200
	snippetBits := make([]string, 0)
	for i, argument := range command.GetArguments() {
		arg := argument
		if i == 0 {
			dir, file := filepath.Split(arg)
			arg = filepath.Join(filepath.Base(dir), file)
		}
		snippetBits = append(snippetBits, arg)
	}
	snippet := strings.Join(snippetBits, " ")
	if len(snippet) > targetLength {
		snippet = snippet[:targetLength-3] + "..."
	}
	return snippet
}

func redisKeyForTaskStatusStream(taskID string) string {
	return fmt.Sprintf("taskStatusStream/%s", taskID)
}

func redisKeyForMonitoredTaskStatusStream(taskID string) string {
	// We choose taskID as the hash input for Redis sharding so that both the PubSub streams and task information for
	// a single task is placed on the same shard.
	return fmt.Sprintf("taskStatusStream/{%s}", taskID)
}

func redisKeyForPendingExecutionID(ctx context.Context, adResource *digest.ResourceName) (string, error) {
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pendingExecution/%s%s", userPrefix, adResource.DownloadString()), nil
}

func redisKeyForPendingExecutionDigest(executionID string) string {
	return fmt.Sprintf("pendingExecutionDigest/%s", executionID)
}

type ExecutionServer struct {
	env                               environment.Env
	cache                             interfaces.Cache
	rdb                               redis.UniversalClient
	streamPubSub                      *pubsub.StreamPubSub
	enableRedisAvailabilityMonitoring bool
}

func Register(env environment.Env) error {
	if !remote_execution_config.RemoteExecutionEnabled() {
		return nil
	}
	// Make sure capabilities server reflect that we're running
	// remote execution.
	executionServer, err := NewExecutionServer(env)
	if err != nil {
		log.Fatalf("Error initializing ExecutionServer: %s", err)
	}
	env.SetRemoteExecutionService(executionServer)
	return nil
}

func NewExecutionServer(env environment.Env) (*ExecutionServer, error) {
	cache := env.GetCache()
	if cache == nil {
		return nil, fmt.Errorf("A cache is required to enable the RemoteExecutionServer")
	}
	if env.GetRemoteExecutionRedisClient() == nil || env.GetRemoteExecutionRedisPubSubClient() == nil {
		return nil, status.FailedPreconditionErrorf("Redis is required for remote execution")
	}
	return &ExecutionServer{
		env:                               env,
		cache:                             cache,
		rdb:                               env.GetRemoteExecutionRedisClient(),
		streamPubSub:                      pubsub.NewStreamPubSub(env.GetRemoteExecutionRedisPubSubClient()),
		enableRedisAvailabilityMonitoring: remote_execution_config.RemoteExecutionEnabled() && *enableRedisAvailabilityMonitoring,
	}, nil
}

func (s *ExecutionServer) RedisAvailabilityMonitoringEnabled() bool {
	return s.enableRedisAvailabilityMonitoring
}

func (s *ExecutionServer) pubSubChannelForExecutionID(executionID string) *pubsub.Channel {
	if s.enableRedisAvailabilityMonitoring {
		return s.streamPubSub.MonitoredChannel(redisKeyForMonitoredTaskStatusStream(executionID))
	}
	return s.streamPubSub.UnmonitoredChannel(redisKeyForTaskStatusStream(executionID))
}

func (s *ExecutionServer) insertExecution(ctx context.Context, executionID, invocationID, snippet string, stage repb.ExecutionStage_Value) error {
	if s.env.GetDBHandle() == nil {
		return status.FailedPreconditionError("database not configured")
	}
	execution := &tables.Execution{
		ExecutionID:    executionID,
		InvocationID:   invocationID,
		Stage:          int64(stage),
		CommandSnippet: snippet,
	}

	var permissions *perms.UserGroupPerm
	if auth := s.env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil && u.GetGroupID() != "" {
			permissions = perms.GroupAuthPermissions(u.GetGroupID())
		}
	}

	if permissions == nil && s.env.GetAuthenticator().AnonymousUsageEnabled() {
		permissions = perms.AnonymousUserPermissions()
	} else if permissions == nil {
		return status.PermissionDeniedErrorf("Anonymous access disabled, permission denied.")
	}

	execution.UserID = permissions.UserID
	execution.GroupID = permissions.GroupID
	execution.Perms = execution.Perms | permissions.Perms

	return s.env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
		return tx.Create(execution).Error
	})
}

func trimStatus(statusMessage string) string {
	if len(statusMessage) > 255 {
		return statusMessage[:255]
	}
	return statusMessage
}

func (s *ExecutionServer) updateExecution(ctx context.Context, executionID string, stage repb.ExecutionStage_Value, op *longrunning.Operation) error {
	if s.env.GetDBHandle() == nil {
		return status.FailedPreconditionError("database not configured")
	}
	execution := &tables.Execution{
		ExecutionID: executionID,
		Stage:       int64(stage),
	}

	if executeResponse := operation.ExtractExecuteResponse(op); executeResponse != nil {
		execution.StatusCode = executeResponse.GetStatus().GetCode()
		execution.StatusMessage = trimStatus(executeResponse.GetStatus().GetMessage())
		execution.ExitCode = executeResponse.GetResult().GetExitCode()
		if details := executeResponse.GetStatus().GetDetails(); details != nil && len(details) > 0 {
			serializedDetails, err := proto.Marshal(details[0])
			if err == nil {
				execution.SerializedStatusDetails = serializedDetails
			} else {
				log.Errorf("Error marshalling status details: %s", err.Error())
			}
		}
		execution.CachedResult = executeResponse.GetCachedResult()

		// Update stats if the operation has been completed.
		if stage == repb.ExecutionStage_COMPLETED && executeResponse.GetMessage() != "" {
			if data, err := base64.StdEncoding.DecodeString(executeResponse.GetMessage()); err == nil {
				summary := &espb.ExecutionSummary{}
				if err := proto.Unmarshal(data, summary); err == nil {
					fillExecutionFromSummary(summary, execution)
				}
			}
		}
	}

	if *enableActionMerging && stage == repb.ExecutionStage_COMPLETED {
		if err := s.deletePendingExecution(ctx, executionID); err != nil {
			log.Warningf("could not delete pending execution %q: %s", executionID, err)
		}
	}

	return s.env.GetDBHandle().TransactionWithOptions(ctx, db.Opts().WithQueryName("upsert_execution"), func(tx *db.DB) error {
		var existing tables.Execution
		if err := tx.Where("execution_id = ?", executionID).First(&existing).Error; err != nil {
			return err
		}
		return tx.Model(&existing).Where("execution_id = ? AND stage != ?", executionID, repb.ExecutionStage_COMPLETED).Updates(execution).Error
	})
}

// getUnvalidatedActionResult fetches an action result from the cache but does
// not validate it.
// N.B. This should only be used if the calling code has already ensured the
// action is valid and may be returned.
func (s *ExecutionServer) getUnvalidatedActionResult(ctx context.Context, r *digest.ResourceName) (*repb.ActionResult, error) {
	cache, err := namespace.ActionCache(ctx, s.cache, r.GetInstanceName())
	if err != nil {
		return nil, err
	}
	data, err := cache.Get(ctx, r.GetDigest())
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, digest.MissingDigestError(r.GetDigest())
		}
		return nil, err
	}
	actionResult := &repb.ActionResult{}
	if err := proto.Unmarshal(data, actionResult); err != nil {
		return nil, err
	}
	return actionResult, nil
}

func (s *ExecutionServer) getActionResultFromCache(ctx context.Context, d *digest.ResourceName) (*repb.ActionResult, error) {
	actionResult, err := s.getUnvalidatedActionResult(ctx, d)
	if err != nil {
		return nil, err
	}
	casCache, err := namespace.CASCache(ctx, s.cache, d.GetInstanceName())
	if err != nil {
		return nil, err
	}
	if err := action_cache_server.ValidateActionResult(ctx, casCache, actionResult); err != nil {
		return nil, err
	}
	return actionResult, nil
}

type streamLike interface {
	Context() context.Context
	Send(*longrunning.Operation) error
}

func (s *ExecutionServer) Execute(req *repb.ExecuteRequest, stream repb.Execution_ExecuteServer) error {
	return s.execute(req, stream)
}

func (s *ExecutionServer) Dispatch(ctx context.Context, req *repb.ExecuteRequest) (string, error) {
	scheduler := s.env.GetSchedulerService()
	if scheduler == nil {
		return "", status.FailedPreconditionErrorf("No scheduler service configured")
	}
	adInstanceDigest := digest.NewResourceName(req.GetActionDigest(), req.GetInstanceName())
	action := &repb.Action{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, adInstanceDigest, action); err != nil {
		log.Errorf("Error fetching action: %s", err.Error())
		return "", err
	}
	cmdInstanceDigest := digest.NewResourceName(action.GetCommandDigest(), req.GetInstanceName())
	command := &repb.Command{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, cmdInstanceDigest, command); err != nil {
		log.Errorf("Error fetching command: %s", err.Error())
		return "", err
	}

	r := digest.NewResourceName(req.GetActionDigest(), req.GetInstanceName())
	executionID, err := r.UploadString()
	if err != nil {
		return "", err
	}

	tracing.AddStringAttributeToCurrentSpan(ctx, "task_id", executionID)

	invocationID := bazel_request.GetInvocationID(ctx)

	if err := s.insertExecution(ctx, executionID, invocationID, generateCommandSnippet(command), repb.ExecutionStage_UNKNOWN); err != nil {
		return "", err
	}

	executionTask := &repb.ExecutionTask{
		ExecuteRequest:  req,
		InvocationId:    invocationID,
		ExecutionId:     executionID,
		Action:          action,
		Command:         command,
		RequestMetadata: bazel_request.GetRequestMetadata(ctx),
	}
	// Allow execution worker to auth to cache (if necessary).
	if jwt, ok := ctx.Value("x-buildbuddy-jwt").(string); ok {
		executionTask.Jwt = jwt
	}

	platformPropOverrides := platform.RemoteHeaderOverrides(ctx)
	if len(platformPropOverrides) > 0 {
		executionTask.PlatformOverrides = &repb.Platform{Properties: platformPropOverrides}
	}

	executionTask.QueuedTimestamp = timestamppb.Now()
	serializedTask, err := proto.Marshal(executionTask)
	if err != nil {
		// Should never happen.
		return "", status.InternalErrorf("Error marshalling execution task %q: %s", executionID, err)
	}

	taskSize := tasksize.Estimate(executionTask)

	props := platform.ParseProperties(executionTask)

	executorGroupID, defaultPool, err := s.env.GetSchedulerService().GetGroupIDAndDefaultPoolForUser(ctx, props.OS, props.UseSelfHostedExecutors)
	if err != nil {
		return "", err
	}

	if props.Pool == "" {
		props.Pool = defaultPool
	}

	taskGroupID := interfaces.AuthAnonymousUser
	if user, err := perms.AuthenticatedUser(ctx, s.env); err == nil {
		taskGroupID = user.GetGroupID()
	}

	metrics.RemoteExecutionRequests.With(prometheus.Labels{metrics.GroupID: taskGroupID, metrics.OS: props.OS, metrics.Arch: props.Arch}).Inc()

	if s.enableRedisAvailabilityMonitoring {
		if err := s.streamPubSub.CreateMonitoredChannel(ctx, redisKeyForMonitoredTaskStatusStream(executionID)); err != nil {
			return "", err
		}
	}

	schedulingMetadata := &scpb.SchedulingMetadata{
		Os:              props.OS,
		Arch:            props.Arch,
		Pool:            props.Pool,
		TaskSize:        taskSize,
		ExecutorGroupId: executorGroupID,
		TaskGroupId:     taskGroupID,
	}
	scheduleReq := &scpb.ScheduleTaskRequest{
		TaskId:         executionID,
		Metadata:       schedulingMetadata,
		SerializedTask: serializedTask,
	}

	if err := s.recordPendingExecution(ctx, executionID, r); err != nil {
		log.Warningf("could not recording pending execution %q: %s", executionID, err)
	}

	if _, err := scheduler.ScheduleTask(ctx, scheduleReq); err != nil {
		_ = s.deletePendingExecution(ctx, executionID)
		return "", status.UnavailableErrorf("Error scheduling execution task %q: %s", executionID, err)
	}

	return executionID, nil
}

// findExistingExecution looks for an identical action that is already pending
// execution.
func (s *ExecutionServer) findPendingExecution(ctx context.Context, adResource *digest.ResourceName) (string, error) {
	executionIDKey, err := redisKeyForPendingExecutionID(ctx, adResource)
	if err != nil {
		return "", err
	}
	executionID, err := s.rdb.Get(ctx, executionIDKey).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	// Validate that the reverse mapping exists as well. The reverse mapping is
	// used to delete the pending task information when the task is done.
	// Bail out if it doesn't exist.
	err = s.rdb.Get(ctx, redisKeyForPendingExecutionDigest(executionID)).Err()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	ok, err := s.env.GetSchedulerService().ExistsTask(ctx, executionID)
	if err != nil {
		return "", err
	}
	if !ok {
		log.Warningf("Pending execution %q does not exist in the scheduler", executionID)
		return "", nil
	}
	return executionID, nil
}

func (s *ExecutionServer) recordPendingExecution(ctx context.Context, executionID string, adResource *digest.ResourceName) error {
	forwardKey, err := redisKeyForPendingExecutionID(ctx, adResource)
	if err != nil {
		return err
	}
	reverseKey := redisKeyForPendingExecutionDigest(executionID)
	pipe := s.rdb.TxPipeline()
	pipe.Set(ctx, forwardKey, executionID, pendingExecutionTTL)
	pipe.Set(ctx, reverseKey, forwardKey, pendingExecutionTTL)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *ExecutionServer) deletePendingExecution(ctx context.Context, executionID string) error {
	pendingExecutionDigestKey := redisKeyForPendingExecutionDigest(executionID)
	pendingExecutionKey, err := s.rdb.Get(ctx, pendingExecutionDigestKey).Result()
	if err != nil {
		return err
	}
	if err := s.rdb.Del(ctx, pendingExecutionKey).Err(); err != nil {
		log.Warningf("could not delete pending execution key %q: %s", pendingExecutionKey, err)
	}
	if err := s.rdb.Del(ctx, pendingExecutionDigestKey).Err(); err != nil {
		log.Warningf("could not delete pending execution digest key %q: %s", pendingExecutionDigestKey, err)
	}
	return nil
}

func (s *ExecutionServer) execute(req *repb.ExecuteRequest, stream streamLike) error {
	adInstanceDigest := digest.NewResourceName(req.GetActionDigest(), req.GetInstanceName())
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}

	executionID := ""
	if !req.GetSkipCacheLookup() {
		if actionResult, err := s.getActionResultFromCache(ctx, adInstanceDigest); err == nil {
			r := digest.NewResourceName(req.GetActionDigest(), req.GetInstanceName())
			executionID, err := r.UploadString()
			if err != nil {
				return err
			}
			stateChangeFn := operation.GetStateChangeFunc(stream, executionID, adInstanceDigest)
			if err := stateChangeFn(repb.ExecutionStage_COMPLETED, operation.ExecuteResponseWithCachedResult(actionResult)); err != nil {
				return err // CHECK (these errors should not happen).
			}
			return nil
		}

		if *enableActionMerging {
			// Check if there's already an identical action pending execution.
			// If so wait on the result of that execution instead of starting a new
			// one.
			ee, err := s.findPendingExecution(ctx, adInstanceDigest)
			if err != nil {
				log.Warningf("could not check for existing execution: %s", err)
			}
			if ee != "" {
				log.Infof("Reusing execution %q for execution request %q", ee, adInstanceDigest.DownloadString())
				executionID = ee
				metrics.RemoteExecutionMergedActions.With(prometheus.Labels{metrics.GroupID: s.getGroupIDForMetrics(ctx)}).Inc()
			}
		}
	}

	// Create a new execution unless we found an existing identical action we
	// can wait on.
	if executionID == "" {
		log.Infof("Scheduling new execution for %q", adInstanceDigest.DownloadString())
		newExecutionID, err := s.Dispatch(ctx, req)
		if err != nil {
			log.Errorf("Error dispatching execution %q: %s", executionID, err.Error())
			return err
		}
		executionID = newExecutionID
		log.Infof("Scheduled execution %q for request %q", executionID, adInstanceDigest.DownloadString())
	}

	waitReq := repb.WaitExecutionRequest{
		Name: executionID,
	}
	return s.waitExecution(&waitReq, stream, waitOpts{isExecuteRequest: true})
}

// WaitExecution waits for an execution operation to complete. When the client initially
// makes the request, the server immediately responds with the current status
// of the execution. The server will leave the request stream open until the
// operation completes, and then respond with the completed operation. The
// server MAY choose to stream additional updates as execution progresses,
// such as to provide an update as to the state of the execution.
func (s *ExecutionServer) WaitExecution(req *repb.WaitExecutionRequest, stream repb.Execution_WaitExecutionServer) error {
	return s.waitExecution(req, stream, waitOpts{isExecuteRequest: false})
}

type InProgressExecution struct {
	server       *ExecutionServer
	clientStream streamLike
	lastStage    repb.ExecutionStage_Value
	opName       string
}

func (e *InProgressExecution) processSerializedOpUpdate(serializedOp string) (done bool, err error) {
	op, err := operation.Decode(serializedOp)
	if err != nil {
		log.Warningf("Could not decode operation update for %q: %v", e.opName, err)
		// Continue to process further updates.
		return false, nil
	}
	return e.processOpUpdate(op)
}

func (e *InProgressExecution) processOpUpdate(op *longrunning.Operation) (done bool, err error) {
	stage := operation.ExtractStage(op)
	log.Debugf("WaitExecution: %q in stage: %s", e.opName, stage)
	if stage < e.lastStage {
		return false, nil
	}
	e.lastStage = stage
	err = e.clientStream.Send(op)
	if err == io.EOF {
		log.Warningf("Caller hung up on operation: %q.", e.opName)
		return true, nil // If the caller hung-up, bail out.
	}
	if err != nil {
		log.Warningf("Error sending operation to caller: %q: %s", e.opName, err.Error())
		return true, err // If some other err happened, bail out.
	}
	if stage == repb.ExecutionStage_COMPLETED {
		return true, nil // If the operation is complete, bail out.
	}
	executeResponse := operation.ExtractExecuteResponse(op)
	if executeResponse != nil {
		if executeResponse.GetStatus().GetCode() != 0 {
			log.Warningf("WaitExecution: %q errored, returning %+v", e.opName, executeResponse.GetStatus())
			return true, gstatus.ErrorProto(executeResponse.GetStatus())
		}
	}
	return false, nil
}

type waitOpts struct {
	// Indicates whether the wait is being called from Execute or WaitExecution RPC.
	isExecuteRequest bool
}

func (s *ExecutionServer) getGroupIDForMetrics(ctx context.Context) string {
	if a := s.env.GetAuthenticator(); a != nil {
		user, err := a.AuthenticatedUser(ctx)
		if err != nil {
			return interfaces.AuthAnonymousUser
		}
		return user.GetGroupID()
	}
	return ""
}

func (s *ExecutionServer) waitExecution(req *repb.WaitExecutionRequest, stream streamLike, opts waitOpts) error {
	log.Debugf("WaitExecution called for: %q", req.GetName())
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}

	actionResource, err := digest.ParseUploadResourceName(req.GetName())
	if err != nil {
		log.Errorf("Could not extract digest from %q: %s", req.GetName(), err)
		return err
	}

	e := InProgressExecution{
		server:       s,
		clientStream: stream,
		opName:       req.GetName(),
	}

	subChan := s.pubSubChannelForExecutionID(req.GetName())

	var subscriber *pubsub.StreamSubscription
	if opts.isExecuteRequest {
		subscriber = s.streamPubSub.SubscribeHead(ctx, subChan)
	} else {
		// If this is a WaitExecution RPC, start the subscription from the last published status, inclusive.
		subscriber = s.streamPubSub.SubscribeTail(ctx, subChan)
	}
	defer subscriber.Close()
	streamPubSubChan := subscriber.Chan()

	if opts.isExecuteRequest {
		// Send a best-effort initial "in progress" update to client.
		// Once Bazel receives the initial update, it will use WaitExecution to handle retry on error instead of
		// requesting a new execution via Execute.
		stateChangeFn := operation.GetStateChangeFunc(stream, req.GetName(), actionResource)
		err = stateChangeFn(repb.ExecutionStage_UNKNOWN, operation.InProgressExecuteResponse())
		if err != nil && err != io.EOF {
			log.Warningf("Could not send initial update: %s", err)
		}
	}

	groupID := s.getGroupIDForMetrics(ctx)
	metrics.RemoteExecutionWaitingExecutionResult.With(prometheus.Labels{metrics.GroupID: groupID}).Inc()
	defer metrics.RemoteExecutionWaitingExecutionResult.With(prometheus.Labels{metrics.GroupID: groupID}).Dec()

	for {
		msg, ok := <-streamPubSubChan
		if !ok {
			return status.UnavailableErrorf("Stream PubSub channel closed for %q", req.GetName())
		}
		var data string
		// If there's an error maintaining the subscription (e.g. because a Redis node went away) send a failed
		// operation message to Bazel so that it retries the execution.
		if msg.Err != nil {
			op, err := operation.AssembleFailed(repb.ExecutionStage_COMPLETED, req.GetName(), actionResource, msg.Err)
			if err != nil {
				return err
			}
			failedData, err := proto.Marshal(op)
			if err != nil {
				return err
			}
			data = base64.StdEncoding.EncodeToString(failedData)
		} else {
			data = msg.Data
		}
		done, err := e.processSerializedOpUpdate(data)
		if done {
			log.Debugf("WaitExecution %q: progress loop exited: err: %v, done: %t", req.GetName(), err, done)
			return err
		}
	}
}

func loopAfterTimeout(ctx context.Context, timeout time.Duration, f func()) {
	for {
		select {
		case <-ctx.Done():
			{
				return
			}
		case <-time.After(timeout):
			{
				f()
			}
		}
	}
}

func (s *ExecutionServer) MarkExecutionFailed(ctx context.Context, taskID string, reason error) error {
	r, err := digest.ParseDownloadResourceName(taskID)
	if err != nil {
		log.Warningf("Could not parse taskID: %s", err)
		return err
	}
	op, err := operation.AssembleFailed(repb.ExecutionStage_COMPLETED, taskID, r, reason)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(op)
	if err != nil {
		return err
	}
	if err := s.streamPubSub.Publish(ctx, s.pubSubChannelForExecutionID(taskID), base64.StdEncoding.EncodeToString(data)); err != nil {
		log.Warningf("MarkExecutionFailed: error publishing task %q on stream pubsub: %s", taskID, err)
		return status.InternalErrorf("Error publishing task %q on stream pubsub: %s", taskID, err)
	}

	if err := s.updateExecution(ctx, taskID, operation.ExtractStage(op), op); err != nil {
		log.Warningf("MarkExecutionFailed: error updating execution: %q: %s", taskID, err)
		return err
	}
	return nil
}

func (s *ExecutionServer) PublishOperation(stream repb.Execution_PublishOperationServer) error {
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}
	lastOp := &longrunning.Operation{}
	lastWrite := time.Now()
	taskID := ""
	stage := repb.ExecutionStage_UNKNOWN
	mu := sync.Mutex{}
	// 80% of executions take < 10 seconds in total. So here, we delay
	// writes to the database if pubsub.Publish is successful, in an
	// attempt to reduce DB load. To ensure that executions complete, even
	// if no pubsub listener receives our published updates, we *always*
	// write the execution on stage == COMPLETE or after 5 seconds have
	// passed with no writes.
	go loopAfterTimeout(ctx, time.Second, func() {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(lastWrite) > 5*time.Second && taskID != "" {
			if err := s.updateExecution(ctx, taskID, stage, lastOp); err != nil {
				log.Warningf("PublishOperation: FlushWrite: error updating execution: %q: %s", taskID, err.Error())
				return
			}
			lastWrite = time.Now()
		}
	})

	for {
		op, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&repb.PublishOperationResponse{})
		}
		if err != nil {
			log.Errorf("PublishOperation: recv err: %s", err.Error())
			return err
		}

		mu.Lock()
		lastOp = op
		taskID = op.GetName()
		stage = operation.ExtractStage(op)
		mu.Unlock()

		log.Debugf("PublishOperation: operation %q stage: %s", taskID, stage)

		if stage == repb.ExecutionStage_COMPLETED {
			response := operation.ExtractExecuteResponse(op)
			if response != nil {
				if err := s.markTaskComplete(ctx, taskID, response); err != nil {
					// Errors updating the router or recording usage are non-fatal.
					log.Error(err.Error())
				}
			}
		}

		data, err := proto.Marshal(op)
		if err != nil {
			return err
		}
		if err := s.streamPubSub.Publish(stream.Context(), s.pubSubChannelForExecutionID(taskID), base64.StdEncoding.EncodeToString(data)); err != nil {
			log.Warningf("Error publishing task %q on stream pubsub: %s", taskID, err)
			return status.InternalErrorf("Error publishing task %q on stream pubsub: %s", taskID, err)
		}

		if stage == repb.ExecutionStage_COMPLETED {
			err := func() error {
				mu.Lock()
				defer mu.Unlock()

				if err := s.updateExecution(ctx, taskID, stage, op); err != nil {
					log.Errorf("PublishOperation: error updating execution: %q: %s", taskID, err.Error())
					return err
				}
				lastWrite = time.Now()
				return nil
			}()
			if err != nil {
				return err
			}
		}
	}
}

// markTaskComplete contains logic to be run when the task is complete but
// before letting the client know that the task has completed.
func (s *ExecutionServer) markTaskComplete(ctx context.Context, taskID string, executeResponse *repb.ExecuteResponse) error {
	actionResourceName, err := digest.ParseUploadResourceName(taskID)
	if err != nil {
		return err
	}
	cmd, err := s.fetchCommandForTask(ctx, actionResourceName)
	if err != nil {
		return err
	}

	router := s.env.GetTaskRouter()
	// Only update the router if a task was actually executed
	if router != nil && !executeResponse.GetCachedResult() {
		nodeID := executeResponse.GetResult().GetExecutionMetadata().GetExecutorId()
		router.MarkComplete(ctx, cmd, actionResourceName.GetInstanceName(), nodeID)
	}

	return s.updateUsage(ctx, cmd, executeResponse)
}

func (s *ExecutionServer) updateUsage(ctx context.Context, cmd *repb.Command, executeResponse *repb.ExecuteResponse) error {
	ut := s.env.GetUsageTracker()
	if ut == nil {
		return nil
	}
	dur, err := executionDuration(executeResponse.GetResult().GetExecutionMetadata())
	if err != nil {
		return err
	}
	counts := &tables.UsageCounts{}
	plat := platform.ParseProperties(&repb.ExecutionTask{Command: cmd})
	if plat.OS == platform.DarwinOperatingSystemName {
		counts.MacExecutionDurationUsec += dur.Microseconds()
	} else if plat.OS == platform.LinuxOperatingSystemName {
		counts.LinuxExecutionDurationUsec += dur.Microseconds()
	} else {
		return status.InternalErrorf("Unsupported platform %s", plat.OS)
	}
	return ut.Increment(ctx, counts)
}

func (s *ExecutionServer) fetchCommandForTask(ctx context.Context, actionResourceName *digest.ResourceName) (*repb.Command, error) {
	action := &repb.Action{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, actionResourceName, action); err != nil {
		return nil, err
	}
	cmdDigest := action.GetCommandDigest()
	cmdInstanceNameDigest := digest.NewResourceName(cmdDigest, actionResourceName.GetInstanceName())
	cmd := &repb.Command{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, cmdInstanceNameDigest, cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

func executionDuration(md *repb.ExecutedActionMetadata) (time.Duration, error) {
	if err := md.GetOutputUploadCompletedTimestamp().CheckValid(); err != nil {
		return 0, err
	}
	if err := md.GetQueuedTimestamp().CheckValid(); err != nil {
		return 0, err
	}
	end := md.GetOutputUploadCompletedTimestamp().AsTime()
	start := md.GetQueuedTimestamp().AsTime()
	dur := end.Sub(start)
	if dur <= 0 {
		return 0, status.InternalErrorf("Execution duration is <= 0")
	}
	return dur, nil
}

func (s *ExecutionServer) Cancel(ctx context.Context, invocationID string) error {
	dbh := s.env.GetDBHandle()
	rows, err := dbh.DB(ctx).Raw(
		"SELECT * FROM Executions WHERE invocation_id = ? AND stage != ?",
		invocationID,
		repb.ExecutionStage_COMPLETED).Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	numCancelled := 0
	for rows.Next() {
		e := &tables.Execution{}
		if err := dbh.DB(ctx).ScanRows(rows, e); err != nil {
			return err
		}
		cancelled, err := s.env.GetSchedulerService().CancelTask(ctx, e.ExecutionID)
		if cancelled {
			numCancelled++
		}
		if err == nil && cancelled {
			err = s.MarkExecutionFailed(ctx, e.ExecutionID, status.CanceledError("invocation cancelled"))
			if err != nil {
				log.Warningf("Could not mark execution %q as cancelled: %s", e.ExecutionID, err)
			}
		}
	}
	log.Infof("Cancelled %d executions for invocation %s", numCancelled, invocationID)
	return nil
}
