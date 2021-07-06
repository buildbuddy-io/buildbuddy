package execution_server

import (
	"context"
	"encoding/base64"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	gstatus "google.golang.org/grpc/status"
)

const (
	// The default operating system platform value, Linux.
	defaultPlatformOSValue = "linux"
	// The default architecture cpu architecture value, amd64.
	defaultPlatformArchValue = "amd64"
	// The key in Bazel platform properties that specifies operating system.
	platformOSKey = "OSFamily"
	// The key in Bazel platform properties that specifies executor cpu architecture.
	platformArchKey = "Arch"
	// The key in Bazel platform properties that specifies executor pool.
	platformPoolKey = "Pool"
)

func timestampToMicros(tsPb *tspb.Timestamp) int64 {
	ts, _ := ptypes.Timestamp(tsPb)
	return timeutil.ToUsec(ts)
}

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
	execution.QueuedTimestampUsec = timestampToMicros(summary.GetExecutedActionMetadata().GetQueuedTimestamp())
	execution.WorkerStartTimestampUsec = timestampToMicros(summary.GetExecutedActionMetadata().GetWorkerStartTimestamp())
	execution.WorkerCompletedTimestampUsec = timestampToMicros(summary.GetExecutedActionMetadata().GetWorkerCompletedTimestamp())
	execution.InputFetchStartTimestampUsec = timestampToMicros(summary.GetExecutedActionMetadata().GetInputFetchStartTimestamp())
	execution.InputFetchCompletedTimestampUsec = timestampToMicros(summary.GetExecutedActionMetadata().GetInputFetchCompletedTimestamp())
	execution.ExecutionStartTimestampUsec = timestampToMicros(summary.GetExecutedActionMetadata().GetExecutionStartTimestamp())
	execution.ExecutionCompletedTimestampUsec = timestampToMicros(summary.GetExecutedActionMetadata().GetExecutionCompletedTimestamp())
	execution.OutputUploadStartTimestampUsec = timestampToMicros(summary.GetExecutedActionMetadata().GetOutputUploadStartTimestamp())
	execution.OutputUploadCompletedTimestampUsec = timestampToMicros(summary.GetExecutedActionMetadata().GetOutputUploadCompletedTimestamp())
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
	return "taskStatusStream/" + taskID
}

type ExecutionServer struct {
	env          environment.Env
	cache        interfaces.Cache
	streamPubSub *pubsub.StreamPubSub
	// If enabled, users may register their own executors.
	// When enabled, the executor group ID becomes part of the executor key.
	enableUserOwnedExecutors bool
}

func NewExecutionServer(env environment.Env) (*ExecutionServer, error) {
	cache := env.GetCache()
	if cache == nil {
		return nil, fmt.Errorf("A cache is required to enable the RemoteExecutionServer")
	}
	if env.GetRemoteExecutionRedisClient() == nil || env.GetRemoteExecutionRedisPubSubClient() == nil {
		return nil, status.FailedPreconditionErrorf("Redis is required for remote execution")
	}
	es := &ExecutionServer{
		env:                      env,
		cache:                    cache,
		enableUserOwnedExecutors: env.GetConfigurator().GetRemoteExecutionConfig().EnableUserOwnedExecutors,
		streamPubSub:             pubsub.NewStreamPubSub(env.GetRemoteExecutionRedisPubSubClient()),
	}
	return es, nil
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

	if permissions == nil && s.env.GetConfigurator().GetAnonymousUsageEnabled() {
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
	if op != nil {
		if executeResponse := operation.ExtractExecuteResponse(op); executeResponse != nil {
			execution.StatusCode = executeResponse.GetStatus().GetCode()
			execution.StatusMessage = trimStatus(executeResponse.GetStatus().GetMessage())
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
	}

	return s.env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
		var existing tables.Execution
		if err := tx.Where("execution_id = ?", executionID).First(&existing).Error; err != nil {
			return err
		}
		return tx.Model(&existing).Where("execution_id = ?", executionID).Updates(execution).Error
	})
}

// getUnvalidatedActionResult fetches an action result from the cache but does
// not validate it.
// N.B. This should only be used if the calling code has already ensured the
// action is valid and may be returned.
func (s *ExecutionServer) getUnvalidatedActionResult(ctx context.Context, d *digest.InstanceNameDigest) (*repb.ActionResult, error) {
	cache := namespace.ActionCache(s.cache, d.GetInstanceName())
	data, err := cache.Get(ctx, d.Digest)
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, digest.MissingDigestError(d.Digest)
		}
		return nil, err
	}
	actionResult := &repb.ActionResult{}
	if err := proto.Unmarshal(data, actionResult); err != nil {
		return nil, err
	}
	return actionResult, nil
}

func (s *ExecutionServer) getActionResultFromCache(ctx context.Context, d *digest.InstanceNameDigest) (*repb.ActionResult, error) {
	actionResult, err := s.getUnvalidatedActionResult(ctx, d)
	if err != nil {
		return nil, err
	}
	casCache := namespace.CASCache(s.cache, d.GetInstanceName())
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
	adInstanceDigest := digest.NewInstanceNameDigest(req.GetActionDigest(), req.GetInstanceName())
	action := &repb.Action{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, adInstanceDigest, action); err != nil {
		log.Errorf("Error fetching action: %s", err.Error())
		return "", err
	}
	cmdInstanceDigest := digest.NewInstanceNameDigest(action.GetCommandDigest(), req.GetInstanceName())
	command := &repb.Command{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, cmdInstanceDigest, command); err != nil {
		log.Errorf("Error fetching command: %s", err.Error())
		return "", err
	}

	executionID, err := digest.UploadResourceName(req.GetActionDigest(), req.GetInstanceName())
	if err != nil {
		return "", err
	}

	tracing.AddStringAttributeToCurrentSpan(ctx, "task_id", executionID)

	invocationID := bazel_request.GetInvocationID(ctx)

	if err := s.insertExecution(ctx, executionID, invocationID, generateCommandSnippet(command), repb.ExecutionStage_UNKNOWN); err != nil {
		return "", err
	}

	executionTask := &repb.ExecutionTask{
		ExecuteRequest: req,
		InvocationId:   invocationID,
		ExecutionId:    executionID,
		Action:         action,
		Command:        command,
	}
	// Allow execution worker to auth to cache (if necessary).
	if jwt, ok := ctx.Value("x-buildbuddy-jwt").(string); ok {
		executionTask.Jwt = jwt
	}
	executionTask.QueuedTimestamp = ptypes.TimestampNow()
	serializedTask, err := proto.Marshal(executionTask)
	if err != nil {
		// Should never happen.
		return "", status.InternalErrorf("Error marshalling execution task %q: %s", executionID, err)
	}

	taskSize := tasksize.Estimate(command)

	os := defaultPlatformOSValue
	arch := defaultPlatformArchValue
	groupID, pool, err := s.env.GetSchedulerService().GetGroupIDAndDefaultPoolForUser(ctx)
	if err != nil {
		return "", err
	}
	for _, property := range command.GetPlatform().GetProperties() {
		if property.Name == platformOSKey {
			os = strings.ToLower(property.Value)
		}
		if property.Name == platformPoolKey && property.Value != platform.DefaultPoolValue {
			pool = strings.ToLower(property.Value)
		}
		if property.Name == platformArchKey {
			arch = strings.ToLower(property.Value)
		}
	}

	schedulingMetadata := &scpb.SchedulingMetadata{
		Os:       os,
		Arch:     arch,
		Pool:     pool,
		TaskSize: taskSize,
		GroupId:  groupID,
	}
	scheduleReq := &scpb.ScheduleTaskRequest{
		TaskId:         executionID,
		Metadata:       schedulingMetadata,
		SerializedTask: serializedTask,
	}
	if _, err := scheduler.ScheduleTask(ctx, scheduleReq); err != nil {
		return "", status.UnavailableErrorf("Error scheduling execution task %q: %s", executionID, err.Error())
	}
	return executionID, nil
}

func (s *ExecutionServer) execute(req *repb.ExecuteRequest, stream streamLike) error {
	adInstanceDigest := digest.NewInstanceNameDigest(req.GetActionDigest(), req.GetInstanceName())
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}

	if !req.GetSkipCacheLookup() {
		if actionResult, err := s.getActionResultFromCache(ctx, adInstanceDigest); err == nil {
			executionID, err := digest.UploadResourceName(req.GetActionDigest(), req.GetInstanceName())
			if err != nil {
				return err
			}
			stateChangeFn := operation.GetStateChangeFunc(stream, executionID, adInstanceDigest)
			if err := stateChangeFn(repb.ExecutionStage_COMPLETED, operation.ExecuteResponseWithResult(actionResult, nil, codes.OK)); err != nil {
				return err // CHECK (these errors should not happen).
			}
			return nil
		}
	}
	executionID, err := s.Dispatch(ctx, req)
	if err != nil {
		log.Errorf("Error dispatching execution %q: %s", executionID, err.Error())
		return err
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
			log.Warningf("Could not determine groupID for metrics: %s", err)
			return "unknown"
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

	e := InProgressExecution{
		server:       s,
		clientStream: stream,
		opName:       req.GetName(),
	}

	var subscriber interfaces.Subscriber
	if opts.isExecuteRequest {
		subscriber = s.streamPubSub.SubscribeHead(ctx, redisKeyForTaskStatusStream(req.GetName()))
	} else {
		// If this is a WaitExecution RPC, start the subscription from the last published status, inclusive.
		subscriber = s.streamPubSub.SubscribeTail(ctx, redisKeyForTaskStatusStream(req.GetName()))
	}
	defer subscriber.Close()
	streamPubSubChan := subscriber.Chan()

	if opts.isExecuteRequest {
		// Send a best-effort initial "in progress" update to client.
		// Once Bazel receives the initial update, it will use WaitExecution to handle retry on error instead of
		// requesting a new execution via Execute.
		instanceName, d, err := digest.ExtractDigestFromUploadResourceName(req.GetName())
		if err != nil {
			log.Errorf("Could not extract digest from %q: %s", req.GetName(), err)
			return err
		}
		id := digest.NewInstanceNameDigest(d, instanceName)
		stateChangeFn := operation.GetStateChangeFunc(stream, req.GetName(), id)
		err = stateChangeFn(repb.ExecutionStage_UNKNOWN, operation.InProgressExecuteResponse())
		if err != nil && err != io.EOF {
			log.Warningf("Could not send initial update: %s", err)
		}
	}

	groupID := s.getGroupIDForMetrics(ctx)
	metrics.RemoteExecutionWaitingExecutionResult.With(prometheus.Labels{metrics.GroupID: groupID}).Inc()
	defer metrics.RemoteExecutionWaitingExecutionResult.With(prometheus.Labels{metrics.GroupID: groupID}).Dec()

	for {
		for {
			serializedOp, ok := <-streamPubSubChan
			if !ok {
				return status.UnavailableErrorf("Stream PubSub channel closed for %q", req.GetName())
			}
			done, err := e.processSerializedOpUpdate(serializedOp)
			if done {
				log.Debugf("WaitExecution %q: progress loop exited: err: %v, done: %t", req.GetName(), err, done)
				return err
			}
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

		// Before publishing the COMPLETED operation, update the task router
		// so that subsequent tasks with similar routing properties can get
		// routed back to the same executor.
		if stage == repb.ExecutionStage_COMPLETED {
			response := operation.ExtractExecuteResponse(op)
			if response != nil {
				if err := s.updateRouter(ctx, taskID, response); err != nil {
					// Errors from updating the router should not be fatal.
					log.Errorf("Failed to update task router for task %s: %s", taskID, err)
				}
			}
		}

		data, err := proto.Marshal(op)
		if err != nil {
			return err
		}
		if err := s.streamPubSub.Publish(stream.Context(), redisKeyForTaskStatusStream(taskID), base64.StdEncoding.EncodeToString(data)); err != nil {
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

func (s *ExecutionServer) updateRouter(ctx context.Context, taskID string, executeResponse *repb.ExecuteResponse) error {
	router := s.env.GetTaskRouter()
	if router == nil {
		return nil
	}
	// If the result was served from cache, nothing was actually executed, so no
	// need to update the task router.
	if executeResponse.GetCachedResult() {
		return nil
	}
	instanceName, d, err := digest.ExtractDigestFromUploadResourceName(taskID)
	if err != nil {
		return err
	}
	actionInstanceNameDigest := digest.NewInstanceNameDigest(d, instanceName)
	action := &repb.Action{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, actionInstanceNameDigest, action); err != nil {
		return err
	}
	cmdDigest := action.GetCommandDigest()
	cmdInstanceNameDigest := digest.NewInstanceNameDigest(cmdDigest, instanceName)
	cmd := &repb.Command{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, cmdInstanceNameDigest, cmd); err != nil {
		return err
	}
	nodeID := executeResponse.GetResult().GetExecutionMetadata().GetExecutorId()
	router.MarkComplete(ctx, cmd, instanceName, nodeID)
	return nil
}
