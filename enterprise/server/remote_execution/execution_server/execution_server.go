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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gcplink"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/action_merger"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/olapdbconfig"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/types/known/timestamppb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
	remote_execution_config "github.com/buildbuddy-io/buildbuddy/server/remote_execution/config"
	gstatus "google.golang.org/grpc/status"
)

const (
	updateExecutionTimeout = 15 * time.Second

	// When an action finishes, schedule the corresponding pubsub channel to
	// be discarded after this time. There may be multiple waiters for a single
	// action so we cannot discard the channel immediately.
	completedPubSubChanExpiration = 15 * time.Minute
)

var (
	enableRedisAvailabilityMonitoring = flag.Bool("remote_execution.enable_redis_availability_monitoring", false, "If enabled, the execution server will detect if Redis has lost state and will ask Bazel to retry executions.")
)

func fillExecutionFromActionMetadata(md *repb.ExecutedActionMetadata, execution *tables.Execution) {
	// IOStats
	execution.FileDownloadCount = md.GetIoStats().GetFileDownloadCount()
	execution.FileDownloadSizeBytes = md.GetIoStats().GetFileDownloadSizeBytes()
	execution.FileDownloadDurationUsec = md.GetIoStats().GetFileDownloadDurationUsec()
	execution.FileUploadCount = md.GetIoStats().GetFileUploadCount()
	execution.FileUploadSizeBytes = md.GetIoStats().GetFileUploadSizeBytes()
	execution.FileUploadDurationUsec = md.GetIoStats().GetFileUploadDurationUsec()
	// Task sizing
	execution.EstimatedMilliCPU = md.GetEstimatedTaskSize().GetEstimatedMilliCpu()
	execution.EstimatedMemoryBytes = md.GetEstimatedTaskSize().GetEstimatedMemoryBytes()
	execution.PeakMemoryBytes = md.GetUsageStats().GetPeakMemoryBytes()
	execution.CPUNanos = md.GetUsageStats().GetCpuNanos()
	// ExecutedActionMetadata
	execution.Worker = md.GetWorker()
	execution.QueuedTimestampUsec = md.GetQueuedTimestamp().AsTime().UnixMicro()
	execution.WorkerStartTimestampUsec = md.GetWorkerStartTimestamp().AsTime().UnixMicro()
	execution.WorkerCompletedTimestampUsec = md.GetWorkerCompletedTimestamp().AsTime().UnixMicro()
	execution.InputFetchStartTimestampUsec = md.GetInputFetchStartTimestamp().AsTime().UnixMicro()
	execution.InputFetchCompletedTimestampUsec = md.GetInputFetchCompletedTimestamp().AsTime().UnixMicro()
	execution.ExecutionStartTimestampUsec = md.GetExecutionStartTimestamp().AsTime().UnixMicro()
	execution.ExecutionCompletedTimestampUsec = md.GetExecutionCompletedTimestamp().AsTime().UnixMicro()
	execution.OutputUploadStartTimestampUsec = md.GetOutputUploadStartTimestamp().AsTime().UnixMicro()
	execution.OutputUploadCompletedTimestampUsec = md.GetOutputUploadCompletedTimestamp().AsTime().UnixMicro()
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

type ExecutionServer struct {
	env                               environment.Env
	cache                             interfaces.Cache
	rdb                               redis.UniversalClient
	streamPubSub                      *pubsub.StreamPubSub
	enableRedisAvailabilityMonitoring bool
}

func Register(env *real_environment.RealEnv) error {
	if !remote_execution_config.RemoteExecutionEnabled() {
		return nil
	}
	// Make sure capabilities server reflect that we're running
	// remote execution.
	executionServer, err := NewExecutionServer(env)
	if err != nil {
		return status.FailedPreconditionErrorf("Error initializing ExecutionServer: %s", err)
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
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

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
	if u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx); err == nil && u.GetGroupID() != "" {
		permissions = perms.DefaultPermissions(u)
	}

	if permissions == nil && s.env.GetAuthenticator().AnonymousUsageEnabled(ctx) {
		permissions = perms.AnonymousUserPermissions()
	} else if permissions == nil {
		return status.PermissionDeniedErrorf("Anonymous access disabled, permission denied.")
	}

	execution.UserID = permissions.UserID
	execution.GroupID = permissions.GroupID
	execution.Perms = execution.Perms | permissions.Perms

	return s.env.GetDBHandle().NewQuery(ctx, "execution_server_create_execution").Create(execution)
}

func (s *ExecutionServer) insertInvocationLink(ctx context.Context, executionID, invocationID string, linkType sipb.StoredInvocationLink_Type) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// Add the invocation links to Redis. MySQL insertion sometimes can take a
	// longer time to finish and the insertion can be finished after the
	// execution is complete.
	redisErr := s.insertInvocationLinkInRedis(ctx, executionID, invocationID, linkType)
	if redisErr != nil {
		log.CtxWarningf(ctx, "failed to add invocation link(exeuction_id: %q invocation_id: %q, link_type: %d) in redis", executionID, invocationID, linkType)
	}

	link := &tables.InvocationExecution{
		InvocationID: invocationID,
		ExecutionID:  executionID,
		Type:         int8(linkType),
	}
	err := s.env.GetDBHandle().NewQuery(ctx, "execution_server_create_invocation_link").Create(link)
	// This probably means there were duplicate actions in a single invocation
	// that were merged. Not an error.
	if err != nil && s.env.GetDBHandle().IsDuplicateKeyError(err) {
		log.CtxWarningf(ctx, "Duplicate execution link while inserting execution %q invocation ID %q link type %s", executionID, invocationID, linkType)
		return nil
	}
	return err
}

func (s *ExecutionServer) insertInvocationLinkInRedis(ctx context.Context, executionID, invocationID string, linkType sipb.StoredInvocationLink_Type) error {
	if s.env.GetExecutionCollector() == nil {
		return nil
	}
	link := &sipb.StoredInvocationLink{
		InvocationId: invocationID,
		ExecutionId:  executionID,
		Type:         linkType,
	}
	return s.env.GetExecutionCollector().AddInvocationLink(ctx, link)
}

func trimStatus(statusMessage string) string {
	if len(statusMessage) > 255 {
		return statusMessage[:252] + "..."
	}
	return statusMessage
}

func (s *ExecutionServer) updateExecution(ctx context.Context, executionID string, stage repb.ExecutionStage_Value, op *longrunning.Operation) error {
	if s.env.GetDBHandle() == nil {
		return status.FailedPreconditionError("database not configured")
	}
	ctx, cancel := background.ExtendContextForFinalization(ctx, updateExecutionTimeout)
	defer cancel()
	execution := &tables.Execution{
		ExecutionID: executionID,
		Stage:       int64(stage),
	}

	if executeResponse := operation.ExtractExecuteResponse(op); executeResponse != nil {
		execution.StatusCode = executeResponse.GetStatus().GetCode()
		execution.StatusMessage = trimStatus(executeResponse.GetStatus().GetMessage())
		execution.ExitCode = executeResponse.GetResult().GetExitCode()
		if details := executeResponse.GetStatus().GetDetails(); len(details) > 0 {
			serializedDetails, err := proto.Marshal(details[0])
			if err == nil {
				execution.SerializedStatusDetails = serializedDetails
			} else {
				log.CtxErrorf(ctx, "Error marshalling status details: %s", err.Error())
			}
		}
		execution.CachedResult = executeResponse.GetCachedResult()
		execution.DoNotCache = executeResponse.GetResult().GetExecutionMetadata().GetDoNotCache()

		// Update stats if the operation has been completed.
		if stage == repb.ExecutionStage_COMPLETED {
			md := executeResponse.GetResult().GetExecutionMetadata()
			// Backwards-compatible fill of the execution with the ExecutionSummary for
			// now. The ExecutionSummary will be removed completely in the future.
			if statsUnset(md) {
				if decodedMetadata, err := decodeMetadataFromExecutionSummary(executeResponse); err == nil {
					md = decodedMetadata
				}
			}
			fillExecutionFromActionMetadata(md, execution)
		}
	}

	if stage == repb.ExecutionStage_COMPLETED {
		if err := action_merger.DeletePendingExecution(ctx, s.rdb, executionID); err != nil {
			log.CtxWarningf(ctx, "could not delete pending execution %q: %s", executionID, err)
		}
	}

	result := s.env.GetDBHandle().GORM(ctx, "execution_server_update_execution").Where(
		"execution_id = ? AND stage != ?", executionID, repb.ExecutionStage_COMPLETED).Updates(execution)
	dbErr := result.Error
	if dbErr == nil && result.RowsAffected == 0 {
		// We want to return an error if the execution simply doesn't exist, but
		// we want to ignore any attempts to update a cancelled execution.
		var count int64
		err := s.env.GetDBHandle().NewQuery(ctx, "execution_server_check_after_noop_update").Raw(`
				SELECT COUNT(*) FROM "Executions" WHERE execution_id = ?
			`,
			executionID).Take(&count)
		if err != nil {
			dbErr = err
		} else if count == 0 {
			dbErr = status.NotFoundErrorf("Unable to update execution; no execution exists with id %s.", executionID)
		}
	}

	if stage == repb.ExecutionStage_COMPLETED {
		if err := s.recordExecution(ctx, executionID); err != nil {
			log.CtxErrorf(ctx, "failed to record execution %q: %s", executionID, err)
		}
	}
	return dbErr
}

func (s *ExecutionServer) recordExecution(ctx context.Context, executionID string) error {
	if s.env.GetExecutionCollector() == nil || !olapdbconfig.WriteExecutionsToOLAPDBEnabled() {
		return nil
	}
	var executionPrimaryDB tables.Execution

	if err := s.env.GetDBHandle().NewQuery(ctx, "execution_server_lookup_execution").Raw(
		`SELECT * FROM "Executions" WHERE execution_id = ?`, executionID).Take(&executionPrimaryDB); err != nil {
		return status.InternalErrorf("failed to look up execution %q: %s", executionID, err)
	}
	// Always clean up invocationLinks in Collector because we are not retrying
	defer func() {
		err := s.env.GetExecutionCollector().DeleteInvocationLinks(ctx, executionID)
		if err != nil {
			log.CtxErrorf(ctx, "failed to clean up invocation links in collector: %s", err)
		}
	}()
	links, err := s.env.GetExecutionCollector().GetInvocationLinks(ctx, executionID)

	if err != nil {
		return status.InternalErrorf("failed to get invocations for execution %q: %s", executionID, err)
	}

	for _, link := range links {
		executionProto := execution.TableExecToProto(&executionPrimaryDB, link)
		inv, err := s.env.GetExecutionCollector().GetInvocation(ctx, link.GetInvocationId())
		if err != nil {
			log.CtxErrorf(ctx, "failed to get invocation %q from ExecutionCollector: %s", link.GetInvocationId(), err)
			continue
		}
		if inv == nil {
			// The invocation hasn't finished yet. Add the execution to ExecutionCollector, and flush it once
			// the invocation is complete
			if err := s.env.GetExecutionCollector().AppendExecution(ctx, link.GetInvocationId(), executionProto); err != nil {
				log.CtxErrorf(ctx, "failed to append execution %q to invocation %q: %s", executionID, link.GetInvocationId(), err)
			} else {
				log.CtxInfof(ctx, "appended execution %q to invocation %q in redis", executionID, link.GetInvocationId())
			}
		} else {
			err = s.env.GetOLAPDBHandle().FlushExecutionStats(ctx, inv, []*repb.StoredExecution{executionProto})
			if err != nil {
				log.CtxErrorf(ctx, "failed to flush execution %q for invocation %q to clickhouse: %s", executionID, link.GetInvocationId(), err)
			} else {
				log.CtxInfof(ctx, "successfully write 1 execution for invocation %q", link.GetInvocationId())
			}
		}

	}
	return nil
}

// getUnvalidatedActionResult fetches an action result from the cache but does
// not validate it.
// N.B. This should only be used if the calling code has already ensured the
// action is valid and may be returned.
func (s *ExecutionServer) getUnvalidatedActionResult(ctx context.Context, r *digest.ResourceName) (*repb.ActionResult, error) {
	cacheResource := digest.NewResourceName(r.GetDigest(), r.GetInstanceName(), rspb.CacheType_AC, r.GetDigestFunction())
	data, err := s.cache.Get(ctx, cacheResource.ToProto())
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
	if err := action_cache_server.ValidateActionResult(ctx, s.cache, d.GetInstanceName(), d.GetDigestFunction(), actionResult); err != nil {
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
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	scheduler := s.env.GetSchedulerService()
	if scheduler == nil {
		return "", status.FailedPreconditionErrorf("No scheduler service configured")
	}
	sizer := s.env.GetTaskSizer()
	if sizer == nil {
		return "", status.FailedPreconditionError("No task sizer configured")
	}
	adInstanceDigest := digest.NewResourceName(req.GetActionDigest(), req.GetInstanceName(), rspb.CacheType_CAS, req.GetDigestFunction())
	action := &repb.Action{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, adInstanceDigest, action); err != nil {
		log.CtxWarningf(ctx, "Error fetching action: %s", err.Error())
		return "", err
	}
	cmdInstanceDigest := digest.NewResourceName(action.GetCommandDigest(), req.GetInstanceName(), rspb.CacheType_CAS, req.GetDigestFunction())
	command := &repb.Command{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, cmdInstanceDigest, command); err != nil {
		log.CtxWarningf(ctx, "Error fetching command: %s", err.Error())
		return "", err
	}

	r := digest.NewResourceName(req.GetActionDigest(), req.GetInstanceName(), rspb.CacheType_CAS, req.GetDigestFunction())
	executionID, err := r.UploadString()
	if err != nil {
		return "", err
	}

	tracing.AddStringAttributeToCurrentSpan(ctx, "task_id", executionID)

	invocationID := bazel_request.GetInvocationID(ctx)
	rmd := bazel_request.GetRequestMetadata(ctx)
	if invocationID == "" {
		log.CtxInfof(ctx, "Execution %q is missing invocation ID metadata. Request metadata: %+v", executionID, rmd)
	}
	// Drop ToolDetails from the request metadata. Executors will include this
	// metadata in all app requests, but the ToolDetails identify the request as
	// being from bazel, which is not desired.
	if rmd != nil {
		rmd = rmd.CloneVT()
		rmd.ToolDetails = nil
	}

	if err := s.insertExecution(ctx, executionID, invocationID, generateCommandSnippet(command), repb.ExecutionStage_UNKNOWN); err != nil {
		return "", err
	}
	if err := s.insertInvocationLink(ctx, executionID, invocationID, sipb.StoredInvocationLink_NEW); err != nil {
		return "", err
	}

	executionTask := &repb.ExecutionTask{
		ExecuteRequest:  req,
		InvocationId:    invocationID,
		ExecutionId:     executionID,
		Action:          action,
		Command:         command,
		RequestMetadata: rmd,
	}
	// Allow execution worker to auth to cache (if necessary).
	if jwt, ok := ctx.Value("x-buildbuddy-jwt").(string); ok {
		executionTask.Jwt = jwt
	}

	platformPropOverrides := platform.RemoteHeaderOverrides(ctx)
	if len(platformPropOverrides) > 0 {
		executionTask.PlatformOverrides = &repb.Platform{Properties: platformPropOverrides}
	}

	taskGroupID := interfaces.AuthAnonymousUser
	if user, err := s.env.GetAuthenticator().AuthenticatedUser(ctx); err == nil {
		taskGroupID = user.GetGroupID()
	}

	props, err := platform.ParseProperties(executionTask)
	if err != nil {
		return "", err
	}

	// Add in secrets for any action explicitly requesting secrets, and all workflows.
	secretService := s.env.GetSecretService()
	if props.IncludeSecrets {
		if secretService == nil {
			return "", status.FailedPreconditionError("Secrets requested but secret service not available")
		}
		envVars, err := secretService.GetSecretEnvVars(ctx, taskGroupID)
		if err != nil {
			return "", err
		}
		envVars, err = gcplink.ExchangeRefreshTokenForAuthToken(ctx, envVars, platform.IsCICommand(command))
		if err != nil {
			return "", err
		}
		executionTask.Command.EnvironmentVariables = append(executionTask.Command.EnvironmentVariables, envVars...)
	}

	executionTask.QueuedTimestamp = timestamppb.Now()
	serializedTask, err := proto.Marshal(executionTask)
	if err != nil {
		// Should never happen.
		return "", status.InternalErrorf("Error marshalling execution task %q: %s", executionID, err)
	}

	taskSize := tasksize.Estimate(executionTask)
	measuredSize := sizer.Get(ctx, executionTask)
	var predictedSize *scpb.TaskSize
	if measuredSize == nil {
		predictedSize = sizer.Predict(ctx, executionTask)
	}

	pool, err := s.env.GetSchedulerService().GetPoolInfo(ctx, props.OS, props.Pool, props.WorkflowID, props.UseSelfHostedExecutors)
	if err != nil {
		return "", err
	}

	metrics.RemoteExecutionRequests.With(prometheus.Labels{metrics.GroupID: taskGroupID, metrics.OS: props.OS, metrics.Arch: props.Arch}).Inc()

	if s.enableRedisAvailabilityMonitoring {
		if err := s.streamPubSub.CreateMonitoredChannel(ctx, redisKeyForMonitoredTaskStatusStream(executionID)); err != nil {
			return "", err
		}
	}

	schedulingMetadata := &scpb.SchedulingMetadata{
		Os:                props.OS,
		Arch:              props.Arch,
		Pool:              pool.Name,
		TaskSize:          taskSize,
		MeasuredTaskSize:  measuredSize,
		PredictedTaskSize: predictedSize,
		ExecutorGroupId:   pool.GroupID,
		TaskGroupId:       taskGroupID,
	}
	scheduleReq := &scpb.ScheduleTaskRequest{
		TaskId:         executionID,
		Metadata:       schedulingMetadata,
		SerializedTask: serializedTask,
	}

	if err := action_merger.RecordQueuedExecution(ctx, s.rdb, executionID, r); err != nil {
		log.CtxWarningf(ctx, "could not record queued pending execution %q: %s", executionID, err)
	}

	if _, err := scheduler.ScheduleTask(ctx, scheduleReq); err != nil {
		ctx, cancel := background.ExtendContextForFinalization(ctx, 10*time.Second)
		defer cancel()
		_ = action_merger.DeletePendingExecution(ctx, s.rdb, executionID)
		return "", status.UnavailableErrorf("Error scheduling execution task %q: %s", executionID, err)
	}

	return executionID, nil
}

func (s *ExecutionServer) execute(req *repb.ExecuteRequest, stream streamLike) error {
	adInstanceDigest := digest.NewResourceName(req.GetActionDigest(), req.GetInstanceName(), rspb.CacheType_CAS, req.GetDigestFunction())
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), s.env)
	if err != nil {
		return err
	}

	downloadString, err := adInstanceDigest.DownloadString()
	if err != nil {
		return err
	}
	invocationID := bazel_request.GetInvocationID(stream.Context())

	executionID := ""
	if !req.GetSkipCacheLookup() {
		if actionResult, err := s.getActionResultFromCache(ctx, adInstanceDigest); err == nil {
			r := digest.NewResourceName(req.GetActionDigest(), req.GetInstanceName(), rspb.CacheType_CAS, req.GetDigestFunction())
			executionID, err := r.UploadString()
			if err != nil {
				return err
			}
			tracing.AddStringAttributeToCurrentSpan(ctx, "execution_result", "cached")
			tracing.AddStringAttributeToCurrentSpan(ctx, "execution_id", executionID)
			stateChangeFn := operation.GetStateChangeFunc(stream, executionID, adInstanceDigest)
			if err := stateChangeFn(repb.ExecutionStage_COMPLETED, operation.ExecuteResponseWithCachedResult(actionResult)); err != nil {
				return err // CHECK (these errors should not happen).
			}
			return nil
		}

		// Check if there's already an identical action pending execution. If
		// so, wait on the result of that execution instead of starting a new
		// one.
		ee, started, err := action_merger.FindPendingExecution(ctx, s.rdb, s.env.GetSchedulerService(), adInstanceDigest)
		if err != nil {
			log.CtxWarningf(ctx, "could not check for existing execution: %s", err)
		}
		if ee != "" {
			ctx = log.EnrichContext(ctx, log.ExecutionIDKey, ee)
			log.CtxInfof(ctx, "Reusing execution %q for execution request %q for invocation %q", ee, downloadString, invocationID)
			executionID = ee
			tracing.AddStringAttributeToCurrentSpan(ctx, "execution_result", "merged")
			tracing.AddStringAttributeToCurrentSpan(ctx, "execution_id", executionID)
			metrics.RemoteExecutionMergedActions.With(prometheus.Labels{metrics.GroupID: s.getGroupIDForMetrics(ctx)}).Inc()
			if started.Before(time.Now()) {
				metrics.RemoteExecutionMergedTimeSavedUsec.
					With(prometheus.Labels{metrics.GroupID: s.getGroupIDForMetrics(ctx)}).
					Observe(float64(time.Since(started).Microseconds()))
			}
			if err := s.insertInvocationLink(ctx, ee, invocationID, sipb.StoredInvocationLink_MERGED); err != nil {
				return err
			}
		}
	}

	// Create a new execution unless we found an existing identical action we
	// can wait on.
	if executionID == "" {
		log.CtxInfof(ctx, "Scheduling new execution for %q for invocation %q", downloadString, invocationID)
		newExecutionID, err := s.Dispatch(ctx, req)
		if err != nil {
			log.CtxWarningf(ctx, "Error dispatching execution for %q: %s", downloadString, err)
			return err
		}
		ctx = log.EnrichContext(ctx, log.ExecutionIDKey, newExecutionID)
		executionID = newExecutionID
		log.CtxInfof(ctx, "Scheduled execution %q for request %q for invocation %q", executionID, downloadString, invocationID)
		tracing.AddStringAttributeToCurrentSpan(ctx, "execution_result", "merged")
		tracing.AddStringAttributeToCurrentSpan(ctx, "execution_id", executionID)
	}

	waitReq := repb.WaitExecutionRequest{
		Name: executionID,
	}
	return s.waitExecution(ctx, &waitReq, stream, waitOpts{isExecuteRequest: true})
}

// WaitExecution waits for an execution operation to complete. When the client initially
// makes the request, the server immediately responds with the current status
// of the execution. The server will leave the request stream open until the
// operation completes, and then respond with the completed operation. The
// server MAY choose to stream additional updates as execution progresses,
// such as to provide an update as to the state of the execution.
func (s *ExecutionServer) WaitExecution(req *repb.WaitExecutionRequest, stream repb.Execution_WaitExecutionServer) error {
	ctx := log.EnrichContext(stream.Context(), log.ExecutionIDKey, req.GetName())
	return s.waitExecution(ctx, req, stream, waitOpts{isExecuteRequest: false})
}

type InProgressExecution struct {
	server       *ExecutionServer
	clientStream streamLike
	lastStage    repb.ExecutionStage_Value
	opName       string
}

func (e *InProgressExecution) processOpUpdate(ctx context.Context, op *longrunning.Operation) (done bool, err error) {
	stage := operation.ExtractStage(op)
	log.CtxInfof(ctx, "WaitExecution: %q in stage: %s", e.opName, stage)
	if stage < e.lastStage {
		return false, nil
	}
	e.lastStage = stage
	err = e.clientStream.Send(op)
	if err == io.EOF {
		log.CtxWarningf(ctx, "Caller hung up on operation: %q.", e.opName)
		return true, nil // If the caller hung-up, bail out.
	}
	if err != nil {
		log.CtxWarningf(ctx, "Error sending operation to caller: %q: %s", e.opName, err.Error())
		return true, err // If some other err happened, bail out.
	}
	if stage == repb.ExecutionStage_COMPLETED {
		return true, nil // If the operation is complete, bail out.
	}
	executeResponse := operation.ExtractExecuteResponse(op)
	if executeResponse != nil {
		if executeResponse.GetStatus().GetCode() != 0 {
			log.CtxWarningf(ctx, "WaitExecution: %q errored, returning %+v", e.opName, executeResponse.GetStatus())
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

func (s *ExecutionServer) waitExecution(ctx context.Context, req *repb.WaitExecutionRequest, stream streamLike, opts waitOpts) error {
	log.CtxInfof(ctx, "WaitExecution called for: %q", req.GetName())
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return err
	}

	actionResource, err := digest.ParseUploadResourceName(req.GetName())
	if err != nil {
		log.CtxErrorf(ctx, "Could not extract digest from %q: %s", req.GetName(), err)
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
		err = stateChangeFn(repb.ExecutionStage_QUEUED, operation.InProgressExecuteResponse())
		if err != nil && err != io.EOF {
			log.CtxWarningf(stream.Context(), "Could not send initial update: %s", err)
		}
	}

	groupID := s.getGroupIDForMetrics(ctx)
	metrics.RemoteExecutionWaitingExecutionResult.With(prometheus.Labels{metrics.GroupID: groupID}).Inc()
	defer metrics.RemoteExecutionWaitingExecutionResult.With(prometheus.Labels{metrics.GroupID: groupID}).Dec()

	for {
		msg, ok := <-streamPubSubChan
		if !ok {
			if ctx.Err() != nil {
				log.CtxInfof(ctx, "WaitExecution %q: client disconnected before action completed: %s", req.GetName(), ctx.Err())
			}
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
		op, err := operation.Decode(data)
		if err != nil {
			log.CtxWarningf(ctx, "Could not decode operation update for %q: %v", e.opName, err)
			// Continue to process further updates.
			continue
		}
		done, err := e.processOpUpdate(ctx, op)
		if done {
			log.CtxDebugf(ctx, "WaitExecution %q: progress loop exited: err: %v, done: %t", req.GetName(), err, done)
			if operation.ExtractStage(op) == repb.ExecutionStage_COMPLETED {
				if err := s.streamPubSub.Expire(ctx, subChan, completedPubSubChanExpiration); err != nil {
					log.CtxWarningf(ctx, "could not expire channel for %q: %s", req.GetName(), err)
				}
			}
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
	r, err := digest.ParseUploadResourceName(taskID)
	if err != nil {
		log.CtxWarningf(ctx, "Could not parse taskID: %s", err)
		return err
	}
	rsp := operation.ErrorResponse(reason)
	op, err := operation.Assemble(repb.ExecutionStage_COMPLETED, taskID, r, rsp)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(op)
	if err != nil {
		return err
	}
	if err := s.streamPubSub.Publish(ctx, s.pubSubChannelForExecutionID(taskID), base64.StdEncoding.EncodeToString(data)); err != nil {
		log.CtxWarningf(ctx, "MarkExecutionFailed: error publishing task %q on stream pubsub: %s", taskID, err)
		return status.InternalErrorf("Error publishing task %q on stream pubsub: %s", taskID, err)
	}
	if err := s.updateExecution(ctx, taskID, operation.ExtractStage(op), op); err != nil {
		log.CtxWarningf(ctx, "MarkExecutionFailed: error updating execution: %q: %s", taskID, err)
		return err
	}
	if err := s.cacheExecuteResponse(ctx, taskID, rsp); err != nil {
		log.CtxWarningf(ctx, "MarkExecutionFailed: failed to cache execute response for execution %q: %s", taskID, err)
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
	// Once the executor has called PublishOperation, we're in EXECUTING stage.
	stage := repb.ExecutionStage_EXECUTING
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
				log.CtxWarningf(ctx, "PublishOperation: FlushWrite: error updating execution: %q: %s", taskID, err.Error())
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
			log.CtxErrorf(ctx, "PublishOperation: recv err: %s", err.Error())
			return err
		}

		mu.Lock()
		lastOp = op
		taskID = op.GetName()
		stage = operation.ExtractStage(op)
		mu.Unlock()

		log.CtxDebugf(ctx, "PublishOperation: operation %q stage: %s", taskID, stage)

		if stage == repb.ExecutionStage_COMPLETED {
			response := operation.ExtractExecuteResponse(op)
			if response != nil {
				if err := s.markTaskComplete(ctx, taskID, response); err != nil {
					// Errors updating the router or recording usage are non-fatal.
					log.CtxErrorf(ctx, "Could not update post-completion metadata for task %q: %s", taskID, err)
				}
			}
		}

		data, err := proto.Marshal(op)
		if err != nil {
			return err
		}
		if err := s.streamPubSub.Publish(ctx, s.pubSubChannelForExecutionID(taskID), base64.StdEncoding.EncodeToString(data)); err != nil {
			log.CtxWarningf(ctx, "Error publishing task %q on stream pubsub: %s", taskID, err)
			return status.InternalErrorf("Error publishing task %q on stream pubsub: %s", taskID, err)
		}

		if stage == repb.ExecutionStage_COMPLETED {
			err := func() error {
				mu.Lock()
				defer mu.Unlock()

				if err := s.updateExecution(ctx, taskID, stage, op); err != nil {
					log.CtxErrorf(ctx, "PublishOperation: error updating execution: %q: %s", taskID, err.Error())
					return status.WrapErrorf(err, "failed to update execution %q", taskID)
				}
				lastWrite = time.Now()
				return nil
			}()
			if err != nil {
				return err
			}

			response := operation.ExtractExecuteResponse(op)
			if response != nil {
				if err := s.cacheExecuteResponse(ctx, taskID, response); err != nil {
					log.CtxErrorf(ctx, "Failed to cache execute response: %s", err)
				}
			}
		}
	}
}

// cacheExecuteResponse caches the ExecuteResponse so that the client can see
// the exact response that was originally returned to Bazel.
func (s *ExecutionServer) cacheExecuteResponse(ctx context.Context, taskID string, response *repb.ExecuteResponse) error {
	taskRN, err := digest.ParseUploadResourceName(taskID)
	if err != nil {
		return err
	}

	// Store the response as an AC entry so that we don't have to explicitly
	// store the digest anywhere. The key is the hash of the task ID, and the
	// value is an ActionResult with stdout_raw set to the marshaled
	// ExecuteResponse.
	d, err := digest.Compute(strings.NewReader(taskID), taskRN.GetDigestFunction())
	if err != nil {
		return err
	}
	arn := digest.NewResourceName(d, taskRN.GetInstanceName(), rspb.CacheType_AC, taskRN.GetDigestFunction())

	b, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	ar := &repb.ActionResult{StdoutRaw: b}

	return cachetools.UploadActionResult(ctx, s.env.GetActionCacheClient(), arn, ar)
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
		executorHostID := executeResponse.GetResult().GetExecutionMetadata().GetWorker()
		router.MarkComplete(ctx, cmd, actionResourceName.GetInstanceName(), executorHostID)
	}

	if sizer := s.env.GetTaskSizer(); sizer != nil {
		md := executeResponse.GetResult().GetExecutionMetadata()
		if err := sizer.Update(ctx, cmd, md); err != nil {
			log.CtxWarningf(ctx, "Failed to update task size: %s", err)
		}
	}

	if err := s.updateUsage(ctx, cmd, executeResponse); err != nil {
		log.CtxWarningf(ctx, "Failed to update usage for ExecuteResponse %+v: %s", executeResponse, err)
	}

	return nil
}

func (s *ExecutionServer) updateUsage(ctx context.Context, cmd *repb.Command, executeResponse *repb.ExecuteResponse) error {
	ut := s.env.GetUsageTracker()
	if ut == nil {
		return nil
	}
	dur, err := executionDuration(executeResponse.GetResult().GetExecutionMetadata())
	if err != nil {
		// If the task encountered an error, it's somewhat expected that the
		// execution duration will be unset, so don't return an error. For
		// example, we may have failed to pull the image, so execution could not
		// even begin. Note that an error doesn't necessarily imply a missing
		// exec duration though; we may get a DeadlineExceeded error if the task
		// times out, but still get an exec duration.
		if execErr := gstatus.ErrorProto(executeResponse.GetStatus()); execErr != nil {
			return nil
		}
		return err
	}
	counts := &tables.UsageCounts{}
	// TODO: Incorporate remote-header overrides here.
	plat, err := platform.ParseProperties(&repb.ExecutionTask{Command: cmd})
	if err != nil {
		return err
	}

	pool, err := s.env.GetSchedulerService().GetPoolInfo(ctx, plat.OS, plat.Pool, plat.WorkflowID, plat.UseSelfHostedExecutors)
	if err != nil {
		return status.InternalErrorf("failed to determine executor pool: %s", err)
	}
	// Only increment execution counts if using shared executors.
	if pool.IsSelfHosted {
		return nil
	}

	if plat.OS == platform.DarwinOperatingSystemName {
		if dur > 0 {
			counts.MacExecutionDurationUsec = dur.Microseconds()
		}
	} else if plat.OS == platform.LinuxOperatingSystemName {
		if dur > 0 {
			counts.LinuxExecutionDurationUsec = dur.Microseconds()
		}
	} else {
		return status.InternalErrorf("Unsupported platform %s", plat.OS)
	}
	usg := executeResponse.GetResult().GetExecutionMetadata().GetUsageStats()
	if usg.GetCpuNanos() > 0 {
		counts.CPUNanos = usg.GetCpuNanos()
	}
	labels, err := usageutil.Labels(ctx)
	if err != nil {
		return status.WrapError(err, "compute usage labels")
	}
	return ut.Increment(ctx, labels, counts)
}

func (s *ExecutionServer) fetchCommandForTask(ctx context.Context, actionResourceName *digest.ResourceName) (*repb.Command, error) {
	action := &repb.Action{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, actionResourceName, action); err != nil {
		return nil, err
	}
	cmdDigest := action.GetCommandDigest()
	cmdInstanceNameDigest := digest.NewResourceName(cmdDigest, actionResourceName.GetInstanceName(), rspb.CacheType_CAS, actionResourceName.GetDigestFunction())
	cmd := &repb.Command{}
	if err := cachetools.ReadProtoFromCAS(ctx, s.cache, cmdInstanceNameDigest, cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

func executionDuration(md *repb.ExecutedActionMetadata) (time.Duration, error) {
	if err := md.GetWorkerStartTimestamp().CheckValid(); err != nil {
		return 0, err
	}
	if err := md.GetWorkerCompletedTimestamp().CheckValid(); err != nil {
		return 0, err
	}
	start := md.GetWorkerStartTimestamp().AsTime()
	end := md.GetWorkerCompletedTimestamp().AsTime()
	dur := end.Sub(start)
	if dur <= 0 {
		return 0, status.InternalErrorf("Execution duration is <= 0")
	}
	return dur, nil
}

func (s *ExecutionServer) Cancel(ctx context.Context, invocationID string) error {
	ids, err := s.executionIDs(ctx, invocationID)
	if err != nil {
		return status.InternalErrorf("failed to lookup execution IDs for invocation %q: %s", invocationID, err)
	}
	numCancelled := 0
	for _, id := range ids {
		ctx := log.EnrichContext(ctx, log.ExecutionIDKey, id)
		log.CtxInfof(ctx, "Cancelling execution %q due to user request for invocation %q", id, invocationID)
		cancelled, err := s.env.GetSchedulerService().CancelTask(ctx, id)
		if cancelled {
			numCancelled++
		}
		if err != nil {
			log.Warningf("Failed to cancel task %q: %s", id, err)
		}
		if err == nil && cancelled {
			err = s.MarkExecutionFailed(ctx, id, status.CanceledError("invocation cancelled"))
			if err != nil {
				log.CtxWarningf(ctx, "Could not mark execution %q as cancelled: %s", id, err)
			}
		}
	}
	log.CtxInfof(ctx, "Cancelled %d executions for invocation %s", numCancelled, invocationID)
	return nil
}

func (s *ExecutionServer) executionIDs(ctx context.Context, invocationID string) ([]string, error) {
	dbh := s.env.GetDBHandle()
	rq := dbh.NewQuery(ctx, "execution_server_get_executions_for_invocation").Raw(
		`SELECT execution_id FROM "Executions" WHERE invocation_id = ? AND stage != ?`,
		invocationID,
		repb.ExecutionStage_COMPLETED)
	ids := make([]string, 0)
	err := db.ScanEach(rq, func(ctx context.Context, e *tables.Execution) error {
		ids = append(ids, e.ExecutionID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func statsUnset(md *repb.ExecutedActionMetadata) bool {
	return (md.GetIoStats().GetFileDownloadCount() == 0 &&
		md.GetIoStats().GetFileDownloadSizeBytes() == 0 &&
		md.GetIoStats().GetFileDownloadDurationUsec() == 0 &&
		md.GetIoStats().GetFileUploadCount() == 0 &&
		md.GetIoStats().GetFileUploadSizeBytes() == 0 &&
		md.GetIoStats().GetFileUploadDurationUsec() == 0 &&
		md.GetUsageStats().GetPeakMemoryBytes() == 0 &&
		md.GetUsageStats().GetCpuNanos() == 0 &&
		md.GetEstimatedTaskSize().GetEstimatedMemoryBytes() == 0 &&
		md.GetEstimatedTaskSize().GetEstimatedMilliCpu() == 0 &&
		md.GetEstimatedTaskSize().GetEstimatedFreeDiskBytes() == 0)

}

func decodeMetadataFromExecutionSummary(resp *repb.ExecuteResponse) (*repb.ExecutedActionMetadata, error) {
	if resp.GetMessage() == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(resp.GetMessage())
	if err != nil {
		return nil, err
	}
	summary := &espb.ExecutionSummary{}
	if err := proto.Unmarshal(data, summary); err != nil {
		return nil, err
	}
	md := summary.GetExecutedActionMetadata()
	if md == nil {
		return nil, nil
	}
	md.IoStats = summary.GetIoStats()
	md.UsageStats = summary.GetUsageStats()
	md.EstimatedTaskSize = summary.GetEstimatedTaskSize()
	return md, nil
}
