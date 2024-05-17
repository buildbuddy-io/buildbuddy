package executor

import (
	"context"
	"flag"
	"fmt"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/metricsutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	// Messages are typically sent back to the client on state changes.
	// During very long build steps (running large tests, linking large
	// objects, etc) no progress can be returned for a very long time.
	// To ensure we keep the connection alive, we start a timer and
	// just repeat the last state change message after every
	// execProgressCallbackPeriod. If this is set to 0, it is disabled.
	execProgressCallbackPeriod = flag.Duration("executor.task_progress_publish_interval", 60*time.Second, "How often tasks should publish progress updates to the app.")
	defaultTaskTimeout         = flag.Duration("executor.default_task_timeout", 8*time.Hour, "Timeout to use for tasks that do not have a timeout set explicitly.")
	maxTaskTimeout             = flag.Duration("executor.max_task_timeout", 24*time.Hour, "Max timeout that can be requested by a task. A value <= 0 means unlimited. An error will be returned if a task requests a timeout greater than this value.")
	slowTaskThreshold          = flag.Duration("executor.slow_task_threshold", 1*time.Hour, "Warn about tasks that take longer than this threshold.")
)

const (
	// Allowed deadline extension for uploading action outputs.
	// The deadline of the original request may be extended by up to this amount
	// in order to give enough time to upload action outputs.
	uploadDeadlineExtension = time.Minute * 1
)

type Executor struct {
	env        environment.Env
	runnerPool interfaces.RunnerPool
	id         string
	hostID     string
}

func NewExecutor(env environment.Env, id, hostID string, runnerPool interfaces.RunnerPool) (*Executor, error) {
	if err := disk.EnsureDirectoryExists(runnerPool.GetBuildRoot()); err != nil {
		return nil, err
	}
	return &Executor{
		env:        env,
		id:         id,
		hostID:     hostID,
		runnerPool: runnerPool,
	}, nil
}

func (s *Executor) ID() string {
	return s.id
}

func (s *Executor) HostID() string {
	return s.hostID
}

func (s *Executor) Warmup() {
	s.runnerPool.Warmup(context.Background())
}

func timevalDuration(tv syscall.Timeval) time.Duration {
	return time.Duration(tv.Sec)*time.Second + time.Duration(tv.Usec)*time.Microsecond
}

func parseTimeout(timeout *durationpb.Duration) (time.Duration, error) {
	if timeout == nil {
		return *defaultTaskTimeout, nil
	}
	requestDuration := timeout.AsDuration()
	if *maxTaskTimeout > 0 && requestDuration > *maxTaskTimeout {
		return 0, status.InvalidArgumentErrorf("requested timeout (%s) is longer than allowed maximum (%s)", requestDuration, *maxTaskTimeout)
	}
	return requestDuration, nil
}

// isTaskMisconfigured returns whether a task failed to execute because of a
// configuration error that will prevent the action from executing properly,
// even if retried.
func isTaskMisconfigured(err error) bool {
	return status.IsInvalidArgumentError(err) ||
		status.IsFailedPreconditionError(err) ||
		status.IsUnauthenticatedError(err)
}

func isClientBazel(task *repb.ExecutionTask) bool {
	// TODO(bduffany): Find a more reliable way to determine this.
	return !platform.IsCICommand(task.GetCommand())
}

func shouldRetry(task *repb.ExecutionTask, taskError error) bool {
	// If the task is invalid / misconfigured, more attempts won't help.
	if isTaskMisconfigured(taskError) {
		return false
	}
	// If the task timed out, respect the timeout and don't keep retrying.
	if status.IsDeadlineExceededError(taskError) {
		return false
	}
	// Bazel has retry functionality built in, so if we know the client is Bazel,
	// let Bazel retry it instead of us doing it.
	return !isClientBazel(task)
}

func (s *Executor) ExecuteTaskAndStreamResults(ctx context.Context, st *repb.ScheduledTask, stream *operation.Publisher) (retry bool, err error) {
	// From here on in we use these liberally, so check that they are setup properly
	// in the environment.
	if s.env.GetActionCacheClient() == nil || s.env.GetByteStreamClient() == nil || s.env.GetContentAddressableStorageClient() == nil {
		return false, status.FailedPreconditionError("No connection to cache backend.")
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	metrics.RemoteExecutionTasksStartedCount.Inc()

	actionMetrics := &ActionMetrics{}
	defer func() {
		actionMetrics.Error = err
		actionMetrics.Report(ctx)
	}()

	if *slowTaskThreshold > 0 {
		stop := canary.StartWithLateFn(*slowTaskThreshold, func(duration time.Duration) {
			log.CtxInfof(ctx, "Task still running after %s", duration)
		}, func(time.Duration) {})
		defer stop()
	}

	task := st.ExecutionTask
	req := task.GetExecuteRequest()
	taskID := task.GetExecutionId()
	adInstanceDigest := digest.NewResourceName(req.GetActionDigest(), req.GetInstanceName(), rspb.CacheType_AC, req.GetDigestFunction())
	digestFunction := adInstanceDigest.GetDigestFunction()
	task.ExecuteRequest.DigestFunction = digestFunction
	acClient := s.env.GetActionCacheClient()

	stateChangeFn := operation.GetStateChangeFunc(stream, taskID, adInstanceDigest)
	finishWithErrFn := func(finalErr error) (retry bool, err error) {
		if shouldRetry(task, finalErr) {
			return true, finalErr
		}
		if err := operation.PublishOperationDone(stream, taskID, adInstanceDigest, finalErr); err != nil {
			return true, err
		}
		return false, finalErr
	}

	md := &repb.ExecutedActionMetadata{
		Worker:               s.hostID,
		QueuedTimestamp:      task.QueuedTimestamp,
		WorkerStartTimestamp: timestamppb.Now(),
		ExecutorId:           s.id,
		IoStats:              &repb.IOStats{},
		EstimatedTaskSize:    st.GetSchedulingMetadata().GetTaskSize(),
		DoNotCache:           task.GetAction().GetDoNotCache(),
	}

	stage := &stagedGauge{estimatedSize: md.EstimatedTaskSize}
	defer stage.End()

	if !req.GetSkipCacheLookup() {
		log.CtxDebugf(ctx, "Checking action cache for existing result.")
		if err := stateChangeFn(repb.ExecutionStage_CACHE_CHECK, operation.InProgressExecuteResponse()); err != nil {
			return true, err
		}
		actionResult, err := cachetools.GetActionResult(ctx, acClient, adInstanceDigest)
		if err == nil {
			if err := stateChangeFn(repb.ExecutionStage_COMPLETED, operation.ExecuteResponseWithCachedResult(actionResult)); err != nil {
				return true, err
			}
			log.CtxDebugf(ctx, "Found existing result in action cache, all done.")
			actionMetrics.Result = actionResult
			actionMetrics.CachedResult = true
			return false, nil
		}
	}

	log.CtxDebugf(ctx, "Getting a runner for task.")
	r, err := s.runnerPool.Get(ctx, st)
	if err != nil {
		return finishWithErrFn(status.WrapErrorf(err, "error creating runner for command"))
	}
	actionMetrics.Isolation = r.GetIsolationType()
	finishedCleanly := false
	defer func() {
		// Note: recycling is done in the foreground here in order to ensure
		// that the runner is fully cleaned up (if applicable) before its
		// resource claims are freed up by the priority_task_scheduler.
		s.runnerPool.TryRecycle(ctx, r, finishedCleanly)
	}()

	log.CtxDebugf(ctx, "Preparing runner for task.")
	stage.Set("pull_image")
	// TODO: don't publish this progress update if we're not actually pulling
	// an image.
	_ = stream.SetState(repb.ExecutionProgress_PULLING_CONTAINER_IMAGE)
	if err := r.PrepareForTask(ctx); err != nil {
		return finishWithErrFn(err)
	}

	md.InputFetchStartTimestamp = timestamppb.Now()

	log.CtxDebugf(ctx, "Downloading inputs.")
	stage.Set("input_fetch")
	_ = stream.SetState(repb.ExecutionProgress_DOWNLOADING_INPUTS)
	if err := r.DownloadInputs(ctx, md.IoStats); err != nil {
		return finishWithErrFn(err)
	}

	md.InputFetchCompletedTimestamp = timestamppb.Now()
	md.ExecutionStartTimestamp = timestamppb.Now()
	execTimeout, err := parseTimeout(task.GetAction().Timeout)
	if err != nil {
		// These errors are failure-specific. Pass through unchanged.
		return finishWithErrFn(err)
	}
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	log.CtxDebugf(ctx, "Executing task.")
	stage.Set("execution")
	_ = stream.SetState(repb.ExecutionProgress_EXECUTING_COMMAND)
	cmdResultChan := make(chan *interfaces.CommandResult, 1)
	go func() {
		cmdResultChan <- r.Run(ctx)
	}()

	// Run a timer that periodically sends update messages back
	// to our caller while execution is ongoing.
	updateTicker := time.NewTicker(*execProgressCallbackPeriod)
	defer updateTicker.Stop()
	var cmdResult *interfaces.CommandResult
	for cmdResult == nil {
		select {
		case cmdResult = <-cmdResultChan:
			updateTicker.Stop()
		case <-updateTicker.C:
			if err := stream.Ping(); err != nil {
				return true, status.UnavailableErrorf("could not publish periodic execution update for %q: %s", taskID, err)
			}
		}
	}

	if cmdResult.ExitCode != 0 {
		log.CtxDebugf(ctx, "%q finished with non-zero exit code (%d). Err: %s, Stdout: %s, Stderr: %s", taskID, cmdResult.ExitCode, cmdResult.Error, cmdResult.Stdout, cmdResult.Stderr)
	}
	// Exit codes < 0 mean that the command either never started or was killed.
	// Make sure we return an error in this case.
	if cmdResult.ExitCode < 0 {
		cmdResult.Error = incompleteExecutionError(ctx, cmdResult.ExitCode, cmdResult.Error)
	}
	if cmdResult.Error != nil {
		log.CtxWarningf(ctx, "Command execution returned error: %s", cmdResult.Error)
	}

	// Note: we continue to upload outputs, stderr, etc. below even if
	// cmdResult.Error is present, because these outputs are helpful
	// for debugging.

	ctx, cancel = background.ExtendContextForFinalization(ctx, uploadDeadlineExtension)
	defer cancel()

	md.UsageStats = cmdResult.UsageStats
	if cmdResult.VMMetadata != nil {
		vmMetadata, err := anypb.New(cmdResult.VMMetadata)
		if err == nil {
			md.AuxiliaryMetadata = []*anypb.Any{vmMetadata}
		} else {
			log.CtxErrorf(ctx, "Could not encode VMMetadata to `any` type: %s", err)
		}
	}
	md.ExecutionCompletedTimestamp = timestamppb.Now()
	md.OutputUploadStartTimestamp = timestamppb.Now()

	actionResult := &repb.ActionResult{}
	actionResult.ExitCode = int32(cmdResult.ExitCode)
	actionMetrics.Result = actionResult
	executeResponse := operation.ExecuteResponseWithResult(actionResult, cmdResult.Error)

	log.CtxDebugf(ctx, "Uploading outputs.")
	stage.Set("output_upload")
	_ = stream.SetState(repb.ExecutionProgress_UPLOADING_OUTPUTS)
	if err := r.UploadOutputs(ctx, md.IoStats, executeResponse, cmdResult); err != nil {
		return finishWithErrFn(status.UnavailableErrorf("Error uploading outputs: %s", err.Error()))
	}
	md.OutputUploadCompletedTimestamp = timestamppb.Now()
	md.WorkerCompletedTimestamp = timestamppb.Now()
	actionResult.ExecutionMetadata = md

	// If the action failed or do_not_cache is set, upload information about the error via a failed
	// ActionResult under an invocation-specific digest, which will not ever be seen by bazel but
	// may be viewed via the Buildbuddy UI.
	if task.GetAction().GetDoNotCache() || cmdResult.Error != nil || cmdResult.ExitCode != 0 {
		resultDigest, err := digest.AddInvocationIDToDigest(req.GetActionDigest(), digestFunction, task.GetInvocationId())
		if err != nil {
			return finishWithErrFn(status.UnavailableErrorf("Error uploading action result: %s", err.Error()))
		}
		adInstanceDigest = digest.NewResourceName(resultDigest, req.GetInstanceName(), rspb.CacheType_AC, digestFunction)
	}
	if err := cachetools.UploadActionResult(ctx, acClient, adInstanceDigest, actionResult); err != nil {
		return finishWithErrFn(status.UnavailableErrorf("Error uploading action result: %s", err.Error()))
	}

	// If there's an error that we know the client won't retry, return an error
	// so that the scheduler can retry it.
	if cmdResult.Error != nil && shouldRetry(task, cmdResult.Error) {
		return finishWithErrFn(cmdResult.Error)
	}
	// Otherwise, send the error back to the client via the ExecuteResponse
	// status.
	if err := stateChangeFn(repb.ExecutionStage_COMPLETED, executeResponse); err != nil {
		log.CtxErrorf(ctx, "Failed to publish ExecuteResponse: %s", err)
		return finishWithErrFn(err)
	}
	if cmdResult.Error == nil && !cmdResult.DoNotRecycle {
		log.CtxDebugf(ctx, "Task finished cleanly.")
		finishedCleanly = true
	}
	return false, nil
}

type ActionMetrics struct {
	Isolation    string
	CachedResult bool
	// Error is any abnormal error that occurred while executing the action.
	Error error
	// Result is the action execution result.
	Result *repb.ActionResult
}

func (m *ActionMetrics) Report(ctx context.Context) {
	if m.CachedResult {
		// Don't report metrics for cached actions for now.
		return
	}
	groupID := ""
	if u, err := auth.UserFromTrustedJWT(ctx); err == nil {
		groupID = u.GetGroupID()
	}
	exitCode := commandutil.NoExitCode
	if m.Result != nil {
		exitCode = int(m.Result.ExitCode)
	}
	metrics.RemoteExecutionCount.With(prometheus.Labels{
		metrics.ExitCodeLabel:            fmt.Sprintf("%d", exitCode),
		metrics.StatusHumanReadableLabel: status.MetricsLabel(m.Error),
		metrics.IsolationTypeLabel:       m.Isolation,
	}).Inc()
	md := m.Result.GetExecutionMetadata()
	if md != nil {
		observeStageDuration(groupID, "queued", md.GetQueuedTimestamp(), md.GetWorkerStartTimestamp())
		observeStageDuration(groupID, "pull_image", md.GetWorkerStartTimestamp(), md.GetInputFetchStartTimestamp())
		observeStageDuration(groupID, "input_fetch", md.GetInputFetchStartTimestamp(), md.GetInputFetchCompletedTimestamp())
		observeStageDuration(groupID, "execution", md.GetExecutionStartTimestamp(), md.GetExecutionCompletedTimestamp())
		observeStageDuration(groupID, "output_upload", md.GetOutputUploadStartTimestamp(), md.GetOutputUploadCompletedTimestamp())
		observeStageDuration(groupID, "worker", md.GetWorkerStartTimestamp(), md.GetWorkerCompletedTimestamp())
	}
	if md.GetIoStats() != nil {
		metrics.FileDownloadCount.Observe(float64(md.IoStats.FileDownloadCount))
		metrics.FileDownloadSizeBytes.Observe(float64(md.IoStats.FileDownloadSizeBytes))
		metrics.FileDownloadDurationUsec.Observe(float64(md.IoStats.FileDownloadDurationUsec))
		metrics.FileUploadCount.Observe(float64(md.IoStats.FileUploadCount))
		metrics.FileUploadSizeBytes.Observe(float64(md.IoStats.FileUploadSizeBytes))
		metrics.FileUploadDurationUsec.Observe(float64(md.IoStats.FileUploadDurationUsec))
	}
}

func incompleteExecutionError(ctx context.Context, exitCode int, err error) error {
	if err == nil {
		alert.UnexpectedEvent("incomplete_command_with_nil_error")
		return status.UnknownErrorf("Command did not complete, for unknown reasons (internal status %d)", exitCode)
	}
	// If the context timed out or was cancelled, ignore any error returned from
	// the command since it's most likely caused by the context error, and we
	// don't want to require container implementations to have to handle these
	// cases explicitly.
	if ctxErr := ctx.Err(); ctxErr != nil {
		log.Infof("Ignoring command error likely caused by %s: %s", ctxErr, err)
		if ctxErr == context.DeadlineExceeded {
			return status.DeadlineExceededError("deadline exceeeded")
		}
		if ctxErr == context.Canceled {
			return status.AbortedError("context canceled")
		}
		alert.UnexpectedEvent("unknown_context_error", "Unknown context error: %s", ctxErr)
		return status.UnknownError(ctxErr.Error())
	}
	return err
}

func observeStageDuration(groupID string, stage string, start *timestamppb.Timestamp, end *timestamppb.Timestamp) {
	startTime := start.AsTime()
	if startTime.IsZero() {
		return
	}
	endTime := end.AsTime()
	if endTime.IsZero() {
		return
	}
	duration := endTime.Sub(startTime)
	metrics.RemoteExecutionExecutedActionMetadataDurationsUsec.With(prometheus.Labels{
		metrics.GroupID:                  metricsutil.FilteredGroupIDLabel(groupID),
		metrics.ExecutedActionStageLabel: stage,
	}).Observe(float64(duration / time.Microsecond))
}

// stagedGauge manages the "tasks executing by stage" gauge for a single task,
// ensuring that the gauge counts are correctly updated on each stage
// transition.
type stagedGauge struct {
	stage         string
	estimatedSize *scpb.TaskSize
}

func (g *stagedGauge) Set(stage string) {
	if prev := g.stage; prev != "" {
		metrics.RemoteExecutionTasksExecuting.
			With(prometheus.Labels{metrics.ExecutedActionStageLabel: prev}).
			Dec()
		metrics.RemoteExecutionAssignedOrQueuedEstimatedMilliCPU.
			Sub(float64(g.estimatedSize.EstimatedMilliCpu))
		metrics.RemoteExecutionAssignedOrQueuedEstimatedRAMBytes.
			Sub(float64(g.estimatedSize.EstimatedMemoryBytes))
	}
	if stage != "" {
		metrics.RemoteExecutionTasksExecuting.
			With(prometheus.Labels{metrics.ExecutedActionStageLabel: stage}).
			Inc()
		metrics.RemoteExecutionAssignedOrQueuedEstimatedMilliCPU.
			Add(float64(g.estimatedSize.EstimatedMilliCpu))
		metrics.RemoteExecutionAssignedOrQueuedEstimatedRAMBytes.
			Add(float64(g.estimatedSize.EstimatedMemoryBytes))
	}
	g.stage = stage
}

func (g *stagedGauge) End() {
	g.Set("")
}
