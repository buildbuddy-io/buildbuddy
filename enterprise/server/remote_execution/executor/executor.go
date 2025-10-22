package executor

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor_auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/metricsutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/docker/go-units"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	// Messages are typically sent back to the client on state changes.
	// During very long build steps (running large tests, linking large
	// objects, etc) no progress can be returned for a very long time.
	// To ensure we keep the connection alive, we start a timer and
	// just repeat the last state change message after every
	// execProgressCallbackPeriod. If this is set to 0, it is disabled.
	execProgressCallbackPeriod       = flag.Duration("executor.task_progress_publish_interval", 60*time.Second, "How often tasks should publish progress updates to the app.")
	defaultTaskTimeout               = flag.Duration("executor.default_task_timeout", 8*time.Hour, "Timeout to use for tasks that do not have a timeout set explicitly.")
	maxTaskTimeout                   = flag.Duration("executor.max_task_timeout", 24*time.Hour, "Max timeout that can be requested by a task. A value <= 0 means unlimited. An error will be returned if a task requests a timeout greater than this value.")
	slowTaskThreshold                = flag.Duration("executor.slow_task_threshold", 1*time.Hour, "Warn about tasks that take longer than this threshold.")
	defaultTerminationGrace          = flag.Duration("executor.default_termination_grace_period", 0, "Default termination grace period for all actions. (Termination grace period is the time to wait between an action timing out and forcefully shutting it down.)")
	maxTerminationGracePeriod        = flag.Duration("executor.max_termination_grace_period", 1*time.Minute, "Max termination grace period that actions can request. An error will be returned if a task requests a grace period greater than this value. (Termination grace period is the time to wait between an action timing out and forcefully shutting it down.)")
	checkActionResultBeforeExecution = flag.Bool("executor.check_action_result_before_execution", true, "If true, the executor will call GetActionResult to verify an action does not already exist before running it.")
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
	hostname   string
}

func NewExecutor(env environment.Env, id, hostID, hostname string, runnerPool interfaces.RunnerPool) (*Executor, error) {
	if err := disk.EnsureDirectoryExists(runnerPool.GetBuildRoot()); err != nil {
		return nil, err
	}
	return &Executor{
		env:        env,
		id:         id,
		hostID:     hostID,
		hostname:   hostname,
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
	ctx := context.Background()
	if executor_auth.APIKey() != "" {
		log.CtxInfo(ctx, "Using executor API key during warmup")
		ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, executor_auth.APIKey())
	}
	s.runnerPool.Warmup(ctx)
}

func timevalDuration(tv syscall.Timeval) time.Duration {
	return time.Duration(tv.Sec)*time.Second + time.Duration(tv.Usec)*time.Microsecond
}

type executionTimeouts struct {
	// TerminateAfter specifies when the task begins graceful termination.
	TerminateAfter time.Duration

	// ForceKillAfter specifies when the task gets forcefully shut down.
	ForceKillAfter time.Duration
}

func parseTimeouts(task *repb.ExecutionTask) (*executionTimeouts, error) {
	props, err := platform.ParseProperties(task)
	if err != nil {
		return nil, status.WrapError(err, "parse execution properties")
	}
	// Use the action timeout if it's set (e.g. --test_timeout for test
	// actions).
	timeout := task.GetAction().GetTimeout().AsDuration()
	// Fall back to the default timeout requested in platform properties if set
	// (e.g. --remote_exec_header=x-buildbuddy-platform.default-timeout=15m)
	if timeout <= 0 {
		timeout = props.DefaultTimeout
	}
	// Fall back to the configured default.
	if timeout <= 0 {
		timeout = *defaultTaskTimeout
	}
	// Enforce the configured max timeout by returning an error if the requested
	// timeout is too long.
	if *maxTaskTimeout > 0 && timeout > *maxTaskTimeout {
		return nil, status.InvalidArgumentErrorf("requested timeout (%s) is longer than allowed maximum (%s)", timeout, *maxTaskTimeout)
	}

	if props.TerminationGracePeriod < 0 {
		return nil, status.InvalidArgumentErrorf("requested termination grace period (%s) is invalid (negative)", props.TerminationGracePeriod)
	}
	if props.TerminationGracePeriod > *maxTerminationGracePeriod {
		return nil, status.InvalidArgumentErrorf("requested termination grace period (%s) is longer than allowed maximum (%s)", props.TerminationGracePeriod, *maxTerminationGracePeriod)
	}

	timeouts := &executionTimeouts{
		TerminateAfter: timeout,
		ForceKillAfter: timeout + props.TerminationGracePeriod,
	}

	return timeouts, nil
}

func isClientBazel(task *repb.ExecutionTask) bool {
	// TODO(bduffany): Find a more reliable way to determine this.
	return !platform.IsCICommand(task.GetCommand(), platform.GetProto(task.GetAction(), task.GetCommand()))
}

func shouldRetry(task *repb.ExecutionTask, taskError error) bool {
	if !platform.Retryable(task) {
		return false
	}

	// If the task is invalid / misconfigured, more attempts won't help.
	if !rexec.Retryable(taskError) {
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

func (s *Executor) ExecuteTaskAndStreamResults(ctx context.Context, st *repb.ScheduledTask, stream interfaces.Publisher) (retry bool, err error) {
	// From here on in we use these liberally, so check that they are setup properly
	// in the environment.
	if s.env.GetActionCacheClient() == nil || s.env.GetByteStreamClient() == nil || s.env.GetContentAddressableStorageClient() == nil {
		return false, status.FailedPreconditionError("No connection to cache backend.")
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	workerStart := s.env.GetClock().Now()
	stage := &stagedGauge{estimatedSize: st.GetSchedulingMetadata().GetTaskSize()}
	stage.Set("init")
	defer stage.End()

	ctx = interceptors.AddAuthToContext(s.env, ctx)
	ctx = bazel_request.ParseRequestMetadataOnce(ctx)

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

	task := st.GetExecutionTask()
	req := task.GetExecuteRequest()
	taskID := task.GetExecutionId()
	adInstanceDigest := digest.NewACResourceName(req.GetActionDigest(), req.GetInstanceName(), req.GetDigestFunction())
	digestFunction := adInstanceDigest.GetDigestFunction()
	task.ExecuteRequest.DigestFunction = digestFunction
	acClient := s.env.GetActionCacheClient()

	auxMetadata := &espb.ExecutionAuxiliaryMetadata{
		WorkerQueuedTimestamp: st.GetWorkerQueuedTimestamp(),
		PlatformOverrides:     task.GetPlatformOverrides(),
		ExecuteRequest:        task.GetExecuteRequest(),
		SchedulingMetadata:    st.GetSchedulingMetadata(),
		ExecutorHostname:      s.hostname,
		Experiments:           task.GetExperiments(),
	}
	actionMetrics.AuxMetadata = auxMetadata
	opStateChangeFn := operation.GetStateChangeFunc(stream, taskID, adInstanceDigest.GetDigest())
	stateChangeFn := operation.StateChangeFunc(func(stage repb.ExecutionStage_Value, execResponse *repb.ExecuteResponse) error {
		if stage == repb.ExecutionStage_COMPLETED {
			if err := appendAuxiliaryMetadata(execResponse.GetResult().GetExecutionMetadata(), auxMetadata); err != nil {
				log.CtxWarningf(ctx, "Failed to append ExecutionAuxiliaryMetadata: %s", err)
			}
		}
		return opStateChangeFn(stage, execResponse)
	})
	md := &repb.ExecutedActionMetadata{
		Worker:                   s.hostID,
		QueuedTimestamp:          task.QueuedTimestamp,
		WorkerStartTimestamp:     timestamppb.New(workerStart),
		WorkerCompletedTimestamp: timestamppb.New(s.env.GetClock().Now()),
		ExecutorId:               s.id,
		IoStats:                  &repb.IOStats{},
		EstimatedTaskSize:        st.GetSchedulingMetadata().GetTaskSize(),
		DoNotCache:               task.GetAction().GetDoNotCache(),
	}
	finishWithErrFn := func(finalErr error) (retry bool, err error) {
		if shouldRetry(task, finalErr) {
			return true, finalErr
		}
		resp := operation.ErrorResponse(finalErr)
		md.WorkerCompletedTimestamp = timestamppb.New(s.env.GetClock().Now())
		if err := appendAuxiliaryMetadata(md, auxMetadata); err != nil {
			log.CtxWarningf(ctx, "Failed to append ExecutionAuxiliaryMetadata: %s", err)
		}
		resp.Result = &repb.ActionResult{
			ExecutionMetadata: md,
		}
		if err := operation.PublishOperationDone(stream, taskID, adInstanceDigest.GetDigest(), resp); err != nil {
			return true, err
		}
		return false, finalErr
	}

	if err := validateCommand(st.GetExecutionTask().GetCommand()); err != nil {
		return finishWithErrFn(status.WrapError(err, "validate command"))
	}

	if *checkActionResultBeforeExecution && !req.GetSkipCacheLookup() {
		log.CtxDebugf(ctx, "Checking action cache for existing result.")
		stage.Set("check_cache")
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
	stage.Set("get_runner")
	r, err := s.runnerPool.Get(ctx, st)
	if err != nil {
		return finishWithErrFn(status.WrapErrorf(err, "error creating runner for command"))
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String("isolation_type", r.GetIsolationType()))
	}
	auxMetadata.RunnerMetadata = r.Metadata()
	auxMetadata.IsolationType = r.GetIsolationType()
	actionMetrics.Isolation = r.GetIsolationType()
	reuseRunner := false
	var cmdResult *interfaces.CommandResult
	defer func() {
		// Respect the DoNotRecycle bit set by the container implementation,
		// since specific implementations will have better knowledge about the
		// runner being in a potentially unrecoverable state.
		if cmdResult != nil && cmdResult.DoNotRecycle {
			reuseRunner = false
		}
		// Note: recycling is done in the foreground here in order to ensure
		// that the runner is fully cleaned up (if applicable) before its
		// resource claims are freed up by the priority_task_scheduler.
		s.runnerPool.TryRecycle(ctx, r, reuseRunner)
	}()

	log.CtxDebugf(ctx, "Preparing runner for task.")
	stage.Set("pull_image")
	// TODO: don't publish this progress update if we're not actually pulling
	// an image.
	_ = stream.SetState(repb.ExecutionProgress_PULLING_CONTAINER_IMAGE)
	if err := r.PrepareForTask(ctx); err != nil {
		return finishWithErrFn(status.WrapError(err, "prepare runner filesystem"))
	}

	md.InputFetchStartTimestamp = timestamppb.New(s.env.GetClock().Now())

	log.CtxDebugf(ctx, "Downloading inputs.")
	stage.Set("input_fetch")
	_ = stream.SetState(repb.ExecutionProgress_DOWNLOADING_INPUTS)
	if err := r.DownloadInputs(ctx); err != nil {
		// If we failed to download inputs, and preserve-workspace is not
		// enabled, then it should be safe to attempt recycling:
		//
		// - If the download failed due to filesystem issues, then we'd expect
		//   the workspace cleanup to either fail (which prevents recycling),
		//   or succeed, getting the filesystem back in a usable state.
		//
		// - If the download failed due to a networking error, then we'll
		//   just cleanup the workspace and it'll be as though nothing
		//   happened.
		//
		// - We really don't expect other types of errors to happen, since the
		//   only IO going on here should be filesystem operations and CAS
		//   downloads.
		if !platform.IsTrue(platform.FindEffectiveValue(task, platform.PreserveWorkspacePropertyName)) {
			reuseRunner = true
		}
		// Coerce DeadlineExceeded error code to Unavailable. We haven't applied
		// the action timeout yet, so any DeadlineExceeded errors at this point
		// would be internal timeouts.
		if status.IsDeadlineExceededError(err) {
			err = status.UnavailableError(status.Message(err))
		}

		// Bazel will attempt to reupload inputs if it sees a
		// FailedPreconditionError with a particular format:
		// https://github.com/buildbuddy-io/buildbuddy/blob/41bd3c440b2c79cb219c3523449db1c7f9d4ce1d/proto/remote_execution.proto#L108-L117.
		//
		// Bazel does *not* attempt to reupload if it sees a NotFound error. So
		// here, we coerce NotFound to this special FailedPrecondition error so
		// that bazel will reupload inputs in the rare cases where inputs are
		// missing.
		//
		// At the time of writing (bazel 8.x), bazel does not actually care
		// about the "subject" part of the spec, and will instead just reupload
		// all inputs. So for now, we just return an empty digest in the subject
		// field.
		if status.IsNotFoundError(err) {
			err = digest.MissingDigestError(&repb.Digest{Hash: digest.EmptySha256})
		}

		return finishWithErrFn(status.WrapError(err, "download inputs"))
	}

	md.InputFetchCompletedTimestamp = timestamppb.New(s.env.GetClock().Now())
	md.ExecutionStartTimestamp = timestamppb.New(s.env.GetClock().Now())
	execTimeouts, err := parseTimeouts(task)
	if err != nil {
		// Don't fail recycling if the user requested invalid timeouts.
		reuseRunner = true
		// These errors are failure-specific. Pass through unchanged.
		return finishWithErrFn(status.WrapError(err, "parse timeouts"))
	}
	auxMetadata.Timeout = durationpb.New(execTimeouts.TerminateAfter)

	now := s.env.GetClock().Now()
	terminateAt := now.Add(execTimeouts.TerminateAfter)
	forceShutdownAt := now.Add(execTimeouts.ForceKillAfter)

	// Canceling the context force-stops the task.
	ctx, cancel := context.WithDeadline(ctx, forceShutdownAt)
	defer cancel()

	// If graceful termination is requested, start a goroutine to request
	// termination after the termination timeout.
	gracefullyTerminated := make(chan struct{})
	if execTimeouts.TerminateAfter < execTimeouts.ForceKillAfter {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(terminateAt)):
				close(gracefullyTerminated)
				log.CtxInfof(ctx, "Sending graceful termination signal")
				if err := r.GracefulTerminate(ctx); err != nil {
					log.CtxWarningf(ctx, "Failed to send graceful termination signal: %s", err)
				}
			}
		}()
	}

	log.CtxDebugf(ctx, "Executing task.")
	stage.Set("execution")
	_ = stream.SetState(repb.ExecutionProgress_EXECUTING_COMMAND)
	cmdResultChan := make(chan *interfaces.CommandResult, 1)
	go func() {
		cmdResultChan <- r.Run(ctx, md.IoStats)
	}()

	// Run a timer that periodically sends update messages back
	// to our caller while execution is ongoing.
	updateTicker := time.NewTicker(*execProgressCallbackPeriod)
	defer updateTicker.Stop()
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

	// Include VFS stats, if VFS is enabled.
	if vs := cmdResult.VfsStats; vs != nil {
		md.IoStats.FileDownloadCount += vs.FileDownloadCount
		md.IoStats.FileDownloadSizeBytes += vs.FileDownloadSizeBytes
		md.IoStats.FileDownloadDurationUsec += vs.FileDownloadDurationUsec

		metrics.VFSCASFilesCount.Add(float64(vs.CasFilesCount))
		metrics.VFSCASFilesAccessedCount.Add(float64(vs.CasFilesAccessedCount))
		metrics.VFSCASFilesSizeBytes.Add(float64(vs.CasFilesSizeBytes))
		metrics.VFSCASFilesAccessedBytes.Add(float64(vs.CasFilesAccessedBytes))
		log.CtxInfof(ctx, "VFS CAS inputs used count %d/%d (%.2f) used size %s/%s (%.2f)",
			vs.CasFilesAccessedCount, vs.CasFilesCount, float64(vs.CasFilesAccessedCount)/float64(vs.CasFilesCount),
			units.HumanSize(float64(vs.CasFilesAccessedBytes)), units.HumanSize(float64(vs.CasFilesSizeBytes)), float64(vs.CasFilesAccessedBytes)/float64(vs.CasFilesSizeBytes))
	}

	if cmdResult.ExitCode != 0 {
		log.CtxDebugf(ctx, "%q finished with non-zero exit code (%d). Err: %s, Stdout: %s, Stderr: %s", taskID, cmdResult.ExitCode, cmdResult.Error, cmdResult.Stdout, cmdResult.Stderr)
	}
	// Exit codes < 0 mean that the command either never started or was killed.
	// Make sure we return an error in this case.
	if cmdResult.ExitCode < 0 {
		cmdResult.Error = incompleteExecutionError(ctx, cmdResult.ExitCode, cmdResult.Error)
	}
	// If the command was terminated gracefully, make sure we return a
	// DeadlineExceeded error.
	select {
	case <-gracefullyTerminated:
		cmdResult.ExitCode = commandutil.NoExitCode
		cmdResult.Error = status.DeadlineExceededError("deadline exceeded")
	default:
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
		if err := appendAuxiliaryMetadata(md, cmdResult.VMMetadata); err != nil {
			return finishWithErrFn(status.InternalErrorf("append auxiliary metadata: %s", err))
		}
	}
	md.ExecutionCompletedTimestamp = timestamppb.New(s.env.GetClock().Now())
	md.OutputUploadStartTimestamp = timestamppb.New(s.env.GetClock().Now())

	actionResult := &repb.ActionResult{}
	actionResult.ExitCode = int32(cmdResult.ExitCode)
	actionMetrics.Result = actionResult
	executeResponse := operation.ExecuteResponseWithResult(actionResult, cmdResult.Error)

	log.CtxDebugf(ctx, "Uploading outputs.")
	stage.Set("output_upload")
	_ = stream.SetState(repb.ExecutionProgress_UPLOADING_OUTPUTS)
	if err := r.UploadOutputs(ctx, md.IoStats, executeResponse, cmdResult); err != nil {
		// If we failed to upload outputs, the runner may still be recyclable -
		// see comments near DownloadInputs.
		if cmdResult.Error == nil && !platform.IsTrue(platform.FindEffectiveValue(task, platform.PreserveWorkspacePropertyName)) {
			reuseRunner = true
		}
		return finishWithErrFn(status.UnavailableErrorf("upload outputs: %s", err.Error()))
	}
	md.OutputUploadCompletedTimestamp = timestamppb.New(s.env.GetClock().Now())
	md.WorkerCompletedTimestamp = timestamppb.New(s.env.GetClock().Now())
	actionResult.ExecutionMetadata = md

	// If there's an error that we know the client won't retry, return an error
	// so that the scheduler can retry it.
	if cmdResult.Error != nil && shouldRetry(task, cmdResult.Error) {
		return finishWithErrFn(status.WrapError(cmdResult.Error, "command execution failed"))
	}
	// Otherwise, send the error back to the client via the ExecuteResponse
	// status.
	if err := stateChangeFn(repb.ExecutionStage_COMPLETED, executeResponse); err != nil {
		log.CtxErrorf(ctx, "Failed to publish ExecuteResponse: %s", err)
		if cmdResult.Error == nil {
			reuseRunner = true
		}
		return finishWithErrFn(status.WrapError(err, "publish execute response"))
	}
	if cmdResult.Error == nil {
		log.CtxDebugf(ctx, "Task finished cleanly.")
		reuseRunner = true
	}
	return false, nil
}

func appendAuxiliaryMetadata(md *repb.ExecutedActionMetadata, message proto.Message) error {
	a, err := anypb.New(message)
	if err != nil {
		return status.InternalErrorf("marshal message type %T to Any: %s", message, err)
	}
	md.AuxiliaryMetadata = append(md.AuxiliaryMetadata, a)
	return nil
}

func validateCommand(cmd *repb.Command) error {
	for _, pathList := range [][]string{
		cmd.GetOutputFiles(),
		cmd.GetOutputDirectories(),
		cmd.GetOutputPaths(),
	} {
		for _, path := range pathList {
			parts := strings.Split(path, string(os.PathSeparator))
			if slices.Contains(parts, "..") {
				return status.InvalidArgumentError("output paths with '..' path components are not allowed")
			}
		}
	}
	return nil
}

type ActionMetrics struct {
	Isolation    string
	CachedResult bool
	// Error is any abnormal error that occurred while executing the action.
	Error error
	// Result is the action execution result.
	Result *repb.ActionResult
	// AuxMetadata is the execution auxiliary metadata.
	AuxMetadata *espb.ExecutionAuxiliaryMetadata
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
		observeStageDuration(ctx, groupID, "queued", md.GetQueuedTimestamp(), md.GetWorkerStartTimestamp())
		observeStageDuration(ctx, groupID, "pull_image", md.GetWorkerStartTimestamp(), md.GetInputFetchStartTimestamp())
		observeStageDuration(ctx, groupID, "input_fetch", md.GetInputFetchStartTimestamp(), md.GetInputFetchCompletedTimestamp())
		observeStageDuration(ctx, groupID, "execution", md.GetExecutionStartTimestamp(), md.GetExecutionCompletedTimestamp())
		observeStageDuration(ctx, groupID, "output_upload", md.GetOutputUploadStartTimestamp(), md.GetOutputUploadCompletedTimestamp())
		observeStageDuration(ctx, groupID, "worker", md.GetWorkerStartTimestamp(), md.GetWorkerCompletedTimestamp())
	}
	if md != nil && m.AuxMetadata != nil {
		observeStageDuration(ctx, groupID, "worker_queued", m.AuxMetadata.GetWorkerQueuedTimestamp(), md.GetWorkerStartTimestamp())
	}
	// If the isolation type supports it, report PSI metrics.
	if md != nil && (m.Isolation == string(platform.PodmanContainerType) || m.Isolation == string(platform.OCIContainerType)) {
		execDuration := md.GetExecutionCompletedTimestamp().AsTime().Sub(md.GetExecutionStartTimestamp().AsTime())
		observePSI("cpu", md.GetUsageStats().GetCpuPressure(), execDuration)
		observePSI("memory", md.GetUsageStats().GetMemoryPressure(), execDuration)
		observePSI("io", md.GetUsageStats().GetIoPressure(), execDuration)
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
			return status.DeadlineExceededError("deadline exceeded")
		}
		if ctxErr == context.Canceled {
			return status.AbortedError("context canceled")
		}
		alert.UnexpectedEvent("unknown_context_error", "Unknown context error: %s", ctxErr)
		return status.UnknownError(ctxErr.Error())
	}
	return err
}

func observeStageDuration(ctx context.Context, groupID string, stage string, start *timestamppb.Timestamp, end *timestamppb.Timestamp) {
	startTime := start.AsTime()
	if startTime.IsZero() {
		return
	}
	endTime := end.AsTime()
	if endTime.IsZero() {
		return
	}
	duration := endTime.Sub(startTime)
	if duration > 20*time.Hour {
		log.CtxInfof(ctx, "Stage %v took longer than 20h. Duration = %v; Start = %v; End = %v", stage, duration, start, end)
	}
	metrics.RemoteExecutionExecutedActionMetadataDurationsUsec.With(prometheus.Labels{
		metrics.GroupID:                  metricsutil.FilteredGroupIDLabel(groupID),
		metrics.ExecutedActionStageLabel: stage,
	}).Observe(float64(duration.Microseconds()))
}

func observePSI(resourceLabel string, psi *repb.PSI, execDuration time.Duration) {
	fullStallFraction := float64(0)
	if execDuration > 0 {
		fullStallFraction = float64(psi.GetFull().GetTotal()) / float64(execDuration.Microseconds())
	}
	metrics.RemoteExecutionTaskPressureStallDurationFraction.With(prometheus.Labels{
		metrics.PSIResourceLabel:  resourceLabel,
		metrics.PSIStallTypeLabel: "full",
	}).Observe(fullStallFraction)
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
