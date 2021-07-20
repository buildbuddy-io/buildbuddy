package executor

import (
	"context"
	"encoding/base64"
	"fmt"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	durationpb "github.com/golang/protobuf/ptypes/duration"
	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	gstatus "google.golang.org/grpc/status"
)

const (
	// Messages are typically sent back to the client on state changes.
	// During very long build steps (running large tests, linking large
	// objects, etc) no progress can be returned for a very long time.
	// To ensure we keep the connection alive, we start a timer and
	// just repeat the last state change message after every
	// execProgressCallbackPeriod. If this is set to 0, it is disabled.
	execProgressCallbackPeriod = 60 * time.Second

	// 7 days? Forever. This is the duration returned when no max duration
	// has been set in the config and no timeout was set in the client
	// request. It's basically the same as "no-timeout".
	infiniteDuration = time.Hour * 24 * 7
	// Allowed deadline extension for uploading action outputs.
	// The deadline of the original request may be extended by up to this amount
	// in order to give enough time to upload action outputs.
	uploadDeadlineExtension = time.Minute * 1
)

type Executor struct {
	env        environment.Env
	runnerPool *runner.Pool
	id         string
	name       string
}

type Options struct {
	// TESTING ONLY: allows the name of the executor to be manually specified instead of deriving it
	// from host information.
	NameOverride string
}

func NewExecutor(env environment.Env, id string, options *Options) (*Executor, error) {
	executorConfig := env.GetConfigurator().GetExecutorConfig()
	if executorConfig == nil {
		return nil, status.FailedPreconditionError("No executor config found")
	}
	if err := disk.EnsureDirectoryExists(executorConfig.GetRootDirectory()); err != nil {
		return nil, err
	}
	name := options.NameOverride
	if name == "" {
		name = base64.StdEncoding.EncodeToString(uuid.NodeID())
	}
	runnerPool, err := runner.NewPool(env)
	if err != nil {
		return nil, err
	}

	s := &Executor{
		env:        env,
		id:         id,
		name:       name,
		runnerPool: runnerPool,
	}
	if hc := env.GetHealthChecker(); hc != nil {
		hc.RegisterShutdownFunction(runnerPool.Shutdown)
	} else {
		return nil, status.FailedPreconditionError("Missing health checker in env")
	}
	go s.runnerPool.WarmupDefaultImage()
	return s, nil
}

func (s *Executor) Name() string {
	return s.name
}

func diffTimestamps(startPb, endPb *tspb.Timestamp) time.Duration {
	start, _ := ptypes.Timestamp(startPb)
	end, _ := ptypes.Timestamp(endPb)
	return end.Sub(start)
}

func diffTimestampsToProto(startPb, endPb *tspb.Timestamp) *durationpb.Duration {
	return ptypes.DurationProto(diffTimestamps(startPb, endPb))
}

func logActionResult(taskID string, md *repb.ExecutedActionMetadata) {
	workTime := diffTimestamps(md.GetWorkerStartTimestamp(), md.GetWorkerCompletedTimestamp())
	fetchTime := diffTimestamps(md.GetInputFetchStartTimestamp(), md.GetInputFetchCompletedTimestamp())
	execTime := diffTimestamps(md.GetExecutionStartTimestamp(), md.GetExecutionCompletedTimestamp())
	uploadTime := diffTimestamps(md.GetOutputUploadStartTimestamp(), md.GetOutputUploadCompletedTimestamp())
	log.Debugf("%q completed action %q [work: %02dms, fetch: %02dms, exec: %02dms, upload: %02dms]",
		md.GetWorker(), taskID, workTime.Milliseconds(), fetchTime.Milliseconds(),
		execTime.Milliseconds(), uploadTime.Milliseconds())
}

func timevalDuration(tv syscall.Timeval) time.Duration {
	return time.Duration(tv.Sec)*time.Second + time.Duration(tv.Usec)*time.Microsecond
}

func parseTimeout(timeout *durationpb.Duration, maxDuration time.Duration) (time.Duration, error) {
	if timeout == nil {
		if maxDuration == 0 {
			return infiniteDuration, nil
		}
		return maxDuration, nil
	}
	requestDuration, err := ptypes.Duration(timeout)
	if err != nil {
		return 0, status.InvalidArgumentErrorf("Unparsable timeout: %s", err.Error())
	}
	if maxDuration != 0 && requestDuration > maxDuration {
		return 0, status.InvalidArgumentErrorf("Specified timeout (%s) longer than allowed maximum (%s).", requestDuration, maxDuration)
	}
	return requestDuration, nil
}

func (s *Executor) ExecuteTaskAndStreamResults(ctx context.Context, task *repb.ExecutionTask, stream operation.StreamLike) error {
	// From here on in we use these liberally, so check that they are setup properly
	// in the environment.
	if s.env.GetActionCacheClient() == nil || s.env.GetByteStreamClient() == nil || s.env.GetContentAddressableStorageClient() == nil {
		return status.FailedPreconditionError("No connection to cache backend.")
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	req := task.GetExecuteRequest()
	taskID := task.GetExecutionId()
	adInstanceDigest := digest.NewInstanceNameDigest(req.GetActionDigest(), req.GetInstanceName())

	acClient := s.env.GetActionCacheClient()

	stateChangeFn := operation.GetStateChangeFunc(stream, taskID, adInstanceDigest)
	finishWithErrFn := operation.GetFinishWithErrFunc(stream, taskID, adInstanceDigest)

	md := &repb.ExecutedActionMetadata{
		Worker:               s.name,
		QueuedTimestamp:      task.QueuedTimestamp,
		WorkerStartTimestamp: ptypes.TimestampNow(),
		ExecutorId:           s.id,
	}

	if !req.GetSkipCacheLookup() {
		if err := stateChangeFn(repb.ExecutionStage_CACHE_CHECK, operation.InProgressExecuteResponse()); err != nil {
			return err // CHECK (these errors should not happen).
		}
		actionResult, err := cachetools.GetActionResult(ctx, acClient, adInstanceDigest)
		if err == nil {
			if err := stateChangeFn(repb.ExecutionStage_COMPLETED, operation.ExecuteResponseWithResult(actionResult, nil /*=summary*/, codes.OK)); err != nil {
				return err // CHECK (these errors should not happen).
			}
			return nil
		}
	}

	r, err := s.runnerPool.Get(ctx, task)
	if err != nil {
		return finishWithErrFn(status.UnavailableErrorf("Error creating runner for command: %s", err.Error()))
	}
	if err := r.PrepareForTask(task); err != nil {
		return finishWithErrFn(err)
	}

	finishedCleanly := false
	defer func() {
		go s.runnerPool.TryRecycle(r, finishedCleanly)
	}()

	md.InputFetchStartTimestamp = ptypes.TimestampNow()
	rxInfo, err := r.Workspace.DownloadInputs(ctx)
	if err != nil {
		return finishWithErrFn(err)
	}
	md.InputFetchCompletedTimestamp = ptypes.TimestampNow()

	if err := stateChangeFn(repb.ExecutionStage_EXECUTING, operation.InProgressExecuteResponse()); err != nil {
		return err // CHECK (these errors should not happen).
	}
	md.ExecutionStartTimestamp = ptypes.TimestampNow()
	maxDuration := infiniteDuration
	if currentDeadline, ok := ctx.Deadline(); ok {
		maxDuration = currentDeadline.Sub(time.Now())
	}
	execDuration, err := parseTimeout(task.GetAction().Timeout, maxDuration)
	if err != nil {
		// These errors are failure-specific. Pass through unchanged.
		return finishWithErrFn(err)
	}
	ctx, cancel := context.WithTimeout(ctx, execDuration)
	defer cancel()

	cmdResultChan := make(chan *interfaces.CommandResult, 1)
	go func() {
		cmdResultChan <- r.Run(ctx, task.GetCommand())
	}()

	// Run a timer that periodically sends update messages back
	// to our caller while execution is ongoing.
	updateTicker := time.NewTicker(execProgressCallbackPeriod)
	var cmdResult *interfaces.CommandResult
	for cmdResult == nil {
		select {
		case cmdResult = <-cmdResultChan:
			updateTicker.Stop()
		case <-updateTicker.C:
			if err := stateChangeFn(repb.ExecutionStage_EXECUTING, operation.InProgressExecuteResponse()); err != nil {
				return status.UnavailableErrorf("could not publish periodic execution update for %q: %s", taskID, err)
			}
		}
	}

	// Only upload action outputs if the error is something that the client can
	// use the action outputs to debug.
	isActionableClientErr := gstatus.Code(cmdResult.Error) == codes.DeadlineExceeded
	if cmdResult.Error != nil && !isActionableClientErr {
		// These errors are failure-specific. Pass through unchanged.
		log.Warningf("Task %q command finished with error: %s", taskID, cmdResult.Error)
		return finishWithErrFn(cmdResult.Error)
	} else {
		log.Infof("Task %q command finished with error: %v", taskID, cmdResult.Error)
	}

	ctx, cancel = background.ExtendContextForFinalization(ctx, uploadDeadlineExtension)
	defer cancel()

	md.ExecutionCompletedTimestamp = ptypes.TimestampNow()
	md.OutputUploadStartTimestamp = ptypes.TimestampNow()

	actionResult := &repb.ActionResult{}
	actionResult.ExitCode = int32(cmdResult.ExitCode)

	txInfo, err := r.Workspace.UploadOutputs(ctx, actionResult, cmdResult)
	if err != nil {
		return finishWithErrFn(status.UnavailableErrorf("Error uploading outputs: %s", err.Error()))
	}
	md.OutputUploadCompletedTimestamp = ptypes.TimestampNow()
	md.WorkerCompletedTimestamp = ptypes.TimestampNow()
	actionResult.ExecutionMetadata = md

	if !task.GetAction().GetDoNotCache() {
		if err := cachetools.UploadActionResult(ctx, acClient, adInstanceDigest, actionResult); err != nil {
			return finishWithErrFn(status.UnavailableErrorf("Error uploading action result: %s", err.Error()))
		}
	}

	metrics.RemoteExecutionCount.With(prometheus.Labels{
		metrics.ExitCodeLabel: fmt.Sprintf("%d", actionResult.ExitCode),
	}).Inc()
	metrics.FileDownloadCount.Observe(float64(rxInfo.FileCount))
	metrics.FileDownloadSizeBytes.Observe(float64(rxInfo.BytesTransferred))
	metrics.FileDownloadDurationUsec.Observe(float64(rxInfo.TransferDuration.Microseconds()))
	metrics.FileUploadCount.Observe(float64(txInfo.FileCount))
	metrics.FileUploadSizeBytes.Observe(float64(txInfo.BytesTransferred))
	metrics.FileUploadDurationUsec.Observe(float64(txInfo.TransferDuration.Microseconds()))
	observeStageDuration("queued", md.GetQueuedTimestamp(), md.GetWorkerStartTimestamp())
	observeStageDuration("input_fetch", md.GetInputFetchStartTimestamp(), md.GetInputFetchCompletedTimestamp())
	observeStageDuration("execution", md.GetExecutionStartTimestamp(), md.GetExecutionCompletedTimestamp())
	observeStageDuration("output_upload", md.GetOutputUploadStartTimestamp(), md.GetOutputUploadCompletedTimestamp())
	observeStageDuration("worker", md.GetWorkerStartTimestamp(), md.GetWorkerCompletedTimestamp())

	execSummary := &espb.ExecutionSummary{
		IoStats: &espb.IOStats{
			// Download
			FileDownloadCount:        rxInfo.FileCount,
			FileDownloadSizeBytes:    rxInfo.BytesTransferred,
			FileDownloadDurationUsec: rxInfo.TransferDuration.Microseconds(),
			// Upload
			FileUploadCount:        txInfo.FileCount,
			FileUploadSizeBytes:    txInfo.BytesTransferred,
			FileUploadDurationUsec: txInfo.TransferDuration.Microseconds(),
		},
		ExecutedActionMetadata: md,
	}
	code := gstatus.Code(cmdResult.Error)
	if err := stateChangeFn(repb.ExecutionStage_COMPLETED, operation.ExecuteResponseWithResult(actionResult, execSummary, code)); err != nil {
		logActionResult(taskID, md)
		return finishWithErrFn(err) // CHECK (these errors should not happen).
	}
	finishedCleanly = true
	return nil
}

func observeStageDuration(stage string, start *timestamppb.Timestamp, end *timestamppb.Timestamp) {
	startTime, err := ptypes.Timestamp(start)
	if err != nil {
		log.Warningf("Could not parse timestamp for '%s' stage: %s", stage, err)
		return
	}
	if startTime.IsZero() {
		return
	}
	endTime, err := ptypes.Timestamp(end)
	if err != nil {
		log.Warningf("Could not parse timestamp for '%s' stage: %s", stage, err)
		return
	}
	if endTime.IsZero() {
		return
	}
	duration := endTime.Sub(startTime)
	metrics.RemoteExecutionExecutedActionMetadataDurationsUsec.With(prometheus.Labels{
		metrics.ExecutedActionStageLabel: stage,
	}).Observe(float64(duration / time.Microsecond))
}
