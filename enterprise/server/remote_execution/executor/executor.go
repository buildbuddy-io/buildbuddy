package executor

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/containerd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	dockerclient "github.com/docker/docker/client"
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
	// TODO(siggisim): Figure out why this needs to be so large for small tests.
	timeoutGracePeriodFactor = 3
	// Allowed deadline extension for uploading action outputs.
	// The deadline of the original request may be extended by up to this amount
	// in order to give enough time to upload action outputs.
	uploadDeadlineExtension = time.Minute * 1
)

var (
	once                  sync.Once
	podIDFromCpusetRegexp = regexp.MustCompile("/kubepods(/.*?)?/pod([a-z0-9\\-]{36})/")
)

func k8sPodID() (string, error) {
	if _, err := os.Stat("/proc/1/cpuset"); err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	buf, err := ioutil.ReadFile("/proc/1/cpuset")
	if err != nil {
		return "", err
	}
	cpuset := string(buf)
	if m := podIDFromCpusetRegexp.FindStringSubmatch(cpuset); m != nil {
		return m[2], nil
	}
	return "", nil
}

type Executor struct {
	env              environment.Env
	buildRoot        string
	dockerClient     *dockerclient.Client
	containerdSocket string
	runnerPool       *runner.Pool
	podID            string
	id               string
	name             string
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

	podID, err := k8sPodID()
	if err != nil {
		return nil, status.FailedPreconditionErrorf("Failed to determine k8s pod ID: %s", err)
	}

	var dockerClient *dockerclient.Client
	containerdSocket := ""
	if executorConfig.ContainerdSocket != "" {
		_, err := os.Stat(executorConfig.ContainerdSocket)
		if os.IsNotExist(err) {
			return nil, status.FailedPreconditionErrorf("Containerd socket %q not found", executorConfig.ContainerdSocket)
		}
		containerdSocket = executorConfig.ContainerdSocket
		log.Info("Using containerd for execution")
		if executorConfig.DockerSocket != "" {
			log.Warning("containerd_socket and docker_socket both specified. Ignoring docker_socket in favor of containerd.")
		}
	} else if executorConfig.DockerSocket != "" {
		_, err := os.Stat(executorConfig.DockerSocket)
		if os.IsNotExist(err) {
			return nil, status.FailedPreconditionErrorf("Docker socket %q not found", executorConfig.DockerSocket)
		}
		dockerSocket := executorConfig.DockerSocket
		dockerClient, err = dockerclient.NewClientWithOpts(
			dockerclient.WithHost(fmt.Sprintf("unix://%s", dockerSocket)),
			dockerclient.WithAPIVersionNegotiation(),
		)
		if err != nil {
			return nil, status.FailedPreconditionErrorf("Failed to create docker client: %s", err)
		}
		log.Info("Using docker for execution")
	}

	name := options.NameOverride
	if name == "" {
		name = base64.StdEncoding.EncodeToString(uuid.NodeID())
	}

	// TODO: Pass HealthChecker explicitly instead of getting it from env.
	runnerPool := runner.NewPool(&executorConfig.RunnerPool)
	if hc := env.GetHealthChecker(); hc != nil {
		hc.RegisterShutdownFunction(runnerPool.Shutdown)
	} else {
		return nil, status.FailedPreconditionError("Missing health checker in env")
	}

	s := &Executor{
		env:              env,
		buildRoot:        executorConfig.GetRootDirectory(),
		dockerClient:     dockerClient,
		containerdSocket: containerdSocket,
		podID:            podID,
		id:               id,
		name:             name,
		runnerPool:       runnerPool,
	}
	go s.pullDefaultImage()
	return s, nil
}

func (s *Executor) Name() string {
	return s.name
}

func (s *Executor) RunnerPool() *runner.Pool {
	return s.runnerPool
}

func (s *Executor) hostBuildRoot() string {
	if s.podID == "" {
		// Probably running on bare metal -- return the build root directly.
		return s.buildRoot
	}
	// Running on k8s -- return the path to the build root on the *host* node.
	// TODO(bduffany): Make this configurable in YAML, populating {{.PodID}} via template.
	// People might have conventions other than executor-data for the volume name + remotebuilds
	// for the build root dir.
	return fmt.Sprintf("/var/lib/kubelet/pods/%s/volumes/kubernetes.io~empty-dir/executor-data/remotebuilds", s.podID)
}

func (s *Executor) pullDefaultImage() {
	if s.dockerClient != nil {
		cfg := s.env.GetConfigurator().GetExecutorConfig()
		runner := docker.NewDockerContainer(
			s.dockerClient, platform.DefaultContainerImage, s.hostBuildRoot(),
			&docker.DockerOptions{
				Socket:                  cfg.DockerSocket,
				EnableSiblingContainers: cfg.DockerSiblingContainers,
				UseHostNetwork:          cfg.DockerNetHost,
				DockerMountMode:         cfg.DockerMountMode,
			},
		)
		start := time.Now()
		// Give the command (which triggers a container pull) up to 1 minute
		// to succeed. In practice I saw clean pulls take about 30 seconds.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		err := runner.PullImageIfNecessary(ctx)
		if err == nil {
			log.Debugf("Pulled default image %q in %s", platform.DefaultContainerImage, time.Since(start))
		} else {
			log.Debugf("Error pulling default image %q: %s", platform.DefaultContainerImage, err)
		}
	}
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

func (s *Executor) props() *platform.ExecutorProperties {
	return &platform.ExecutorProperties{
		ContainerType: s.containerType(),
	}
}

func (s *Executor) containerType() platform.ContainerType {
	if s.dockerClient != nil {
		return platform.DockerContainerType
	}
	if s.containerdSocket != "" {
		return platform.ContainerdContainerType
	}
	return platform.BareContainerType
}

func (s *Executor) getOrCreateRunner(ctx context.Context, task *repb.ExecutionTask) (*runner.CommandRunner, error) {
	props, err := platform.ParseProperties(task.GetCommand().GetPlatform(), s.props())
	if err != nil {
		return nil, err
	}
	// TODO: This mutates the task; find a cleaner way to do this.
	platform.ApplyOverrides(s.env, props, task.GetCommand())

	user, err := auth.UserFromTrustedJWT(ctx)
	// PermissionDenied and Unimplemented both imply that this is an
	// anonymous execution, so ignore those.
	if err != nil && !status.IsPermissionDeniedError(err) && !status.IsUnimplementedError(err) {
		return nil, err
	}

	instanceName := task.GetExecuteRequest().GetInstanceName()

	workerKey := props.PersistentWorkerKey
	if props.PersistentWorker && workerKey == "" {
		workerArgs, _ := runner.SplitArgsIntoWorkerArgsAndFlagFiles(task.GetCommand().GetArguments())
		workerKey = strings.Join(workerArgs, " ")
	}

	if props.RecycleRunner {
		if user == nil {
			return nil, status.InvalidArgumentError(
				"runner recycling is not supported for anonymous builds " +
					`(recycling was requested via platform property "recycle-runner=true")`)
		}

		r := s.runnerPool.Take(&runner.Query{
			User:           user,
			ContainerImage: props.ContainerImage,
			WorkflowID:     props.WorkflowID,
			InstanceName:   instanceName,
			WorkerKey:      workerKey,
		})
		if r != nil {
			log.Info("Reusing workspace for task.")
			r.PlatformProperties = props
			return r, nil
		}
	}
	wsOpts := &workspace.Opts{Preserve: props.PreserveWorkspace}
	ws, err := workspace.New(s.env, s.buildRoot, wsOpts)
	if err != nil {
		return nil, err
	}
	ctr := s.newContainer(props)
	return &runner.CommandRunner{
		ACL:                runner.ACLForUser(user),
		PlatformProperties: props,
		InstanceName:       instanceName,
		WorkerKey:          workerKey,
		Container:          ctr,
		Workspace:          ws,
	}, nil
}

func (s *Executor) newContainer(props *platform.Properties) container.CommandContainer {
	switch s.containerType() {
	case platform.DockerContainerType:
		cfg := s.env.GetConfigurator().GetExecutorConfig()
		return docker.NewDockerContainer(
			s.dockerClient, props.ContainerImage, s.hostBuildRoot(),
			&docker.DockerOptions{
				Socket:                  cfg.DockerSocket,
				EnableSiblingContainers: cfg.DockerSiblingContainers,
				UseHostNetwork:          cfg.DockerNetHost,
				DockerMountMode:         cfg.DockerMountMode,
				ForceRoot:               props.DockerForceRoot,
			},
		)
	case platform.ContainerdContainerType:
		return containerd.NewContainerdContainer(s.containerdSocket, props.ContainerImage, s.hostBuildRoot())
	default:
		return bare.NewBareCommandContainer()
	}
}

func (s *Executor) ExecuteTaskAndStreamResults(task *repb.ExecutionTask, stream operation.StreamLike) error {
	// From here on in we use these liberally, so check that they are setup properly
	// in the environment.
	if s.env.GetActionCacheClient() == nil || s.env.GetByteStreamClient() == nil || s.env.GetContentAddressableStorageClient() == nil {
		return status.FailedPreconditionError("No connection to cache backend.")
	}

	req := task.GetExecuteRequest()
	taskID := task.GetExecutionId()
	adInstanceDigest := digest.NewInstanceNameDigest(req.GetActionDigest(), req.GetInstanceName())

	ctx := stream.Context()
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

	r, err := s.getOrCreateRunner(ctx, task)
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
	ctx, cancel := context.WithTimeout(ctx, execDuration*timeoutGracePeriodFactor)
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
