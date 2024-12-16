package podman

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/block_io"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/soci_store"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	// TODO(#2523): remove this flag.
	podmanPullLogLevel = flag.String("executor.podman.pull_log_level", "", "Level at which to log `podman pull` command output. Should be one of the standard log levels, all lowercase.")

	// Note: to get these cgroup paths, run a podman container like
	//     podman run --rm --name sleepy busybox sleep infinity
	// then look at the output of
	//     find /sys/fs/cgroup | grep libpod-$(podman container inspect sleepy | jq -r '.[0].Id')

	privateImageStreamingEnabled = flag.Bool("executor.podman.enable_private_image_streaming", false, "If set and --executor.podman.enable_image_streaming is set, all private (authenticated) podman images are streamed using soci artifacts generated and stored in the apps.")

	pullTimeout   = flag.Duration("executor.podman.pull_timeout", 10*time.Minute, "Timeout for image pulls.")
	parallelPulls = flag.Int("executor.podman.parallel_pulls", 0, "The system-wide maximum number of image layers to be pulled from remote container registries simultaneously. If set to 0, no value is set and podman will use its default value.")

	podmanRuntime       = flag.String("executor.podman.runtime", "", "Enables running podman with other runtimes, like gVisor (runsc).")
	podmanStorageDriver = flag.String("executor.podman.storage_driver", "overlay", "The podman storage driver to use.")
	podmanEnableStats   = flag.Bool("executor.podman.enable_stats", true, "Whether to enable cgroup-based podman stats.")
	transientStore      = flag.Bool("executor.podman.transient_store", false, "Enables --transient-store for podman commands.", flag.Deprecated("--transient-store is now always applied if the podman version supports it"))
	podmanDNS           = flag.String("executor.podman.dns", "8.8.8.8", "Specifies a custom DNS server for podman to use. Defaults to 8.8.8.8. If set to empty, no --dns= flag will be passed to podman.")
	podmanGPU           = flag.String("executor.podman.gpus", "", "Specifies the value of the --gpus= flag to pass to podman. Set to 'all' to pass all GPUs.")
	podmanPidsLimit     = flag.String("executor.podman.pids_limit", "", "Specifies the value of the --pids-limit= flag to pass to podman. Set to '-1' for unlimited PIDs. The default is 2048 on systems that support pids cgroup controller.")

	// Additional time used to kill the container if the command doesn't exit cleanly
	containerFinalizationTimeout = 10 * time.Second

	storageErrorRegex = regexp.MustCompile(`(?s)A storage corruption might have occurred.*Error: readlink.*no such file or directory`)
	userRegex         = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*(:[a-z0-9_-]*)?$`)

	// Min podman version supporting the '--transient-store' flag.
	transientStoreMinVersion = semver.MustParse("4.4.0")

	// A map from image name to pull status. This is used to avoid parallel pulling of the same image.
	pullOperations sync.Map

	databaseLockedRegexp = regexp.MustCompile("transaction.*: database is locked")
)

const (
	// Podman exit codes
	podmanInternalExitCode           = 125
	podmanCommandNotRunnableExitCode = 126
	podmanCommandNotFoundExitCode    = 127
	podmanCommandOutOfRangeExitCode  = 255

	// podmanExecSIGKILLExitCode is the exit code returned by `podman exec` when the exec
	// process is killed due to the parent container being removed.
	podmanExecSIGKILLExitCode = 137
	podmanExecSIGTERMExitCode = 143

	podmanDefaultNetworkIPRange = "10.88.0.0/16"
	podmanDefaultNetworkGateway = "10.88.0.1"
	podmanDefaultNetworkBridge  = "cni-podman0"

	// --cidfile to be written by podman before we give up.
	pollCIDTimeout = 15 * time.Second

	// How long to cache the result of `podman image exists` when it returns
	// true. A short duration is used to help recover from rare scenarios in
	// which the image might be deleted externally.
	imageExistsCacheTTL = 5 * time.Minute
	// Max number of entries to store in the `podman image exists` cache.
	imageExistsCacheSize = 1000
)

type pullStatus struct {
	mu     *sync.RWMutex
	pulled bool
}

type Provider struct {
	env              environment.Env
	podmanVersion    *semver.Version
	cgroupPaths      *cgroup.Paths
	buildRoot        string
	sociStore        soci_store.Store
	imageExistsCache *imageExistsCache
}

func NewProvider(env environment.Env, buildRoot string) (*Provider, error) {
	// Eagerly init podman version so we can crash the executor if it fails.
	podmanVersion, err := getPodmanVersion(env.GetServerContext(), env.GetCommandRunner())
	if err != nil {
		return nil, status.WrapError(err, "podman version")
	}
	if podmanVersion.LessThan(transientStoreMinVersion) {
		log.Warningf("Detected podman version %s does not support --transient-store option, which significantly improves performance. Consider upgrading podman.", podmanVersion)
	}

	sociStore, err := soci_store.Init(env)
	if err != nil {
		return nil, err
	}

	imageExistsCache, err := newImageExistsCache()
	if err != nil {
		return nil, err
	}

	if *parallelPulls < 0 {
		return nil, status.InvalidArgumentErrorf("executor.podman.parallel_pulls must not be negative (was %d)", *parallelPulls)
	} else if *parallelPulls > 0 {
		containersConf := fmt.Sprintf(`
[engine]
image_parallel_copies = %d`, *parallelPulls)
		if err := os.MkdirAll("/etc/containers", 0644); err != nil {
			return nil, err
		}
		if err := os.WriteFile("/etc/containers/containers.conf", []byte(containersConf), 0644); err != nil {
			return nil, status.UnavailableErrorf("could not write containers.conf: %s", err)
		}
	}

	return &Provider{
		env:              env,
		podmanVersion:    podmanVersion,
		cgroupPaths:      &cgroup.Paths{},
		sociStore:        sociStore,
		buildRoot:        buildRoot,
		imageExistsCache: imageExistsCache,
	}, nil
}

func getPodmanVersion(ctx context.Context, commandRunner interfaces.CommandRunner) (*semver.Version, error) {
	var stdout, stderr bytes.Buffer
	stdio := &interfaces.Stdio{Stdout: &stdout, Stderr: &stderr}
	result := runPodman(ctx, commandRunner, nil /*=version*/, "version", stdio, "--format={{.Client.Version}}")
	if result.Error != nil || result.ExitCode != 0 {
		return nil, status.InternalErrorf("command failed: %s: %s", result.Error, stderr.String())
	}
	v, err := semver.NewVersion(strings.TrimSpace(stdout.String()))
	if err != nil {
		return nil, status.InternalErrorf("parse podman version output %q: %s (stderr: %q)", stdout.String(), err, stderr.String())
	}
	return v, nil
}

func (p *Provider) New(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
	imageIsPublic := args.Props.ContainerRegistryUsername == "" && args.Props.ContainerRegistryPassword == ""
	imageIsStreamable := (imageIsPublic || *privateImageStreamingEnabled)
	if imageIsStreamable {
		if err := p.sociStore.WaitUntilReady(); err != nil {
			return nil, status.UnavailableErrorf("soci-store unavailable: %s", err)
		}

	}

	// Re-use docker flags for podman.
	networkMode, err := flagutil.GetDereferencedValue[string]("executor.docker_network")
	if err != nil {
		return nil, err
	}
	capAdd, err := flagutil.GetDereferencedValue[string]("docker_cap_add")
	if err != nil {
		return nil, err
	}
	devices, err := flagutil.GetDereferencedValue[[]container.DockerDeviceMapping]("executor.docker_devices")
	if err != nil {
		return nil, err
	}
	volumes, err := flagutil.GetDereferencedValue[[]string]("executor.docker_volumes")
	if err != nil {
		return nil, err
	}

	return &podmanCommandContainer{
		env:               p.env,
		podmanVersion:     p.podmanVersion,
		cgroupPaths:       p.cgroupPaths,
		image:             args.Props.ContainerImage,
		imageIsStreamable: imageIsStreamable,
		sociStore:         p.sociStore,
		imageExistsCache:  p.imageExistsCache,
		buildRoot:         p.buildRoot,
		blockDevice:       args.BlockDevice,
		options: &PodmanOptions{
			ForceRoot:          args.Props.DockerForceRoot,
			Init:               args.Props.DockerInit,
			User:               args.Props.DockerUser,
			Network:            args.Props.DockerNetwork,
			DefaultNetworkMode: networkMode,
			CapAdd:             capAdd,
			Devices:            devices,
			Volumes:            volumes,
			EnableStats:        *podmanEnableStats,
		},
	}, nil
}

type PodmanOptions struct {
	ForceRoot          bool
	Init               bool
	User               string
	DefaultNetworkMode string
	Network            string
	CapAdd             string
	Devices            []container.DockerDeviceMapping
	Volumes            []string
	// EnableStats determines whether to enable the stats API. This also enables
	// resource monitoring while tasks are in progress.
	EnableStats bool
}

// podmanCommandContainer containerizes a single command's execution using a Podman container.
type podmanCommandContainer struct {
	env              environment.Env
	podmanVersion    *semver.Version
	imageExistsCache *imageExistsCache
	cgroupPaths      *cgroup.Paths

	image       string
	buildRoot   string
	workDir     string
	blockDevice *block_io.Device

	imageIsStreamable bool
	sociStore         soci_store.Store

	options *PodmanOptions

	// name is the container name.
	name string

	stats container.UsageStats

	// cid contains the container ID read from the cidfile.
	cid atomic.Value

	mu sync.Mutex // protects(removed)
	// removed is a flag that is set once Remove is called (before actually
	// removing the container).
	removed bool
}

func addUserArgs(args []string, options *PodmanOptions) []string {
	if options.ForceRoot {
		args = append(args, "--user=0:0")
	} else if options.User != "" && userRegex.MatchString(options.User) {
		args = append(args, "--user="+options.User)
		parts := strings.Split(options.User, ":")
		// If the user is set to UID:GUID, tell podman not to add entries
		// to /etc/passwd and /etc/group which mimics the docker behavior.
		if len(parts) == 2 {
			_, err := strconv.Atoi(parts[0])
			isUID := err == nil
			_, err = strconv.Atoi(parts[1])
			isGID := err == nil
			if isUID && isGID {
				args = append(args, "--passwd=false")
			}
		}
	}
	return args
}

func (c *podmanCommandContainer) getPodmanRunArgs(workDir string) []string {
	args := []string{
		"--hostname",
		"localhost",
		"--workdir",
		workDir,
		"--name",
		c.name,
		"--rm",
		"--cidfile",
		c.cidFilePath(),
		"--volume",
		fmt.Sprintf(
			"%s:%s",
			filepath.Join(c.buildRoot, filepath.Base(workDir)),
			workDir,
		),
	}
	args = addUserArgs(args, c.options)
	networkMode := c.options.DefaultNetworkMode
	// Translate network platform prop to the equivalent Podman network mode, to
	// allow overriding the default configured mode.
	switch strings.ToLower(c.options.Network) {
	case "off":
		networkMode = "none"
	case "bridge": // use Podman default (bridge)
		networkMode = ""
	default: // ignore other values for now, sticking to the configured default.
	}
	if networkMode != "" {
		args = append(args, "--network="+networkMode)
	}
	// "--dns" and "--dns=search" flags are invalid when --network is set to none
	// or "container:id"
	if networkMode != "none" && !strings.HasPrefix(networkMode, "container") {
		if *podmanDNS != "" {
			args = append(args, "--dns="+*podmanDNS)
		}
		args = append(args, "--dns-search=.")
	}
	if c.options.CapAdd != "" {
		args = append(args, "--cap-add="+c.options.CapAdd)
	}
	if *podmanGPU != "" {
		args = append(args, "--gpus="+*podmanGPU)
	}
	if *podmanPidsLimit != "" {
		args = append(args, "--pids-limit="+*podmanPidsLimit)
	}
	for _, device := range c.options.Devices {
		deviceSpecs := make([]string, 0)
		if device.PathOnHost != "" {
			deviceSpecs = append(deviceSpecs, device.PathOnHost)
		}
		if device.PathInContainer != "" {
			deviceSpecs = append(deviceSpecs, device.PathInContainer)
		}
		if device.CgroupPermissions != "" {
			deviceSpecs = append(deviceSpecs, device.CgroupPermissions)
		}
		args = append(args, "--device="+strings.Join(deviceSpecs, ":"))
	}
	for _, volume := range c.options.Volumes {
		args = append(args, "--volume="+volume)
	}
	args = append(args, c.sociStore.GetPodmanArgs()...)
	if c.options.Init {
		args = append(args, "--init")
	}
	return args
}

func (c *podmanCommandContainer) IsolationType() string {
	return "podman"
}

func (c *podmanCommandContainer) Run(ctx context.Context, command *repb.Command, workDir string, creds oci.Credentials) *interfaces.CommandResult {
	c.workDir = workDir
	defer os.RemoveAll(c.cidFilePath())
	result := &interfaces.CommandResult{
		CommandDebugString: fmt.Sprintf("(podman) %s", command.GetArguments()),
		ExitCode:           commandutil.NoExitCode,
	}
	containerName, err := generateContainerName()
	c.name = containerName
	if err != nil {
		result.Error = status.UnavailableErrorf("failed to generate podman container name: %s", err)
		return result
	}

	if err := container.PullImageIfNecessary(ctx, c.env, c, creds, c.image); err != nil {
		result.Error = status.UnavailableErrorf("failed to pull docker image: %s", err)
		return result
	}

	podmanRunArgs := c.getPodmanRunArgs(workDir)
	for _, envVar := range command.GetEnvironmentVariables() {
		podmanRunArgs = append(podmanRunArgs, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	podmanRunArgs = append(podmanRunArgs, c.image)
	podmanRunArgs = append(podmanRunArgs, command.Arguments...)
	result = c.doWithStatsTracking(ctx, func(ctx context.Context) *interfaces.CommandResult {
		return c.runPodman(ctx, "run", &interfaces.Stdio{}, podmanRunArgs...)
	})

	if result.ExitCode == podmanCommandNotRunnableExitCode {
		log.CtxInfof(ctx, "podman failed to run command")
	} else if result.ExitCode == podmanCommandNotFoundExitCode {
		log.CtxInfof(ctx, "podman failed to find command")
	} else if result.ExitCode == podmanExecSIGKILLExitCode {
		log.CtxInfof(ctx, "podman receieved SIGKILL")
		result.ExitCode = commandutil.KilledExitCode
		result.Error = commandutil.ErrSIGKILL
	} else if result.ExitCode == podmanExecSIGTERMExitCode {
		log.CtxInfof(ctx, "podman receieved SIGTERM")
		result.ExitCode = commandutil.KilledExitCode
		result.Error = commandutil.ErrSIGKILL
	}

	if err := c.maybeCleanupCorruptedImages(ctx, result); err != nil {
		log.Warningf("Failed to remove corrupted image: %s", err)
	}
	if exitedCleanly := result.ExitCode >= 0; !exitedCleanly {
		if err = c.killContainerIfRunning(ctx); err != nil {
			log.Warningf("Failed to shut down podman container: %s", err)
		}
	}
	return result
}

// Wraps a function call with container.TrackStats, ensuring that prometheus
// metrics are updated while the function is executing, and that the UsageStats
// field is populated after execution.
func (c *podmanCommandContainer) doWithStatsTracking(ctx context.Context, runPodmanFn func(ctx context.Context) *interfaces.CommandResult) *interfaces.CommandResult {
	stop := c.stats.TrackExecution(ctx, func(ctx context.Context) (*repb.UsageStats, error) {
		if !c.options.EnableStats {
			return nil, nil
		}
		cid, err := c.getCID(ctx)
		if err != nil {
			return nil, err
		}
		return c.cgroupPaths.Stats(ctx, cid, c.blockDevice)
	})
	res := runPodmanFn(ctx)
	stop()
	// statsCh will report stats for processes inside the container, and
	// res.UsageStats will report stats for the podman process itself.
	// Combine these stats to get the total usage.
	podmanProcessStats := res.UsageStats
	taskStats := c.stats.TaskStats()
	if taskStats == nil {
		taskStats = &repb.UsageStats{}
	}
	combinedStats := taskStats.CloneVT()
	combinedStats.CpuNanos += podmanProcessStats.GetCpuNanos()
	if podmanProcessStats.GetPeakMemoryBytes() > taskStats.GetPeakMemoryBytes() {
		combinedStats.PeakMemoryBytes = podmanProcessStats.GetPeakMemoryBytes()
	}
	res.UsageStats = combinedStats
	return res
}

func (c *podmanCommandContainer) Create(ctx context.Context, workDir string) error {
	containerName, err := generateContainerName()
	if err != nil {
		return status.UnavailableErrorf("failed to generate podman container name: %s", err)
	}
	c.name = containerName
	c.workDir = workDir

	podmanRunArgs := c.getPodmanRunArgs(workDir)
	podmanRunArgs = append(podmanRunArgs, c.image)
	podmanRunArgs = append(podmanRunArgs, "sleep", "infinity")
	createResult := c.runPodman(ctx, "create", &interfaces.Stdio{}, podmanRunArgs...)
	if err := c.maybeCleanupCorruptedImages(ctx, createResult); err != nil {
		log.Warningf("Failed to remove corrupted image: %s", err)
	}

	if err = createResult.Error; err != nil {
		return status.UnavailableErrorf("failed to create container: %s", err)
	}

	if createResult.ExitCode != 0 {
		return status.UnknownErrorf("podman create failed: exit code %d, stderr: %s", createResult.ExitCode, createResult.Stderr)
	}

	startResult := c.runPodman(ctx, "start", &interfaces.Stdio{}, c.name)
	if startResult.Error != nil {
		return startResult.Error
	}
	if startResult.ExitCode != 0 {
		return status.UnknownErrorf("podman start failed: exit code %d, stderr: %s", startResult.ExitCode, startResult.Stderr)
	}
	return nil
}

func (c *podmanCommandContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	podmanRunArgs := make([]string, 0, 2*len(cmd.GetEnvironmentVariables())+len(cmd.Arguments)+1)
	for _, envVar := range cmd.GetEnvironmentVariables() {
		podmanRunArgs = append(podmanRunArgs, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	podmanRunArgs = addUserArgs(podmanRunArgs, c.options)
	if stdio.Stdin != nil {
		podmanRunArgs = append(podmanRunArgs, "--interactive")
	}
	podmanRunArgs = append(podmanRunArgs, c.name)
	podmanRunArgs = append(podmanRunArgs, cmd.Arguments...)
	res := c.doWithStatsTracking(ctx, func(ctx context.Context) *interfaces.CommandResult {
		return c.runPodman(ctx, "exec", stdio, podmanRunArgs...)
	})
	// Podman doesn't provide a way to find out whether an exec process was
	// killed. Instead, `podman exec` returns 137 (= 128 + SIGKILL(9)). However,
	// this exit code is also valid as a regular exit code returned by a command
	// during a normal execution, so we are overly cautious here and only
	// interpret this code specially when the container was removed and we are
	// expecting a SIGKILL as a result.
	c.mu.Lock()
	removed := c.removed
	c.mu.Unlock()
	if removed && (res.ExitCode == podmanExecSIGKILLExitCode || res.ExitCode == podmanExecSIGTERMExitCode) {
		res.ExitCode = commandutil.KilledExitCode
		res.Error = commandutil.ErrSIGKILL
	}
	return res
}

func (c *podmanCommandContainer) Signal(ctx context.Context, sig syscall.Signal) error {
	if c.name == "" {
		return status.FailedPreconditionError("container is not created")
	}
	res := c.runPodman(ctx, "kill", nil, c.name, "--signal", fmt.Sprintf("%d", sig))
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode != 0 {
		return status.UnavailableErrorf("failed to signal container: exit code %d: %q", res.ExitCode, string(res.Stderr))
	}
	return nil
}

func (c *podmanCommandContainer) IsImageCached(ctx context.Context) (bool, error) {
	if c.imageExistsCache.Exists(c.image) {
		return true, nil
	}

	res := c.runPodman(ctx, "image", &interfaces.Stdio{}, "exists", c.image)
	if res.Error != nil {
		return false, res.Error
	}
	// Exit code 0 = exists, 1 = does not exist. Other exit codes indicate
	// errors.
	if !(res.ExitCode == 0 || res.ExitCode == 1) {
		return false, status.InternalErrorf("'podman image exists' failed (code %d): stderr: %q", res.ExitCode, string(res.Stderr))
	}

	if res.ExitCode == 1 {
		return false, nil
	}

	c.imageExistsCache.Add(c.image)
	return true, nil
}

func (c *podmanCommandContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	psi, _ := pullOperations.LoadOrStore(c.image, &pullStatus{&sync.RWMutex{}, false})
	ps, ok := psi.(*pullStatus)
	if !ok {
		alert.UnexpectedEvent("psi cannot be cast to *pullStatus")
		return status.InternalError("PullImage failed: cannot get pull status")
	}

	ps.mu.RLock()
	alreadyPulled := ps.pulled
	ps.mu.RUnlock()

	if alreadyPulled {
		return c.pullImage(ctx, creds)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if c.imageIsStreamable {
		c.sociStore.GetArtifacts(ctx, c.env, c.image, creds)
	}

	startTime := time.Now()
	if err := c.pullImage(ctx, creds); err != nil {
		return err
	}
	pullLatency := time.Since(startTime)
	log.Infof("podman pulled image %s in %s", c.image, pullLatency)
	metrics.PodmanColdImagePullLatencyMsec.
		With(prometheus.Labels{metrics.ContainerImageTag: c.image}).
		Observe(float64(pullLatency.Milliseconds()))
	ps.pulled = true
	return nil
}

// cidFilePath returns the path to the container's cidfile. Podman will write
// the container's ID to this file since we run containers with the --cidfile
// arg. The file is given the same name as the task workspace directory, plus a
// ".cid" extension, such as "/tmp/remote_build/{{WORKSPACE_ID}}.cid". Note:
// this logic depends on the workspace parent directory already existing.
func (c *podmanCommandContainer) cidFilePath() string {
	return c.workDir + ".cid"
}

func (c *podmanCommandContainer) getCID(ctx context.Context) (string, error) {
	if cid := c.cid.Load(); cid != nil {
		return cid.(string), nil
	}
	cidPath := c.cidFilePath()
	waitOpts := disk.WaitOpts{Timeout: pollCIDTimeout}
	if err := disk.WaitUntilExists(ctx, cidPath, waitOpts); err != nil {
		return "", err
	}
	var cid string
	// Retry in case the cidfile is empty, to avoid relying on podman to
	// atomically create and write the cidfile.
	r := retry.DefaultWithContext(ctx)
	for r.Next() {
		b, err := disk.ReadFile(ctx, cidPath)
		if err != nil {
			continue
		}
		cid = strings.TrimSpace(string(b))
		if cid == "" {
			continue
		}
		break
	}
	c.cid.Store(cid)
	return cid, ctx.Err()
}

func (c *podmanCommandContainer) pullImage(ctx context.Context, creds oci.Credentials) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	podmanArgs := make([]string, 0, 2)

	if c.imageIsStreamable {
		// Make the image credentials available to the soci-store
		c.sociStore.PutCredentials(ctx, c.image, creds)

		// We still need to run "podman pull" even when image streaming is
		// enabled to populate the layer info and avoid spitting a bunch of
		// pull-time logging into the run-time logs.
		podmanArgs = append(podmanArgs, c.sociStore.GetPodmanArgs()...)
	}

	if !creds.IsEmpty() {
		podmanArgs = append(podmanArgs, fmt.Sprintf("--creds=%s", creds.String()))
	}

	if *podmanPullLogLevel != "" {
		podmanArgs = append(podmanArgs, fmt.Sprintf("--log-level=%s", *podmanPullLogLevel))
	}

	podmanArgs = append(podmanArgs, c.image)
	// Use server context instead of ctx to make sure that "podman pull" is not killed when the context
	// is cancelled. If "podman pull" is killed when copying a parent layer, it will result in
	// corrupted storage.  More details see https://github.com/containers/storage/issues/1136.
	pullCtx, cancel := context.WithTimeout(c.env.GetServerContext(), *pullTimeout)
	defer cancel()
	output := lockingbuffer.New()
	stdio := &interfaces.Stdio{Stderr: output, Stdout: output}
	if *podmanPullLogLevel != "" {
		stdio.Stderr = io.MultiWriter(stdio.Stderr, log.CtxWriter(ctx, "[podman pull] "))
		stdio.Stdout = io.MultiWriter(stdio.Stdout, log.CtxWriter(ctx, "[podman pull] "))
	}
	pullResult := c.runPodman(pullCtx, "pull", stdio, podmanArgs...)
	if pullResult.Error != nil {
		return pullResult.Error
	}
	if pullResult.ExitCode != 0 {
		return status.UnavailableErrorf("podman pull failed: exit code %d, output: %s", pullResult.ExitCode, output.String())
	}

	// Since we just pulled the image, we can skip the next call to 'podman
	// image exists'.
	c.imageExistsCache.Add(c.image)
	return nil
}

func (c *podmanCommandContainer) Remove(ctx context.Context) error {
	c.mu.Lock()
	c.removed = true
	c.mu.Unlock()
	os.RemoveAll(c.cidFilePath()) // intentionally ignoring error.
	res := c.runPodman(ctx, "kill", &interfaces.Stdio{}, "--signal=KILL", c.name)
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode == 0 || strings.Contains(string(res.Stderr), "no such container") {
		return nil
	}
	return status.UnknownErrorf("podman remove failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
}

func (c *podmanCommandContainer) Pause(ctx context.Context) error {
	res := c.runPodman(ctx, "pause", &interfaces.Stdio{}, c.name)
	if res.ExitCode != 0 {
		return status.UnknownErrorf("podman pause failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
	}
	return nil
}

func (c *podmanCommandContainer) Unpause(ctx context.Context) error {
	res := c.runPodman(ctx, "unpause", &interfaces.Stdio{}, c.name)
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode != 0 {
		return status.UnknownErrorf("podman unpause failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
	}
	return nil
}

func (c *podmanCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	return c.stats.TaskStats(), nil
}

func (c *podmanCommandContainer) runPodman(ctx context.Context, subCommand string, stdio *interfaces.Stdio, args ...string) *interfaces.CommandResult {
	return runPodman(ctx, c.env.GetCommandRunner(), c.podmanVersion, subCommand, stdio, args...)
}

func runPodman(ctx context.Context, commandRunner interfaces.CommandRunner, podmanVersion *semver.Version, subCommand string, stdio *interfaces.Stdio, args ...string) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx)
	tracing.AddStringAttributeToCurrentSpan(ctx, "podman.subcommand", subCommand)
	defer span.End()
	command := []string{"podman"}

	// Append "global" podman args, which come before any subcommand.

	if podmanVersion != nil && !podmanVersion.LessThan(transientStoreMinVersion) {
		// Use transient store to reduce contention.
		// See https://github.com/containers/podman/issues/19824
		command = append(command, "--transient-store")
	}
	command = append(command, fmt.Sprintf("--storage-driver=%s", *podmanStorageDriver))
	if *podmanRuntime != "" {
		command = append(command, fmt.Sprintf("--runtime=%s", *podmanRuntime))
	}
	if filepath.Base(*podmanRuntime) == "runsc" {
		// gVisor will attempt to setup cgroups, but podman has
		// already done that, so tell gVisor not to.
		command = append(command, "--runtime-flag=ignore-cgroups")
	}

	// Append subcommand and args.

	command = append(command, subCommand)
	command = append(command, args...)
	// Note: we don't collect stats on the podman process, and instead use
	// cgroups for stats accounting.
	result := commandRunner.Run(ctx, &repb.Command{Arguments: command}, "" /*=workDir*/, nil /*=statsListener*/, stdio)

	// If the disk is under heavy load, podman may fail with "database is
	// locked". Detect these and return a retryable error.
	if (result.ExitCode == podmanCommandNotRunnableExitCode ||
		result.ExitCode == podmanInternalExitCode ||
		result.ExitCode == podmanCommandNotFoundExitCode ||
		result.ExitCode == podmanCommandOutOfRangeExitCode) &&
		databaseLockedRegexp.Match(result.Stderr) {
		result.ExitCode = commandutil.NoExitCode
		result.Error = status.UnavailableErrorf("podman failed: %q", strings.TrimSpace(string(result.Stderr)))
	}

	return result
}

func generateContainerName() (string, error) {
	suffix, err := random.RandomString(20)
	if err != nil {
		return "", err
	}
	return "buildbuddy-exec-" + suffix, nil
}

func (c *podmanCommandContainer) killContainerIfRunning(ctx context.Context) error {
	ctx, cancel := background.ExtendContextForFinalization(ctx, containerFinalizationTimeout)
	defer cancel()

	err := c.Remove(ctx)
	if err != nil && strings.Contains(err.Error(), "Error: can only kill running containers.") {
		// This is expected.
		return nil
	}
	return err
}

// An image can be corrupted if "podman pull" command is killed when pulling a parent layer.
// More details can be found at https://github.com/containers/storage/issues/1136. When this
// happens when need to remove the image before re-pulling the image in order to fix it.
func (c *podmanCommandContainer) maybeCleanupCorruptedImages(ctx context.Context, result *interfaces.CommandResult) error {
	if result.ExitCode != podmanInternalExitCode {
		return nil
	}
	if !storageErrorRegex.MatchString(string(result.Stderr)) {
		return nil
	}
	result.Error = status.UnavailableError("a storage corruption occurred")
	result.ExitCode = commandutil.NoExitCode
	return c.removeImage(ctx, c.image)
}

func (c *podmanCommandContainer) removeImage(ctx context.Context, imageName string) error {
	ctx, cancel := background.ExtendContextForFinalization(ctx, containerFinalizationTimeout)
	defer cancel()

	defer c.imageExistsCache.Remove(imageName)

	result := c.runPodman(ctx, "rmi", &interfaces.Stdio{}, imageName)
	if result.Error != nil {
		return result.Error
	}
	if result.ExitCode == 0 || strings.Contains(string(result.Stderr), "image not known") {
		return nil
	}
	return status.UnknownErrorf("podman rmi failed: %s", string(result.Stderr))
}

func configureSecondaryNetwork(ctx context.Context) error {
	// Add ip rule to lookup rt1
	// Equivalent to "ip rule add to 10.88.0.0/16 lookup rt1"
	if err := networking.AddIPRuleIfNotPresent(ctx, []string{"to", podmanDefaultNetworkIPRange}); err != nil {
		return err
	}
	if err := networking.AddIPRuleIfNotPresent(ctx, []string{"from", podmanDefaultNetworkIPRange}); err != nil {
		return err
	}

	// Add ip route to routing table rt1
	// Equivalent to "ip route add 10.88.0.0/16 via 10.88.0.1 dev cni-podman0 table rt1"
	route := []string{podmanDefaultNetworkIPRange, "via", podmanDefaultNetworkGateway, "dev", podmanDefaultNetworkBridge}
	if err := networking.AddRouteIfNotPresent(ctx, route); err != nil {
		return err
	}

	return nil
}

// ConfigureIsolation setsup network isolation so that container traffic is
// isolated from other traffic. Isolation will consist either of a second
// network interface or private IP range blackholing, depending on config
// options.
func ConfigureIsolation(ctx context.Context) error {
	if !networking.IsSecondaryNetworkEnabled() {
		return nil
	}

	// Run a dummy container so that the podman network gets initialized.
	// This is to make sure that any iptables rules that we add get added
	// earlier in the chain than the podman ones.
	podmanVersion, err := getPodmanVersion(ctx, commandutil.CommandRunner{})
	if err != nil {
		return status.WrapError(err, "podman version")
	}
	result := runPodman(ctx, commandutil.CommandRunner{}, podmanVersion, "run", &interfaces.Stdio{}, "--rm", "busybox", "sh")
	if result.Error != nil {
		return status.UnknownErrorf("failed to setup podman default network: podman run failed: %s (stderr: %q)", result.Error, string(result.Stderr))
	}
	if result.ExitCode != 0 {
		return status.UnknownErrorf("failed to setup podman default network: podman run exited with code %d: %q", result.ExitCode, string(result.Stderr))
	}

	if networking.IsSecondaryNetworkEnabled() {
		return configureSecondaryNetwork(ctx)
	}

	return nil
}

type imageExistsCache struct {
	mu  sync.Mutex
	lru *lru.LRU[time.Time]
}

func newImageExistsCache() (*imageExistsCache, error) {
	l, err := lru.NewLRU(&lru.Config[time.Time]{
		MaxSize: imageExistsCacheSize,
		SizeFn:  func(time.Time) int64 { return 1 },
	})
	if err != nil {
		return nil, err
	}
	return &imageExistsCache{lru: l}, nil
}

func (c *imageExistsCache) Exists(image string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.lru.Get(image)
	return ok && time.Since(t) < imageExistsCacheTTL
}

func (c *imageExistsCache) Add(image string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Add(image, time.Now())
}

func (c *imageExistsCache) Remove(image string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Remove(image)
}
