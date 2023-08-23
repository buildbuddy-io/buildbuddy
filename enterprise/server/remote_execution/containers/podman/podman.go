package podman

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	sspb "github.com/awslabs/soci-snapshotter/proto"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
	godigest "github.com/opencontainers/go-digest"
)

var (
	// TODO(#2523): remove this flag.
	podmanPullLogLevel = flag.String("executor.podman.pull_log_level", "", "Level at which to log `podman pull` command output. Should be one of the standard log levels, all lowercase.")

	// Note: to get these cgroup paths, run a podman container like
	//     podman run --rm --name sleepy busybox sleep infinity
	// then look at the output of
	//     find /sys/fs/cgroup | grep libpod-$(podman container inspect sleepy | jq -r '.[0].Id')

	memUsagePathTemplate = flag.String("executor.podman.memory_usage_path_template", "/sys/fs/cgroup/memory/libpod_parent/libpod-{{.ContainerID}}/memory.usage_in_bytes", "Go template specifying a path pointing to a container's current memory usage, in bytes. Templated with `ContainerID`.")
	cpuUsagePathTemplate = flag.String("executor.podman.cpu_usage_path_template", "/sys/fs/cgroup/cpuacct/libpod_parent/libpod-{{.ContainerID}}/cpuacct.usage", "Go template specifying a path pointing to a container's total CPU usage, in CPU nanoseconds. Templated with `ContainerID`.")

	// TODO(iain): delete executor.podman.run_soci_snapshotter flag once
	// the snapshotter doesn't need root permissions to run.
	sociStoreLogLevel            = flag.String("executor.podman.soci_store_log_level", "", "The level at which the soci-store should log. Should be one of the standard log levels, all lowercase.")
	runSociStoreLocally          = flag.Bool("executor.podman.run_soci_store", true, "If true, runs the soci store locally if needed for image streaming.")
	imageStreamingEnabled        = flag.Bool("executor.podman.enable_image_streaming", false, "If set, all public (non-authenticated) podman images are streamed using soci artifacts generated and stored in the apps.")
	privateImageStreamingEnabled = flag.Bool("executor.podman.enable_private_image_streaming", false, "If set and --executor.podman.enable_image_streaming is set, all private (authenticated) podman images are streamed using soci artifacts generated and stored in the apps.")

	pullTimeout = flag.Duration("executor.podman.pull_timeout", 10*time.Minute, "Timeout for image pulls.")

	sociArtifactStoreTarget = flag.String("executor.podman.soci_artifact_store_target", "", "The GRPC url to use to access the SociArtifactStore GRPC service.")
	sociStoreKeychainPort   = flag.Int("executor.podman.soci_store_keychain_port", 1989, "The port on which the soci-store local keychain service is exposed, for sharing credentials for streaming private container images.")
	podmanRuntime           = flag.String("executor.podman.runtime", "", "Enables running podman with other runtimes, like gVisor (runsc).")
	podmanEnableStats       = flag.Bool("executor.podman.enable_stats", false, "Whether to enable cgroup-based podman stats.")

	// Additional time used to kill the container if the command doesn't exit cleanly
	containerFinalizationTimeout = 10 * time.Second

	storageErrorRegex = regexp.MustCompile(`(?s)A storage corruption might have occurred.*Error: readlink.*no such file or directory`)
	userRegex         = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*(:[a-z0-9_-]*)?$`)

	// A map from image name to pull status. This is used to avoid parallel pulling of the same image.
	pullOperations sync.Map
)

const (
	podmanInternalExitCode = 125
	// podmanExecSIGKILLExitCode is the exit code returned by `podman exec` when the exec
	// process is killed due to the parent container being removed.
	podmanExecSIGKILLExitCode = 137

	podmanDefaultNetworkIPRange = "10.88.0.0/16"
	podmanDefaultNetworkGateway = "10.88.0.1"
	podmanDefaultNetworkBridge  = "cni-podman0"

	// --cidfile to be written by podman before we give up.
	pollCIDTimeout = 15 * time.Second
	// statsPollInterval controls how often we will poll the cgroupfs to determine
	// a container's resource usage.
	statsPollInterval = 50 * time.Millisecond

	// argument to podman to enable the use of the soci store for streaming
	enableStreamingStoreArg = "--storage-opt=additionallayerstore=/var/lib/soci-store/store:ref"

	// The directory where soci artifact blobs are stored
	sociBlobDirectory = "/var/lib/soci-snapshotter-grpc/content/blobs/sha256/"

	// The directory whre soci indexes are stored
	sociIndexDirectory = "/var/lib/soci-snapshotter-grpc/indexes/"

	sociStorePath         = "/var/lib/soci-store/store"
	sociStoreCredCacheTtl = 1 * time.Hour
)

type pullStatus struct {
	mu     *sync.RWMutex
	pulled bool
}

type Provider struct {
	env                     environment.Env
	imageCacheAuth          *container.ImageCacheAuthenticator
	buildRoot               string
	imageStreamingEnabled   bool
	sociArtifactStoreClient socipb.SociArtifactStoreClient
	sociStoreKeychainClient sspb.LocalKeychainClient
}

func runSociStore(ctx context.Context) {
	for {
		log.Infof("Starting soci store")
		args := []string{fmt.Sprintf("--local_keychain_port=%d", *sociStoreKeychainPort)}
		if *sociStoreLogLevel != "" {
			args = append(args, fmt.Sprintf("--log-level=%s", *sociStoreLogLevel))
		}
		args = append(args, sociStorePath)
		cmd := exec.CommandContext(ctx, "soci-store", args...)
		logWriter := log.Writer("[socistore] ")
		cmd.Stderr = logWriter
		cmd.Stdout = logWriter
		cmd.Run()

		log.Infof("Detected soci store crash, restarting")
		metrics.PodmanSociStoreCrashes.Inc()
		// If the store crashed, the path must be unmounted to recover.
		syscall.Unmount(sociStorePath, 0)

		time.Sleep(500 * time.Millisecond)
	}
}

func NewProvider(env environment.Env, imageCacheAuthenticator *container.ImageCacheAuthenticator, buildRoot string) (*Provider, error) {
	var sociArtifactStoreClient socipb.SociArtifactStoreClient = nil
	var sociStoreKeychainClient sspb.LocalKeychainClient = nil
	if *imageStreamingEnabled {
		if *runSociStoreLocally {
			go runSociStore(env.GetServerContext())
		}

		// Configures podman to check soci store for image data.
		storageConf := `
[storage]
driver = "overlay"
runroot = "/run/containers/storage"
graphroot = "/var/lib/containers/storage"
[storage.options]
additionallayerstores=["/var/lib/soci-store/store:ref"]
`
		if err := os.WriteFile("/etc/containers/storage.conf", []byte(storageConf), 0644); err != nil {
			return nil, status.UnavailableErrorf("could not write storage config: %s", err)
		}

		if err := disk.WaitUntilExists(context.Background(), sociStorePath, disk.WaitOpts{}); err != nil {
			return nil, status.UnavailableErrorf("soci-store failed to start: %s", err)
		}

		// TODO(iain): there's a concurrency bug in soci-store that causes it
		// to die occasionally. Report the executor as unhealthy if the
		// soci-store directory doesn't exist for any reason.
		env.GetHealthChecker().AddHealthCheck(
			"soci_store", interfaces.CheckerFunc(
				func(ctx context.Context) error {
					if _, err := os.Stat(sociStorePath); err == nil {
						return nil
					} else {
						return fmt.Errorf("soci-store died (stat returned: %s)", err)
					}
				},
			),
		)

		var err error
		sociArtifactStoreClient, err = intializeSociArtifactStoreClient(env, *sociArtifactStoreTarget)
		if err != nil {
			return nil, err
		}
		sociStoreKeychainClient, err = initializeSociStoreKeychainClient(env, fmt.Sprintf("grpc://localhost:%d", *sociStoreKeychainPort))
		if err != nil {
			return nil, err
		}
	}
	return &Provider{
		env:                     env,
		imageCacheAuth:          imageCacheAuthenticator,
		imageStreamingEnabled:   *imageStreamingEnabled,
		sociArtifactStoreClient: sociArtifactStoreClient,
		sociStoreKeychainClient: sociStoreKeychainClient,
		buildRoot:               buildRoot,
	}, nil
}

func intializeSociArtifactStoreClient(env environment.Env, target string) (socipb.SociArtifactStoreClient, error) {
	conn, err := grpc_client.DialTarget(target)
	if err != nil {
		return nil, err
	}
	log.Infof("Connecting to app internal target: %s", target)
	env.GetHealthChecker().AddHealthCheck(
		"grpc_soci_artifact_store_connection", healthcheck.NewGRPCHealthCheck(conn))
	return socipb.NewSociArtifactStoreClient(conn), nil
}

func initializeSociStoreKeychainClient(env environment.Env, target string) (sspb.LocalKeychainClient, error) {
	conn, err := grpc_client.DialTarget(target)
	if err != nil {
		return nil, err
	}
	log.Infof("Connecting to soci store local keychain target: %s", target)
	env.GetHealthChecker().AddHealthCheck(
		"grpc_soci_store_keychain_connection", healthcheck.NewGRPCHealthCheck(conn))
	return sspb.NewLocalKeychainClient(conn), nil
}

func (p *Provider) New(ctx context.Context, props *platform.Properties, _ *repb.ScheduledTask, _ *rnpb.RunnerState, _ string) (container.CommandContainer, error) {
	imageIsPublic := props.ContainerRegistryUsername == "" && props.ContainerRegistryPassword == ""
	imageIsStreamable := p.imageStreamingEnabled && (imageIsPublic || *privateImageStreamingEnabled)
	if imageIsStreamable {
		if err := disk.WaitUntilExists(context.Background(), sociStorePath, disk.WaitOpts{}); err != nil {
			return nil, status.UnavailableErrorf("soci-store not available: %s", err)
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
		env:                     p.env,
		imageCacheAuth:          p.imageCacheAuth,
		image:                   props.ContainerImage,
		imageIsStreamable:       imageIsStreamable,
		sociArtifactStoreClient: p.sociArtifactStoreClient,
		sociStoreKeychainClient: p.sociStoreKeychainClient,

		buildRoot: p.buildRoot,
		options: &PodmanOptions{
			ForceRoot:            props.DockerForceRoot,
			Init:                 props.DockerInit,
			User:                 props.DockerUser,
			Network:              props.DockerNetwork,
			DefaultNetworkMode:   networkMode,
			CapAdd:               capAdd,
			Devices:              devices,
			Volumes:              volumes,
			Runtime:              *podmanRuntime,
			EnableStats:          *podmanEnableStats,
			EnableImageStreaming: props.EnablePodmanImageStreaming,
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
	Runtime            string
	// EnableStats determines whether to enable the stats API. This also enables
	// resource monitoring while tasks are in progress.
	EnableStats          bool
	EnableImageStreaming bool
}

// podmanCommandContainer containerizes a single command's execution using a Podman container.
type podmanCommandContainer struct {
	env            environment.Env
	imageCacheAuth *container.ImageCacheAuthenticator

	image     string
	buildRoot string
	workDir   string

	imageIsStreamable       bool
	sociArtifactStoreClient socipb.SociArtifactStoreClient
	sociStoreKeychainClient sspb.LocalKeychainClient

	options *PodmanOptions

	// name is the container name.
	name string

	stats containerStats

	// cid contains the container ID read from the cidfile.
	cid atomic.Value

	mu sync.Mutex // protects(removed)
	// removed is a flag that is set once Remove is called (before actually
	// removing the container).
	removed bool
}

func NewPodmanCommandContainer(env environment.Env, imageCacheAuth *container.ImageCacheAuthenticator, image, buildRoot string, options *PodmanOptions) container.CommandContainer {
	return &podmanCommandContainer{
		env:            env,
		imageCacheAuth: imageCacheAuth,
		image:          image,
		buildRoot:      buildRoot,
		options:        options,
	}
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
		args = append(args, "--dns=8.8.8.8")
		args = append(args, "--dns-search=.")
	}
	if c.options.CapAdd != "" {
		args = append(args, "--cap-add="+c.options.CapAdd)
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
	if c.options.Runtime != "" {
		args = append(args, "--runtime="+c.options.Runtime)
		if filepath.Base(c.options.Runtime) == "runsc" {
			// gVisor will attempt to setup cgroups, but podman has
			// already done that, so tell gVisor not to.
			args = append(args, "--runtime-flag=ignore-cgroups")
		}
	}
	if c.imageIsStreamable {
		args = append(args, enableStreamingStoreArg)
	}
	if c.options.Init {
		args = append(args, "--init")
	}
	return args
}

func (c *podmanCommandContainer) Run(ctx context.Context, command *repb.Command, workDir string, creds container.PullCredentials) *interfaces.CommandResult {
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

	if err := container.PullImageIfNecessary(ctx, c.env, c.imageCacheAuth, c, creds, c.image); err != nil {
		result.Error = status.UnavailableErrorf("failed to pull docker image: %s", err)
		return result
	}

	stopMonitoring, statsCh := c.monitor(ctx)
	defer stopMonitoring()

	podmanRunArgs := c.getPodmanRunArgs(workDir)
	for _, envVar := range command.GetEnvironmentVariables() {
		podmanRunArgs = append(podmanRunArgs, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	podmanRunArgs = append(podmanRunArgs, c.image)
	podmanRunArgs = append(podmanRunArgs, command.Arguments...)
	result = runPodman(ctx, "run", &container.Stdio{}, podmanRunArgs...)

	// Stop monitoring so that we can get stats.
	stopMonitoring()
	result.UsageStats = <-statsCh

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
	createResult := runPodman(ctx, "create", &container.Stdio{}, podmanRunArgs...)
	if err := c.maybeCleanupCorruptedImages(ctx, createResult); err != nil {
		log.Warningf("Failed to remove corrupted image: %s", err)
	}

	if err = createResult.Error; err != nil {
		return status.UnavailableErrorf("failed to create container: %s", err)
	}

	if createResult.ExitCode != 0 {
		return status.UnknownErrorf("podman create failed: exit code %d, stderr: %s", createResult.ExitCode, createResult.Stderr)
	}

	startResult := runPodman(ctx, "start", &container.Stdio{}, c.name)
	if startResult.Error != nil {
		return startResult.Error
	}
	if startResult.ExitCode != 0 {
		return status.UnknownErrorf("podman start failed: exit code %d, stderr: %s", startResult.ExitCode, startResult.Stderr)
	}
	return nil
}

func (c *podmanCommandContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *container.Stdio) *interfaces.CommandResult {
	// Reset usage stats since we're running a new task. Note: This throws away
	// any resource usage between the initial "Create" call and now, but that's
	// probably fine for our needs right now.
	c.stats.Reset()
	stopMonitoring, statsCh := c.monitor(ctx)
	defer stopMonitoring()
	podmanRunArgs := make([]string, 0, 2*len(cmd.GetEnvironmentVariables())+len(cmd.Arguments)+1)
	for _, envVar := range cmd.GetEnvironmentVariables() {
		podmanRunArgs = append(podmanRunArgs, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	podmanRunArgs = addUserArgs(podmanRunArgs, c.options)
	if strings.ToLower(c.options.Network) == "off" {
		podmanRunArgs = append(podmanRunArgs, "--network=none")
	}
	if stdio.Stdin != nil {
		podmanRunArgs = append(podmanRunArgs, "--interactive")
	}
	podmanRunArgs = append(podmanRunArgs, c.name)
	podmanRunArgs = append(podmanRunArgs, cmd.Arguments...)
	// Podman doesn't provide a way to find out whether an exec process was
	// killed. Instead, `podman exec` returns 137 (= 128 + SIGKILL(9)). However,
	// this exit code is also valid as a regular exit code returned by a command
	// during a normal execution, so we are overly cautious here and only
	// interpret this code specially when the container was removed and we are
	// expecting a SIGKILL as a result.
	res := runPodman(ctx, "exec", stdio, podmanRunArgs...)
	stopMonitoring()
	res.UsageStats = <-statsCh
	c.mu.Lock()
	removed := c.removed
	c.mu.Unlock()
	if removed && res.ExitCode == podmanExecSIGKILLExitCode {
		res.ExitCode = commandutil.KilledExitCode
		res.Error = commandutil.ErrSIGKILL
	}
	return res
}

func (c *podmanCommandContainer) IsImageCached(ctx context.Context) (bool, error) {
	if c.imageIsStreamable {
		return true, nil
	}

	// Try to avoid the `pull` command which results in a network roundtrip.
	listResult := runPodman(ctx, "image", &container.Stdio{}, "inspect", "--format={{.ID}}", c.image)
	if listResult.ExitCode == podmanInternalExitCode {
		return false, nil
	} else if listResult.Error != nil {
		return false, listResult.Error
	}

	if strings.TrimSpace(string(listResult.Stdout)) != "" {
		// Found at least one image matching the ref; `docker run` should succeed
		// without pulling the image.
		return true, nil
	}
	return false, nil
}

func (c *podmanCommandContainer) PullImage(ctx context.Context, creds container.PullCredentials) error {
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
		startTime := time.Now()
		if err := c.getSociArtifacts(ctx, creds); err != nil {
			return err
		}
		metrics.PodmanGetSociArtifactsLatencyUsec.
			With(prometheus.Labels{metrics.ContainerImageTag: c.image}).
			Observe(float64(time.Since(startTime).Microseconds()))
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

func (c *podmanCommandContainer) getSociArtifacts(ctx context.Context, creds container.PullCredentials) error {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, c.env)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(sociBlobDirectory, 0644); err != nil {
		return err
	}
	req := socipb.GetArtifactsRequest{
		Image: c.image,
		Credentials: &rgpb.Credentials{
			Username: creds.Username,
			Password: creds.Password,
		},
		Platform: &rgpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
			// TODO: support CPU variants. Details here:
			// https://github.com/opencontainers/image-spec/blob/v1.0.0/image-index.md
		},
	}
	resp, err := c.sociArtifactStoreClient.GetArtifacts(ctx, &req)
	if err != nil {
		log.Infof("Error fetching soci artifacts %v", err)
		// Fall back to pulling the image without streaming.
		return nil
	}
	for _, artifact := range resp.Artifacts {
		if artifact.Type == socipb.Type_UNKNOWN_TYPE {
			return status.InternalErrorf("SociArtifactStore returned unknown artifact with hash %s" + artifact.Digest.Hash)
		} else if artifact.Type == socipb.Type_SOCI_INDEX {
			// Write the index file, this is what the snapshotter uses to
			// associate the requested image with its soci index.
			if err = os.MkdirAll(sociIndexDirectory, 0644); err != nil {
				return err
			}
			sociIndexDigest := godigest.NewDigestFromEncoded(godigest.SHA256, artifact.Digest.Hash)
			imageDigest := godigest.NewDigestFromEncoded(godigest.SHA256, resp.ImageId)
			if err = os.WriteFile(sociIndex(imageDigest), []byte(sociIndexDigest.String()), 0644); err != nil {
				return err
			}
		}
		blobFile, err := os.Create(sociBlob(artifact.Digest))
		defer blobFile.Close()
		if err != nil {
			return err
		}
		resourceName := digest.NewResourceName(artifact.Digest, "" /*=instanceName -- not used */, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
		if err = cachetools.GetBlob(ctx, c.env.GetByteStreamClient(), resourceName, blobFile); err != nil {
			return err
		}
	}
	return nil
}

func sociIndex(digest godigest.Digest) string {
	return sociIndexDirectory + digest.Encoded()
}

func sociBlob(digest *repb.Digest) string {
	return sociBlobDirectory + digest.Hash
}

// cidFilePath returns the path to the container's cidfile. Podman will write
// the container's ID to this file since we run containers with the --cidfile
// arg. The file is given the same name as the task workspace directory, plus a
// ".cid" extension, such as "/tmp/remote_build/{{WORKSPACE_ID}}.cid". Note:
// this logic depends on the workspace parent directory already existing.
func (c *podmanCommandContainer) cidFilePath() string {
	return c.workDir + ".cid"
}

// monitor starts a goroutine to monitor the container's resource usage. The
// returned func stops monitoring. It must be called, or else a goroutine leak
// may occur. Monitoring can safely be stopped more than once. The returned
// channel should be received from at most once, *after* calling the returned
// stop function. The received value can be nil if stats were not successfully
// sampled at least once.
func (c *podmanCommandContainer) monitor(ctx context.Context) (context.CancelFunc, chan *repb.UsageStats) {
	ctx, cancel := context.WithCancel(ctx)
	result := make(chan *repb.UsageStats, 1)
	go func() {
		defer close(result)
		if !c.options.EnableStats {
			return
		}
		defer container.Metrics.Unregister(c)
		var last *repb.UsageStats
		var lastErr error

		start := time.Now()
		defer func() {
			// Only log an error if the task ran long enough that we could
			// reasonably expect to sample stats at least once while it was
			// executing. Note that we can't sample stats until podman creates
			// the container, which can take a few hundred ms or possibly longer
			// if the executor is heavily loaded.
			dur := time.Since(start)
			if last == nil && dur > 1*time.Second && lastErr != nil {
				log.Warningf("Failed to read container stats: %s", lastErr)
			}
		}()

		timer := time.NewTicker(statsPollInterval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				result <- last
				return
			case <-timer.C:
				stats, err := c.Stats(ctx)
				if err != nil {
					lastErr = err
					continue
				}
				container.Metrics.Observe(c, stats)
				last = stats
			}
		}
	}()
	return cancel, result
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

func (c *podmanCommandContainer) pullImage(ctx context.Context, creds container.PullCredentials) error {
	podmanArgs := make([]string, 0, 2)

	if c.imageIsStreamable {
		// Ideally we would not have to do a "podman pull" when image streaming
		// is enabled, but we suspect there's a concurrency bug in podman
		// related to looking up additional layer information from providers
		// like soci-store. To work around this bug, we do a single synchronous
		// pull (locked by caller) which causes the layer metadata to be
		// pre-populated in podman storage.
		// The pull can be removed once there's a new podman version that
		// includes the fix for this bug.
		// TODO(iain): diagnose and report the bug.
		podmanArgs = append(podmanArgs, enableStreamingStoreArg)

		if !creds.IsEmpty() {
			// The soci-store, which streams container images from the remote
			// repository and makes them available to the executor via a FUSE
			// runs as a separate process and thus does not have access to the
			// user-provided container access credentials. To address this,
			// send the credentials to the store via this gRPC service so it
			// can cache and use them.
			putCredsReq := sspb.PutCredentialsRequest{
				ImageName: c.image,
				Credentials: &sspb.Credentials{
					Username: creds.Username,
					Password: creds.Password,
				},
				ExpiresInSeconds: int64(sociStoreCredCacheTtl.Seconds()),
			}
			if _, err := c.sociStoreKeychainClient.PutCredentials(ctx, &putCredsReq); err != nil {
				return err
			}
		}
	}

	if !creds.IsEmpty() {
		podmanArgs = append(podmanArgs, fmt.Sprintf(
			"--creds=%s:%s",
			creds.Username,
			creds.Password,
		))
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
	stdio := &container.Stdio{Stderr: output, Stdout: output}
	if *podmanPullLogLevel != "" {
		stdio.Stderr = io.MultiWriter(stdio.Stderr, log.CtxWriter(ctx, "[podman pull] "))
		stdio.Stdout = io.MultiWriter(stdio.Stdout, log.CtxWriter(ctx, "[podman pull] "))
	}
	pullResult := runPodman(pullCtx, "pull", stdio, podmanArgs...)
	if pullResult.Error != nil {
		return pullResult.Error
	}
	if pullResult.ExitCode != 0 {
		return status.UnavailableErrorf("podman pull failed: exit code %d, output: %s", pullResult.ExitCode, output.String())
	}
	return nil
}

func (c *podmanCommandContainer) Remove(ctx context.Context) error {
	c.mu.Lock()
	c.removed = true
	c.mu.Unlock()
	os.RemoveAll(c.cidFilePath()) // intentionally ignoring error.
	res := runPodman(ctx, "kill", &container.Stdio{}, "--signal=KILL", c.name)
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode == 0 || strings.Contains(string(res.Stderr), "no such container") {
		return nil
	}
	return status.UnknownErrorf("podman remove failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
}

func (c *podmanCommandContainer) Pause(ctx context.Context) error {
	res := runPodman(ctx, "pause", &container.Stdio{}, c.name)
	if res.ExitCode != 0 {
		return status.UnknownErrorf("podman pause failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
	}
	return nil
}

func (c *podmanCommandContainer) Unpause(ctx context.Context) error {
	res := runPodman(ctx, "unpause", &container.Stdio{}, c.name)
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode != 0 {
		return status.UnknownErrorf("podman unpause failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
	}
	return nil
}

// readRawStats reads the raw stats from the cgroup fs. Note that this does not
// work in rootless mode for cgroup v1.
func (c *podmanCommandContainer) readRawStats(ctx context.Context) (*repb.UsageStats, error) {
	cid, err := c.getCID(ctx)
	if err != nil {
		return nil, err
	}
	memUsagePath := strings.ReplaceAll(*memUsagePathTemplate, "{{.ContainerID}}", cid)
	cpuUsagePath := strings.ReplaceAll(*cpuUsagePathTemplate, "{{.ContainerID}}", cid)

	memUsageBytes, err := readInt64FromFile(memUsagePath)
	if err != nil {
		return nil, err
	}
	cpuNanos, err := readInt64FromFile(cpuUsagePath)
	if err != nil {
		return nil, err
	}
	return &repb.UsageStats{
		MemoryBytes: memUsageBytes,
		CpuNanos:    cpuNanos,
	}, nil
}

func (c *podmanCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	if !c.options.EnableStats {
		return nil, nil
	}

	current, err := c.readRawStats(ctx)
	if err != nil {
		return nil, err
	}

	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()

	stats := proto.Clone(current).(*repb.UsageStats)
	stats.CpuNanos = stats.CpuNanos - c.stats.baselineCPUNanos
	if current.MemoryBytes > c.stats.peakMemoryUsageBytes {
		c.stats.peakMemoryUsageBytes = current.MemoryBytes
	}
	stats.PeakMemoryBytes = c.stats.peakMemoryUsageBytes
	c.stats.last = current
	return stats, nil
}

func (c *podmanCommandContainer) State(ctx context.Context) (*rnpb.ContainerState, error) {
	return nil, status.UnimplementedError("not implemented")
}

func runPodman(ctx context.Context, subCommand string, stdio *container.Stdio, args ...string) *interfaces.CommandResult {
	command := []string{
		"podman",
		subCommand,
	}

	command = append(command, args...)
	// Note: we don't collect stats on the podman process, and instead use
	// cgroups for stats accounting.
	result := commandutil.Run(ctx, &repb.Command{Arguments: command}, "" /*=workDir*/, nil /*=statsListener*/, stdio)
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
	return removeImage(ctx, c.image)
}

func removeImage(ctx context.Context, imageName string) error {
	ctx, cancel := background.ExtendContextForFinalization(ctx, containerFinalizationTimeout)
	defer cancel()

	result := runPodman(ctx, "rmi", &container.Stdio{}, imageName)
	if result.Error != nil {
		return result.Error
	}
	if result.ExitCode == 0 || strings.Contains(string(result.Stderr), "image not known") {
		return nil
	}
	return status.UnknownErrorf("podman rmi failed: %s", string(result.Stderr))
}

// Configure the secondary network for podman so that traffic from podman will be routed through
// the secondary network interface instead of the primary network.
func ConfigureSecondaryNetwork(ctx context.Context) error {
	if !networking.IsSecondaryNetworkEnabled() {
		// No need to configure secondary network for podman.
		return nil
	}
	// Hack: run a dummy podman container to setup default podman bridge network in ip route.
	// "podman run --rm busybox sh". This should setup the following in ip route:
	// "10.88.0.0/16 dev cni-podman0 proto kernel scope link src 10.88.0.1 linkdown"
	result := runPodman(ctx, "run", &container.Stdio{}, "--rm", "busybox", "sh")
	if result.Error != nil {
		return result.Error
	}
	if result.ExitCode != 0 {
		return status.UnknownError("failed to setup podman default network")
	}

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

// readInt64FromFile reads a file expected to contain a single int64.
func readInt64FromFile(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

type containerStats struct {
	mu sync.Mutex
	// last is the last recorded stats.
	last *repb.UsageStats
	// peakMemoryUsageBytes is the max memory usage from the last task execution.
	// This is reset between tasks so that we can determine a task's peak memory
	// usage when using a recycled runner.
	peakMemoryUsageBytes int64
	// baselineCPUNanos is the CPU usage from when a task last finished executing.
	// This is needed so that we can determine a task's CPU usage when using a
	// recycled runner.
	baselineCPUNanos int64
}

// Reset resets resource usage counters in preparation for a new task, so that
// the new task's resource usage can be accounted for. It should be called
// at the beginning of Exec() in the container lifecycle.
func (s *containerStats) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		s.baselineCPUNanos = 0
	} else {
		s.baselineCPUNanos = s.last.CpuNanos
	}
	s.last = nil
	s.peakMemoryUsageBytes = 0
}
