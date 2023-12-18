package podman

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
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
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

var (
	// TODO(#2523): remove this flag.
	podmanPullLogLevel = flag.String("executor.podman.pull_log_level", "", "Level at which to log `podman pull` command output. Should be one of the standard log levels, all lowercase.")

	// Note: to get these cgroup paths, run a podman container like
	//     podman run --rm --name sleepy busybox sleep infinity
	// then look at the output of
	//     find /sys/fs/cgroup | grep libpod-$(podman container inspect sleepy | jq -r '.[0].Id')

	memUsagePathTemplate = flag.String("executor.podman.memory_usage_path_template", "", "Go template specifying a path pointing to a container's current memory usage, in bytes. {{.ContainerID}} will be replaced by the containerID.", flag.Deprecated("This is now detected automatically. If paths aren't detected properly, please report a bug."))
	cpuUsagePathTemplate = flag.String("executor.podman.cpu_usage_path_template", "", "Go template specifying a path pointing to a container's total CPU usage, in CPU nanoseconds. {{.ContainerID}} will be replaced by the containerID.", flag.Deprecated("This is now detected automatically. If paths aren't detected properly, please report a bug."))

	privateImageStreamingEnabled = flag.Bool("executor.podman.enable_private_image_streaming", false, "If set and --executor.podman.enable_image_streaming is set, all private (authenticated) podman images are streamed using soci artifacts generated and stored in the apps.")

	pullTimeout = flag.Duration("executor.podman.pull_timeout", 10*time.Minute, "Timeout for image pulls.")

	podmanRuntime       = flag.String("executor.podman.runtime", "", "Enables running podman with other runtimes, like gVisor (runsc).")
	podmanStorageDriver = flag.String("executor.podman.storage_driver", "overlay", "The podman storage driver to use.")
	podmanEnableStats   = flag.Bool("executor.podman.enable_stats", false, "Whether to enable cgroup-based podman stats.")
	transientStore      = flag.Bool("executor.podman.transient_store", false, "Enables --transient-store for podman commands.", flag.Deprecated("--transient-store is now always applied if the podman version supports it"))
	podmanDNS           = flag.String("executor.podman.dns", "8.8.8.8", "Specifies a custom DNS server for podman to use. Defaults to 8.8.8.8. If set to empty, no --dns= flag will be passed to podman.")

	// Additional time used to kill the container if the command doesn't exit cleanly
	containerFinalizationTimeout = 10 * time.Second

	storageErrorRegex = regexp.MustCompile(`(?s)A storage corruption might have occurred.*Error: readlink.*no such file or directory`)
	userRegex         = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*(:[a-z0-9_-]*)?$`)

	// Detected podman version.
	podmanVersion *semver.Version
	// Min podman version supporting the '--transient-store' flag.
	transientStoreMinVersion = semver.MustParse("4.3.0")

	// A map from image name to pull status. This is used to avoid parallel pulling of the same image.
	pullOperations sync.Map
)

const (
	// Podman exit codes
	podmanInternalExitCode           = 125
	podmanCommandNotRunnableExitCode = 126
	podmanCommandNotFoundExitCode    = 127

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
	cgroupPaths      *cgroup.Paths
	buildRoot        string
	sociStore        soci_store.Store
	imageExistsCache *imageExistsCache
}

func NewProvider(env environment.Env, buildRoot string) (*Provider, error) {
	// Eagerly init podman version so we can crash the executor if it fails.
	podmanVersion, err := getPodmanVersion()
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

	return &Provider{
		env: env,
		cgroupPaths: &cgroup.Paths{
			MemoryTemplate: *memUsagePathTemplate,
			CPUTemplate:    *cpuUsagePathTemplate,
		},
		sociStore:        sociStore,
		buildRoot:        buildRoot,
		imageExistsCache: imageExistsCache,
	}, nil
}

var getPodmanVersion = sync.OnceValues(func() (*semver.Version, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("podman", "version", fmt.Sprintf("--storage-driver=%s", *podmanStorageDriver), "--format={{.Client.Version}}")
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, status.InternalErrorf("command failed: %s: %s", err, stderr.String())
	}
	return semver.NewVersion(strings.TrimSpace(stdout.String()))
})

func (p *Provider) New(ctx context.Context, props *platform.Properties, _ *repb.ScheduledTask, _ *rnpb.RunnerState, _ string) (container.CommandContainer, error) {
	imageIsPublic := props.ContainerRegistryUsername == "" && props.ContainerRegistryPassword == ""
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
		cgroupPaths:       p.cgroupPaths,
		image:             props.ContainerImage,
		imageIsStreamable: imageIsStreamable,
		sociStore:         p.sociStore,
		imageExistsCache:  p.imageExistsCache,
		buildRoot:         p.buildRoot,
		options: &PodmanOptions{
			ForceRoot:          props.DockerForceRoot,
			Init:               props.DockerInit,
			User:               props.DockerUser,
			Network:            props.DockerNetwork,
			DefaultNetworkMode: networkMode,
			CapAdd:             capAdd,
			Devices:            devices,
			Volumes:            volumes,
			Runtime:            *podmanRuntime,
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
	Runtime            string
	// EnableStats determines whether to enable the stats API. This also enables
	// resource monitoring while tasks are in progress.
	EnableStats bool
}

// podmanCommandContainer containerizes a single command's execution using a Podman container.
type podmanCommandContainer struct {
	env              environment.Env
	imageExistsCache *imageExistsCache
	cgroupPaths      *cgroup.Paths

	image     string
	buildRoot string
	workDir   string

	imageIsStreamable bool
	sociStore         soci_store.Store

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
	args = append(args, c.sociStore.GetPodmanArgs()...)
	if c.options.Init {
		args = append(args, "--init")
	}
	return args
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

	stopMonitoring, statsCh := c.monitor(ctx)
	defer stopMonitoring()

	podmanRunArgs := c.getPodmanRunArgs(workDir)
	for _, envVar := range command.GetEnvironmentVariables() {
		podmanRunArgs = append(podmanRunArgs, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	podmanRunArgs = append(podmanRunArgs, c.image)
	podmanRunArgs = append(podmanRunArgs, command.Arguments...)
	result = runPodman(ctx, "run", &commandutil.Stdio{}, podmanRunArgs...)

	if result.ExitCode == podmanCommandNotRunnableExitCode {
		log.CtxInfof(ctx, "podman failed to run command")
	} else if result.ExitCode == podmanCommandNotFoundExitCode {
		log.CtxInfof(ctx, "podman failed to find command")
	}

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
	createResult := runPodman(ctx, "create", &commandutil.Stdio{}, podmanRunArgs...)
	if err := c.maybeCleanupCorruptedImages(ctx, createResult); err != nil {
		log.Warningf("Failed to remove corrupted image: %s", err)
	}

	if err = createResult.Error; err != nil {
		return status.UnavailableErrorf("failed to create container: %s", err)
	}

	if createResult.ExitCode != 0 {
		return status.UnknownErrorf("podman create failed: exit code %d, stderr: %s", createResult.ExitCode, createResult.Stderr)
	}

	startResult := runPodman(ctx, "start", &commandutil.Stdio{}, c.name)
	if startResult.Error != nil {
		return startResult.Error
	}
	if startResult.ExitCode != 0 {
		return status.UnknownErrorf("podman start failed: exit code %d, stderr: %s", startResult.ExitCode, startResult.Stderr)
	}
	return nil
}

func (c *podmanCommandContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *commandutil.Stdio) *interfaces.CommandResult {
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
	if c.imageExistsCache.Exists(c.image) {
		return true, nil
	}

	res := runPodman(ctx, "image", &commandutil.Stdio{}, "exists", c.image)
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

func (c *podmanCommandContainer) pullImage(ctx context.Context, creds oci.Credentials) error {
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
	stdio := &commandutil.Stdio{Stderr: output, Stdout: output}
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
	res := runPodman(ctx, "kill", &commandutil.Stdio{}, "--signal=KILL", c.name)
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode == 0 || strings.Contains(string(res.Stderr), "no such container") {
		return nil
	}
	return status.UnknownErrorf("podman remove failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
}

func (c *podmanCommandContainer) Pause(ctx context.Context) error {
	res := runPodman(ctx, "pause", &commandutil.Stdio{}, c.name)
	if res.ExitCode != 0 {
		return status.UnknownErrorf("podman pause failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
	}
	return nil
}

func (c *podmanCommandContainer) Unpause(ctx context.Context) error {
	res := runPodman(ctx, "unpause", &commandutil.Stdio{}, c.name)
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode != 0 {
		return status.UnknownErrorf("podman unpause failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
	}
	return nil
}

func (c *podmanCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	if !c.options.EnableStats {
		return nil, nil
	}

	cid, err := c.getCID(ctx)
	if err != nil {
		return nil, err
	}

	current, err := c.cgroupPaths.Stats(ctx, cid)
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

func runPodman(ctx context.Context, subCommand string, stdio *commandutil.Stdio, args ...string) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx)
	tracing.AddStringAttributeToCurrentSpan(ctx, "podman.subcommand", subCommand)
	defer span.End()
	command := []string{"podman"}
	podmanVersion, err := getPodmanVersion()
	if err != nil {
		return commandutil.ErrorResult(err)
	}
	if !podmanVersion.LessThan(transientStoreMinVersion) {
		// Use transient store to reduce contention.
		// See https://github.com/containers/podman/issues/19824
		command = append(command, "--transient-store")
	}
	command = append(command, fmt.Sprintf("--storage-driver=%s", *podmanStorageDriver))
	command = append(command, subCommand)
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
	return c.removeImage(ctx, c.image)
}

func (c *podmanCommandContainer) removeImage(ctx context.Context, imageName string) error {
	ctx, cancel := background.ExtendContextForFinalization(ctx, containerFinalizationTimeout)
	defer cancel()

	defer c.imageExistsCache.Remove(imageName)

	result := runPodman(ctx, "rmi", &commandutil.Stdio{}, imageName)
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
	result := runPodman(ctx, "run", &commandutil.Stdio{}, "--rm", "busybox", "sh")
	if result.Error != nil {
		return status.UnknownErrorf("failed to setup podman default network: podman run failed: %s (stderr: %q)", result.Error, string(result.Stderr))
	}
	if result.ExitCode != 0 {
		return status.UnknownErrorf("failed to setup podman default network: podman run exited with code %d: %q", result.ExitCode, string(result.Stderr))
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
