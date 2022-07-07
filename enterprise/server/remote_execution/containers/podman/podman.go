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
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	regpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	// Note: to get these cgroup paths, run a podman container like
	//     podman run --rm --name sleepy busybox sleep infinity
	// then look at the output of
	//     find /sys/fs/cgroup | grep libpod-$(podman container inspect sleepy | jq -r '.[0].Id')

	memUsagePathTemplate             = flag.String("executor.podman.memory_usage_path_template", "/sys/fs/cgroup/memory/libpod_parent/libpod-{{.ContainerID}}/memory.usage_in_bytes", "Go template specifying a path pointing to a container's current memory usage, in bytes. Templated with `ContainerID`.")
	cpuUsagePathTemplate             = flag.String("executor.podman.cpu_usage_path_template", "/sys/fs/cgroup/cpuacct/libpod_parent/libpod-{{.ContainerID}}/cpuacct.usage", "Go template specifying a path pointing to a container's total CPU usage, in CPU nanoseconds. Templated with `ContainerID`.")
	imageStreamingRegistryGRPCTarget = flag.String("executor.podman.image_streaming.registry_grpc_target", "", "gRPC endpoint of BuildBuddy registry")
	imageStreamingRegistryHTTPTarget = flag.String("executor.podman.image_streaming.registry_http_target", "", "HTTP endpoint of the BuildBuddy registry")

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

	// optImageRefCacheSize is the size of the cache used to store mappings from
	// the original image name to the optimized image name.
	optImageRefCacheSize = 1000
)

type optImageCache struct {
	cache interfaces.LRU
}

func (c *optImageCache) get(key string) (string, error) {
	if v, ok := c.cache.Get(key); ok {
		return v.(string), nil
	}
	return "", nil
}

func (c *optImageCache) put(key string, optImage string) {
	c.cache.Add(key, optImage)
}

func newOptImageCache() (*optImageCache, error) {
	l, err := lru.NewLRU(&lru.Config{
		SizeFn: func(value interface{}) int64 {
			return 1
		},
		MaxSize: optImageRefCacheSize,
	})
	if err != nil {
		return nil, err
	}
	return &optImageCache{cache: l}, nil
}

type pullStatus struct {
	mu     *sync.RWMutex
	pulled bool
}

type Provider struct {
	env                   environment.Env
	imageCacheAuth        *container.ImageCacheAuthenticator
	buildRoot             string
	imageStreamingEnabled bool
	regClient             regpb.RegistryClient
	optImageCache         *optImageCache
}

func NewProvider(env environment.Env, imageCacheAuthenticator *container.ImageCacheAuthenticator, buildRoot string) (*Provider, error) {
	c, err := newOptImageCache()
	if err != nil {
		return nil, err
	}
	var regClient regpb.RegistryClient
	if *imageStreamingRegistryGRPCTarget != "" {
		conn, err := grpc_client.DialTarget(*imageStreamingRegistryGRPCTarget)
		if err != nil {
			return nil, err
		}
		regClient = regpb.NewRegistryClient(conn)
	}

	imageStreamingEnabled := *imageStreamingRegistryHTTPTarget != "" && *imageStreamingRegistryGRPCTarget != ""
	if imageStreamingEnabled {
		storeConf := `
no_background_fetch = true
`
		if _, err := disk.WriteFile(env.GetServerContext(), "/etc/stargz-store/config.toml", []byte(storeConf)); err != nil {
			return nil, status.UnavailableErrorf("could not write stargzstore config: %s", err)
		}

		log.Infof("Starting stargz store")
		cmd := exec.CommandContext(env.GetServerContext(), "stargz-store", "/var/lib/stargz-store/store")
		logWriter := log.Writer("[stargzstore] ")
		cmd.Stderr = logWriter
		cmd.Stdout = logWriter
		if err := cmd.Start(); err != nil {
			return nil, status.UnavailableErrorf("could not start stargz store: %s", err)
		}

		// Configures podman to check stargz store for image data.
		storageConf := `
[storage]
driver = "overlay"
runroot = "/run/containers/storage"
graphroot = "/var/lib/containers/storage"
[storage.options]
additionallayerstores=["/var/lib/stargz-store/store:ref"]
`
		if err := os.WriteFile("/etc/containers/storage.conf", []byte(storageConf), 0644); err != nil {
			return nil, status.UnavailableErrorf("could not write storage config: %s", err)
		}
	}

	return &Provider{
		env:                   env,
		imageCacheAuth:        imageCacheAuthenticator,
		buildRoot:             buildRoot,
		imageStreamingEnabled: imageStreamingEnabled,
		regClient:             regClient,
		optImageCache:         c,
	}, nil
}

func (p *Provider) NewContainer(image string, options *PodmanOptions) container.CommandContainer {
	return &podmanCommandContainer{
		env:                   p.env,
		imageCacheAuth:        p.imageCacheAuth,
		image:                 image,
		registryClient:        p.regClient,
		imageStreamingEnabled: *imageStreamingRegistryGRPCTarget != "" && *imageStreamingRegistryHTTPTarget != "",
		optImageCache:         p.optImageCache,
		buildRoot:             p.buildRoot,
		options:               options,
	}
}

type PodmanOptions struct {
	ForceRoot bool
	User      string
	Network   string
	CapAdd    string
	Devices   []container.DockerDeviceMapping
	Volumes   []string
	Runtime   string
	// EnableStats determines whether to enable the stats API. This also enables
	// resource monitoring while tasks are in progress.
	EnableStats bool
}

// podmanCommandContainer containerizes a command's execution using a Podman container.
// between containers.
type podmanCommandContainer struct {
	env            environment.Env
	imageCacheAuth *container.ImageCacheAuthenticator

	image     string
	buildRoot string
	workDir   string

	imageStreamingEnabled bool
	optImageCache         *optImageCache
	registryClient        regpb.RegistryClient

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
		"--dns",
		"8.8.8.8",
		"--dns-search",
		".",
		"--volume",
		fmt.Sprintf(
			"%s:%s",
			filepath.Join(c.buildRoot, filepath.Base(workDir)),
			workDir,
		),
	}
	if c.options.ForceRoot {
		args = append(args, "--user=0:0")
	} else if c.options.User != "" && userRegex.MatchString(c.options.User) {
		args = append(args, "--user="+c.options.User)
	}
	if strings.ToLower(c.options.Network) == "off" {
		args = append(args, "--network=none")
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

	image, err := c.targetImage(ctx)
	if err != nil {
		result.Error = err
		return result
	}

	podmanRunArgs := c.getPodmanRunArgs(workDir)
	for _, envVar := range command.GetEnvironmentVariables() {
		podmanRunArgs = append(podmanRunArgs, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	podmanRunArgs = append(podmanRunArgs, image)
	podmanRunArgs = append(podmanRunArgs, command.Arguments...)
	result = runPodman(ctx, "run", &container.Stdio{}, podmanRunArgs...)

	// Stop monitoring so that we can get stats.
	stopMonitoring()
	if stats := <-statsCh; stats != nil {
		result.UsageStats = stats.ToProto()
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

func (c *podmanCommandContainer) optImageRefKey(ctx context.Context) (string, error) {
	groupID := ""
	u, err := perms.AuthenticatedUser(ctx, c.env)
	if err != nil {
		if !perms.IsAnonymousUserError(err) {
			return "", err
		}
	} else {
		groupID = u.GetGroupID()
	}

	return fmt.Sprintf("%s-%s", groupID, c.image), nil
}

func (c *podmanCommandContainer) targetImage(ctx context.Context) (string, error) {
	if !c.imageStreamingEnabled {
		return c.image, nil
	}

	key, err := c.optImageRefKey(ctx)
	if err != nil {
		return "", err
	}
	optImage, err := c.optImageCache.get(key)
	if err != nil {
		return "", err
	}
	if optImage == "" {
		return "", status.FailedPreconditionErrorf("optimized image not yet resolved")
	}
	return optImage, nil
}

func (c *podmanCommandContainer) resolveTargetImage(ctx context.Context, credentials container.PullCredentials) (string, error) {
	if !c.imageStreamingEnabled {
		return c.image, nil
	}

	if c.registryClient == nil || *imageStreamingRegistryHTTPTarget == "" {
		return "", status.FailedPreconditionErrorf("streaming enabled, but registry client or http target are not set")
	}

	log.CtxInfof(ctx, "Resolving optimized image for %q", c.image)
	req := &regpb.GetOptimizedImageRequest{
		Image: c.image,
		Platform: &regpb.Platform{
			Arch: runtime.GOARCH,
			Os:   runtime.GOOS,
		},
	}
	if !credentials.IsEmpty() {
		req.ImageCredentials = &regpb.Credentials{
			Username: credentials.Username,
			Password: credentials.Password,
		}
	}

	// Clear the JWT from the RPC so that the blobs are stored as anonymous data
	// until we implement CAS auth for image streaming.
	rpcCtx := context.WithValue(ctx, "x-buildbuddy-jwt", nil)
	rsp, err := c.registryClient.GetOptimizedImage(rpcCtx, req)
	if err != nil {
		return "", status.UnavailableErrorf("could not resolve optimized image for %q: %s", c.image, err)
	}
	optImage := fmt.Sprintf("%s/%s", *imageStreamingRegistryHTTPTarget, rsp.GetOptimizedImage())
	log.CtxInfof(ctx, "Resolved optimized image %q for %q", optImage, c.image)
	key, err := c.optImageRefKey(ctx)
	c.optImageCache.put(key, optImage)
	return optImage, nil
}

func (c *podmanCommandContainer) Create(ctx context.Context, workDir string) error {
	containerName, err := generateContainerName()
	if err != nil {
		return status.UnavailableErrorf("failed to generate podman container name: %s", err)
	}
	c.name = containerName
	c.workDir = workDir

	podmanRunArgs := c.getPodmanRunArgs(workDir)
	image, err := c.targetImage(ctx)
	if err != nil {
		return err
	}
	podmanRunArgs = append(podmanRunArgs, image)
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
	if c.options.ForceRoot {
		podmanRunArgs = append(podmanRunArgs, "--user=0:0")
	} else if c.options.User != "" && userRegex.MatchString(c.options.User) {
		podmanRunArgs = append(podmanRunArgs, "--user="+c.options.User)
	}
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
	if stats := <-statsCh; stats != nil {
		res.UsageStats = stats.ToProto()
	}
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
	if c.imageStreamingEnabled {
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
	if err := c.pullImage(ctx, creds); err != nil {
		return err
	}
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
func (c *podmanCommandContainer) monitor(ctx context.Context) (context.CancelFunc, chan *container.Stats) {
	ctx, cancel := context.WithCancel(ctx)
	result := make(chan *container.Stats, 1)
	go func() {
		defer close(result)
		if !c.options.EnableStats {
			return
		}
		defer container.Metrics.Unregister(c)
		var last *container.Stats
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
	targetImage := c.image
	if c.imageStreamingEnabled {
		// Always re-resolve image when a pull is requested. This takes care of
		// re-validating the passed credentials.
		img, err := c.resolveTargetImage(ctx, creds)
		if err != nil {
			return err
		}

		// Ideally we would not have to do a "podman pull" when image streaming
		// is enabled, but there's a concurrency bug in podman related to
		// looking up additional layer information from providers like
		// stargz-store. To work around this bug, we do a single synchronous
		// pull (locked by caller) which causes the layer metadata to be
		// pre-populated in podman storage.
		// The pull can be removed once there's a new podman version that
		// includes the fix for
		// https://github.com/containers/storage/issues/1263
		targetImage = img
	}

	podmanArgs := make([]string, 0, 2)
	if !creds.IsEmpty() {
		podmanArgs = append(podmanArgs, fmt.Sprintf(
			"--creds=%s:%s",
			creds.Username,
			creds.Password,
		))
	}
	podmanArgs = append(podmanArgs, targetImage)
	// Use server context instead of ctx to make sure that "podman pull" is not killed when the context
	// is cancelled. If "podman pull" is killed when copying a parent layer, it will result in
	// corrupted storage.  More details see https://github.com/containers/storage/issues/1136.
	pullResult := runPodman(c.env.GetServerContext(), "pull", &container.Stdio{}, podmanArgs...)
	if pullResult.Error != nil {
		return pullResult.Error
	}
	if pullResult.ExitCode != 0 {
		return status.UnknownErrorf("podman pull failed: exit code %d, stderr: %s", pullResult.ExitCode, string(pullResult.Stderr))
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
func (c *podmanCommandContainer) readRawStats(ctx context.Context) (*container.Stats, error) {
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
	return &container.Stats{
		MemoryUsageBytes: memUsageBytes,
		CPUNanos:         cpuNanos,
	}, nil
}

func (c *podmanCommandContainer) Stats(ctx context.Context) (*container.Stats, error) {
	if !c.options.EnableStats {
		return &container.Stats{}, nil
	}

	current, err := c.readRawStats(ctx)
	if err != nil {
		return nil, err
	}

	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()

	stats := *current // copy
	stats.CPUNanos = stats.CPUNanos - c.stats.baselineCPUNanos
	if current.MemoryUsageBytes > c.stats.peakMemoryUsageBytes {
		c.stats.peakMemoryUsageBytes = current.MemoryUsageBytes
	}
	stats.PeakMemoryUsageBytes = c.stats.peakMemoryUsageBytes
	c.stats.last = current
	return &stats, nil
}

func runPodman(ctx context.Context, subCommand string, stdio *container.Stdio, args ...string) *interfaces.CommandResult {
	command := []string{
		"podman",
		subCommand,
	}

	command = append(command, args...)
	result := commandutil.Run(ctx, &repb.Command{Arguments: command}, "" /*=workDir*/, stdio)
	return result
}

func generateContainerName() (string, error) {
	suffix, err := random.RandomString(20)
	if err != nil {
		return "", err
	}
	return "buildbuddy_exec_" + suffix, nil
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
	last *container.Stats
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
		s.baselineCPUNanos = s.last.CPUNanos
	}
	s.last = nil
	s.peakMemoryUsageBytes = 0
}
