package ociruntime

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "embed"
	mrand "math/rand/v2"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/unixcred"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

var (
	Runtime        = flag.String("executor.oci.runtime", "", "OCI runtime")
	runtimeRoot    = flag.String("executor.oci.runtime_root", "", "Root directory for storage of container state (see <runtime> --help for default)")
	imageCacheRoot = flag.String("executor.oci.image_cache_root", "", "Root directory for cached OCI images. Defaults to './executor/oci/images' relative to the configured executor.root_directory")
	pidsLimit      = flag.Int64("executor.oci.pids_limit", 2048, "PID limit for OCI runtime. Set to -1 for unlimited PIDs.")
	cpuLimit       = flag.Int("executor.oci.cpu_limit", 0, "Hard limit for CPU resources, expressed as CPU count. Default (0) is no limit.")
	dns            = flag.String("executor.oci.dns", "8.8.8.8", "Specifies a custom DNS server for use inside OCI containers. If set to the empty string, mount /etc/resolv.conf from the host.")
	netPoolSize    = flag.Int("executor.oci.network_pool_size", 0, "Limit on the number of networks to be reused between containers. Setting to 0 disables pooling. Setting to -1 uses the recommended default.")
)

const (
	ociVersion = "1.1.0-rc.3" // matches podman

	// Execution root directory path relative to the container rootfs directory.
	execrootPath = "/buildbuddy-execroot"

	// Fake image ref indicating that busybox should be manually provisioned.
	// TODO: get rid of this
	TestBusyboxImageRef = "test.buildbuddy.io/busybox"

	// Image cache layout version. This should be incremented when making
	// backwards-compatible changes to image cache storage, and older version
	// directories can be cleaned up.
	imageCacheVersion = "v1" // TODO: add automatic cleanup if this is bumped.
)

//go:embed seccomp.json
var seccompJSON []byte
var seccomp specs.LinuxSeccomp

//go:embed hosts
var hostsFile []byte

func init() {
	if err := json.Unmarshal(seccompJSON, &seccomp); err != nil {
		panic("Embedded seccomp profile is not valid JSON: " + err.Error())
	}
}

var (
	// Allowed capabilities.
	// TODO: allow customizing this (for self-hosted executors).
	capabilities = []string{
		"CAP_CHOWN",
		"CAP_DAC_OVERRIDE",
		"CAP_FOWNER",
		"CAP_FSETID",
		"CAP_KILL",
		"CAP_NET_BIND_SERVICE",
		"CAP_SETFCAP",
		"CAP_SETGID",
		"CAP_SETPCAP",
		"CAP_SETUID",
		"CAP_SYS_CHROOT",
	}

	// Environment variables applied to all executed commands.
	// These can be overridden either by the image or the command.
	baseEnv = []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOSTNAME=localhost",
	}
)

type provider struct {
	env environment.Env

	// Root directory where all container runtime information will be located.
	// Each subdirectory corresponds to a created container instance.
	containersRoot string

	// Root directory where cached images are stored.
	// This directory is structured like the following:
	//
	// - {imageCacheRoot}/v1/
	//   - {hashingAlgorithm}/
	//     - {hash}/
	//       - /bin/ # layer contents
	//       - /usr/
	//       - ...
	imageCacheRoot string

	imageStore  *ImageStore
	cgroupPaths *cgroup.Paths

	// Configured runtime path.
	runtime string

	networkPool *networking.ContainerNetworkPool
}

func NewProvider(env environment.Env, buildRoot string) (*provider, error) {
	// Enable masquerading on the host if it isn't enabled already.
	if err := networking.EnableMasquerading(env.GetServerContext()); err != nil {
		return nil, status.WrapError(err, "enable masquerading")
	}

	// Try to find a usable runtime if the runtime flag is not explicitly set.
	rt := *Runtime
	if rt == "" {
		for _, r := range []string{"crun", "runc", "runsc"} {
			if _, err := exec.LookPath(r); err == nil {
				rt = r
				break
			}
		}
	}
	if rt == "" {
		return nil, status.FailedPreconditionError("could not find a usable container runtime in PATH")
	}

	// TODO: make these root dirs configurable via flag
	containersRoot := filepath.Join(buildRoot, "executor", "oci", "run")
	if err := os.MkdirAll(containersRoot, 0755); err != nil {
		return nil, err
	}
	imgRoot := *imageCacheRoot
	if imgRoot == "" {
		imgRoot = filepath.Join(buildRoot, "executor", "oci", "images")
	}
	if err := os.MkdirAll(filepath.Join(imgRoot, imageCacheVersion), 0755); err != nil {
		return nil, err
	}
	imageStore := NewImageStore(imgRoot)

	networkPool := networking.NewContainerNetworkPool(*netPoolSize)
	env.GetHealthChecker().RegisterShutdownFunction(networkPool.Shutdown)

	return &provider{
		env:            env,
		runtime:        rt,
		containersRoot: containersRoot,
		cgroupPaths:    &cgroup.Paths{},
		imageCacheRoot: imgRoot,
		imageStore:     imageStore,
		networkPool:    networkPool,
	}, nil
}

func (p *provider) New(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
	return &ociContainer{
		env:            p.env,
		runtime:        p.runtime,
		containersRoot: p.containersRoot,
		cgroupPaths:    p.cgroupPaths,
		imageCacheRoot: p.imageCacheRoot,
		imageStore:     p.imageStore,
		networkPool:    p.networkPool,

		imageRef:       args.Props.ContainerImage,
		networkEnabled: args.Props.DockerNetwork != "off",
		user:           args.Props.DockerUser,
		forceRoot:      args.Props.DockerForceRoot,
	}, nil
}

type ociContainer struct {
	env environment.Env

	runtime        string
	cgroupPaths    *cgroup.Paths
	containersRoot string
	imageCacheRoot string
	imageStore     *ImageStore

	cid              string
	workDir          string
	overlayfsMounted bool
	stats            container.UsageStats
	networkPool      *networking.ContainerNetworkPool
	network          *networking.ContainerNetwork

	imageRef       string
	networkEnabled bool
	user           string
	forceRoot      bool
}

// Returns the OCI bundle directory for the container.
func (c *ociContainer) bundlePath() string {
	return filepath.Join(c.containersRoot, c.cid)
}

// Returns the standard rootfs path expected by crun.
func (c *ociContainer) rootfsPath() string {
	return filepath.Join(c.bundlePath(), "rootfs")
}

// Returns the standard config.json path expected by crun.
func (c *ociContainer) configPath() string {
	return filepath.Join(c.bundlePath(), "config.json")
}

// containerName returns the container short-name.
func (c *ociContainer) containerName() string {
	const cidPrefixLen = 12
	if len(c.cid) <= cidPrefixLen {
		return c.cid
	}
	return c.cid[:cidPrefixLen]
}

// createBundle creates the OCI bundle directory, which includes the OCI spec
// file (config.json), the rootfs directory, and other supplementary data files
// (e.g. the 'hosts' file which will be mounted to /etc/hosts).
func (c *ociContainer) createBundle(ctx context.Context, cmd *repb.Command) error {
	if err := os.MkdirAll(c.bundlePath(), 0755); err != nil {
		return fmt.Errorf("mkdir -p %s: %w", c.bundlePath(), err)
	}

	hostnamePath := filepath.Join(c.bundlePath(), "hostname")
	if err := os.WriteFile(hostnamePath, []byte("localhost"), 0644); err != nil {
		return fmt.Errorf("write %s: %w", hostnamePath, err)
	}
	// Note: we don't add 'host.containers.internal' here because we don't
	// support networking across containers.
	hostsFileLines := strings.Split(strings.TrimSpace(string(hostsFile)), "\n")
	if c.network.HostNetwork() != nil {
		hostsFileLines = append(hostsFileLines, fmt.Sprintf("%s %s", c.network.HostNetwork().NamespacedIP(), c.containerName()))
	} else {
		hostsFileLines = append(hostsFileLines, fmt.Sprintf("127.0.0.1 %s", c.containerName()))
	}
	hostsBytes := []byte(strings.Join(hostsFileLines, "\n") + "\n")
	if err := os.WriteFile(filepath.Join(c.bundlePath(), "hosts"), hostsBytes, 0644); err != nil {
		return fmt.Errorf("write hosts file: %w", err)
	}
	if *dns != "" {
		dnsLine := "nameserver " + *dns + "\n"
		if err := os.WriteFile(filepath.Join(c.bundlePath(), "resolv.conf"), []byte(dnsLine), 0644); err != nil {
			return fmt.Errorf("write resolv.conf file: %w", err)
		}
	}

	// Create rootfs
	if err := c.createRootfs(ctx); err != nil {
		return fmt.Errorf("create rootfs: %w", err)
	}

	// Create config.json from the image config and command
	image, ok := c.imageStore.CachedImage(c.imageRef)
	if !ok {
		return fmt.Errorf("image must be cached before creating OCI bundle")
	}
	cmd, err := withImageConfig(cmd, image)
	if err != nil {
		return fmt.Errorf("apply image config to command: %w", err)
	}
	spec, err := c.createSpec(ctx, cmd)
	if err != nil {
		return fmt.Errorf("create spec: %w", err)
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	if err := os.WriteFile(c.configPath(), b, 0644); err != nil {
		return err
	}

	return nil
}

func (c *ociContainer) IsolationType() string {
	return "oci" // TODO: make const in platform.go
}

func (c *ociContainer) IsImageCached(ctx context.Context) (bool, error) {
	_, ok := c.imageStore.CachedImage(c.imageRef)
	return ok, nil
}

func (c *ociContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	if c.imageRef == TestBusyboxImageRef {
		return nil
	}
	if _, err := c.imageStore.Pull(ctx, c.imageRef, creds); err != nil {
		return status.WrapError(err, "pull OCI image")
	}
	return nil
}

func (c *ociContainer) Run(ctx context.Context, cmd *repb.Command, workDir string, creds oci.Credentials) *interfaces.CommandResult {
	c.workDir = workDir
	cid, err := newCID()
	if err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("generate cid: %s", err))
	}
	c.cid = cid

	if err := container.PullImageIfNecessary(ctx, c.env, c, creds, c.imageRef); err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("pull image: %s", err))
	}
	if err := c.createNetwork(ctx); err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("create network: %s", err))
	}
	if err := c.createBundle(ctx, cmd); err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("create OCI bundle: %s", err))
	}

	return c.doWithStatsTracking(ctx, func(ctx context.Context) *interfaces.CommandResult {
		return c.invokeRuntime(ctx, nil /*=cmd*/, &interfaces.Stdio{}, 0 /*=waitDelay*/, "run", "--bundle="+c.bundlePath(), c.cid)
	})
}

func (c *ociContainer) Create(ctx context.Context, workDir string) error {
	c.workDir = workDir
	cid, err := newCID()
	if err != nil {
		return status.UnavailableErrorf("generate cid: %s", err)
	}
	c.cid = cid

	if err := c.createNetwork(ctx); err != nil {
		return status.UnavailableErrorf("create network: %s", err)
	}
	pid1 := &repb.Command{Arguments: []string{"sleep", "999999999999"}}
	// Provision bundle directory (OCI config JSON, rootfs, etc.)
	if err := c.createBundle(ctx, pid1); err != nil {
		return status.UnavailableErrorf("create OCI bundle: %s", err)
	}
	// Creating the container, at least with crun, already invokes the entrypoint and has it
	// inherit the stdout and stderr create is invoked with:
	// https://github.com/containers/crun/blob/f44da38333321335611d45638401e99f5f9548f2/src/libcrun/container.c#L2909
	// https://github.com/containers/crun/blob/f44da38333321335611d45638401e99f5f9548f2/src/libcrun/container.c#L2459C9-L2459C36
	// https://github.com/containers/crun/blob/f44da38333321335611d45638401e99f5f9548f2/src/libcrun/linux.c#L4950
	// https://github.com/containers/crun/blob/f44da38333321335611d45638401e99f5f9548f2/src/libcrun/container.c#L1548
	// By default, exec.Cmd.Wait() will wait until both the process has exited and the stdout and
	// stderr pipes have been closed. But since these pipes are inherited by the sleep pid1 process,
	// they are never closed. We use a very short waitDelay to forcibly close the pipes right after
	// the process exit.
	result := c.invokeRuntime(ctx, &repb.Command{}, &interfaces.Stdio{}, 1*time.Nanosecond, "create", "--bundle="+c.bundlePath(), c.cid)
	if err := asError(result); err != nil {
		return status.UnavailableErrorf("create container: %s", err)
	}
	// Start container
	if err := c.invokeRuntimeSimple(ctx, "start", c.cid); err != nil {
		return status.UnavailableErrorf("start container: %s", err)
	}
	return nil
}

func (c *ociContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	// Reset CPU usage and peak memory since we're starting a new task.
	c.stats.Reset()
	args := []string{"exec", "--cwd=" + execrootPath}
	// Respect command env. Note, when setting any --env vars at all, it
	// completely overrides the env from the bundle, rather than just adding
	// to it. So we specify the complete env here, including the base env,
	// image env, and command env.
	for _, e := range baseEnv {
		args = append(args, "--env="+e)
	}
	image, ok := c.imageStore.CachedImage(c.imageRef)
	if !ok {
		return commandutil.ErrorResult(status.UnavailableError("exec called before pulling image"))
	}
	cmd, err := withImageConfig(cmd, image)
	if err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("apply image config: %s", err))
	}
	for _, e := range cmd.GetEnvironmentVariables() {
		args = append(args, fmt.Sprintf("--env=%s=%s", e.GetName(), e.GetValue()))
	}
	args = append(args, c.cid)

	return c.doWithStatsTracking(ctx, func(ctx context.Context) *interfaces.CommandResult {
		return c.invokeRuntime(ctx, cmd, stdio, 1*time.Microsecond, args...)
	})
}

func (c *ociContainer) Signal(ctx context.Context, sig syscall.Signal) error {
	if c.cid == "" {
		return status.FailedPreconditionError("container is not created")
	}
	return c.invokeRuntimeSimple(ctx, "kill", "--all", c.cid, fmt.Sprintf("%d", sig))
}

func (c *ociContainer) Pause(ctx context.Context) error {
	return c.invokeRuntimeSimple(ctx, "pause", c.cid)
}

func (c *ociContainer) Unpause(ctx context.Context) error {
	return c.invokeRuntimeSimple(ctx, "resume", c.cid)
}

//nolint:nilness
func (c *ociContainer) Remove(ctx context.Context) error {
	if c.cid == "" {
		// We haven't created anything yet
		return nil
	}

	var firstErr error

	if err := c.invokeRuntimeSimple(ctx, "delete", "--force", c.cid); err != nil && firstErr == nil {
		firstErr = status.UnavailableErrorf("delete container: %s", err)
	}

	if c.overlayfsMounted {
		if err := syscall.Unmount(c.rootfsPath(), syscall.MNT_FORCE); err != nil && firstErr == nil {
			firstErr = status.UnavailableErrorf("unmount overlayfs: %s", err)
		}
	}

	if err := c.cleanupNetwork(ctx); err != nil && firstErr == nil {
		firstErr = status.UnavailableErrorf("cleanup network: %s", err)
	}

	if err := os.RemoveAll(c.bundlePath()); err != nil && firstErr == nil {
		firstErr = status.UnavailableErrorf("remove bundle: %s", err)
	}

	return firstErr
}

func (c *ociContainer) createNetwork(ctx context.Context) error {
	// TODO: should we pool loopback-only networks too?
	if c.networkEnabled {
		network := c.networkPool.Get(ctx)
		if network != nil {
			c.network = network
			return nil
		}
	}

	loopbackOnly := !c.networkEnabled
	network, err := networking.CreateContainerNetwork(ctx, loopbackOnly)
	if err != nil {
		return status.WrapError(err, "create network")
	}
	c.network = network
	return nil
}

func (c *ociContainer) cleanupNetwork(ctx context.Context) error {
	n := c.network
	c.network = nil

	if n == nil {
		return nil
	}

	// Add to the pool but only if this is not a loopback-only network.
	if c.networkEnabled {
		if c.networkPool.Add(ctx, n) {
			return nil
		}
	}

	return n.Cleanup(ctx)
}

func (c *ociContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	lifetimeStats, err := c.cgroupPaths.Stats(ctx, c.cid)
	if err != nil {
		return nil, err
	}
	c.stats.Update(lifetimeStats)
	return c.stats.TaskStats(), nil
}

// Instruments an OCI runtime call with monitor() to ensure that resource usage
// metrics are updated while the function is being executed, and that the
// resource usage results are populated in the returned CommandResult.
func (c *ociContainer) doWithStatsTracking(ctx context.Context, invokeRuntimeFn func(ctx context.Context) *interfaces.CommandResult) *interfaces.CommandResult {
	stop, statsCh := container.TrackStats(ctx, c)
	res := invokeRuntimeFn(ctx)
	stop()
	// statsCh will report stats for processes inside the container, and
	// res.UsageStats will report stats for the container runtime itself.
	// Combine these stats to get the total usage.
	runtimeProcessStats := res.UsageStats
	taskStats := <-statsCh
	if taskStats == nil {
		taskStats = &repb.UsageStats{}
	}
	combinedStats := taskStats.CloneVT()
	combinedStats.CpuNanos += runtimeProcessStats.GetCpuNanos()
	if runtimeProcessStats.GetPeakMemoryBytes() > taskStats.GetPeakMemoryBytes() {
		combinedStats.PeakMemoryBytes = runtimeProcessStats.GetPeakMemoryBytes()
	}
	res.UsageStats = combinedStats
	return res
}

func (c *ociContainer) createRootfs(ctx context.Context) error {
	if err := os.MkdirAll(c.rootfsPath(), 0755); err != nil {
		return fmt.Errorf("create rootfs dir: %w", err)
	}

	// For testing only, support a fake image ref that means "install busybox
	// manually".
	// TODO: improve testing setup and get rid of this
	if c.imageRef == TestBusyboxImageRef {
		return installBusybox(ctx, c.rootfsPath())
	}

	if c.imageRef == "" {
		// No image specified (sandbox-only).
		return nil
	}

	// Create an overlayfs with the pulled image layers.
	var lowerDirs []string
	image, ok := c.imageStore.CachedImage(c.imageRef)
	if !ok {
		return fmt.Errorf("bad state: attempted to create rootfs before pulling image")
	}
	// overlayfs "lowerdir" mount args are ordered from uppermost to lowermost,
	// but manifest layers are ordered from lowermost to uppermost. So we
	// iterate in reverse order when building the lowerdir args.
	for i := len(image.Layers) - 1; i >= 0; i-- {
		layer := image.Layers[i]
		path := layerPath(c.imageCacheRoot, layer.DiffID)
		// Skip empty dirs - these can cause conflicts since they will always
		// have the same digest, and also just add more overhead.
		// TODO: precompute this
		children, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("read layer dir: %w", err)
		}
		if len(children) == 0 {
			continue
		}
		lowerDirs = append(lowerDirs, path)
	}
	// Create workdir and upperdir.
	workdir := filepath.Join(c.bundlePath(), "tmp", "rootfs.work")
	if err := os.MkdirAll(workdir, 0755); err != nil {
		return fmt.Errorf("create overlay workdir: %w", err)
	}
	upperdir := filepath.Join(c.bundlePath(), "tmp", "rootfs.upper")
	if err := os.MkdirAll(upperdir, 0755); err != nil {
		return fmt.Errorf("create overlay upperdir: %w", err)
	}

	// TODO: do this mount inside a namespace so that it gets removed even if
	// the executor crashes (also needed for rootless support)

	// - userxattr is needed for compatibility with older kernels
	// - volatile disables fsync, as a performance optimization
	options := fmt.Sprintf(
		"lowerdir=%s,upperdir=%s,workdir=%s,userxattr,volatile",
		strings.Join(lowerDirs, ":"), upperdir, workdir)
	log.CtxDebugf(ctx, "Mounting overlayfs to %q, options=%q", c.rootfsPath(), options)
	if err := syscall.Mount("none", c.rootfsPath(), "overlay", 0, options); err != nil {
		return fmt.Errorf("mount overlayfs: %w", err)
	}
	c.overlayfsMounted = true
	return nil
}

func installBusybox(ctx context.Context, path string) error {
	busyboxPath, err := exec.LookPath("busybox")
	if err != nil {
		return fmt.Errorf("find busybox in PATH: %w", err)
	}
	binDir := filepath.Join(path, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return fmt.Errorf("mkdir -p %s: %w", binDir, err)
	}
	if err := disk.CopyViaTmpSibling(busyboxPath, filepath.Join(binDir, "busybox")); err != nil {
		return fmt.Errorf("copy busybox binary: %w", err)
	}
	b, err := exec.CommandContext(ctx, busyboxPath, "--list").Output()
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	names := strings.Split(strings.TrimSpace(string(b)), "\n")
	for _, name := range names {
		if name == "busybox" {
			continue
		}
		if err := os.Symlink("busybox", filepath.Join(binDir, name)); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Join(path, "usr"), 0755); err != nil {
		return err
	}
	if err := os.Symlink("../bin", filepath.Join(path, "usr", "bin")); err != nil {
		return err
	}
	return nil
}

func (c *ociContainer) createSpec(ctx context.Context, cmd *repb.Command) (*specs.Spec, error) {
	env := append(baseEnv, commandutil.EnvStringList(cmd)...)
	var pids *specs.LinuxPids
	if *pidsLimit >= 0 {
		pids = &specs.LinuxPids{Limit: *pidsLimit}
	}
	image, _ := c.imageStore.CachedImage(c.imageRef)
	user, err := getUser(ctx, image, c.rootfsPath(), c.user, c.forceRoot)
	if err != nil {
		return nil, fmt.Errorf("get container user: %w", err)
	}

	cpuSpecs := &specs.LinuxCPU{}
	if *cpuLimit != 0 {
		period := 100 * time.Millisecond
		cpuSpecs = &specs.LinuxCPU{
			Quota:  pointer(int64(*cpuLimit) * period.Microseconds()),
			Period: pointer(uint64(period.Microseconds())),
		}
	}

	spec := specs.Spec{
		Version: ociVersion,
		Process: &specs.Process{
			Terminal: false,
			User:     *user,
			Args:     cmd.GetArguments(),
			Cwd:      execrootPath,
			Env:      env,
			Rlimits: []specs.POSIXRlimit{
				{Type: "RLIMIT_NPROC", Hard: 4194304, Soft: 4194304},
			},
			Capabilities: &specs.LinuxCapabilities{
				Bounding:  capabilities,
				Effective: capabilities,
				Permitted: capabilities,
			},
			// TODO: apparmor
			ApparmorProfile: "",
		},
		Root: &specs.Root{
			Path:     c.rootfsPath(),
			Readonly: false,
		},
		Hostname: c.containerName(),
		Mounts: []specs.Mount{
			{
				Destination: "/proc",
				Type:        "proc",
				Source:      "proc",
				Options:     []string{"nosuid", "noexec", "nodev"},
			},
			{
				Destination: "/dev",
				Type:        "tmpfs",
				Source:      "tmpfs",
				Options:     []string{"nosuid", "strictatime", "mode=755", "size=65536k"},
			},
			{
				Destination: "/sys",
				Type:        "sysfs",
				Source:      "sysfs",
				Options:     []string{"nosuid", "noexec", "nodev", "ro"},
			},
			{
				Destination: "/dev/pts",
				Type:        "devpts",
				Source:      "devpts",
				Options: []string{
					"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620",
					// TODO: gid=5 doesn't work in some circumstances.
					// See https://github.com/containers/podman/blob/b8d95a5893572b37c8257407e964ad06ba87ade6/pkg/specgen/generate/oci_linux.go#L141-L173
					"gid=5",
				},
			},
			{
				Destination: "/dev/mqueue",
				Type:        "mqueue",
				Source:      "mqueue",
				Options:     []string{"nosuid", "noexec", "nodev"},
			},
			{
				Destination: "/etc/hosts",
				Type:        "bind",
				Source:      filepath.Join(c.bundlePath(), "hosts"),
				Options:     []string{"bind", "rprivate"},
			},
			{
				Destination: "/dev/shm",
				Type:        "tmpfs",
				Source:      "shm",
				Options:     []string{"rw", "nosuid", "nodev", "noexec", "relatime", "size=64000k", "inode64"},
			},
			// TODO: .containerenv
			// {
			// 		Destination: "/run/.containerenv",
			// 		Type:        "bind",
			// 		Source:      "/run/containers/storage/overlay-containers/99133d16f4f9d0678f87972c01209e308ebafc074f333822805a633620f12507/userdata/.containerenv",
			// 		Options:     []string{"bind", "rprivate"},
			// },
			{
				Destination: "/etc/hostname",
				Type:        "bind",
				Source:      filepath.Join(c.bundlePath(), "hostname"),
				Options:     []string{"bind", "rprivate"},
			},
			{
				Destination: "/sys/fs/cgroup",
				Type:        "cgroup",
				Source:      "cgroup",
				Options:     []string{"rprivate", "nosuid", "noexec", "nodev", "relatime", "ro"},
			},
			{
				Destination: execrootPath,
				Type:        "bind",
				Source:      c.workDir,
				Options:     []string{"bind", "rprivate"},
			},
		},
		Annotations: map[string]string{
			// Annotate with podman's default stop signal.
			// TODO: is this strictly needed?
			"org.opencontainers.image.stopSignal": syscall.SIGTERM.String(),
		},
		Linux: &specs.Linux{
			// TODO: set up cgroups
			CgroupsPath: "",
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.PIDNamespace},
				{Type: specs.IPCNamespace},
				{Type: specs.UTSNamespace},
				{Type: specs.MountNamespace},
				{Type: specs.CgroupNamespace},
				{
					Type: specs.NetworkNamespace,
					Path: c.network.NamespacePath(),
				},
			},
			Seccomp: &seccomp,
			Devices: []specs.LinuxDevice{},
			Sysctl: map[string]string{
				"net.ipv4.ping_group_range": fmt.Sprintf("%d %d", user.GID, user.GID),
			},
			Resources: &specs.LinuxResources{
				Pids: pids,
				CPU:  cpuSpecs,
			},
			// TODO: grok MaskedPaths and ReadonlyPaths - just copied from podman.
			MaskedPaths: []string{
				"/proc/acpi",
				"/proc/kcore",
				"/proc/keys",
				"/proc/latency_stats",
				"/proc/timer_list",
				"/proc/timer_stats",
				"/proc/sched_debug",
				"/proc/scsi",
				"/sys/firmware",
				"/sys/fs/selinux",
				"/sys/dev/block",
			},
			ReadonlyPaths: []string{
				"/proc/asound",
				"/proc/bus",
				"/proc/fs",
				"/proc/irq",
				"/proc/sys",
				"/proc/sysrq-trigger",
			},
		},
	}
	if *dns != "" {
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Destination: "/etc/resolv.conf",
			Type:        "bind",
			Source:      filepath.Join(c.bundlePath(), "resolv.conf"),
			Options:     []string{"bind", "rprivate"},
		})
	} else {
		if _, err := os.Stat("/etc/resolv.conf"); err == nil {
			spec.Mounts = append(spec.Mounts, specs.Mount{
				Destination: "/etc/resolv.conf",
				Type:        "bind",
				Source:      "/etc/resolv.conf",
				Options:     []string{"bind", "rprivate"},
			})
		}
	}

	return &spec, nil
}

func (c *ociContainer) invokeRuntimeSimple(ctx context.Context, args ...string) error {
	res := c.invokeRuntime(ctx, &repb.Command{}, &interfaces.Stdio{}, 0, args...)
	return asError(res)
}

func asError(res *interfaces.CommandResult) error {
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode != 0 {
		if len(res.Stderr) > 0 {
			return fmt.Errorf("%s", strings.TrimSpace(string(res.Stderr)))
		} else if len(res.Stdout) > 0 {
			return fmt.Errorf("%s", strings.TrimSpace(string(res.Stdout)))
		}
		return fmt.Errorf("exit code %d", res.ExitCode)
	}
	return nil
}

func (c *ociContainer) invokeRuntime(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio, waitDelay time.Duration, args ...string) *interfaces.CommandResult {
	start := time.Now()
	defer func() {
		// TODO: better profiling/tracing
		log.CtxDebugf(ctx, "[%s] %s %s\n", time.Since(start), c.runtime, args[0])
	}()

	globalArgs := []string{
		// "strace", "-o", "/tmp/strace.log",
		c.runtime,
		"--log-format=json",
	}
	runtimeName := filepath.Base(c.runtime)
	if runtimeName == "crun" {
		globalArgs = append(globalArgs, "--cgroup-manager=cgroupfs")
	}
	if *runtimeRoot != "" {
		globalArgs = append(globalArgs, "--root="+*runtimeRoot)
	}

	runtimeArgs := append(globalArgs, args...)
	runtimeArgs = append(runtimeArgs, command.GetArguments()...)

	// working dir for crun itself doesn't really matter - just default to cwd.
	wd, err := os.Getwd()
	if err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("getwd: %s", err))
	}

	log.CtxDebugf(ctx, "Running %v", runtimeArgs)

	cmd := exec.CommandContext(ctx, runtimeArgs[0], runtimeArgs[1:]...)
	cmd.Dir = wd
	var stdout *bytes.Buffer
	var stderr *bytes.Buffer
	// If stdio is nil, the output will be discarded.
	if stdio != nil {
		cmd.Stdin = stdio.Stdin
		if stdio.Stdout == nil {
			stdout = &bytes.Buffer{}
			cmd.Stdout = stdout
		} else {
			stdout = nil
			cmd.Stdout = stdio.Stdout
		}
		if stdio.Stderr == nil {
			stderr = &bytes.Buffer{}
			cmd.Stderr = stderr
		} else {
			stderr = nil
			cmd.Stderr = stdio.Stderr
		}
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// In the "run" case, start the runtime in its own pid namespace so that
	// when it is killed, the container process gets killed automatically
	// instead of getting reparented and continuing to execute.
	// TODO: figure out why this is only needed for run and not exec.
	if args[0] == "run" {
		cmd.SysProcAttr.Cloneflags = syscall.CLONE_NEWPID
	}

	cmd.WaitDelay = waitDelay
	runError := cmd.Run()
	if errors.Is(runError, exec.ErrWaitDelay) {
		// The stdio streams were forcibly closed after a non-zero waitDelay. Any error from the
		// process takes precedence over ErrWaitDelay, so we can ignore the error here without
		// a risk of shadowing a more important error.
		runError = nil
	}
	code, err := commandutil.ExitCode(ctx, cmd, runError)
	result := &interfaces.CommandResult{
		ExitCode: code,
		Error:    err,
	}
	if stdout != nil {
		result.Stdout = stdout.Bytes()
	}
	if stderr != nil {
		result.Stderr = stderr.Bytes()
	}
	return result
}

func getUser(ctx context.Context, image *Image, rootfsPath string, dockerUserProp string, dockerForceRootProp bool) (*specs.User, error) {
	// TODO: for rootless support we'll need to handle the case where the
	// executor user doesn't have permissions to access files created as the
	// requested user ID
	spec := ""
	if image != nil {
		spec = image.Config.User
	}
	if dockerUserProp != "" {
		spec = dockerUserProp
	}
	if dockerForceRootProp {
		spec = "0"
	}
	if spec == "" {
		// Inherit the current uid/gid.
		spec = fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	}

	user, group, err := container.ParseUserGroup(spec)
	if err != nil {
		return nil, fmt.Errorf(`invalid "USER[:GROUP]" spec %q`, spec)
	}

	var uid, gid uint32
	username := user.Name

	// If the user is non-numeric then we need to look it up from /etc/passwd.
	// If no gid is specified then we need to find the user entry in /etc/passwd
	// to know what group they are in.
	if user.Name != "" || group == nil {
		userRecord, err := unixcred.LookupUser(filepath.Join(rootfsPath, "/etc/passwd"), user)
		if (err == unixcred.ErrUserNotFound || os.IsNotExist(err)) && user.Name == "" {
			// If no user was found in /etc/passwd and we specified only a
			// numeric user ID then just set the group ID to 0 (root). This is
			// what docker/podman do, presumably because it's usually safe to
			// assume that gid 0 exists.
			uid = user.ID
			gid = 0
		} else if err != nil {
			return nil, fmt.Errorf("lookup user %q in /etc/passwd: %w", user, err)
		} else {
			uid = userRecord.UID
			username = userRecord.Username
			if group == nil {
				gid = userRecord.GID
			}
		}
	} else {
		uid = user.ID
	}

	if group != nil {
		// If a group was specified by name then look it up from /etc/group.
		if group.Name != "" {
			groupRecord, err := unixcred.LookupGroup(filepath.Join(rootfsPath, "/etc/group"), user)
			if err != nil {
				return nil, fmt.Errorf("lookup group %q in /etc/group: %w", group, err)
			}
			gid = groupRecord.GID
		} else {
			gid = group.ID
		}
	}

	gids := []uint32{gid}

	// If no group is explicitly specified and we have a username, then
	// search /etc/group for additional groups that the user might be in
	// (/etc/group lists members by username, not by uid).
	if group == nil && username != "" {
		groups, err := unixcred.GetGroupsWithUser(filepath.Join(rootfsPath, "/etc/group"), username)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("lookup groups with user %q in /etc/passwd: %w", username, err)
		}
		for _, g := range groups {
			gids = append(gids, g.GID)
		}
	}
	slices.Sort(gids)
	gids = slices.Compact(gids)

	return &specs.User{
		UID:            uid,
		GID:            gid,
		AdditionalGids: gids,
		Umask:          pointer(uint32(022)), // 0644 file perms by default
	}, nil
}

func pointer[T any](val T) *T {
	return &val
}

func newCID() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}

// layerPath returns the path where the extracted image layer with the given
// hash is stored on disk.
func layerPath(imageCacheRoot string, hash ctr.Hash) string {
	return filepath.Join(imageCacheRoot, imageCacheVersion, hash.Algorithm, hash.Hex)
}

// ImageStore handles image layer storage for OCI containers.
type ImageStore struct {
	layersDir      string
	imagePullGroup singleflight.Group[string, *Image]
	layerPullGroup singleflight.Group[string, any]

	mu           sync.RWMutex
	cachedImages map[string]*Image
}

// Image represents a cached image, including all layer digests and image
// configuration.
type Image struct {
	// Layers holds the image layers from lowermost to uppermost.
	Layers []*ImageLayer

	// Config holds various image settings such as user and environment
	// directives.
	Config ctr.Config
}

// ImageLayer represents a resolved image layer.
type ImageLayer struct {
	// DiffID is the uncompressed image digest.
	DiffID ctr.Hash
}

func NewImageStore(layersDir string) *ImageStore {
	return &ImageStore{
		layersDir:    layersDir,
		cachedImages: map[string]*Image{},
	}
}

// Pull downloads and extracts image layers to a directory, skipping layers
// that have already been downloaded, and deduping concurrent downloads for the
// same layer.
// Pull always re-authenticates the credentials with the image registry.
// Each layer is extracted to a subdirectory given by {algorithm}/{hash}, e.g.
// "sha256/abc123".
func (s *ImageStore) Pull(ctx context.Context, imageName string, creds oci.Credentials) (*Image, error) {
	key := hash.Strings(imageName, creds.Username, creds.Password)
	image, _, err := s.imagePullGroup.Do(ctx, key, func(ctx context.Context) (*Image, error) {
		image, err := s.pull(ctx, imageName, creds)
		if err != nil {
			return nil, err
		}

		s.mu.Lock()
		s.cachedImages[imageName] = image
		s.mu.Unlock()

		return image, nil
	})
	return image, err
}

// CachedLayers returns references to the cached image layers if the image
// has been pulled. The second return value indicates whether the image has
// been pulled - if false, the returned slice of layers will be nil.
func (s *ImageStore) CachedImage(imageName string) (image *Image, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// TODO: make ImageStore a param of NewProvider and move this logic to a
	// test image store
	if imageName == TestBusyboxImageRef {
		return &Image{}, true
	}

	image, ok = s.cachedImages[imageName]
	return image, ok
}

func (s *ImageStore) pull(ctx context.Context, imageName string, creds oci.Credentials) (*Image, error) {
	img, err := oci.Resolve(ctx, imageName, oci.RuntimePlatform(), creds)
	if err != nil {
		return nil, status.WrapError(err, "resolve image")
	}
	layers, err := img.Layers()
	if err != nil {
		return nil, status.UnavailableErrorf("get image layers: %s", err)
	}

	resolvedImage := &Image{
		Layers: make([]*ImageLayer, 0, len(layers)),
	}

	// Download and extract layers concurrently.
	var eg errgroup.Group
	eg.SetLimit(min(8, runtime.NumCPU()))
	for _, layer := range layers {
		layer := layer
		resolvedLayer := &ImageLayer{}
		resolvedImage.Layers = append(resolvedImage.Layers, resolvedLayer)
		eg.Go(func() error {
			d, err := layer.DiffID()
			if err != nil {
				return status.UnavailableErrorf("get layer digest: %s", err)
			}
			resolvedLayer.DiffID = d

			destDir := layerPath(s.layersDir, d)

			// If the destination directory already exists then we can skip
			// the download.
			if _, err := os.Stat(destDir); err != nil {
				if !os.IsNotExist(err) {
					return status.UnavailableErrorf("stat layer directory: %s", err)
				}
			} else {
				return nil
			}

			size, err := layer.Size()
			if err != nil {
				return status.UnavailableErrorf("get layer size: %s", err)
			}
			start := time.Now()
			log.CtxDebugf(ctx, "Pulling layer %s (%.2f MiB)", d.Hex, float64(size)/1e6)
			defer func() { log.CtxDebugf(ctx, "Pulled layer %s in %s", d.Hex, time.Since(start)) }()

			// Images often share layers - dedupe individual layer pulls.
			// Note that each layer pull is also authorized, so include
			// the credentials in the key here too.
			key := hash.Strings(destDir, creds.Username, creds.Password)
			_, _, err = s.layerPullGroup.Do(ctx, key, func(ctx context.Context) (any, error) {
				return nil, downloadLayer(ctx, layer, destDir)
			})
			return err
		})
	}
	// Fetch image config file concurrently with layer downloads.
	eg.Go(func() error {
		f, err := img.ConfigFile()
		if err != nil {
			return status.UnavailableErrorf("get image config file: %s", err)
		}
		resolvedImage.Config = f.Config
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return resolvedImage, nil
}

// downloadLayer downloads and extracts the given layer to the given destination
// dir. The extracted layer is suitable for use as an overlayfs lowerdir.
func downloadLayer(ctx context.Context, layer ctr.Layer, destDir string) error {
	rc, err := layer.Uncompressed()
	if err != nil {
		return status.UnavailableErrorf("get layer reader: %s", err)
	}
	defer rc.Close()

	tempUnpackDir := destDir + tmpSuffix()
	if err := os.MkdirAll(tempUnpackDir, 0755); err != nil {
		return status.UnavailableErrorf("create layer unpack dir: %s", err)
	}
	defer os.RemoveAll(tempUnpackDir)

	tr := tar.NewReader(rc)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.UnavailableErrorf("download and extract layer tarball: %s", err)
		}

		target := filepath.Join(tempUnpackDir, header.Name)
		base := filepath.Base(target)
		dir := filepath.Dir(target)

		const whiteoutPrefix = ".wh."
		// Handle whiteout
		if strings.HasPrefix(base, whiteoutPrefix) {
			// Directory whiteout
			if base == whiteoutPrefix+whiteoutPrefix+".opq" {
				if err := unix.Setxattr(dir, "trusted.overlay.opaque", []byte{'y'}, 0); err != nil {
					return fmt.Errorf("setxattr on deleted dir: %w", err)
				}
				continue
			}

			// File whiteout: Mark the file for deletion in overlayfs.
			originalBase := base[len(whiteoutPrefix):]
			originalPath := filepath.Join(dir, originalBase)
			if err := unix.Mknod(originalPath, unix.S_IFCHR, 0); err != nil {
				return fmt.Errorf("mknod for whiteout marker: %w", err)
			}
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return status.UnavailableErrorf("create directory: %s", err)
			}
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, os.FileMode(header.Mode))
			if err != nil {
				return status.UnavailableErrorf("create file: %s", err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return status.UnavailableErrorf("copy file content: %s", err)
			}
			f.Close()
		case tar.TypeSymlink:
			if err := os.Symlink(header.Linkname, target); err != nil {
				return status.UnavailableErrorf("create symlink: %s", err)
			}
		case tar.TypeLink:
			if err := os.Link(filepath.Join(tempUnpackDir, header.Linkname), target); err != nil {
				return status.UnavailableErrorf("create hard link: %s", err)
			}
		default:
			return status.UnavailableErrorf("unsupported tar entry type %q", header.Typeflag)
		}
	}

	if err := os.Rename(tempUnpackDir, destDir); err != nil {
		// If the dest dir already exists then it's most likely because we were
		// pulling the same layer concurrently with different credentials.
		if os.IsExist(err) {
			log.CtxDebugf(ctx, "Ignoring temp layer dir rename failure %q (likely due to concurrent layer download)", err)
			return nil
		}

		return status.UnavailableErrorf("rename temp layer dir: %s", err)
	}

	return nil
}

func withImageConfig(cmd *repb.Command, image *Image) (*repb.Command, error) {
	// Apply any env vars from the image which aren't overridden by the command
	cmdVarNames := make(map[string]bool, len(cmd.EnvironmentVariables))
	for _, cmdVar := range cmd.GetEnvironmentVariables() {
		cmdVarNames[cmdVar.GetName()] = true
	}
	imageEnv, err := commandutil.EnvProto(image.Config.Env)
	if err != nil {
		return nil, status.WrapError(err, "parse image env")
	}
	outEnv := slices.Clone(cmd.EnvironmentVariables)
	for _, imageVar := range imageEnv {
		if cmdVarNames[imageVar.GetName()] {
			continue
		}
		outEnv = append(outEnv, imageVar)
	}

	// TODO: ENTRYPOINT, CMD

	// Return a copy of the command but with the image config applied
	out := cmd.CloneVT()
	out.EnvironmentVariables = outEnv
	return out, nil
}

func tmpSuffix() string {
	return fmt.Sprintf(".%d.tmp", mrand.Int64N(1e18))
}
