package ociruntime

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "embed"
	mrand "math/rand/v2"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/block_io"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor_auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ociconv"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/fsync"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/buildbuddy-io/buildbuddy/server/util/unixcred"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	// Exit code 139 represents 11 (SIGSEGV signal) + 128 https://tldp.org/LDP/abs/html/exitcodes.html
	ociSIGSEGVExitCode = 139

	// Statusz section name.
	imagesStatuszSectionName = "ociruntime_images"
)

var (
	Runtime                 = flag.String("executor.oci.runtime", "", "OCI runtime")
	runtimeRoot             = flag.String("executor.oci.runtime_root", "", "Root directory for storage of container state (see <runtime> --help for default)")
	dns                     = flag.String("executor.oci.dns", "8.8.8.8", "Specifies a custom DNS server for use inside OCI containers. If set to the empty string, mount /etc/resolv.conf from the host.")
	netPoolSize             = flag.Int("executor.oci.network_pool_size", -1, "Limit on the number of networks to be reused between containers. Setting to 0 disables pooling. Setting to -1 uses the recommended default.")
	defaultNetworkMode      = flag.String("executor.oci.default_network_mode", "", "Default network mode: either 'bridge' or 'off'. Can be overridden per-action with the 'dockerNetwork' platform property.")
	enableLxcfs             = flag.Bool("executor.oci.enable_lxcfs", false, "Use lxcfs to fake cpu info inside containers.")
	capAdd                  = flag.Slice("executor.oci.cap_add", []string{}, "Capabilities to add to all OCI containers.")
	mounts                  = flag.Slice("executor.oci.mounts", []specs.Mount{}, "Additional mounts to add to all OCI containers. This is an array of OCI mount specs as described here: https://github.com/opencontainers/runtime-spec/blob/main/config.md#mounts")
	devices                 = flag.Slice("executor.oci.devices", []specs.LinuxDevice{}, "Additional devices to add to all OCI containers. This is an array of OCI linux device specs as described here: https://github.com/opencontainers/runtime-spec/blob/main/config.md#configuration-schema-example")
	enablePersistentVolumes = flag.Bool("executor.oci.enable_persistent_volumes", false, "Enables persistent volumes that can be shared between actions within a group. Only supported for OCI isolation type.")
	enableTini              = flag.Bool("executor.oci.enable_tini", false, "If true, run all OCI containers with tini as pid 1.")
	enableCgroupMemoryLimit = flag.Bool("executor.oci.enable_cgroup_memory_limit", false, "If true, sets cgroup memory.max based on resource requests to limit how much memory a task can claim.")
	minPIDsLimit            = flag.Int64("executor.oci.min_pids_limit", 0, "Min value to use for pids.max (PID limit). The scheduler may set a higher value for larger tasks. This can be used for rare cases where the scheduler does not provide a high enough limit.")
	cgroupMemoryCushion     = flag.Float64("executor.oci.cgroup_memory_limit_cushion", 0, "If executor.oci.enable_cgroup_memory_limit is true, allow tasks to consume (1 + cgroup_memory_limit_cushion) * EstimatedMemoryBytes")
	enableImageEviction     = flag.Bool("executor.oci.image_eviction_enabled", false, "If true, track OCI image layers in the filecache LRU for eviction. When enabled, unused image layers can be evicted to make room for other cached files.")

	errSIGSEGV = status.UnavailableErrorf("command was terminated by SIGSEGV, likely due to a memory issue")
)

const (
	ociVersion = "1.1.0-rc.3" // matches podman

	// Default execution root directory path relative to the container rootfs directory.
	defaultExecrootPath = "/buildbuddy-execroot"

	// Image cache layout version.
	//
	// This should be incremented when making backwards-compatible changes to
	// image cache storage. Version directories which don't match this version
	// are cleaned up automatically on startup.
	//
	// Must match the versionDirRegexp below ("v" followed by an integer).
	imageCacheVersion = "v2"

	// Maximum length of overlayfs mount options string.
	maxMntOptsLength = 4095

	// Path to tini binary in the container.
	tiniMountPoint = "/usr/local/buildbuddy-container-tools/tini"
)

var (
	versionDirRegexp = regexp.MustCompile(`^v\d+$`)

	// Fake /proc/cgroups content to mount into the container.
	fakeProcCgroupsContent = getFakeProcCgroupsContent()
)

// Set via x_defs from the BUILD file
var (
	crunRlocationpath string
)

//go:embed seccomp.json
var seccompJSON []byte
var seccomp specs.LinuxSeccomp

//go:embed hosts
var hostsFile []byte

//go:embed tini
var tini []byte

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

	// These files will be overridden by lxcfs when
	// executor.oci.enable_lxcfs == true. They contain information about
	// the number of CPUs on the running system, and are often used by
	// the workloads inside containers to configure parallelism. Overriding
	// them to correct values (based on the container size) prevents
	// workloads from trying to over-allocate CPU and then not having the
	// resources to do that work.
	lxcfsFiles = []string{
		"/proc/cpuinfo",
		"/proc/diskstats",
		"/proc/meminfo",
		"/proc/stat",
		"/proc/swaps",
		"/proc/uptime",
		"/proc/slabinfo",
		"/sys/devices/system/cpu",
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

	// Path to the configured tini binary or the one found in $PATH if none was
	// configured. Only set if executor.oci.tini_enabled == true.
	tiniPath string

	networkPool *networking.ContainerNetworkPool

	// Optional. "" if executor.oci.enable_lxcfs == false.
	// lxcfs mount dir -- files in here can be bind mounted into a container
	// to provide "fake" cpu info that is appropriate to the container's
	// configured memory and cpu.
	lxcfsMount string
}

func NewProvider(env environment.Env, buildRoot, cacheRoot string) (*provider, error) {
	if !slices.Contains([]string{"", "bridge", "off"}, *defaultNetworkMode) {
		return nil, fmt.Errorf("unsupported 'executor.oci.default_network_mode' setting %q", *defaultNetworkMode)
	}

	// Enable masquerading on the host if it isn't enabled already.
	if err := networking.EnableMasquerading(env.GetServerContext()); err != nil {
		return nil, status.WrapError(err, "enable masquerading")
	}

	// Try to find a usable runtime if the runtime flag is not explicitly set.
	rt := *Runtime
	if rt == "" && crunRlocationpath != "" {
		runfilePath, err := runfiles.Rlocation(crunRlocationpath)
		if err != nil {
			log.Infof("crun rlocation lookup failed (falling back to PATH lookup): %s", err)
		} else {
			if _, err := os.Stat(runfilePath); err != nil {
				log.Infof("Failed to stat crun binary from runfiles (falling back to PATH lookup): %s", err)
			} else {
				rt = runfilePath
			}
		}
	}
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
	log.Infof("Located OCI runtime binary at %s", rt)

	// Configure the tini binary path.
	binDir := filepath.Join(buildRoot, "executor", "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return nil, status.FailedPreconditionErrorf("failed to create executor bin directory: %s", err)
	}
	tiniPath := filepath.Join(binDir, "tini")
	if err := os.WriteFile(tiniPath, tini, 0755); err != nil {
		return nil, status.FailedPreconditionErrorf("failed to write tini binary to %s: %s", tiniPath, err)
	}

	lxcfsMount := "" // set below if configured.
	// Enable lxcfs, if configured.
	if *enableLxcfs {
		lxcfsMountDir := "/var/lib/lxcfs"
		if err := os.MkdirAll(lxcfsMountDir, 0755); err != nil {
			return nil, err
		}

		// Unmount if it's already mounted (container restarted)
		if err := syscall.Unmount(lxcfsMountDir, 0); err == nil {
			log.Infof("successfully unmounted dead lxcfs path, re-mounting shortly")
		}

		// Mount lxcfs fuse fs on lxcfsMountDir.
		c := exec.CommandContext(env.GetServerContext(), "lxcfs", "-f", "--enable-cfs", lxcfsMountDir)
		c.Stdout = log.Writer("[LXCFS] ")
		c.Stderr = log.Writer("[LXCFS] ")
		if err := c.Start(); err != nil {
			return nil, err
		}

		// Wait (in bg) for foregrounded lxcfs process . It will exit
		// when the executor dies.
		go func() {
			if err := c.Wait(); err != nil {
				log.Errorf("[LXCFS] err: %s", err)
			}
			// While the server is alive, attempt to keep lxcfs
			// running if it was configured.
			for {
				select {
				case <-env.GetServerContext().Done():
					return
				default:
				}

				// If LXCFS has exited, attempt to restart it.
				log.Infof("[LXCFS] process died; attempting to restart...")
				c = exec.CommandContext(env.GetServerContext(), "lxcfs", "-f", "--enable-cfs", lxcfsMountDir)
				c.Stdout = log.Writer("[LXCFS] ")
				c.Stderr = log.Writer("[LXCFS] ")
				if err := c.Start(); err != nil {
					log.Errorf("[LXCFS] start err: %s", err)
				}
				if err := c.Wait(); err != nil {
					log.Errorf("[LXCFS] err: %s", err)
				}
			}
		}()
		testPath := filepath.Join(lxcfsMountDir, lxcfsFiles[0])
		if err := disk.WaitUntilExists(env.GetServerContext(), testPath, disk.WaitOpts{}); err != nil {
			return nil, status.UnavailableErrorf("lxcfs did not mount %q: %s", testPath, err)
		}
		log.Infof("lxcfs mounted on %q", lxcfsMountDir)
		lxcfsMount = lxcfsMountDir
	}
	containersRoot := filepath.Join(buildRoot, "executor", "oci", "run")
	if err := os.MkdirAll(containersRoot, 0755); err != nil {
		return nil, err
	}
	imageCacheRoot := filepath.Join(cacheRoot, "images", "oci")
	if err := os.MkdirAll(filepath.Join(imageCacheRoot, imageCacheVersion), 0755); err != nil {
		return nil, err
	}
	if err := cleanStaleImageCacheDirs(imageCacheRoot); err != nil {
		log.Warningf("Failed to clean up old image cache versions: %s", err)
	}
	resolver, err := oci.NewResolver(env)
	if err != nil {
		return nil, err
	}
	if *enableImageEviction && env.GetFileCache() == nil {
		return nil, status.FailedPreconditionError("FileCache is required for OCI image storage")
	}
	imageStore, err := NewImageStore(resolver, imageCacheRoot, env.GetFileCache())
	if err != nil {
		return nil, err
	}
	statusz.AddSection(imagesStatuszSectionName, "OCI images", imageStore)

	networkPool := networking.NewContainerNetworkPool(*netPoolSize)
	env.GetHealthChecker().RegisterShutdownFunction(networkPool.Shutdown)

	return &provider{
		env:            env,
		runtime:        rt,
		tiniPath:       tiniPath,
		containersRoot: containersRoot,
		cgroupPaths:    &cgroup.Paths{},
		imageCacheRoot: imageCacheRoot,
		imageStore:     imageStore,
		networkPool:    networkPool,
		lxcfsMount:     lxcfsMount,
	}, nil
}

func (p *provider) New(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
	networkMode, err := platform.GetEffectiveDockerNetwork(args.Props.Network, args.Props.DockerNetwork)
	if err != nil {
		return nil, err
	}
	if networkMode == "" {
		networkMode = *defaultNetworkMode
	}

	execroot := args.Props.ExecrootPath
	if execroot == "" {
		execroot = defaultExecrootPath
	}
	if !filepath.IsAbs(execroot) {
		return nil, status.InvalidArgumentErrorf("execroot-path platform property must be an absolute path, got %q", execroot)
	}

	container := &ociContainer{
		env:            p.env,
		runtime:        p.runtime,
		tiniPath:       p.tiniPath,
		containersRoot: p.containersRoot,
		cgroupPaths:    p.cgroupPaths,
		imageCacheRoot: p.imageCacheRoot,
		imageStore:     p.imageStore,
		networkPool:    p.networkPool,
		lxcfsMount:     p.lxcfsMount,

		blockDevice:        args.BlockDevice,
		cgroupParent:       args.CgroupParent,
		cgroupSettings:     &scpb.CgroupSettings{},
		imageRef:           args.Props.ContainerImage,
		networkEnabled:     networkMode != "off",
		isPersistentWorker: args.Props.PersistentWorkerKey != "",
		tiniEnabled:        args.Props.DockerInit || *enableTini,
		user:               args.Props.DockerUser,
		forceRoot:          args.Props.DockerForceRoot,
		execrootPath:       execroot,
		persistentVolumes:  args.Props.PersistentVolumes,

		milliCPU:      args.Task.GetSchedulingMetadata().GetTaskSize().GetEstimatedMilliCpu(),
		memoryBytes:   args.Task.GetSchedulingMetadata().GetTaskSize().GetEstimatedMemoryBytes(),
		useOCIFetcher: args.Props.UseOCIFetcher,
	}
	if settings := args.Task.GetSchedulingMetadata().GetCgroupSettings(); settings != nil {
		container.cgroupSettings = settings
	}

	return container, nil
}

type ociContainer struct {
	env environment.Env

	runtime        string
	tiniPath       string
	tiniEnabled    bool
	cgroupPaths    *cgroup.Paths
	cgroupParent   string
	cgroupSettings *scpb.CgroupSettings
	blockDevice    *block_io.Device
	containersRoot string
	imageCacheRoot string
	imageStore     *ImageStore
	lockedImage    *LockedImage

	cid                    string
	workDir                string
	mergedMounts           []string
	overlayfsMounted       bool
	persistentVolumes      []platform.PersistentVolume
	persistentVolumeMounts []specs.Mount
	stats                  container.UsageStats
	networkPool            *networking.ContainerNetworkPool
	network                *networking.ContainerNetwork
	lxcfsMount             string
	releaseCPUs            func()
	isPersistentWorker     bool

	imageRef       string
	networkEnabled bool
	user           string
	forceRoot      bool
	execrootPath   string

	milliCPU      int64 // milliCPU allocation from task size
	memoryBytes   int64 // memory allocation from task size in bytes
	useOCIFetcher bool
}

// Assert [*ociContainer] implements [container.StatsRecorder].
var _ container.StatsRecorder = (*ociContainer)(nil)

// Returns the OCI bundle directory for the container.
func (c *ociContainer) bundlePath() string {
	return filepath.Join(c.containersRoot, c.cid)
}

// Returns the standard rootfs path expected by crun.
func (c *ociContainer) rootfsPath() string {
	return filepath.Join(c.bundlePath(), "rootfs")
}

func (c *ociContainer) cgroupRootRelativePath() string {
	return filepath.Join(c.cgroupParent, c.cid)
}

func (c *ociContainer) cgroupPath() string {
	return filepath.Join("/sys/fs/cgroup", c.cgroupRootRelativePath())
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

func (c *ociContainer) initPersistentVolumes(ctx context.Context) error {
	if len(c.persistentVolumes) == 0 {
		return nil
	}
	if !*enablePersistentVolumes {
		return status.UnimplementedError("persistent volumes are not enabled")
	}

	partition := "default"
	if executor_auth.APIKey() != "" {
		// If authentication is enabled, host mounts are only available to
		// authenticated users.
		c, err := claims.ClaimsFromContext(ctx)
		if err != nil {
			return status.UnauthenticatedErrorf("persistent volumes require authentication")
		}
		partition = c.GroupID
	}

	for _, volume := range c.persistentVolumes {
		// Initialize the volume at "{build_root}/volumes/{partition}/{volume_name}"
		// Example: "/buildbuddy/executor/buildroot/volumes/GR123/node_modules_cache"
		hostPath := filepath.Join(filepath.Dir(c.workDir), "volumes", partition, volume.Name())
		if err := os.MkdirAll(hostPath, 0755); err != nil {
			return fmt.Errorf("create persistent volume backing path %q: %w", hostPath, err)
		}
		c.persistentVolumeMounts = append(c.persistentVolumeMounts, specs.Mount{
			Destination: volume.ContainerPath(),
			Type:        "bind",
			Source:      hostPath,
			Options:     []string{"bind", "rprivate"},
		})
	}
	return nil
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

	// Setup cgroup
	if err := c.setupCgroup(ctx); err != nil {
		return fmt.Errorf("setup cgroup: %w", err)
	}
	// Create backing file for fake /proc/cgroups mount
	if err := os.WriteFile(filepath.Join(c.bundlePath(), "proc_cgroups"), []byte(fakeProcCgroupsContent), 0644); err != nil {
		return fmt.Errorf("write proc_cgroups file: %w", err)
	}

	// Create config.json from the image config and command
	if c.lockedImage == nil {
		return fmt.Errorf("image must be cached before creating OCI bundle")
	}
	cmd, err := withImageConfig(cmd, c.lockedImage.Image)
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
	if c.lockedImage != nil {
		return true, nil
	}

	lockedImage, ok := c.imageStore.GetCachedImageAndLock(ctx, c.imageRef)
	if !ok {
		return false, nil
	}

	c.lockedImage = lockedImage
	return true, nil
}

func (c *ociContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	if c.lockedImage != nil {
		return nil
	}

	lockedImage, err := c.imageStore.PullAndLockImage(ctx, c.imageRef, creds, c.useOCIFetcher)
	if err != nil {
		return status.WrapError(err, "pull OCI image")
	}
	c.lockedImage = lockedImage
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
	if c.tiniEnabled {
		cmd = cmd.CloneVT()
		cmd.Arguments = append([]string{tiniMountPoint, "--"}, cmd.Arguments...)
	}
	if err := c.initPersistentVolumes(ctx); err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("init persistent volumes: %s", err))
	}
	if err := c.createBundle(ctx, cmd); err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("create OCI bundle: %s", err))
	}

	return c.doWithStatsTracking(ctx, func(ctx context.Context) *interfaces.CommandResult {
		// Use --keep to prevent the cgroup from being deleted when the
		// container exits, since we still want to be able to look at stats,
		// events, etc. after completion.
		return c.invokeRuntime(ctx, nil /*=cmd*/, &interfaces.Stdio{}, 0 /*=waitDelay*/, "run", "--keep", "--bundle="+c.bundlePath(), c.cid)
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
	if c.tiniEnabled {
		pid1.Arguments = append(
			[]string{tiniMountPoint, "--"},
			pid1.Arguments...,
		)
	}
	if err := c.initPersistentVolumes(ctx); err != nil {
		return status.UnavailableErrorf("init persistent volumes: %s", err)
	}
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
	args := []string{"exec", "--cwd=" + filepath.Join(c.execrootPath, cmd.GetWorkingDirectory())}
	// Respect command env. Note, when setting any --env vars at all, it
	// completely overrides the env from the bundle, rather than just adding
	// to it. So we specify the complete env here, including the base env,
	// image env, and command env.
	for _, e := range baseEnv {
		args = append(args, "--env="+e)
	}
	if c.lockedImage == nil {
		return commandutil.ErrorResult(status.UnavailableError("exec called before pulling image"))
	}
	cmd, err := withImageConfig(cmd, c.lockedImage.Image)
	if err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("apply image config: %s", err))
	}
	for _, e := range cmd.GetEnvironmentVariables() {
		args = append(args, fmt.Sprintf("--env=%s=%s", e.GetName(), e.GetValue()))
	}
	args = append(args, c.cid)

	// If this Exec() is running a long-lived persistent worker, then don't
	// start the stats polling loop. Instead, the caller is responsible for
	// tracking stats separately by wrapping each work request with RecordStats.
	// This way, we only record stats while work requests are in progress.
	if c.isPersistentWorker {
		return c.invokeRuntime(ctx, cmd, stdio, 1*time.Microsecond, args...)
	}
	return c.doWithStatsTracking(ctx, func(ctx context.Context) *interfaces.CommandResult {
		return c.invokeRuntime(ctx, cmd, stdio, 1*time.Microsecond, args...)
	})
}

func (c *ociContainer) RecordStats(ctx context.Context) func() (*repb.UsageStats, error) {
	stop := c.stats.TrackExecution(ctx, func(ctx context.Context) (*repb.UsageStats, error) {
		return c.cgroupPaths.Stats(ctx, c.cid, c.blockDevice)
	})
	return func() (*repb.UsageStats, error) {
		stop()
		return c.stats.TaskStats(), nil
	}
}

func (c *ociContainer) Signal(ctx context.Context, sig syscall.Signal) error {
	if c.cid == "" {
		return status.FailedPreconditionError("container is not created")
	}
	return c.invokeRuntimeSimple(ctx, "kill", "--all", c.cid, fmt.Sprintf("%d", sig))
}

func (c *ociContainer) Pause(ctx context.Context) error {
	err := c.invokeRuntimeSimple(ctx, "pause", c.cid)

	if c.releaseCPUs != nil {
		c.releaseCPUs()
	}

	return err
}

func (c *ociContainer) Unpause(ctx context.Context) error {
	// Setup cgroup
	if err := c.setupCgroup(ctx); err != nil {
		return fmt.Errorf("setup cgroup: %w", err)
	}

	return c.invokeRuntimeSimple(ctx, "resume", c.cid)
}

func (c *ociContainer) Remove(ctx context.Context) error {
	defer func() {
		if c.lockedImage != nil {
			c.lockedImage.Unlock()
			c.lockedImage = nil
		}
	}()

	if c.cid == "" {
		// We haven't created anything yet
		return nil
	}

	var firstErr error

	if err := c.invokeRuntimeSimple(ctx, "delete", "--force", c.cid); err != nil {
		firstErr = status.UnavailableErrorf("delete container: %s", err)
	}

	if len(c.mergedMounts) > 0 {
		for _, merged := range c.mergedMounts {
			if err := unix.Unmount(merged, unix.MNT_FORCE); err != nil && firstErr == nil {
				firstErr = status.UnavailableErrorf("unmount overlayfs: %s", err)
			}
		}
	}

	if c.overlayfsMounted {
		if err := unix.Unmount(c.rootfsPath(), unix.MNT_FORCE); err != nil && firstErr == nil {
			firstErr = status.UnavailableErrorf("unmount overlayfs: %s", err)
		}
	}

	if err := c.cleanupNetwork(ctx); err != nil && firstErr == nil {
		firstErr = status.UnavailableErrorf("cleanup network: %s", err)
	}

	if err := os.RemoveAll(c.bundlePath()); err != nil && firstErr == nil {
		firstErr = status.UnavailableErrorf("remove bundle: %s", err)
	}

	// Remove the cgroup in case the delete command didn't work as expected.
	if err := os.Remove(c.cgroupPath()); err != nil && firstErr == nil && !os.IsNotExist(err) {
		firstErr = status.UnavailableErrorf("remove container cgroup: %s", err)
	}

	if c.releaseCPUs != nil {
		c.releaseCPUs()
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
	return c.stats.TaskStats(), nil
}

// Instruments an OCI runtime call with monitor() to ensure that resource usage
// metrics are updated while the function is being executed, and that the
// resource usage results are populated in the returned CommandResult.
// Also incorporates cgroup events into the command result - oom_kill events
// in particular are translated to errors.
func (c *ociContainer) doWithStatsTracking(ctx context.Context, invokeRuntimeFn func(ctx context.Context) *interfaces.CommandResult) *interfaces.CommandResult {
	stop := c.stats.TrackExecution(ctx, func(ctx context.Context) (*repb.UsageStats, error) {
		return c.cgroupPaths.Stats(ctx, c.cid, c.blockDevice)
	})
	res := invokeRuntimeFn(ctx)
	stop()
	// statsCh will report stats for processes inside the container, and
	// res.UsageStats will report stats for the container runtime itself.
	// Combine these stats to get the total usage.
	runtimeProcessStats := res.UsageStats
	taskStats := c.stats.TaskStats()
	if taskStats == nil {
		taskStats = &repb.UsageStats{}
	}
	combinedStats := taskStats.CloneVT()
	combinedStats.CpuNanos += runtimeProcessStats.GetCpuNanos()
	if runtimeProcessStats.GetPeakMemoryBytes() > taskStats.GetPeakMemoryBytes() {
		combinedStats.PeakMemoryBytes = runtimeProcessStats.GetPeakMemoryBytes()
	}
	res.UsageStats = combinedStats

	// If there was an oom_kill event, return an error instead of a normal exit
	// status.
	if err := c.checkOOMKill(ctx, res); err != nil {
		res.ExitCode = commandutil.KilledExitCode
		res.Error = err
		return res
	}

	// Check whether the pid limit was exceeded, and just log it for now so that
	// it can be diagnosed.
	if err := c.checkPIDLimitExceeded(ctx, res); err != nil {
		log.CtxWarning(ctx, status.Message(err))
	}

	if c.network != nil {
		networkStats, err := c.network.Stats(ctx)
		if err != nil {
			log.CtxWarningf(ctx, "Failed to get network stats: %s", err)
		} else {
			res.UsageStats.NetworkStats = networkStats
		}
	}

	return res
}

// checkOOMKill checks for oom_kill memory events in the cgroup and returns an
// Unavailable error if found.
func (c *ociContainer) checkOOMKill(ctx context.Context, res *interfaces.CommandResult) error {
	memoryEvents, err := cgroup.ReadMemoryEvents(c.cgroupPath())
	if err != nil {
		log.CtxWarningf(ctx, "Failed to get memory events: %s", err)
		return nil
	}
	if memoryEvents["oom_kill"] == 0 {
		return nil
	}
	if res.ExitCode == 0 {
		log.CtxWarningf(ctx, "Task succeeded, but cgroup reported oom_kill events.")
		return nil
	}
	return status.UnavailableError("task process or child process killed by oom killer")
}

// checkPIDLimitExceeded checks for pid limit exceeded events in the cgroup and
// returns an Unavailable error if found.
func (c *ociContainer) checkPIDLimitExceeded(ctx context.Context, res *interfaces.CommandResult) error {
	pidsEvents, err := cgroup.ReadPidsEvents(c.cgroupPath())
	if err != nil {
		log.CtxWarningf(ctx, "Failed to get pids events: %s", err)
		return nil
	}
	if pidsEvents["max"] == 0 {
		return nil
	}
	if res.ExitCode == 0 {
		log.CtxWarningf(ctx, "Task succeeded, but cgroup reported pid limit exceeded events.")
		return nil
	}
	return status.UnavailableErrorf("pid limit exceeded (maximum number of pids allowed is %d)", c.cgroupSettings.GetPidsMax())
}

func (c *ociContainer) setupCgroup(ctx context.Context) error {
	// Lease CPUs for task execution, and set cleanup function.
	leaseID := uuid.New()
	numaNode, leasedCPUs, cleanupFunc := c.env.GetCPULeaser().Acquire(c.milliCPU, leaseID)
	log.CtxInfof(ctx, "Lease %s granted %+v cpus on node %d", leaseID, leasedCPUs, numaNode)
	c.releaseCPUs = cleanupFunc
	c.cgroupSettings.CpusetCpus = toInt32s(leasedCPUs)
	c.cgroupSettings.NumaNode = new(int32(numaNode))
	if *enableCgroupMemoryLimit && c.memoryBytes > 0 {
		c.cgroupSettings.MemoryLimitBytes = new(int64(c.memoryBytes + int64(float64(c.memoryBytes)*(*cgroupMemoryCushion))))
	}
	if c.cgroupSettings.PidsMax != nil && *minPIDsLimit > 0 {
		c.cgroupSettings.PidsMax = new(int64(max(c.cgroupSettings.GetPidsMax(), *minPIDsLimit)))
	}

	path := c.cgroupPath()
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("create cgroup: %w", err)
	}
	if err := cgroup.Setup(ctx, path, c.cgroupSettings, c.blockDevice); err != nil {
		return fmt.Errorf("configure cgroup: %w", err)
	}
	return nil
}

func (c *ociContainer) createRootfs(ctx context.Context) error {
	if err := os.MkdirAll(c.rootfsPath(), 0755); err != nil {
		return fmt.Errorf("create rootfs dir: %w", err)
	}

	if c.imageRef == "" {
		// No image specified (sandbox-only).
		return nil
	}

	// Create an overlayfs with the pulled image layers.
	if c.lockedImage == nil {
		return fmt.Errorf("bad state: attempted to create rootfs before pulling image")
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

	// - userxattr is needed for compatibility with older kernels
	// - volatile disables fsync, as a performance optimization
	optionsTpl := "lowerdir=%s,upperdir=%s,workdir=%s,userxattr,volatile"
	tplLen := len(optionsTpl) - 3*len("%s")
	var lowerDirs []string
	for _, layer := range c.lockedImage.Layers {
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
		newLowerDirs := append(lowerDirs, path)
		mergedWorkdir := filepath.Join(c.bundlePath(), "tmp", fmt.Sprintf("merged%d.work", len(c.mergedMounts)))
		mergedUpperdir := filepath.Join(c.bundlePath(), "tmp", fmt.Sprintf("merged%d.upper", len(c.mergedMounts)))
		mntOptsLen := tplLen + len(strings.Join(append(c.mergedMounts, newLowerDirs...), ":")) + max(
			// mergedWorkdir and mergedUpperdir are always longer than workDir and upperdir.
			// So this `max` is	not strictly necessary, but it's here to fend	off future changes.
			len(mergedWorkdir)+len(mergedUpperdir),
			len(workdir)+len(upperdir),
		)
		if len(newLowerDirs) == 1 || mntOptsLen <= maxMntOptsLength {
			lowerDirs = newLowerDirs
			continue
		}

		// If the total length of the lowerDirs exceeds the kernel page size,
		// create a merged overlay mount to reduce the number of layers.
		if err := os.MkdirAll(mergedWorkdir, 0755); err != nil {
			return fmt.Errorf("create overlay workdir: %w", err)
		}
		if err := os.MkdirAll(mergedUpperdir, 0755); err != nil {
			return fmt.Errorf("create overlay upperdir: %w", err)
		}
		merged := filepath.Join(c.bundlePath(), "tmp", fmt.Sprintf("merged%d", len(c.mergedMounts)))
		if err := os.MkdirAll(merged, 0755); err != nil {
			return fmt.Errorf("create overlay merged: %w", err)
		}
		slices.Reverse(lowerDirs)
		mntOpts := fmt.Sprintf(optionsTpl, strings.Join(lowerDirs, ":"), mergedUpperdir, mergedWorkdir)
		log.CtxDebugf(ctx, "Mounting merged overlayfs to %q, options=%q, len=%d", merged, mntOpts, len(mntOpts))
		if len(mntOpts) > maxMntOptsLength {
			return fmt.Errorf("mount options too long: %d / %d. Consider using container image with fewer layers.", len(mntOpts), maxMntOptsLength)
		}
		if err := unix.Mount("none", merged, "overlay", 0, mntOpts); err != nil {
			return fmt.Errorf("mount overlayfs: %w", err)
		}
		c.mergedMounts = append(c.mergedMounts, merged)
		lowerDirs = []string{path}
	}
	if len(c.mergedMounts) != 0 {
		lowerDirs = append(c.mergedMounts, lowerDirs...)
	}

	// overlayfs "lowerdir" mount args are ordered from uppermost to lowermost,
	// but manifest layers are ordered from lowermost to uppermost. So we need to
	// reverse the order before constructing the mount option.
	slices.Reverse(lowerDirs)

	// TODO: do this mount inside a namespace so that it gets removed even if
	// the executor crashes (also needed for rootless support)
	options := fmt.Sprintf(optionsTpl, strings.Join(lowerDirs, ":"), upperdir, workdir)
	if len(options) > maxMntOptsLength {
		return fmt.Errorf("mount options too long: %d / %d. Consider using container image with fewer layers.", len(options), maxMntOptsLength)
	}
	log.CtxDebugf(ctx, "Mounting overlayfs to %q, options=%q, length=%d", c.rootfsPath(), options, len(options))
	if err := unix.Mount("none", c.rootfsPath(), "overlay", 0, options); err != nil {
		return fmt.Errorf("mount overlayfs: %w", err)
	}
	c.overlayfsMounted = true
	return nil
}

func (c *ociContainer) createSpec(ctx context.Context, cmd *repb.Command) (*specs.Spec, error) {
	env := append(baseEnv, commandutil.EnvStringList(cmd)...)
	user, err := getUser(ctx, c.lockedImage.Image, c.rootfsPath(), c.user, c.forceRoot)
	if err != nil {
		return nil, fmt.Errorf("get container user: %w", err)
	}

	caps := append(capabilities, *capAdd...)
	spec := specs.Spec{
		Version: ociVersion,
		Process: &specs.Process{
			Terminal: false,
			User:     *user,
			Args:     cmd.GetArguments(),
			Cwd:      filepath.Join(c.execrootPath, cmd.GetWorkingDirectory()),
			Env:      env,
			Rlimits: []specs.POSIXRlimit{
				{Type: "RLIMIT_NPROC", Hard: 4194304, Soft: 4194304},
			},
			Capabilities: &specs.LinuxCapabilities{
				Bounding:  caps,
				Effective: caps,
				Permitted: caps,
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
				Options:     []string{"rw", "nosuid", "nodev", "noexec", "relatime", "size=64000k"},
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
				// See comment on getFakeProcCgroupsContent
				Destination: "/proc/cgroups",
				Type:        "bind",
				Source:      filepath.Join(c.bundlePath(), "proc_cgroups"),
				Options:     []string{"bind", "rprivate", "ro"},
			},
			{
				Destination: c.execrootPath,
				Type:        "bind",
				Source:      c.workDir,
				Options:     []string{"bind", "rprivate"},
			},
		},
		Annotations: map[string]string{
			// Annotate with podman's default stop signal.
			// TODO: is this strictly needed?
			"org.opencontainers.image.stopSignal": unix.SIGTERM.String(),
		},
		Linux: &specs.Linux{
			CgroupsPath: c.cgroupRootRelativePath(),
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
	if c.lxcfsMount != "" {
		for _, mountpoint := range lxcfsFiles {
			spec.Mounts = append(spec.Mounts, specs.Mount{
				Destination: mountpoint,
				Type:        "bind",
				Source:      filepath.Join(c.lxcfsMount, mountpoint),
				Options:     []string{"bind", "rprivate"},
			})
		}
	}
	if c.tiniEnabled {
		// Bind-mount tini readonly into the container.
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Destination: tiniMountPoint,
			Type:        "bind",
			Source:      c.tiniPath,
			Options:     []string{"bind", "rprivate", "ro"},
		})
	}
	spec.Mounts = append(spec.Mounts, c.persistentVolumeMounts...)
	spec.Mounts = append(spec.Mounts, *mounts...)
	spec.Linux.Devices = append(spec.Linux.Devices, *devices...)
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
			if *commandutil.DebugStreamCommandOutputs {
				cmd.Stdout = io.MultiWriter(stdout, log.Writer("[crun] "))
			}
		} else {
			stdout = nil
			cmd.Stdout = stdio.Stdout
		}
		if stdio.Stderr == nil {
			stderr = &bytes.Buffer{}
			cmd.Stderr = stderr
			if *commandutil.DebugStreamCommandOutputs {
				cmd.Stderr = io.MultiWriter(stderr, log.Writer("[crun] "))
			}
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

	// Some actions are prone to SIGSEGV when running on an executor that is close to its memory limits.
	// Return a retryable error so we can make sure that the failure is not infrastructure related.
	if code == ociSIGSEGVExitCode {
		log.CtxWarning(ctx, "action exited with SIGSEGV")
		code = commandutil.NoExitCode
		err = errSIGSEGV
	}

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
		spec = image.ConfigFile.Config.User
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
		Umask:          new(uint32(022)), // 0644 file perms by default
	}, nil
}

func toInt32s(in []int) []int32 {
	out := make([]int32, len(in))
	for i, l := range in {
		out[i] = int32(l)
	}
	return out
}

func newCID() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// layerPath returns the path where the extracted image layer with the given
// hash is stored on disk.
func layerPath(imageCacheRoot string, hash ctr.Hash) string {
	return filepath.Join(imageCacheRoot, imageCacheVersion, hash.Algorithm, hash.Hex)
}

// ImageStore handles image layer storage for OCI containers.
type ImageStore struct {
	resolver       *oci.Resolver
	layersDir      string
	imagePullGroup singleflight.Group[string, *Image]
	layerPullGroup singleflight.Group[string, any]
	// fileCache is used for managing layer eviction.
	fileCache interfaces.FileCache

	mu           sync.RWMutex
	cachedImages map[string]*Image
}

// LockedImage represents a cached image that will not be evicted from filecache
// until Unlock is called.
type LockedImage struct {
	*Image
	unlockFns []func()
}

func (l *LockedImage) Unlock() {
	for _, unlock := range l.unlockFns {
		unlock()
	}
	l.unlockFns = nil
}

// Image represents a cached image, including all layer digests and image
// configuration.
type Image struct {
	// Layers holds the image layers from lowermost to uppermost.
	Layers []*ImageLayer

	// ConfigFile holds various image settings such as user and environment
	// directives.
	ConfigFile ctr.ConfigFile
}

// ImageLayer represents a resolved image layer.
type ImageLayer struct {
	// Path is the path where the layer is stored on disk.
	Path string
	// Size is the size of the layer in bytes.
	Size int64
	// DiffID is the uncompressed image digest.
	DiffID ctr.Hash
}

func NewImageStore(resolver *oci.Resolver, layersDir string, fileCache interfaces.FileCache) (*ImageStore, error) {
	s := &ImageStore{
		resolver:     resolver,
		layersDir:    layersDir,
		fileCache:    fileCache,
		cachedImages: map[string]*Image{},
	}

	if *enableImageEviction {
		// Populate the filecache by scanning existing layer directories.
		// This ensures that existing layers are tracked for LRU eviction.
		if err := s.populateFileCache(); err != nil {
			return nil, status.InternalErrorf("populate filecache with existing image layers: %s", err)
		}
	}

	return s, nil
}

// populateFileCache scans the layers directory and registers existing layer
// directories with the filecache. This ensures that layers from previous runs
// are tracked for LRU eviction.
func (s *ImageStore) populateFileCache() error {
	ctx := context.Background()

	// The layers directory structure is: {layersDir}/{version}/{algorithm}/{hash}/
	// e.g., /cache/images/oci/v2/sha256/abc123.../
	// We only scan the current version directory.
	versionDir := filepath.Join(s.layersDir, imageCacheVersion)
	entries, err := os.ReadDir(versionDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read	layers directory: %w", err)
	}

	var populatedCount int
	for _, algorithmEntry := range entries {
		if !algorithmEntry.IsDir() {
			continue
		}
		algorithmDir := filepath.Join(versionDir, algorithmEntry.Name())
		layerEntries, err := os.ReadDir(algorithmDir)
		if err != nil {
			return fmt.Errorf("read layer algorithm directory %s: %w", algorithmDir, err)
		}

		for _, layerEntry := range layerEntries {
			if !layerEntry.IsDir() {
				continue
			}
			layerPath := filepath.Join(algorithmDir, layerEntry.Name())
			info, err := layerEntry.Info()
			if err != nil {
				log.CtxWarningf(ctx, "Failed to get info for layer %s: %s", layerPath, err)
				continue
			}

			// Get the size of the layer directory.
			size, err := disk.DirSize(layerPath)
			if err != nil {
				log.CtxWarningf(ctx, "Failed to get size of layer %s: %s", layerPath, err)
				continue
			}

			// Track the layer directory in the filecache.
			unlock, err := s.fileCache.TrackExternalDirectory(ctx, layerPath, size)
			if err != nil {
				if status.IsNotFoundError(err) {
					// Layer not in cache yet, will be added when used.
					continue
				}
				log.CtxWarningf(ctx, "Failed to track layer %s in filecache: %s", layerPath, err)
				continue
			}
			// Immediately unlock since we're just populating the cache.
			unlock()
			populatedCount++
			log.CtxDebugf(ctx, "Populated filecache with layer %s (size=%d, mtime=%s)", layerPath, size, info.ModTime())
		}
	}

	if populatedCount > 0 {
		log.Infof("Populated filecache with %d existing image layers", populatedCount)
	}
	return nil
}

// PullAndLockImage downloads and extracts image layers to a directory, skipping
// layers that have already been downloaded, and deduping concurrent downloads
// for the same layer.
//
// It always re-authenticates the credentials with the image registry.
// Each layer is extracted to a subdirectory given by {algorithm}/{hash}, e.g.
// "sha256/abc123".
//
// The returned image is locked to prevent eviction from filecache while it is
// in use. The caller is responsible for unlocking the image once it is no
// longer being used.
func (s *ImageStore) PullAndLockImage(ctx context.Context, imageName string, creds oci.Credentials, useOCIFetcher bool) (*LockedImage, error) {
	key := hash.Strings(imageName, creds.Username, creds.Password)
	image, _, err := s.imagePullGroup.Do(ctx, key, func(ctx context.Context) (*Image, error) {
		image, err := s.pull(ctx, imageName, creds, useOCIFetcher)
		if err != nil {
			return nil, err
		}

		s.mu.Lock()
		s.cachedImages[imageName] = image
		s.mu.Unlock()

		return image, nil
	})
	if err != nil {
		return nil, err
	}
	return s.lockImage(ctx, image)
}

// GetCachedImageAndLock atomically (1) gets a reference to the cached image,
// and (2) locks the image if it exists to prevent it from being evicted from
// cache. The caller MUST call [*LockedImage.Unlock] on the returned image once
// it is no longer used, in order to re-mark the image as eligible for eviction.
func (s *ImageStore) GetCachedImageAndLock(ctx context.Context, imageName string) (_ *LockedImage, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	image, ok := s.cachedImages[imageName]
	if !ok {
		return nil, false
	}

	lockedImage, err := s.lockImage(ctx, image)
	if err != nil {
		// An image layer was evicted - don't report the image as cached.
		return nil, false
	}

	return lockedImage, true
}

func (s *ImageStore) lockImage(ctx context.Context, image *Image) (*LockedImage, error) {
	// If image cache eviction is disabled, return a LockedImage with no-op
	// unlock since we're not tracking layers in the filecache.
	if !*enableImageEviction {
		return &LockedImage{Image: image}, nil
	}

	// Lock all image layers to prevent eviction.
	var unlockFns []func()
	for _, layer := range image.Layers {
		unlock, err := s.fileCache.TrackExternalDirectory(ctx, layer.Path, layer.Size)
		if err != nil {
			// Unlock any layers that we were able to lock, to ensure we don't
			// end up with a partially locked image.
			for _, unlock := range unlockFns {
				unlock()
			}
			if !status.IsNotFoundError(err) {
				log.CtxErrorf(ctx, "Failed to track cached image layer directory %q: %s", layer.Path, err)
			}
			return nil, status.UnavailableErrorf("track cached image layer directory: %s", err)
		}
		unlockFns = append(unlockFns, unlock)
	}
	return &LockedImage{
		Image:     image,
		unlockFns: unlockFns,
	}, nil
}

func (s *ImageStore) pull(ctx context.Context, imageName string, creds oci.Credentials, useOCIFetcher bool) (*Image, error) {
	img, err := s.resolver.Resolve(ctx, imageName, oci.RuntimePlatform(), creds, useOCIFetcher)
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
		eg.Go(func() (err error) {
			d, err := layer.DiffID()
			if err != nil {
				return status.UnavailableErrorf("get layer digest: %s", err)
			}
			resolvedLayer.DiffID = d

			destDir := layerPath(s.layersDir, d)
			resolvedLayer.Path = destDir

			size, err := layer.Size()
			if err != nil {
				return status.UnavailableErrorf("get layer size: %s", err)
			}
			resolvedLayer.Size = size

			// Check whether the layer already exists and can be reused.
			if *enableImageEviction {
				// Check via the filecache LRU - this also temporarily locks the
				// layer to prevent eviction during the existence check.
				unlock, err := s.fileCache.TrackExternalDirectory(ctx, destDir, size)
				if err != nil {
					if !status.IsNotFoundError(err) {
						return status.UnavailableErrorf("stat layer directory: %s", err)
					}
					// Layer not in cache, fall through to download it.
				} else {
					// Layer exists in cache. Release the lock for now - we will
					// properly re-lock the layers individually later, once we
					// are outside of the singleflight group.
					//
					// This does introduce a tiny period where the layer could
					// get evicted if there is a high filecache churn rate, but
					// we assume this will be rare enough that a client retry is
					// acceptable in this case.
					unlock()
					return nil
				}
			} else {
				// Image cache eviction disabled - just check if dir exists on disk.
				if _, err := os.Stat(destDir); err == nil {
					return nil
				}
			}

			// Layer dir does not exist in cache, so we need to download it.

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
		resolvedImage.ConfigFile = *f
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return resolvedImage, nil
}

// downloadLayer downloads and extracts the given layer to the given destination
// dir. The extracted layer is suitable for use as an overlayfs lowerdir.
//
// For reference implementations, see:
//   - Podman: https://github.com/containers/storage/blob/664fe5d9b95004e1be3eee004d56a1715c8ca790/pkg/archive/archive.go#L707-L729
//   - Moby (Docker): https://github.com/moby/moby/blob/9633556bef3eb20dfe888903660c3df89a73605b/pkg/archive/archive.go#L726-L735
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

	// Track all operations using [fsync.Root] so that once we're done unpacking
	// the layer we can ensure it gets persisted to disk (to avoid corruption).
	root := fsync.NewRoot(tempUnpackDir, nil)

	tr := tar.NewReader(rc)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.UnavailableErrorf("download and extract layer tarball: %s", err)
		}

		if slices.Contains(strings.Split(header.Name, string(os.PathSeparator)), "..") {
			return status.InvalidArgumentErrorf("invalid tar header: name %q is invalid", header.Name)
		}

		// filepath.Join applies filepath.Clean to all arguments
		file := filepath.Join(tempUnpackDir, header.Name)
		base := filepath.Base(file)
		dir := filepath.Dir(file)

		if header.Typeflag == tar.TypeDir ||
			header.Typeflag == tar.TypeReg ||
			header.Typeflag == tar.TypeSymlink ||
			header.Typeflag == tar.TypeLink {
			// Ensure that parent dir exists
			if err := os.MkdirAll(dir, os.ModePerm); err != nil {
				return status.UnavailableErrorf("create directory: %s", err)
			}
		} else {
			log.CtxDebugf(ctx, "Ignoring unsupported tar header %q type %q in oci layer", header.Name, header.Typeflag)
			continue
		}

		const whiteoutPrefix = ".wh."
		// Handle whiteout
		if strings.HasPrefix(base, whiteoutPrefix) {
			// Directory whiteout
			if base == whiteoutPrefix+whiteoutPrefix+".opq" {
				if err := root.Setxattr(dir, "trusted.overlay.opaque", []byte{'y'}, 0); err != nil {
					return status.UnavailableErrorf("setxattr on deleted dir: %s", err)
				}
				continue
			}

			// File whiteout: Mark the file for deletion in overlayfs.
			originalBase := base[len(whiteoutPrefix):]
			originalPath := filepath.Join(dir, originalBase)
			if err := root.Mknod(originalPath, unix.S_IFCHR, 0); err != nil {
				return status.UnavailableErrorf("mknod for whiteout marker: %s", err)
			}
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := root.MkdirAll(file, os.FileMode(header.Mode), header.Uid, header.Gid); err != nil {
				return status.UnavailableErrorf("create directory: %s", err)
			}
		case tar.TypeReg:
			if err := root.CreateFile(file, os.FileMode(header.Mode), tr, header.Uid, header.Gid); err != nil {
				return status.UnavailableErrorf("create file: %s", err)
			}
		case tar.TypeSymlink:
			// Symlink's target is only evaluated at runtime, inside the container context.
			// So it's safe to have the symlink targeting paths outside unpackdir.
			if err := root.Symlink(header.Linkname, file, header.Uid, header.Gid); err != nil {
				return status.UnavailableErrorf("create symlink: %s", err)
			}
		case tar.TypeLink:
			target := filepath.Join(tempUnpackDir, header.Linkname)
			if !strings.HasPrefix(target, tempUnpackDir) {
				return status.InvalidArgumentErrorf("invalid tar header: link name %q is invalid", header.Linkname)
			}
			// Note that this will call linkat(2) without AT_SYMLINK_FOLLOW,
			// so if target is a symlink, the hardlink will point to the symlink itself and not the symlink target.
			if err := root.Link(target, file); err != nil {
				return status.UnavailableErrorf("create hard link: %s", err)
			}
		}
	}

	if err := root.Sync(); err != nil {
		return status.UnavailableErrorf("sync layer paths: %s", err)
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

	if err := fsync.SyncPath(filepath.Dir(destDir)); err != nil {
		return status.UnavailableErrorf("sync layer dir: %s", err)
	}

	return nil
}

// Statusz returns statusz page contents for the image store.
func (s *ImageStore) Statusz(ctx context.Context) string {
	var h strings.Builder
	h.WriteString(`<ul>`)
	names := slices.Collect(maps.Keys(s.cachedImages))
	slices.Sort(names)
	for _, name := range names {
		downloadURL := fmt.Sprintf(
			"%s/%s/download?name=%s",
			statusz.BasePath, imagesStatuszSectionName, url.QueryEscape(name))
		h.WriteString(`<li>`)
		h.WriteString(`<a href="` + downloadURL + `" target="_blank">`)
		h.WriteString(html.EscapeString(name))
		h.WriteString(`</a>`)
		h.WriteString(`</li>`)
	}
	h.WriteString(`</ul>`)
	return h.String()
}

func (s *ImageStore) ServeStatusz(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/download" {
		s.download(w, r)
		return
	}
	http.NotFound(w, r)
}

var filenameUnsafeChars = regexp.MustCompile(`[^a-zA-Z0-9_\-]`)

func sanitizeFilename(name string) string {
	return filenameUnsafeChars.ReplaceAllString(name, "_")
}

// downloads a tarball image that can be loaded with 'docker load'.
func (s *ImageStore) download(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	s.mu.RLock()
	image, ok := s.cachedImages[name]
	s.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()

	downloadTmpDir := filepath.Join(s.layersDir, "image-download"+tmpSuffix())
	if err := os.MkdirAll(downloadTmpDir, 0755); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(downloadTmpDir)

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.tar", sanitizeFilename(name)))
	w.Header().Set("Content-Type", "application/x-tar")
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	if err := s.writeDockerImageTarball(ctx, w, image, name, downloadTmpDir); err != nil {
		if ctx.Err() != nil {
			// Request was cancelled
			return
		} else {
			log.CtxWarningf(ctx, "Failed to export docker image tarball: %s", err)
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func withImageConfig(cmd *repb.Command, image *Image) (*repb.Command, error) {
	// Apply any env vars from the image which aren't overridden by the command
	cmdVarNames := make(map[string]bool, len(cmd.EnvironmentVariables))
	for _, cmdVar := range cmd.GetEnvironmentVariables() {
		cmdVarNames[cmdVar.GetName()] = true
	}
	imageEnv, err := commandutil.EnvProto(image.ConfigFile.Config.Env)
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

	// Return a copy of the command but with the image config applied
	out := cmd.CloneVT()
	out.Arguments = append(image.ConfigFile.Config.Entrypoint, cmd.Arguments...)
	out.EnvironmentVariables = outEnv
	return out, nil
}

func tmpSuffix() string {
	return fmt.Sprintf(".%d.tmp", mrand.Int64N(1e18))
}

// Removes any versioned image cache directories under the given path which
// do not match the current version.
func cleanStaleImageCacheDirs(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("read dir %q: %w", root, err)
	}
	for _, e := range entries {
		if e.Name() == imageCacheVersion || !versionDirRegexp.MatchString(e.Name()) {
			continue
		}
		path := filepath.Join(root, e.Name())
		log.Infof("Removing stale image cache at %q", path)
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("remove %q: %w", path, err)
		}
	}
	return nil
}

// writeDockerImageTarball constructs a Docker-compatible .tar archive and
// writes it to the provided io.Writer.
func (s *ImageStore) writeDockerImageTarball(ctx context.Context, w io.Writer, image *Image, imageName, tmpDir string) error {
	tarWriter := tar.NewWriter(w)

	// Add each layer to the tarball, storing the diff IDs for each.
	// The layer will be named {diffID}.tar.gz in the tarball.
	var layerFiles []*os.File
	var layerDiffIDs []ctr.Hash
	var layerFileNames []string
	for _, layer := range image.Layers {
		compressedTarball, err := os.CreateTemp(tmpDir, "docker-layer-*.tar.gz")
		if err != nil {
			return fmt.Errorf("failed to create temp file for layer %q: %w", layer.DiffID.Hex, err)
		}
		defer os.Remove(compressedTarball.Name())
		defer compressedTarball.Close()
		diffID, err := s.writeCompressedLayerTarball(ctx, compressedTarball.Name(), layer)
		if err != nil {
			return fmt.Errorf("write compressed layer %q: %w", layer.DiffID.Hex, err)
		}
		layerFiles = append(layerFiles, compressedTarball)
		layerFileNames = append(layerFileNames, fmt.Sprintf("%s.tar.gz", layer.DiffID.Hex))
		layerDiffIDs = append(layerDiffIDs, *diffID)
	}

	// Prepare the image configuration and manifest
	cfg := image.ConfigFile // shallow copy
	cfg.RootFS = ctr.RootFS{
		Type:    "layers",
		DiffIDs: layerDiffIDs,
	}
	cfg.History = nil
	cfg.Created = ctr.Time{Time: time.Now()}

	configBytes, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal image config: %w", err)
	}
	configHash := sha256.Sum256(configBytes)
	configHex := hex.EncodeToString(configHash[:])
	configFilename := fmt.Sprintf("%s.json", configHex)

	// Generate a new tag based on the image name.
	// e.g. "ubuntu@sha256:..." -> "ubuntu:buildbuddy-exported-20250101000000"
	repoTag := imageName
	repoTag, _, _ = strings.Cut(repoTag, "@")
	repoTag, _, _ = strings.Cut(repoTag, ":")
	repoTag += ":buildbuddy-executor-exported-" + time.Now().Format("20060102150405")

	manifest := []struct {
		Config   string   `json:"Config"`
		RepoTags []string `json:"RepoTags"`
		Layers   []string `json:"Layers"`
	}{
		{
			Config:   configFilename,
			RepoTags: []string{repoTag},
			Layers:   layerFileNames,
		},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}
	// Add manifest and config file to the tarball.
	if err := addFileToTar(tarWriter, "manifest.json", manifestBytes); err != nil {
		return fmt.Errorf("failed to add manifest.json to tar: %w", err)
	}
	if err := addFileToTar(tarWriter, configFilename, configBytes); err != nil {
		return fmt.Errorf("failed to add config to tar: %w", err)
	}
	// Add each layer to the tarball.
	for i, layer := range image.Layers {
		f := layerFiles[i]
		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("stat layer %q: %w", layer.DiffID.Hex, err)
		}
		layerHeader := &tar.Header{
			Name:    layerFileNames[i],
			Mode:    0644,
			Size:    fi.Size(),
			ModTime: time.Now(),
		}
		if err := tarWriter.WriteHeader(layerHeader); err != nil {
			return fmt.Errorf("failed to write layer header: %w", err)
		}
		// Stream the layer from the temp file into the final tar writer.
		// We need to seek back to the start of the file before we can read it.
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek temp layer file: %w", err)
		}
		if _, err := io.Copy(tarWriter, f); err != nil {
			return fmt.Errorf("failed to stream layer from temp file to final tar: %w", err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		return fmt.Errorf("failed to flush tar writer: %w", err)
	}
	return nil
}

func (s *ImageStore) writeCompressedLayerTarball(ctx context.Context, compressedTarballPath string, layer *ImageLayer) (*ctr.Hash, error) {
	f, err := os.Create(compressedTarballPath)
	if err != nil {
		return nil, fmt.Errorf("create compressed layer tarball: %w", err)
	}
	defer f.Close()

	gzipWriter := gzip.NewWriter(f)
	diffIDHasher := sha256.New()

	// We use an io.MultiWriter to pipe the output of the 'tar' command to two places:
	// 1. The gzip.Writer, which compresses the layer and writes it to our temp file.
	// 2. The sha256.Hasher, which calculates the diffID of the *uncompressed* layer.
	uncompressedWriter := io.MultiWriter(gzipWriter, diffIDHasher)

	layerPath := layerPath(s.layersDir, layer.DiffID)
	if err := ociconv.OverlayfsLayerToTarball(ctx, uncompressedWriter, layerPath); err != nil {
		return nil, fmt.Errorf("failed to convert layer to tarball: %w", err)
	}
	// The gzip.Writer must be closed to flush all buffered data to the
	// underlying file.
	if err := gzipWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close gzip writer: %w", err)
	}

	diffID := &ctr.Hash{
		Algorithm: "sha256",
		Hex:       hex.EncodeToString(diffIDHasher.Sum(nil)),
	}

	return diffID, nil
}

// addFileToTar is a helper to write a file (represented by a byte slice) into a tar archive.
func addFileToTar(tw *tar.Writer, filename string, content []byte) error {
	hdr := &tar.Header{
		Name:    filename,
		Mode:    0644,
		Size:    int64(len(content)),
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("failed to write header for %s: %w", filename, err)
	}
	if _, err := tw.Write(content); err != nil {
		return fmt.Errorf("failed to write content for %s: %w", filename, err)
	}
	return nil
}

// Returns the contents of the fake /proc/cgroups file to be mounted into the
// container.
//
// /proc/cgroups is a file that existed in cgroups v1. The file provided
// information about enabled v1 controllers. However, even if the system has
// cgroup v2 enabled, the kernel still creates and populates this file, in an
// attempt to avoid breaking older software that might depend on this v1 file's
// existence. The populated data is essentially "fake" - it lists cgroup v1
// controllers that no longer exist, and it reports all controllers as enabled.
//
// However, the data populated in this fake /proc/cgroups file seems to be
// incomplete in some situations. In particular, the "cpuset" row can be
// missing, even if the cgroup v2 cpuset controller is enabled. This breaks
// Java's "UseContainerSupport" mechanism, which is what allows the JVM to limit
// heap usage based on cgroup v2 hard limits. More specifically, when the JVM
// initializes its container support, it reads this /proc/cgroups file, and if
// any expected rows are missing (including the cpuset row), it skips enabling
// container support. As a result, Java programs that use a lot of memory may
// not run GC when they are getting close to their memory limit, and will wind
// up getting OOM-killed.
//
// Arguably, this is a bug in the JVM, since the JVM should be reading
// /sys/fs/cgroup/cgroup.controllers instead of /proc/cgroups. This bug has been
// fixed, but (unfortunately) the fix is only available in JVM 25+, which does
// not yet have widespread adoption. The fix commit is here:
// https://github.com/openjdk/jdk/commit/9c5ed23eac7470f56d498e9c4d3c51c2f80fd571
//
// NOTE(bduffany): at some point, it might be nice to figure out why the cpuset
// row is missing from /proc/cgroups. After briefly looking at the kernel
// source, it seems like we might be able to fix this by setting the
// 'cgroup_v1_proc' kernel boot param, but for now it seems less painful (and
// probably more portable) to fake out this file, and it should be fine to do so
// since it's already technically a fake file anyway. In any case, the relevant
// kernel source is here:
// https://github.com/torvalds/linux/blob/07e27ad16399afcd693be20211b0dfae63e0615f/kernel/cgroup/cgroup-v1.c#L676-L705
func getFakeProcCgroupsContent() string {
	out := "#subsys_name\thierarchy\tnum_cgroups\tenabled\n"
	for _, v1Controller := range []string{
		"cpuset", "cpu", "cpuacct", "blkio", "memory", "devices", "freezer",
		"net_cls", "perf_event", "net_prio", "hugetlb", "pids", "rdma", "misc",
	} {
		out += v1Controller + "\t0\t1\t1\n"
	}
	return out
}
