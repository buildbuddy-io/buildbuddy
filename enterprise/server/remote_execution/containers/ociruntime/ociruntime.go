package ociruntime

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	_ "embed"
	mrand "math/rand/v2"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

var (
	Runtime     = flag.String("executor.oci.runtime", "", "OCI runtime")
	runtimeRoot = flag.String("executor.oci.runtime_root", "", "Root directory for storage of container state (see <runtime> --help for default)")
)

const (
	ociVersion = "1.1.0-rc.3" // matches podman

	// Execution root directory path relative to the container rootfs directory.
	execrootPath = "/buildbuddy-execroot"

	// Fake image ref indicating that busybox should be manually provisioned.
	// TODO: get rid of this
	TestBusyboxImageRef = "test.buildbuddy.io/busybox"
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
		"CAP_AUDIT_WRITE",
		"CAP_CHOWN",
		"CAP_DAC_OVERRIDE",
		"CAP_FOWNER",
		"CAP_FSETID",
		"CAP_KILL",
		"CAP_MKNOD",
		"CAP_NET_BIND_SERVICE",
		"CAP_NET_RAW",
		"CAP_SETFCAP",
		"CAP_SETGID",
		"CAP_SETPCAP",
		"CAP_SETUID",
		"CAP_SYS_CHROOT",
	}
)

type provider struct {
	env environment.Env

	// Root directory where all container runtime information will be located.
	// Each subdirectory corresponds to a created container instance.
	containersRoot string

	// Root directory where all image layer contents will be located.
	// This directory is structured like the following:
	//
	// - {layersRoot}/
	//   - {hashingAlgorithm}/
	//     - {hash}/
	//       - /bin/ # layer contents
	//       - /usr/
	//       - ...
	layersRoot string

	// Configured runtime path.
	runtime string
}

func NewProvider(env environment.Env, buildRoot string) (*provider, error) {
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
	layersRoot := filepath.Join(buildRoot, "executor", "oci", "layers")
	if err := os.MkdirAll(layersRoot, 0755); err != nil {
		return nil, err
	}
	return &provider{
		env:            env,
		runtime:        rt,
		containersRoot: containersRoot,
		layersRoot:     layersRoot,
	}, nil
}

func (p *provider) New(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
	return &ociContainer{
		env:            p.env,
		runtime:        p.runtime,
		containersRoot: p.containersRoot,
		layersRoot:     p.layersRoot,
		imageRef:       args.Props.ContainerImage,
	}, nil
}

type ociContainer struct {
	env environment.Env

	runtime        string
	containersRoot string
	layersRoot     string

	cid     string
	workDir string

	imageRef         string
	layerDigests     []ctr.Hash
	overlayfsMounted bool
}

// Returns the OCI bundle directory for the container.
func (c *ociContainer) bundlePath() string {
	return filepath.Join(c.containersRoot, c.cid)
}

// Returns the standard rootfs path expected by crun.
func (c *ociContainer) rootfsPath() string {
	return filepath.Join(c.bundlePath(), "rootfs")
}

// Returns the root path where overlay workdir and upperdir for this container
// are stored.
func (c *ociContainer) overlayTmpPath() string {
	return c.workDir + ".overlay"
}

// Returns the standard config.json path expected by crun.
func (c *ociContainer) configPath() string {
	return filepath.Join(c.bundlePath(), "config.json")
}

func (c *ociContainer) hostname() string {
	return c.cid
}

// createBundle creates the OCI bundle directory, which includes the OCI spec
// file (config.json), the rootfs directory, and other supplementary data files
// (e.g. the 'hosts' file which will be mounted to /etc/hosts).
func (c *ociContainer) createBundle(ctx context.Context, cmd *repb.Command) error {
	if err := os.MkdirAll(c.bundlePath(), 0755); err != nil {
		return fmt.Errorf("mkdir -p %s: %w", c.bundlePath(), err)
	}

	hostnamePath := filepath.Join(c.bundlePath(), "hostname")
	if err := os.WriteFile(hostnamePath, []byte(c.hostname()), 0644); err != nil {
		return fmt.Errorf("write %s: %w", hostnamePath, err)
	}
	// TODO: append 'hosts.container.internal' and <cid> host to match podman?
	if err := os.WriteFile(filepath.Join(c.bundlePath(), "hosts"), hostsFile, 0644); err != nil {
		return fmt.Errorf("write hosts file: %w", err)
	}

	// Create rootfs
	if err := c.createRootfs(ctx); err != nil {
		return fmt.Errorf("create rootfs: %w", err)
	}

	// Create config.json
	spec, err := c.createSpec(cmd)
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
	return false, nil // TODO: implement
}

func (c *ociContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	if c.imageRef == TestBusyboxImageRef {
		return nil
	}
	layers, err := pull(ctx, c.layersRoot, c.imageRef, creds)
	if err != nil {
		return status.WrapError(err, "pull OCI image")
	}
	var layerDigests []ctr.Hash
	for _, layer := range layers {
		d, err := layer.Digest()
		if err != nil {
			return status.UnavailableErrorf("get layer digest: %s", err)
		}
		layerDigests = append(layerDigests, d)
	}
	c.layerDigests = layerDigests
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

	if err := c.createBundle(ctx, cmd); err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf("create OCI bundle: %s", err))
	}

	return c.invokeRuntime(ctx, nil /*=cmd*/, &interfaces.Stdio{}, 0 /*=waitDelay*/, "run", "--bundle="+c.bundlePath(), c.cid)
}

func (c *ociContainer) Create(ctx context.Context, workDir string) error {
	c.workDir = workDir
	cid, err := newCID()
	if err != nil {
		return status.UnavailableErrorf("generate cid: %s", err)
	}
	c.cid = cid

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
	// TODO: Should we specify a non-zero waitDelay so that a process that spawns children won't
	//  block forever?
	return c.invokeRuntime(ctx, cmd, stdio, 0, "exec", "--cwd="+execrootPath, c.cid)
}

func (c *ociContainer) Pause(ctx context.Context) error {
	return nil // TODO: implement
}

func (c *ociContainer) Unpause(ctx context.Context) error {
	return nil // TODO: implement
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

	if err := os.RemoveAll(c.bundlePath()); err != nil && firstErr == nil {
		firstErr = status.UnavailableErrorf("remove bundle: %s", err)
	}

	return firstErr
}

func (c *ociContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	// TODO: read stats from cgroupfs
	return nil, nil
}

func (c *ociContainer) createRootfs(ctx context.Context) error {
	if err := os.MkdirAll(c.rootfsPath(), 0755); err != nil {
		return fmt.Errorf("create rootfs dir: %w", err)
	}

	// For testing only, support a fake image ref that means "install busybox
	// manually".
	// TODO: improve testing setup and get rid of this
	if c.imageRef == TestBusyboxImageRef {
		return installBusybox(c.rootfsPath())
	}

	if c.imageRef == "" {
		// No image specified (sandbox-only).
		return nil
	}

	// Create an overlayfs with the pulled image layers.
	var lowerDirs []string
	for _, d := range c.layerDigests {
		lowerDirs = append(lowerDirs, layerPath(c.layersRoot, d))
	}
	// Create workdir and upperdir.
	workdir := filepath.Join(c.overlayTmpPath(), "work")
	if err := os.MkdirAll(workdir, 0755); err != nil {
		return fmt.Errorf("create overlay workdir: %w", err)
	}
	upperdir := filepath.Join(c.overlayTmpPath(), "upper")
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
	if err := syscall.Mount("none", c.rootfsPath(), "overlay", 0, options); err != nil {
		return fmt.Errorf("mount overlayfs: %w", err)
	}
	c.overlayfsMounted = true
	return nil
}

func installBusybox(path string) error {
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
	b, err := exec.Command(busyboxPath, "--list").Output()
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

func (c *ociContainer) createSpec(cmd *repb.Command) (*specs.Spec, error) {
	// TODO: respect environment variables inside the container (ENV directives)
	env := append([]string{
		// TODO: make sure this PATH matches podman's behavior
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}, commandutil.EnvStringList(cmd)...)

	spec := specs.Spec{
		Version: ociVersion,
		Process: &specs.Process{
			Terminal: false,
			// TODO: parse USER[:GROUP] from dockerUser
			User: specs.User{
				UID:   0,
				GID:   0,
				Umask: pointer(uint32(022)), // 0644 file perms by default
			},
			Args: cmd.GetArguments(),
			Cwd:  execrootPath,
			Env:  env,
			// TODO: rlimits
			Rlimits: []specs.POSIXRlimit{},
			// TODO: audit these
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
		Hostname: c.hostname(),
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
			// TODO: enable devpts
			// {
			// 	Destination: "/dev/pts",
			// 	Type:        "devpts",
			// 	Source:      "devpts",
			// 	Options: []string{
			// 		"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620",
			// 		// TODO: gid=5 doesn't work in some circumstances.
			// 		// See https://github.com/containers/podman/blob/b8d95a5893572b37c8257407e964ad06ba87ade6/pkg/specgen/generate/oci_linux.go#L141-L173
			// 		"gid=5",
			// 	},
			// },
			{
				Destination: "/dev/mqueue",
				Type:        "mqueue",
				Source:      "mqueue",
				Options:     []string{"nosuid", "noexec", "nodev"},
			},
			// TODO: resolv.conf
			// {
			// 		Destination: "/etc/resolv.conf",
			// 		Type:        "bind",
			// 		Source:      "/run/containers/storage/overlay-containers/99133d16f4f9d0678f87972c01209e308ebafc074f333822805a633620f12507/userdata/resolv.conf",
			// 		Options:     []string{"bind", "rprivate"},
			// },
			{
				Destination: "/etc/hosts",
				Type:        "bind",
				Source:      filepath.Join(c.bundlePath(), "hosts"),
				Options:     []string{"bind", "rprivate"},
			},
			// TODO: shm
			// {
			// 		Destination: "/dev/shm",
			// 		Type:        "bind",
			// 		Source:      "/run/containers/storage/overlay-containers/99133d16f4f9d0678f87972c01209e308ebafc074f333822805a633620f12507/userdata/shm",
			// 		Options:     []string{"bind", "rprivate", "nosuid", "noexec", "nodev"},
			// },
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
				// TODO: setup networking and set the correct namespace path
				// here
				{Type: specs.NetworkNamespace},
			},
			Seccomp: &seccomp,
			Devices: []specs.LinuxDevice{},
			Resources: &specs.LinuxResources{
				// TODO: networking
				Network: nil,
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

	// Provision devices based on host device node info.
	// TODO: set up devices the same way podman does.
	for _, path := range []string{
		"/dev/null",
		"/dev/zero",
		"/dev/random",
		"/dev/urandom",
	} {
		d, err := statDevice(path)
		if err != nil {
			return nil, fmt.Errorf("get %s device spec: %w", path, err)
		}
		spec.Linux.Devices = append(spec.Linux.Devices, *d)
		spec.Linux.Resources.Devices = append(spec.Linux.Resources.Devices, specs.LinuxDeviceCgroup{
			Allow:  true,
			Access: "rw",
			Type:   d.Type,
			Major:  &d.Major,
			Minor:  &d.Minor,
		})
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

	cmd := exec.Command(runtimeArgs[0], runtimeArgs[1:]...)
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

func statDevice(path string) (*specs.LinuxDevice, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("could not stat device file: %v", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("unexpected type %T", err)
	}

	// Get device type (block or character)
	var devType string
	switch t := stat.Mode & syscall.S_IFMT; t {
	case syscall.S_IFBLK:
		devType = "b" // Block device
	case syscall.S_IFCHR:
		devType = "c" // Character device
	default:
		return nil, fmt.Errorf("unsupported device type 0x%x", t)
	}

	return &specs.LinuxDevice{
		Path:     path,
		Type:     devType,
		Major:    int64(unix.Major(stat.Rdev)),
		Minor:    int64(unix.Minor(stat.Rdev)),
		FileMode: pointer(os.FileMode(stat.Mode)),
		UID:      pointer(stat.Uid),
		GID:      pointer(stat.Gid),
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
func layerPath(layersDir string, hash ctr.Hash) string {
	return filepath.Join(layersDir, hash.Algorithm, hash.Hex)
}

// pull downloads and extracts image layers to a directory.
// Each layer is extracted to a subdirectory given by {algorithm}/{hash}, e.g.
// "sha256/abc123".
func pull(ctx context.Context, layersDir, imageName string, creds oci.Credentials) ([]ctr.Layer, error) {
	img, err := oci.Resolve(ctx, imageName, oci.RuntimePlatform(), creds)
	if err != nil {
		return nil, status.WrapError(err, "resolve image")
	}
	layers, err := img.Layers()
	if err != nil {
		return nil, status.UnavailableErrorf("get image layers: %s", err)
	}

	// Download and extract layers concurrently.
	// TODO: dedupe layer pulls
	var eg errgroup.Group
	eg.SetLimit(min(8, runtime.NumCPU()))
	for _, layer := range layers {
		layer := layer
		eg.Go(func() error {
			d, err := layer.Digest()
			if err != nil {
				return status.UnavailableErrorf("get layer digest: %s", err)
			}

			destDir := layerPath(layersDir, d)

			// If the destination directory already exists then we can skip
			// the download.
			if _, err := os.Stat(destDir); err != nil {
				if !os.IsNotExist(err) {
					return status.UnavailableErrorf("stat layer directory: %s", err)
				}
			} else {
				return nil
			}

			rc, err := layer.Compressed()
			if err != nil {
				return status.UnavailableErrorf("get layer reader: %s", err)
			}
			defer rc.Close()

			tempUnpackDir := destDir + tmpSuffix()
			if err := os.MkdirAll(tempUnpackDir, 0755); err != nil {
				return status.UnavailableErrorf("create layer unpack dir: %s", err)
			}
			defer os.RemoveAll(tempUnpackDir)

			// TODO: avoid tar command.
			cmd := exec.CommandContext(ctx, "tar", "--no-same-owner", "--extract", "--gzip", "--directory", tempUnpackDir)
			var stderr bytes.Buffer
			cmd.Stdin = rc
			cmd.Stderr = &stderr
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := cmd.Run(); err != nil {
				return status.UnavailableErrorf("download and extract layer tarball: %s: %q", err, stderr.String())
			}

			if err := os.Rename(tempUnpackDir, destDir); err != nil {
				return status.UnavailableErrorf("rename temp layer dir: %s", err)
			}

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return layers, nil
}

func tmpSuffix() string {
	return fmt.Sprintf(".%d.tmp", mrand.Int64N(1e18))
}
