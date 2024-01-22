package linux_sandbox

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/sandboxutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ociconv"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	_ "embed"

	lspb "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/proto/execution_statistics"
	sbdpb "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/proto/sandboxd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/sandboxd_client"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

var (
	logDebugOutput = flag.Bool("executor.linux_sandbox.log_debug_output", false, "If true, capture debug output from the linux-sandbox and log it at INFO level.")
	debugStderr    = flag.Bool("debug_linux_sandbox_stderr", false, "If true, stream linux-sandbox debug output directly to stderr. Incompatible with other debug log flags. For development only.")
)

const (
	// Workspace directory relative to the sandbox execution root.
	// This is where the action inputs and outputs will be located.
	workspaceDirName = "workspace"
	// File name where the binary stats file will be written, relative to the
	// sandbox execroot.
	statsFileName = "stats.out"
	// File name where the debug logs will be written (if enabled), relative to
	// the sandbox execroot.
	debugLogFileName = "debug.out"
)

var (
	// Host dirs that are allowed to be mounted into the sandbox
	// when no container-image is requested.
	//
	// TODO: Make this configurable
	// TODO: filter this list to just the dirs that exist
	allowedHostMounts = []string{
		"bin",
		"lib",
		"lib32",
		"lib64",
		"usr",
		// bash cannot resolve binaries in PATH like awk (required for
		// genrules), even if PATH is set properly, unless /etc is mounted.
		// TODO: there must be some config under /etc that makes this work,
		// figure out what it is and mount just that config instead of the whole
		// dir.
		"etc",
	}

	// Paths that should not be mounted into the sandbox, whether from the host
	// FS or the container FS.
	disallowedMounts = []string{
		// Special filesystems (these will be overridden by host dirs)
		"dev",
		"proc",
		"sys",
	}
)

//go:embed linux-sandbox
var toolBytes []byte

//go:embed sandboxd-bin
var sandboxdBytes []byte

type Provider struct {
	env environment.Env
	// Path to the linux-sandbox binary.
	toolPath string
	sandboxd *sandboxd_client.Sandboxd
	// Path to the executor build root dir, where container images will be
	// extracted (if applicable).
	buildRoot string
}

func createBin(dir, name string, b []byte) (string, error) {
	path := filepath.Join(dir, name)
	// Remove existing executable first (fixes "text file busy" errors in tests)
	if err := os.RemoveAll(path); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, b, 0755); err != nil {
		return "", err
	}
	return path, nil
}

func NewProvider(env environment.Env, buildRoot string) (*Provider, error) {
	// Write the embedded linux-sandbox binary to
	// {buildRoot}/executor/bin/linux-sandbox.
	// TODO: find a better home for the binary.
	executorBinDir := filepath.Join(buildRoot, "executor/bin")
	if err := os.MkdirAll(executorBinDir, 0755); err != nil {
		return nil, err
	}
	toolPath, err := createBin(executorBinDir, "linux-sandbox", toolBytes)
	if err != nil {
		return nil, err
	}
	sandboxdPath, err := createBin(executorBinDir, "sandboxd", sandboxdBytes)
	if err != nil {
		return nil, err
	}
	// Create sandboxd socket
	runPath := filepath.Join(buildRoot, "executor/run")
	if err := os.MkdirAll(runPath, 0755); err != nil {
		return nil, err
	}
	socket := filepath.Join(runPath, fmt.Sprintf("sandbox.%d.sock", rand.Uint64()))

	var sandboxd *sandboxd_client.Sandboxd
	if os.Getuid() != 0 {
		s, err := sandboxd_client.StartSandboxd(sandboxdPath, socket)
		if err != nil {
			return nil, status.WrapError(err, "start sandboxd")
		}
		sandboxd = s
	}

	// Debug: show tool paths available to the host
	// (used when no container-image is passed)
	b, _ := exec.Command("sh", "-c", `
		find "/usr/lib/gcc/x86_64-linux-gnu/" -maxdepth 1 -mindepth 1
	`).Output()
	log.Debugf("linux-sandbox: found host tool paths:\n%s", strings.TrimSpace(string(b)))

	return &Provider{
		env:       env,
		toolPath:  toolPath,
		sandboxd:  sandboxd,
		buildRoot: buildRoot,
	}, nil
}

func (p *Provider) New(ctx context.Context, props *platform.Properties, task *repb.ScheduledTask, state *rnpb.RunnerState, workDir string) (container.CommandContainer, error) {
	return &sandbox{
		env:       p.env,
		imageRef:  props.ContainerImage,
		toolPath:  p.toolPath,
		sandboxd:  p.sandboxd,
		buildRoot: p.buildRoot,
	}, nil
}

func (p *Provider) Close() error {
	if p.sandboxd != nil {
		return p.sandboxd.Close()
	}
	return nil
}

// sandbox is a container implementation which uses the linux-sandbox
// binary that ships with bazel.
type sandbox struct {
	env environment.Env
	// Path to the linux-sandbox binary.
	toolPath string
	// sandboxd handle
	sandboxd *sandboxd_client.Sandboxd
	// Path to the executor build root dir, where container images will be
	// extracted (if applicable).
	buildRoot string
	// OCI image ref that will be used to back the sandbox root FS.
	imageRef string
	// Path to the extraced OCI image root FS.
	rootFSDir string
	// Path to the action working directory.
	workDir string
	// Paths to be unmounted by sandboxd in Remove().
	pathsToUnmount []string
}

var _ container.CommandContainer = (*sandbox)(nil)

func (s *sandbox) IsolationType() string {
	return string(platform.LinuxSandboxContainerType)
}

func (s *sandbox) Run(ctx context.Context, command *repb.Command, workDir string, creds oci.Credentials) *interfaces.CommandResult {
	if err := container.PullImageIfNecessary(ctx, s.env, s, creds, s.imageRef); err != nil {
		return commandutil.ErrorResult(status.WrapError(err, "pull image"))
	}
	if err := s.Create(ctx, workDir); err != nil {
		return commandutil.ErrorResult(err)
	}
	return s.Exec(ctx, command, nil /*=stdio*/)
}

func (s *sandbox) IsImageCached(ctx context.Context) (bool, error) {
	if s.imageRef == "" {
		return false, nil
	}
	rootFSDir, err := ociconv.CachedRootFSPath(ctx, s.buildRoot, s.imageRef)
	if err != nil {
		return false, err
	}
	if rootFSDir != "" {
		s.rootFSDir = rootFSDir
		return true, nil
	}
	return false, nil
}

func (s *sandbox) PullImage(ctx context.Context, creds oci.Credentials) error {
	if s.imageRef == "" {
		// Use host root dirs for sandbox.
		return nil
	}
	// TODO: use dockerClient if docker is configured.
	rootFSDir, err := ociconv.ExtractContainerImage(ctx, nil /*=dockerClient*/, s.buildRoot, s.imageRef, creds)
	if err != nil {
		return err
	}
	s.rootFSDir = rootFSDir
	return nil
}

func (s *sandbox) Create(ctx context.Context, workDir string) error {
	s.workDir = workDir
	if err := os.MkdirAll(s.execroot(), 0755); err != nil {
		return status.WrapError(err, "create sandbox root")
	}
	return nil
}

func (s *sandbox) Exec(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	{
		start := time.Now()
		defer func() { log.Debugf("Exec() took %s", time.Since(start)) }()
	}

	execroot := s.execroot()
	// Move workdir into sandbox exec root. Once the action is complete, move it
	// back, because the executor will need to upload outputs from there.
	//
	// Note: we can't just mount the workspace using -M/-m flags - if we do
	// that, it winds up appearing empty. The mounting logic in linux-sandbox
	// requires the workspace to be a regular directory under the execroot, not
	// a mount. TODO: is this approach compatible with the current persistent
	// workers impl?
	if err := os.Rename(s.workDir, filepath.Join(execroot, workspaceDirName)); err != nil {
		return commandutil.ErrorResult(status.WrapError(err, "move workspace to sandbox root"))
	}
	defer func() {
		if err := os.Rename(filepath.Join(execroot, workspaceDirName), s.workDir); err != nil {
			log.CtxErrorf(ctx, "Failed to move sandbox workspace root back to build root: %s", err)
		}
	}()
	linuxSandboxArgs := []string{
		s.toolPath,
		// Run as root uid within the user namespace created by the sandbox, to
		// match podman's default behavior.
		// TODO: support non-root
		"-R",
		// Enable hermetic mode with the execRoot dir mounted to /
		"-h", execroot,
		// Set working directory (relative to sandbox root).
		"-W", filepath.Join(execroot, workspaceDirName),
		// Make /tmp and /dev/shm writable (to match bazel's local behavior)
		"-w", "/tmp",
		"-w", "/dev/shm",
		// Make hostname inside the sandbox equal "localhost"
		"-H",
		// Create a new network namespace with loopback (disables networking).
		// TODO: allow enabling networking
		"-N",
		// Configure stats file
		"-S", filepath.Join(execroot, statsFileName),
	}
	mounts, err := s.setupMounts(ctx)
	if err != nil {
		return commandutil.ErrorResult(err)
	}
	for _, m := range mounts {
		linuxSandboxArgs = append(linuxSandboxArgs, "-M", m.LinuxSandboxSource)
		if m.LinuxSandboxTarget != m.LinuxSandboxSource {
			linuxSandboxArgs = append(linuxSandboxArgs, "-m", m.LinuxSandboxTarget)
		}
	}
	if *debugStderr {
		linuxSandboxArgs = append(linuxSandboxArgs, "-D", "/dev/stderr")
	} else if *logDebugOutput {
		logPath := filepath.Join(execroot, debugLogFileName)
		linuxSandboxArgs = append(linuxSandboxArgs, "-D", logPath)
		defer func() {
			b, err := os.ReadFile(logPath)
			if err != nil {
				return
			}
			if len(b) == 0 {
				log.CtxInfof(ctx, "Sandbox debug logs: <empty>")
			} else {
				log.CtxInfof(ctx, "Sandbox debug logs:\n%s", string(b))
			}
		}()
	}
	// Append command
	linuxSandboxArgs = append(linuxSandboxArgs, "--")
	linuxSandboxArgs = append(linuxSandboxArgs, command.GetArguments()...)

	// TODO: apply ENV vars from the image
	// TODO: respect ENTRYPOINT in the image

	command = command.CloneVT()
	command.Arguments = linuxSandboxArgs
	// Set HOME for better compatibility with podman/docker.
	// TODO: maybe also set HOSTNAME?
	if commandutil.Getenv(command, "HOME") == nil {
		command.EnvironmentVariables = append(command.EnvironmentVariables, &repb.Command_EnvironmentVariable{
			Name: "HOME",
			// TODO: non-root support?
			Value: "/root",
		})
	}

	// TODO: test that we can still read stats if the command times out
	// TODO: set up a stats listener so that prom metrics are somewhat accurate
	// while the cmd is running

	var sandboxdMounts []*sbdpb.Mount
	for _, m := range mounts {
		if m.SandboxdMount != nil {
			sandboxdMounts = append(sandboxdMounts, m.SandboxdMount)
		}
	}

	// If we're root, mount and exec directly - we don't need pseudo-root
	// via sandboxd. It just adds more overhead.
	var res *interfaces.CommandResult
	if os.Getuid() == 0 {
		for _, m := range sandboxdMounts {
			if err := sandboxutil.SetupMount(m); err != nil {
				return commandutil.ErrorResult(err)
			}
		}
		res = commandutil.Run(ctx, command, "", nil, stdio)
	} else {
		res = s.sandboxd.Exec(ctx, command, sandboxdMounts, "" /*=user*/, nil /*=statsListener*/, stdio)
	}
	// Read stats file if it exists and append to res
	stats, err := s.readStatsFile()
	if err != nil {
		log.CtxWarningf(ctx, "Failed to read linux-sandbox stats: %s", err)
	} else {
		res.UsageStats = stats
	}
	return res
}

// tmpDir returns a temporary directory for this sandbox instance.
// It is created as a sibling of the action working directory to ensure it is
// on the same filesystem.
func (s *sandbox) tmpDir() string {
	return s.workDir + ".tmp"
}

// execroot returns the local directory path backing the sandbox's root FS.
func (s *sandbox) execroot() string {
	return filepath.Join(s.tmpDir(), "execroot")
}

func (s *sandbox) hostRoot() string {
	hostRoot := s.rootFSDir
	if s.imageRef == "" {
		// container-image not set: use the host FS as the root dir.
		hostRoot = "/"
	}
	return hostRoot
}

type Mount struct {
	SandboxdMount *sbdpb.Mount

	LinuxSandboxSource string
	LinuxSandboxTarget string
}

func (s *sandbox) setupMounts(ctx context.Context) (mounts []Mount, err error) {
	// Ideally, we could mount the root FS directly. However (TLDR) it is not
	// possible without patching linux-sandbox.
	//
	// More details: mounting the root dir directly would require a bazel patch
	// to set MS_REC on the syscall which self-mounts the sandbox root (see
	// MountSandboxAndGoThere() in linux-sandbox-pid1.cc). This is because the
	// workspace dir would need to be bind-mounted on top of the execroot, but
	// wouldn't be visible without MS_REC. Instead of bind-mounting, we could
	// alternatively place it in the upperdir of the overlayfs, but this has a
	// severe performance penalty on workspace file IO operations compared to
	// bind mounting.
	//
	// So to avoid patching bazel, we instead take this approach: For each root
	// dir, set up an individual overlay mount to enable copy-on-write for that
	// dir. For each root symlink or regular file, copy into the execroot. File
	// contents are fully copied - for now we just hope that there are no large
	// files in the root dir :) For the workspace dir, we can then bind-mount to
	// /workspace. We don't need MS_REC for any of this, because all of the
	// mounts are siblings of each other, rather than nested.
	hostRoot := s.hostRoot()
	entries, err := os.ReadDir(s.hostRoot())
	if err != nil {
		return nil, status.InternalErrorf("read root FS dir: %s", err)
	}
	for _, e := range entries {
		name := e.Name()
		hostPath := filepath.Join(s.hostRoot(), name)
		sandboxPath := filepath.Join(s.execroot(), name)

		// Do not create overlays for "special" filesystems.
		// These will be mounted from the host.
		if slices.Contains(disallowedMounts, name) {
			continue
		}
		// Only mount explicitly configured host dirs.
		if hostRoot == "/" && !slices.Contains(allowedHostMounts, name) {
			continue
		}
		// Skip tmp for now because linux-sandbox depends on provisioning its
		// own tmp
		// DO NOT SUBMIT: verify this?
		if name == "tmp" {
			continue
		}
		if name == workspaceDirName {
			log.CtxWarningf(ctx, "Sandbox setup: skipping %s (directory name is reserved for the action workspace dir)", hostPath)
			continue
		}

		// For directories, create an overlay mount.
		if e.Type().IsDir() {
			// For regular dirs, set up an overlayfs.
			overlayLower := hostPath
			overlayWork := filepath.Join(s.tmpDir(), "overlay/work")
			overlayUpper := filepath.Join(s.tmpDir(), "overlay/upper")
			overlayTarget := filepath.Join(s.tmpDir(), "overlay/mnt", name)
			for _, d := range []string{overlayWork, overlayUpper, overlayTarget} {
				if err := os.MkdirAll(d, 0755); err != nil {
					return nil, status.WrapErrorf(err, "mkdir %s", err)
				}
			}
			m := Mount{
				SandboxdMount: &sbdpb.Mount{
					Source:         "overlay",
					Target:         overlayTarget,
					Filesystemtype: "overlay",
					Flags:          uint64(syscall.MS_RELATIME),
					Data:           fmt.Sprintf("lowerdir=%s,workdir=%s,upperdir=%s", overlayLower, overlayWork, overlayUpper),
				},
				LinuxSandboxSource: overlayTarget,
				LinuxSandboxTarget: "/" + name,
			}
			mounts = append(mounts, m)
			s.pathsToUnmount = append(s.pathsToUnmount, overlayTarget)
			continue
		}

		info, err := e.Info()
		if err != nil {
			return nil, status.WrapErrorf(err, "info %s", hostPath)
		}

		// Recreate symlinks in the sandbox root.
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(hostPath)
			if err != nil {
				return nil, status.WrapErrorf(err, "readlink %s", hostPath)
			}
			if err := os.Symlink(target, sandboxPath); err != nil {
				return nil, status.WrapErrorf(err, "create symlink %s in sandbox", sandboxPath)
			}
			continue
		}

		// Do a full copy for regular files.
		// Hopefully there aren't too many of these :)
		//
		// TODO: reflink instead if the filesystem supports it.
		if info.Mode().IsRegular() {
			if err := disk.CopyViaTmpSibling(hostPath, sandboxPath); err != nil {
				return nil, status.WrapErrorf(err, "copy %s to %s", hostPath, sandboxPath)
			}
			continue
		}

		// Other directory entries are ignored.
		log.CtxInfof(ctx, "Sandbox setup: skipping %s (unsupported file mode)", hostPath)
	}

	// Mount virtual filesystems from the host, not the extracted image, since
	// these are the "real thing".
	//
	// TODO: make these configurable?
	directHostMounts := []string{"/proc", "/dev", "/sys"}
	for _, d := range directHostMounts {
		mounts = append(mounts, Mount{
			// Sandboxd does not need to mount these - linux-sandbox will mount
			// from the host directly.
			SandboxdMount:      nil,
			LinuxSandboxSource: d,
			LinuxSandboxTarget: d,
		})
	}

	return mounts, nil
}

func (s *sandbox) readStatsFile() (*repb.UsageStats, error) {
	path := filepath.Join(s.execroot(), statsFileName)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, status.WrapErrorf(err, "read %s", path)
	}
	stats := &lspb.ExecutionStatistics{}
	if err := proto.Unmarshal(b, stats); err != nil {
		return nil, status.WrapError(err, "unmarshal stats")
	}
	ru := stats.GetResourceUsage()
	userCPU := secUsecDuration(ru.GetUtimeSec(), ru.GetUtimeUsec())
	systemCPU := secUsecDuration(ru.GetStimeSec(), ru.GetStimeUsec())
	return &repb.UsageStats{
		CpuNanos:        (userCPU + systemCPU).Nanoseconds(),
		PeakMemoryBytes: ru.GetMaxrss() * 1024,
	}, nil
}

func (s *sandbox) Unpause(ctx context.Context) error {
	return nil
}

func (s *sandbox) Pause(ctx context.Context) error {
	return nil
}

func (s *sandbox) Remove(ctx context.Context) error {
	var lastErr error
	if os.Getuid() == 0 {
		// We're root - don't use sandboxd
		for _, p := range s.pathsToUnmount {
			if err := syscall.Unmount(p, 0); err != nil {
				lastErr = status.WrapError(err, "clean sandboxd dir")
				log.CtxWarningf(ctx, "%s", lastErr)
			}
		}
	} else {
		req := &sbdpb.CleanRequest{UnmountPaths: s.pathsToUnmount}
		_, err := s.sandboxd.GetClient().Clean(ctx, req)
		if err != nil {
			lastErr = status.WrapError(err, "clean sandboxd-created resources")
			log.CtxWarningf(ctx, "%s", lastErr)
		}
	}
	if err := os.RemoveAll(s.tmpDir()); err != nil {
		lastErr = status.WrapError(err, "remove sandbox temp directory")
		log.CtxWarningf(ctx, "%s", lastErr)
	}
	return lastErr
}

func (s *sandbox) Stats(ctx context.Context) (*repb.UsageStats, error) {
	// TODO: implement?
	return nil, nil
}

func (s *sandbox) State(ctx context.Context) (*rnpb.ContainerState, error) {
	return nil, status.UnimplementedError("not implemented")
}

func secUsecDuration(sec, usec int64) time.Duration {
	return time.Duration(sec*1e9 + usec*1e3)
}
