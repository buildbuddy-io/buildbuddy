package linux_sandbox

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ociconv"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	_ "embed"
	lspb "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/proto"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

var (
	enableDefaultMounts = flag.Bool("executor.linux_sandbox.enable_default_mounts", true, "Enable the default mount pairs: /bin, /usr, etc.")
	enableOverlayfs     = flag.Bool("executor.linux_sandbox.enable_overlayfs", true, "Enables overlayfs with linux-sandbox, ensuring that writes do not affect the underlying filesystem.")
	customMounts        = flag.Slice("executor.linux_sandbox.mounts", []string{}, `Directories to mount in the sandbox rootfs, specified as "/hostpath:/sandboxpath" pairs. "/path" can be used as shorthand for "/path:/path".`)
	debug               = flag.Bool("executor.linux_sandbox.debug", false, "If true, capture debug output from the linux-sandbox and log it at INFO level.")
)

const (
	// File name where the binary stats file will be written, relative to the
	// sandbox execroot.
	statsFileName = "stats.out"
	// File name where the debug logs will be written (if enabled), relative to
	// the sandbox execroot.
	debugLogFileName = "debug.out"
)

var (
	// TODO: filter this list to just the dirs that exist
	defaultMounts = []string{
		"/bin",
		"/lib",
		"/lib32",
		"/lib64",
		"/usr",
		// TODO: is it correct to just mount proc or do we need a separate
		// syscall to mount procfs?
		"/proc",
		// bash cannot resolve binaries in PATH like awk (required for
		// genrules), even if PATH is set properly, unless /etc is mounted.
		// TODO: there must be some config under /etc that makes this work,
		// figure out what it is and mount just that config instead of the whole
		// dir.
		"/etc",
	}
)

//go:embed linux-sandbox
var toolBytes []byte

type provider struct {
	env environment.Env
	// Path to the linux-sandbox binary.
	toolPath string
	// Path to the executor build root dir, where container images will be
	// extracted (if applicable).
	buildRoot string
}

func NewProvider(env environment.Env, buildRoot string) (container.Provider, error) {
	// Write the embedded linux-sandbox binary to
	// {buildRoot}/executor/bin/linux-sandbox.
	// TODO: find a better home for the binary.
	executorBinDir := filepath.Join(buildRoot, "executor", "bin")
	if err := os.MkdirAll(executorBinDir, 0755); err != nil {
		return nil, err
	}
	toolPath := filepath.Join(executorBinDir, "linux-sandbox")
	if err := os.WriteFile(toolPath, toolBytes, 0755); err != nil {
		return nil, err
	}

	// Debug: show tool paths available to the host
	// (used when no container-image is passed)
	b, _ := exec.Command("sh", "-c", `
		find "/usr/lib/gcc/x86_64-linux-gnu/" -maxdepth 1 -mindepth 1
	`).Output()
	log.Debugf("linux-sandbox: found host tool paths:\n%s", strings.TrimSpace(string(b)))

	return &provider{
		env:       env,
		toolPath:  toolPath,
		buildRoot: buildRoot,
	}, nil
}

func (p *provider) New(ctx context.Context, props *platform.Properties, task *repb.ScheduledTask, state *rnpb.RunnerState, workDir string) (container.CommandContainer, error) {
	return &sandbox{
		env:       p.env,
		imageRef:  props.ContainerImage,
		toolPath:  p.toolPath,
		buildRoot: p.buildRoot,
	}, nil
}

// sandbox is a container implementation which uses the linux-sandbox
// binary that ships with bazel.
type sandbox struct {
	env environment.Env
	// Path to the linux-sandbox binary.
	toolPath string
	// Path to the executor build root dir, where container images will be
	// extracted (if applicable).
	buildRoot string
	// OCI image ref that will be used to back the sandbox root FS.
	imageRef string
	// Path to the extraced OCI image root FS.
	rootFSDir string
	// Path to the action working directory.
	workDir string
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
	defer func() {
		ctx, cancel := background.ExtendContextForFinalization(ctx, 5*time.Second)
		defer cancel()
		if err := s.Remove(ctx); err != nil {
			log.CtxErrorf(ctx, "Failed to clean up sandbox: %s", err)
		}
	}()
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
	return nil
}

func (s *sandbox) Exec(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	// Setup sandbox execution root dir
	execRoot := s.execRoot()
	if err := os.MkdirAll(execRoot, 0755); err != nil {
		return commandutil.ErrorResult(status.WrapError(err, "create sandbox root"))
	}
	// Move workdir into sandbox exec root. Once the action is complete, move it
	// back.
	// TODO: is this approach compatible with the current persistent workers
	// impl?
	// TODO: can we use a mount instead?
	const workspaceDirName = "workspace"
	if err := os.Rename(s.workDir, filepath.Join(execRoot, workspaceDirName)); err != nil {
		return commandutil.ErrorResult(status.WrapError(err, "move workspace to sandbox root"))
	}
	defer func() {
		if err := os.Rename(filepath.Join(execRoot, workspaceDirName), s.workDir); err != nil {
			log.CtxErrorf(ctx, "Failed to move sandbox workspace root back to build root: %s", err)
		}
		// Clean up sandbox root
		if err := os.RemoveAll(execRoot); err != nil {
			log.CtxErrorf(ctx, "Failed to clean up sandbox execution root: %s", err)
		}
	}()
	args := []string{
		s.toolPath,
		// Run as root, to match podman's default behavior.
		// TODO: support non-root
		"-R",
		// Set hermetic sandbox root
		"-h", execRoot,
		// Set working directory (relative to sandbox root).
		"-W", filepath.Join(execRoot, workspaceDirName),
		// Make the workspace directory writable
		"-w", filepath.Join(execRoot, workspaceDirName),
		// Make /tmp and /dev/shm writable (to match bazel's local behavior)
		"-w", "/tmp",
		"-w", "/dev/shm",
		// Make hostname inside the sandbox equal "localhost"
		"-H",
		// Create a new network namespace with loopback (disables networking).
		// TODO: allow enabling networking
		"-N",
		// Configure stats file
		"-S", filepath.Join(execRoot, statsFileName),
	}
	mounts, err := s.getMounts(ctx)
	if err != nil {
		return commandutil.ErrorResult(err)
	}
	log.CtxDebugf(ctx, "Sandbox mounts: %v", mounts)
	for _, m := range mounts {
		parts := strings.SplitN(m, ":", 2)
		switch len(parts) {
		case 1:
			args = append(args, "-M", parts[0])
		case 2:
			args = append(args, "-M", parts[0], "-m", parts[1])
		default:
			return commandutil.ErrorResult(status.InvalidArgumentErrorf("invalid mount pair %q", m))
		}
	}
	if *debug {
		logPath := filepath.Join(execRoot, debugLogFileName)
		args = append(args, "-D", logPath)
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
	args = append(args, "--")
	args = append(args, command.GetArguments()...)

	// TODO: apply env vars from the image too.

	command = command.CloneVT()
	command.Arguments = args
	// Set HOME for better compatibility with podman/docker.
	// TODO: maybe also set HOSTNAME?
	if commandutil.Getenv(command, "HOME") == nil {
		command.EnvironmentVariables = append(command.EnvironmentVariables, &repb.Command_EnvironmentVariable{
			Name: "HOME",
			// TODO: non-root support
			Value: "/root",
		})
	}

	// TODO: test that we can still read stats if the command times out
	// TODO: set up a stats listener so that prom metrics are somewhat accurate
	// while the cmd is running

	// NOTE: we don't set workDir in Run here, since the sandbox process will
	// make sure to change to the working directory we set in args.
	res := commandutil.Run(ctx, command, "", nil /*=statsListener*/, stdio)
	// Read stats file if it exists and append to res
	stats, err := s.readStatsFile()
	if err != nil {
		log.CtxErrorf(ctx, "Failed to read linux-sandbox stats: %s", err)
	} else {
		res.UsageStats = stats
	}
	return res
}

// execRoot returns the local directory path backing the sandbox's root FS.
func (s *sandbox) execRoot() string {
	return s.workDir + ".execroot"
}

// Returns the mount args to be used for the sandbox. If a container-image is
// requested, this will return the root directory paths from the extracted
// image's root filesystem. Otherwise, this will return the default mounts if
// enabled, plus any mounts set via executor configuration.
func (s *sandbox) getMounts(ctx context.Context) (mounts []string, err error) {
	if s.imageRef == "" {
		if *enableDefaultMounts {
			mounts = append(mounts, defaultMounts...)
		}
		mounts = append(mounts, *customMounts...)
	} else {
		// Mount all root paths in the extracted image.
		// TODO: these will be mounted mutably - use overlayfs instead.
		entries, err := os.ReadDir(s.rootFSDir)
		if err != nil {
			return nil, status.InternalErrorf("read root FS dir: %s", err)
		}
		for _, e := range entries {
			name := e.Name()

			mounts = append(mounts, fmt.Sprintf("%s:%s", filepath.Join(s.rootFSDir, name), "/"+name))
		}
		// Mount special filesystems from the host.
		// In particular, some actions rely on things like /proc/self/exe
		// TODO: make these configurable?
		mounts = append(mounts, "/proc", "/dev", "/sys")
	}

	// Set up a home dir for the root user.

	return mounts, nil
}

func (s *sandbox) readStatsFile() (*repb.UsageStats, error) {
	path := filepath.Join(s.execRoot(), statsFileName)
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
	return nil
}

func (s *sandbox) Stats(ctx context.Context) (*repb.UsageStats, error) {
	// TODO: implement
	return nil, nil
}

func (s *sandbox) State(ctx context.Context) (*rnpb.ContainerState, error) {
	return nil, status.UnimplementedError("not implemented")
}

func secUsecDuration(sec, usec int64) time.Duration {
	return time.Duration(sec*1e9 + usec*1e3)
}
