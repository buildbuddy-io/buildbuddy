package bare

import (
	"bytes"
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	enableStats    = flag.Bool("executor.bare.enable_stats", false, "Whether to enable stats for bare command execution.")
	enableLogFiles = flag.Bool("executor.bare.enable_log_files", false, "Whether to send bare runner output to log files for debugging. These files are stored adjacent to the task directory and are deleted when the task is complete.")
	enableTmpdir   = flag.Bool("executor.bare.enable_tmpdir", false, "If provided, set a default TMPDIR in each command's environment variables to a temporary directory provisioned for each task.")
)

const (
	// TMPDIR environment variable used to specify a directory made available
	// for programs that need a place to create temporary files. This name is
	// standardized by POSIX and is widely supported:
	// https://pubs.opengroup.org/onlinepubs/9799919799/basedefs/V1_chap08.html#tag_08_01
	posixTmpdirEnvironmentVariableName = "TMPDIR"
)

type Opts struct {
	// EnableStats specifies whether to collect stats while the command is
	// in progress.
	EnableStats bool

	// EnableTmpdir specifies whether to set TMPDIR in each command's
	// environment variables to a temporary directory provisioned for each
	// task. The directory will be removed when the task is complete.
	EnableTmpdir bool
}

type Provider struct {
}

func (p *Provider) New(ctx context.Context, _ *container.Init) (container.CommandContainer, error) {
	opts := &Opts{
		EnableStats:  *enableStats,
		EnableTmpdir: *enableTmpdir,
	}
	return NewBareCommandContainer(opts), nil
}

// bareCommandContainer executes commands directly, without any isolation
// between containers.
type bareCommandContainer struct {
	opts    *Opts
	signal  chan syscall.Signal
	workDir string
	tmpDir  string
}

func NewBareCommandContainer(opts *Opts) container.CommandContainer {
	return &bareCommandContainer{
		opts:   opts,
		signal: make(chan syscall.Signal, 1),
	}
}

func (c *bareCommandContainer) IsolationType() string {
	return "bare"
}

func (c *bareCommandContainer) Run(ctx context.Context, command *repb.Command, workDir string, creds oci.Credentials) *interfaces.CommandResult {
	if err := c.Create(ctx, workDir); err != nil {
		return commandutil.ErrorResult(err)
	}
	return c.exec(ctx, command, workDir, nil /*=stdio*/)
}

func (c *bareCommandContainer) Create(ctx context.Context, workDir string) error {
	c.workDir = workDir
	if c.opts.EnableTmpdir {
		// Create a temp dir as a sibling of the workdir.
		c.tmpDir = workDir + ".tmp"
		if err := os.MkdirAll(c.tmpDir, 0755); err != nil {
			return status.UnavailableErrorf("failed to create tempdir: %s", err)
		}
	}
	return nil
}

func (c *bareCommandContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	return c.exec(ctx, cmd, c.workDir, stdio)
}

func (c *bareCommandContainer) Signal(ctx context.Context, sig syscall.Signal) error {
	select {
	case c.signal <- sig:
		return nil
	default:
		return status.UnavailableErrorf("failed to send signal %q: channel buffer is full", sig)
	}
}

func (c *bareCommandContainer) exec(ctx context.Context, cmd *repb.Command, workDir string, stdio *interfaces.Stdio) (result *interfaces.CommandResult) {
	var statsListener procstats.Listener
	if c.opts.EnableStats {
		// Setting the stats listener to non-nil enables stats reporting in
		// commandutil.RunWithOpts.
		statsListener = func(*repb.UsageStats) {}
	}

	if *enableLogFiles {
		stdoutPath := workDir + ".stdout"
		stdoutFile, err := os.Create(stdoutPath)
		if err != nil {
			return commandutil.ErrorResult(status.UnavailableErrorf("create stdout log file: %s", err))
		}
		defer stdoutFile.Close()
		defer os.Remove(stdoutPath)

		stderrPath := workDir + ".stderr"
		stderrFile, err := os.Create(stderrPath)
		if err != nil {
			return commandutil.ErrorResult(status.UnavailableErrorf("create stderr log file: %s", err))
		}
		defer stderrFile.Close()
		defer os.Remove(stderrPath)

		if stdio == nil {
			stdio = &interfaces.Stdio{}
		}
		// We want to set stdio.{Stdout,Stderr} in order to write to log files.
		// But setting stdout/stderr disables the default buffering behavior
		// (into the CommandResult Stdout/Stderr fields). So we need to manually
		// buffer stdout/stderr here.
		if stdio.Stdout == nil {
			stdoutBuf := &bytes.Buffer{}
			stdio.Stdout = stdoutBuf
			defer func() { result.Stdout = stdoutBuf.Bytes() }()
		}
		if stdio.Stderr == nil {
			stderrBuf := &bytes.Buffer{}
			stdio.Stderr = stderrBuf
			defer func() { result.Stderr = stderrBuf.Bytes() }()
		}
		stdio.Stdout = io.MultiWriter(stdio.Stdout, stdoutFile)
		stdio.Stderr = io.MultiWriter(stdio.Stderr, stderrFile)
	}

	// Set TMPDIR if not already set.
	if c.tmpDir != "" {
		// Make sure TMPDIR path is absolute.
		tmpDirAbsPath, err := filepath.Abs(c.tmpDir)
		if err != nil {
			return commandutil.ErrorResult(status.UnavailableErrorf("make TMPDIR absolute: %s", err))
		}
		if _, ok := rexec.LookupEnv(cmd.EnvironmentVariables, posixTmpdirEnvironmentVariableName); !ok {
			cmd = cmd.CloneVT()
			cmd.EnvironmentVariables = append(cmd.EnvironmentVariables, &repb.Command_EnvironmentVariable{
				Name:  posixTmpdirEnvironmentVariableName,
				Value: tmpDirAbsPath,
			})
		}
	}

	return commandutil.RunWithOpts(ctx, cmd, &commandutil.RunOpts{
		Dir:           workDir,
		StatsListener: statsListener,
		Stdio:         stdio,
		Signal:        c.signal,
	})
}

func (c *bareCommandContainer) IsImageCached(ctx context.Context) (bool, error) { return false, nil }
func (c *bareCommandContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	return nil
}
func (c *bareCommandContainer) Start(ctx context.Context) error { return nil }

func (c *bareCommandContainer) Remove(ctx context.Context) error {
	if c.tmpDir != "" {
		if err := os.RemoveAll(c.tmpDir); err != nil {
			return status.UnavailableErrorf("failed to remove tempdir: %s", err)
		}
		c.tmpDir = ""
	}
	return nil
}

func (c *bareCommandContainer) Pause(ctx context.Context) error   { return nil }
func (c *bareCommandContainer) Unpause(ctx context.Context) error { return nil }

func (c *bareCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	return nil, nil
}
