package applecontainer

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	appleContainerExecSIGKILLExitCode = 137
	appleContainerExecSIGTERMExitCode = 143
)

type Provider struct {
	env       environment.Env
	buildRoot string
}

func NewProvider(env environment.Env, buildRoot string) (*Provider, error) {
	return &Provider{
		env:       env,
		buildRoot: buildRoot,
	}, nil
}

func (p *Provider) New(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
	return &appleContainerCommandContainer{
		env:       p.env,
		image:     args.Props.ContainerImage,
		buildRoot: p.buildRoot,
		forceRoot: args.Props.DockerForceRoot,
		user:      args.Props.DockerUser,
	}, nil
}

type appleContainerCommandContainer struct {
	env   environment.Env
	image string

	buildRoot string
	workDir   string

	forceRoot bool
	user      string

	name    string
	mu      sync.Mutex
	removed bool
}

func (c *appleContainerCommandContainer) baseRunArgs(workDir string) []string {
	hostWorkDir := filepath.Join(c.buildRoot, filepath.Base(workDir))
	args := []string{
		"--name", c.name,
		"--workdir", workDir,
		"--volume", fmt.Sprintf("%s:%s", hostWorkDir, workDir),
	}

	if c.forceRoot {
		args = append(args, "--user=0:0")
	} else if c.user != "" {
		args = append(args, "--user="+c.user)
	}

	return args
}

func (c *appleContainerCommandContainer) IsolationType() string {
	return "applecontainer"
}

func (c *appleContainerCommandContainer) Run(ctx context.Context, command *repb.Command, workDir string, creds oci.Credentials) *interfaces.CommandResult {
	c.workDir = workDir
	result := &interfaces.CommandResult{
		CommandDebugString: fmt.Sprintf("(applecontainer) %s", command.GetArguments()),
		ExitCode:           commandutil.NoExitCode,
	}

	name, err := generateContainerName()
	if err != nil {
		result.Error = status.UnavailableErrorf("failed to generate apple container name: %s", err)
		return result
	}
	c.name = name

	if err := container.PullImageIfNecessary(ctx, c.env, c, creds, c.image); err != nil {
		result.Error = status.UnavailableErrorf("failed to pull container image: %s", err)
		return result
	}

	runArgs := c.baseRunArgs(workDir)
	for _, envVar := range command.GetEnvironmentVariables() {
		runArgs = append(runArgs, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	runArgs = append(runArgs, c.image)
	runArgs = append(runArgs, command.Arguments...)

	result = c.runAppleContainer(ctx, "run", &interfaces.Stdio{}, runArgs...)

	if result.ExitCode == appleContainerExecSIGKILLExitCode {
		log.CtxInfof(ctx, "apple container received SIGKILL")
		result.ExitCode = commandutil.KilledExitCode
		result.Error = commandutil.ErrSIGKILL
	} else if result.ExitCode == appleContainerExecSIGTERMExitCode {
		log.CtxInfof(ctx, "apple container received SIGTERM")
		result.ExitCode = commandutil.KilledExitCode
		result.Error = commandutil.ErrSIGKILL
	}

	if result.ExitCode < 0 {
		if err := c.killContainerIfRunning(ctx); err != nil {
			log.Warningf("Failed to shut down apple container: %s", err)
		}
	}

	return result
}

func (c *appleContainerCommandContainer) Create(ctx context.Context, workDir string) error {
	name, err := generateContainerName()
	if err != nil {
		return status.UnavailableErrorf("failed to generate apple container name: %s", err)
	}
	c.name = name
	c.workDir = workDir

	args := c.baseRunArgs(workDir)
	args = append(args, c.image, "sleep", "infinity")

	res := c.runAppleContainer(ctx, "create", &interfaces.Stdio{}, args...)
	if res.Error != nil {
		return status.UnavailableErrorf("failed to create container: %s", res.Error)
	}
	if res.ExitCode != 0 {
		return status.UnknownErrorf("apple container create failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
	}

	start := c.runAppleContainer(ctx, "start", &interfaces.Stdio{}, c.name)
	if start.Error != nil {
		return start.Error
	}
	if start.ExitCode != 0 {
		return status.UnknownErrorf("apple container start failed: exit code %d, stderr: %s", start.ExitCode, string(start.Stderr))
	}

	return nil
}

func (c *appleContainerCommandContainer) Exec(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	args := make([]string, 0, 2*len(command.GetEnvironmentVariables())+len(command.Arguments)+2)
	for _, envVar := range command.GetEnvironmentVariables() {
		args = append(args, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	if c.forceRoot {
		args = append(args, "--user=0:0")
	} else if c.user != "" {
		args = append(args, "--user="+c.user)
	}
	if stdio.Stdin != nil {
		args = append(args, "--interactive")
	}
	args = append(args, c.name)
	args = append(args, command.Arguments...)

	res := c.runAppleContainer(ctx, "exec", stdio, args...)

	c.mu.Lock()
	removed := c.removed
	c.mu.Unlock()
	if removed && (res.ExitCode == appleContainerExecSIGKILLExitCode || res.ExitCode == appleContainerExecSIGTERMExitCode) {
		res.ExitCode = commandutil.KilledExitCode
		res.Error = commandutil.ErrSIGKILL
	}

	return res
}

func (c *appleContainerCommandContainer) Signal(ctx context.Context, sig syscall.Signal) error {
	if c.name == "" {
		return status.FailedPreconditionError("container is not created")
	}
	res := c.runAppleContainer(ctx, "kill", nil, c.name, "--signal", fmt.Sprintf("%d", sig))
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode != 0 {
		return status.UnavailableErrorf("failed to signal container: exit code %d: %q", res.ExitCode, string(res.Stderr))
	}
	return nil
}

func (c *appleContainerCommandContainer) IsImageCached(ctx context.Context) (bool, error) {
	var stdout bytes.Buffer
	stdio := &interfaces.Stdio{Stdout: &stdout}

	res := c.runAppleContainer(ctx, "image", stdio, "list", "--quiet")
	if res.Error != nil {
		return false, res.Error
	}
	if res.ExitCode != 0 {
		return false, status.InternalErrorf("'container image list' failed (code %d): stderr: %q", res.ExitCode, string(res.Stderr))
	}

	images := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	for _, img := range images {
		if strings.Contains(img, c.image) {
			return true, nil
		}
	}

	return false, nil
}

func (c *appleContainerCommandContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	if !creds.IsEmpty() {
		var stdin bytes.Buffer
		stdin.WriteString(creds.Password)
		stdio := &interfaces.Stdio{Stdin: &stdin}

		registry := c.image
		if idx := strings.Index(c.image, "/"); idx != -1 {
			registry = c.image[:idx]
		}

		login := c.runAppleContainer(ctx, "registry", stdio, "login", "--username", creds.Username, "--password-stdin", registry)
		if login.Error != nil {
			return login.Error
		}
		if login.ExitCode != 0 {
			return status.UnavailableErrorf("apple container registry login failed: exit code %d", login.ExitCode)
		}
	}

	pull := c.runAppleContainer(ctx, "image", &interfaces.Stdio{}, "pull", c.image)
	if pull.Error != nil {
		return pull.Error
	}
	if pull.ExitCode != 0 {
		return status.UnavailableErrorf("apple container pull failed: exit code %d, stderr: %s", pull.ExitCode, string(pull.Stderr))
	}

	return nil
}

func (c *appleContainerCommandContainer) Remove(ctx context.Context) error {
	c.mu.Lock()
	c.removed = true
	c.mu.Unlock()

	res := c.runAppleContainer(ctx, "delete", &interfaces.Stdio{}, "-f", c.name)
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode == 0 || strings.Contains(string(res.Stderr), "no such container") {
		return nil
	}
	return status.UnknownErrorf("apple container remove failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
}

func (c *appleContainerCommandContainer) Pause(ctx context.Context) error {
	return status.UnimplementedError("pause not implemented for apple container")
}

func (c *appleContainerCommandContainer) Unpause(ctx context.Context) error {
	return status.UnimplementedError("unpause not implemented for apple container")
}

func (c *appleContainerCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	return &repb.UsageStats{}, nil
}

func (c *appleContainerCommandContainer) runAppleContainer(ctx context.Context, subCommand string, stdio *interfaces.Stdio, args ...string) *interfaces.CommandResult {
	command := []string{"container", subCommand}
	command = append(command, args...)

	return c.env.GetCommandRunner().Run(ctx, &repb.Command{Arguments: command}, "", nil, stdio)
}

func generateContainerName() (string, error) {
	suffix, err := random.RandomString(20)
	if err != nil {
		return "", err
	}
	return "buildbuddy-exec-" + suffix, nil
}

func (c *appleContainerCommandContainer) killContainerIfRunning(ctx context.Context) error {
	err := c.Remove(ctx)
	if err != nil && strings.Contains(err.Error(), "no such container") {
		return nil
	}
	return err
}
