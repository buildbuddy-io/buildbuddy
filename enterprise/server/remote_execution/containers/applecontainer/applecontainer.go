package applecontainer

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// Apple Container exit codes
	appleContainerExecSIGKILLExitCode = 137
	appleContainerExecSIGTERMExitCode = 143

	containerFinalizationTimeout = 10 * time.Second
)

var (
	appleContainerNetwork = flag.String("executor.apple_container_network", "", "If set, set apple container --network to this value by default. Can be overridden per-action with the `dockerNetwork` exec property, which accepts values 'off' (--network=none) or 'bridge' (--network=<default>).")
	appleContainerCapAdd  = flag.String("apple_container_cap_add", "", "Sets --cap-add= on the apple container command. Comma separated.")
	appleContainerDevices = flag.Slice("executor.apple_container_devices", []container.DockerDeviceMapping{}, `Configure devices that will be available inside the sandbox container. Format is --executor.apple_container_devices='[{"PathOnHost":"/dev/foo","PathInContainer":"/some/dest","CgroupPermissions":"see,docker,docs"}]'`)
	appleContainerVolumes = flag.Slice("executor.apple_container_volumes", []string{}, "Additional --volume arguments to be passed to apple container.")
	appleContainerGPU     = flag.String("executor.apple_container_gpus", "", "Specifies the value of the --gpus= flag to pass to apple container. Set to 'all' to pass all GPUs.")

	// A map from image name to pull status to avoid parallel pulling of the same image
	pullOperations sync.Map
)

type pullStatus struct {
	mu     *sync.RWMutex
	pulled bool
}

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
	// Re-use docker flags for apple container
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

	return &appleContainerCommandContainer{
		env:       p.env,
		image:     args.Props.ContainerImage,
		buildRoot: p.buildRoot,
		options: &AppleContainerOptions{
			ForceRoot:          args.Props.DockerForceRoot,
			Init:               args.Props.DockerInit,
			User:               args.Props.DockerUser,
			Network:            args.Props.DockerNetwork,
			DefaultNetworkMode: networkMode,
			CapAdd:             capAdd,
			Devices:            devices,
			Volumes:            volumes,
		},
	}, nil
}

type AppleContainerOptions struct {
	ForceRoot          bool
	Init               bool
	User               string
	DefaultNetworkMode string
	Network            string
	CapAdd             string
	Devices            []container.DockerDeviceMapping
	Volumes            []string
}

// appleContainerCommandContainer containerizes a single command's execution using Apple Container.
type appleContainerCommandContainer struct {
	env   environment.Env
	image string

	buildRoot string
	workDir   string

	options *AppleContainerOptions

	// name is the container name
	name string

	mu sync.Mutex // protects(removed)
	// removed is a flag that is set once Remove is called
	removed bool
}

func addUserArgs(args []string, options *AppleContainerOptions) []string {
	if options.ForceRoot {
		args = append(args, "--user=0:0")
	} else if options.User != "" {
		args = append(args, "--user="+options.User)
	}
	return args
}

func (c *appleContainerCommandContainer) getAppleContainerRunArgs(workDir string) []string {
	args := []string{
		"--name", c.name,
		"--workdir", workDir,
		"--volume", fmt.Sprintf("%s:%s",
			filepath.Join(c.buildRoot, filepath.Base(workDir)),
			workDir,
		),
	}

	args = addUserArgs(args, c.options)

	networkMode := c.options.DefaultNetworkMode
	// Translate network platform prop to the equivalent Apple Container network mode
	switch strings.ToLower(c.options.Network) {
	case "off":
		networkMode = "none"
	case "bridge":
		networkMode = ""
	default:
		// ignore other values for now, sticking to the configured default
	}
	if networkMode != "" {
		args = append(args, "--network="+networkMode)
	}

	if c.options.CapAdd != "" {
		args = append(args, "--cap-add="+c.options.CapAdd)
	}

	if *appleContainerGPU != "" {
		args = append(args, "--gpus="+*appleContainerGPU)
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

	if c.options.Init {
		args = append(args, "--init")
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
	containerName, err := generateContainerName()
	c.name = containerName
	if err != nil {
		result.Error = status.UnavailableErrorf("failed to generate apple container name: %s", err)
		return result
	}

	if err := container.PullImageIfNecessary(ctx, c.env, c, creds, c.image); err != nil {
		result.Error = status.UnavailableErrorf("failed to pull container image: %s", err)
		return result
	}

	appleContainerRunArgs := c.getAppleContainerRunArgs(workDir)
	for _, envVar := range command.GetEnvironmentVariables() {
		appleContainerRunArgs = append(appleContainerRunArgs, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	appleContainerRunArgs = append(appleContainerRunArgs, c.image)
	appleContainerRunArgs = append(appleContainerRunArgs, command.Arguments...)

	result = c.runAppleContainer(ctx, "run", &interfaces.Stdio{}, appleContainerRunArgs...)

	// Handle special exit codes
	if result.ExitCode == appleContainerExecSIGKILLExitCode {
		log.CtxInfof(ctx, "apple container received SIGKILL")
		result.ExitCode = commandutil.KilledExitCode
		result.Error = commandutil.ErrSIGKILL
	} else if result.ExitCode == appleContainerExecSIGTERMExitCode {
		log.CtxInfof(ctx, "apple container received SIGTERM")
		result.ExitCode = commandutil.KilledExitCode
		result.Error = commandutil.ErrSIGKILL
	}

	if exitedCleanly := result.ExitCode >= 0; !exitedCleanly {
		if err := c.killContainerIfRunning(ctx); err != nil {
			log.Warningf("Failed to shut down apple container: %s", err)
		}
	}
	return result
}

func (c *appleContainerCommandContainer) Create(ctx context.Context, workDir string) error {
	containerName, err := generateContainerName()
	if err != nil {
		return status.UnavailableErrorf("failed to generate apple container name: %s", err)
	}
	c.name = containerName
	c.workDir = workDir

	appleContainerRunArgs := c.getAppleContainerRunArgs(workDir)
	appleContainerRunArgs = append(appleContainerRunArgs, c.image)
	appleContainerRunArgs = append(appleContainerRunArgs, "sleep", "infinity")

	createResult := c.runAppleContainer(ctx, "create", &interfaces.Stdio{}, appleContainerRunArgs...)
	if err := createResult.Error; err != nil {
		return status.UnavailableErrorf("failed to create container: %s", err)
	}

	if createResult.ExitCode != 0 {
		return status.UnknownErrorf("apple container create failed: exit code %d, stderr: %s", createResult.ExitCode, createResult.Stderr)
	}

	startResult := c.runAppleContainer(ctx, "start", &interfaces.Stdio{}, c.name)
	if startResult.Error != nil {
		return startResult.Error
	}
	if startResult.ExitCode != 0 {
		return status.UnknownErrorf("apple container start failed: exit code %d, stderr: %s", startResult.ExitCode, startResult.Stderr)
	}
	return nil
}

func (c *appleContainerCommandContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	appleContainerRunArgs := make([]string, 0, 2*len(cmd.GetEnvironmentVariables())+len(cmd.Arguments)+1)
	for _, envVar := range cmd.GetEnvironmentVariables() {
		appleContainerRunArgs = append(appleContainerRunArgs, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	appleContainerRunArgs = addUserArgs(appleContainerRunArgs, c.options)
	if stdio.Stdin != nil {
		appleContainerRunArgs = append(appleContainerRunArgs, "--interactive")
	}
	appleContainerRunArgs = append(appleContainerRunArgs, c.name)
	appleContainerRunArgs = append(appleContainerRunArgs, cmd.Arguments...)

	res := c.runAppleContainer(ctx, "exec", stdio, appleContainerRunArgs...)

	// Handle SIGKILL/SIGTERM if the container was removed
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
	var stdout, stderr bytes.Buffer
	stdio := &interfaces.Stdio{Stdout: &stdout, Stderr: &stderr}

	// Use 'container image list --quiet' to check if image exists
	res := c.runAppleContainer(ctx, "image", stdio, "list", "--quiet")
	if res.Error != nil {
		return false, res.Error
	}
	if res.ExitCode != 0 {
		return false, status.InternalErrorf("'container image list' failed (code %d): stderr: %q", res.ExitCode, string(res.Stderr))
	}

	// Check if our image is in the list
	images := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	for _, img := range images {
		if strings.Contains(img, c.image) {
			return true, nil
		}
	}

	return false, nil
}

func (c *appleContainerCommandContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	psi, _ := pullOperations.LoadOrStore(c.image, &pullStatus{&sync.RWMutex{}, false})
	ps, ok := psi.(*pullStatus)
	if !ok {
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

	startTime := time.Now()
	if err := c.pullImage(ctx, creds); err != nil {
		return err
	}
	pullLatency := time.Since(startTime)
	log.Infof("apple container pulled image %s in %s", c.image, pullLatency)
	ps.pulled = true
	return nil
}

func (c *appleContainerCommandContainer) pullImage(ctx context.Context, creds oci.Credentials) error {
	appleContainerArgs := make([]string, 0, 2)

	if !creds.IsEmpty() {
		// Apple Container requires registry login before pulling
		var stdin bytes.Buffer
		stdin.WriteString(creds.Password)
		stdio := &interfaces.Stdio{Stdin: &stdin}

		// Extract registry from image reference
		registry := c.image
		if idx := strings.Index(c.image, "/"); idx != -1 {
			registry = c.image[:idx]
		}

		loginResult := c.runAppleContainer(ctx, "registry", stdio, "login", "--username", creds.Username, "--password-stdin", registry)
		if loginResult.Error != nil {
			return loginResult.Error
		}
		if loginResult.ExitCode != 0 {
			return status.UnavailableErrorf("apple container registry login failed: exit code %d", loginResult.ExitCode)
		}
	}

	appleContainerArgs = append(appleContainerArgs, c.image)

	pullResult := c.runAppleContainer(ctx, "image", &interfaces.Stdio{}, append([]string{"pull"}, appleContainerArgs...)...)
	if pullResult.Error != nil {
		return pullResult.Error
	}
	if pullResult.ExitCode != 0 {
		return status.UnavailableErrorf("apple container pull failed: exit code %d, stderr: %s", pullResult.ExitCode, string(pullResult.Stderr))
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
	// Apple Container doesn't have a direct pause command
	// We could implement this using signals or other mechanisms if needed
	return status.UnimplementedError("pause not implemented for apple container")
}

func (c *appleContainerCommandContainer) Unpause(ctx context.Context) error {
	// Apple Container doesn't have a direct unpause command
	return status.UnimplementedError("unpause not implemented for apple container")
}

func (c *appleContainerCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	// Apple Container doesn't expose detailed stats via CLI
	// We could parse logs or use other mechanisms if needed
	return &repb.UsageStats{}, nil
}

func (c *appleContainerCommandContainer) runAppleContainer(ctx context.Context, subCommand string, stdio *interfaces.Stdio, args ...string) *interfaces.CommandResult {
	command := []string{"container", subCommand}
	command = append(command, args...)

	return c.env.GetCommandRunner().Run(ctx, &repb.Command{Arguments: command}, "" /*=workDir*/, nil /*=statsListener*/, stdio)
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
