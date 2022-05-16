package podman

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
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
)

type pullStatus struct {
	mu     *sync.RWMutex
	pulled bool
}

type PodmanOptions struct {
	ForceRoot bool
	User      string
	Network   string
	CapAdd    string
	Devices   []container.DockerDeviceMapping
	Volumes   []string
	Runtime   string
}

// podmanCommandContainer containerizes a command's execution using a Podman container.
// between containers.
type podmanCommandContainer struct {
	env            environment.Env
	imageCacheAuth *container.ImageCacheAuthenticator

	image     string
	buildRoot string

	options *PodmanOptions

	// name is the container name.
	name string

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

	podmanRunArgs := c.getPodmanRunArgs(workDir)

	for _, envVar := range command.GetEnvironmentVariables() {
		podmanRunArgs = append(podmanRunArgs, "--env", fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	podmanRunArgs = append(podmanRunArgs, c.image)
	podmanRunArgs = append(podmanRunArgs, command.Arguments...)
	result = runPodman(ctx, "run", nil, nil, podmanRunArgs...)
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

	podmanRunArgs := c.getPodmanRunArgs(workDir)
	podmanRunArgs = append(podmanRunArgs, c.image)
	podmanRunArgs = append(podmanRunArgs, "sleep", "infinity")
	createResult := runPodman(ctx, "create", nil, nil, podmanRunArgs...)
	if err := c.maybeCleanupCorruptedImages(ctx, createResult); err != nil {
		log.Warningf("Failed to remove corrupted image: %s", err)
	}

	if err = createResult.Error; err != nil {
		return status.UnavailableErrorf("failed to create container: %s", err)
	}

	if createResult.ExitCode != 0 {
		return status.UnknownErrorf("podman create failed: exit code %d, stderr: %s", createResult.ExitCode, createResult.Stderr)
	}

	startResult := runPodman(ctx, "start", nil, nil, c.name)
	if startResult.Error != nil {
		return startResult.Error
	}
	if startResult.ExitCode != 0 {
		return status.UnknownErrorf("podman start failed: exit code %d, stderr: %s", startResult.ExitCode, startResult.Stderr)
	}
	return nil
}

func (c *podmanCommandContainer) Exec(ctx context.Context, cmd *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
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
	podmanRunArgs = append(podmanRunArgs, c.name)
	podmanRunArgs = append(podmanRunArgs, cmd.Arguments...)
	// Podman doesn't provide a way to find out whether an exec process was
	// killed. Instead, `podman exec` returns 137 (= 128 + SIGKILL(9)). However,
	// this exit code is also valid as a regular exit code returned by a command
	// during a normal execution, so we are overly cautious here and only
	// interpret this code specially when the container was removed and we are
	// expecting a SIGKILL as a result.
	res := runPodman(ctx, "exec", stdin, stdout, podmanRunArgs...)
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
	// Try to avoid the `pull` command which results in a network roundtrip.
	listResult := runPodman(ctx, "image", nil /*=stdin*/, nil /*=stdout*/, "inspect", "--format={{.ID}}", c.image)
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
		return c.pullImage(creds)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := c.pullImage(creds); err != nil {
		return err
	}
	ps.pulled = true
	return nil
}

func (c *podmanCommandContainer) pullImage(creds container.PullCredentials) error {
	podmanArgs := make([]string, 0, 2)
	if !creds.IsEmpty() {
		podmanArgs = append(podmanArgs, fmt.Sprintf(
			"--creds=%s:%s",
			creds.Username,
			creds.Password,
		))
	}
	podmanArgs = append(podmanArgs, c.image)
	// Use server context instead of ctx to make sure that "podman pull" is not killed when the context
	// is cancelled. If "podman pull" is killed when copying a parent layer, it will result in
	// corrupted storage.  More details see https://github.com/containers/storage/issues/1136.
	pullResult := runPodman(c.env.GetServerContext(), "pull", nil /*=stdin*/, nil /*=stdout*/, podmanArgs...)
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
	res := runPodman(ctx, "kill", nil /*=stdin*/, nil /*=stdout*/, "--signal=KILL", c.name)
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode == 0 || strings.Contains(string(res.Stderr), "no such container") {
		return nil
	}
	return status.UnknownErrorf("podman remove failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
}

func (c *podmanCommandContainer) Pause(ctx context.Context) error {
	res := runPodman(ctx, "pause", nil /*=stdin*/, nil /*=stdout*/, c.name)
	if res.ExitCode != 0 {
		return status.UnknownErrorf("podman pause failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
	}
	return nil
}

func (c *podmanCommandContainer) Unpause(ctx context.Context) error {
	res := runPodman(ctx, "unpause", nil /*=stdin*/, nil /*=stdout*/, c.name)
	if res.Error != nil {
		return res.Error
	}
	if res.ExitCode != 0 {
		return status.UnknownErrorf("podman unpause failed: exit code %d, stderr: %s", res.ExitCode, string(res.Stderr))
	}
	return nil
}

func (c *podmanCommandContainer) Stats(ctx context.Context) (*container.Stats, error) {
	return &container.Stats{}, nil
}

func runPodman(ctx context.Context, subCommand string, stdin io.Reader, stdout io.Writer, args ...string) *interfaces.CommandResult {
	command := []string{
		"podman",
		subCommand,
	}

	command = append(command, args...)
	result := commandutil.Run(ctx, &repb.Command{Arguments: command}, "" /*=workDir*/, stdin, stdout)
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

	result := runPodman(ctx, "rmi", nil, nil, imageName)
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
	result := runPodman(ctx, "run", nil, nil, "--rm", "busybox", "sh")
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
