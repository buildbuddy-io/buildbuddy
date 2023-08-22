package docker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/docker/docker/pkg/stdcopy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	dockertypes "github.com/docker/docker/api/types"
	dockercontainer "github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	units "github.com/docker/go-units"
	gstatus "google.golang.org/grpc/status"
)

const (
	// dockerExecSIGKILLExitCode is returned in the ExitCode field of a docker
	// exec inspect result when an exec process is terminated due to receiving
	// SIGKILL.
	dockerExecSIGKILLExitCode = 137

	dockerDaemonErrorCode        = 125
	containerFinalizationTimeout = 10 * time.Second
	defaultDockerUlimit          = int64(65535)
)

var (
	dockerMountMode         = flag.String("executor.docker_mount_mode", "", "Sets the mount mode of volumes mounted to docker images. Useful if running on SELinux https://www.projectatomic.io/blog/2015/06/using-volumes-with-docker-can-cause-problems-with-selinux/")
	dockerNetHost           = flagutil.New("executor.docker_net_host", false, "Sets --net=host on the docker command. Intended for local development only.", flagutil.DeprecatedTag("Use --executor.docker_network=host instead."))
	dockerNetwork           = flag.String("executor.docker_network", "", "If set, set docker/podman --network to this value by default. Can be overridden per-action with the `dockerNetwork` exec property, which accepts values 'off' (--network=none) or 'bridge' (--network=<default>).")
	dockerCapAdd            = flag.String("docker_cap_add", "", "Sets --cap-add= on the docker command. Comma separated.")
	dockerSiblingContainers = flag.Bool("executor.docker_sibling_containers", false, "If set, mount the configured Docker socket to containers spawned for each action, to enable Docker-out-of-Docker (DooD). Takes effect only if docker_socket is also set. Should not be set by executors that can run untrusted code.")
	dockerDevices           = flagutil.New("executor.docker_devices", []container.DockerDeviceMapping{}, `Configure (docker) devices that will be available inside the sandbox container. Format is --executor.docker_devices='[{"PathOnHost":"/dev/foo","PathInContainer":"/some/dest","CgroupPermissions":"see,docker,docs"}]'`)
	dockerVolumes           = flagutil.New("executor.docker_volumes", []string{}, "Additional --volume arguments to be passed to docker or podman.")
	dockerInheritUserIDs    = flag.Bool("executor.docker_inherit_user_ids", false, "If set, run docker containers using the same uid and gid as the user running the executor process.")
)

type Provider struct {
	env            environment.Env
	client         *dockerclient.Client
	imageCacheAuth *container.ImageCacheAuthenticator
	buildRoot      string
}

var (
	initDockerClientOnce sync.Once
	dockerClient         *dockerclient.Client
	initErr              error
)

func NewClient() (*dockerclient.Client, error) {
	initDockerClientOnce.Do(func() {
		if platform.DockerSocket() == "" {
			return
		}
		_, err := os.Stat(platform.DockerSocket())
		if os.IsNotExist(err) {
			initErr = status.FailedPreconditionErrorf("Docker socket %q not found", platform.DockerSocket())
			return
		}
		if err != nil {
			initErr = status.FailedPreconditionErrorf("Failed to stat docker socket %q: %s", platform.DockerSocket(), err)
			return
		}

		dockerSocket := platform.DockerSocket()
		if !strings.Contains(dockerSocket, "://") {
			dockerSocket = fmt.Sprintf("unix://%s", dockerSocket)
		}
		dockerClient, initErr = dockerclient.NewClientWithOpts(
			dockerclient.WithHost(dockerSocket),
			dockerclient.WithAPIVersionNegotiation(),
		)
	})

	return dockerClient, initErr
}

func NewProvider(env environment.Env, imageCacheAuth *container.ImageCacheAuthenticator, hostBuildRoot string) (*Provider, error) {
	client, err := NewClient()
	if err != nil {
		return nil, status.FailedPreconditionErrorf("Failed to create docker client: %s", err)
	}

	return &Provider{
		env:            env,
		client:         client,
		imageCacheAuth: imageCacheAuth,
		buildRoot:      hostBuildRoot,
	}, nil
}

func (p *Provider) New(ctx context.Context, props *platform.Properties, task *repb.ScheduledTask, state *rnpb.RunnerState, workingDir string) (container.CommandContainer, error) {
	opts := &DockerOptions{
		ForceRoot:               props.DockerForceRoot,
		DockerInit:              props.DockerInit,
		DockerUser:              props.DockerUser,
		DockerNetwork:           props.DockerNetwork,
		Socket:                  platform.DockerSocket(),
		EnableSiblingContainers: *dockerSiblingContainers,
		UseHostNetwork:          *dockerNetHost,
		DockerMountMode:         *dockerMountMode,
		DockerCapAdd:            *dockerCapAdd,
		DockerDevices:           *dockerDevices,
		DefaultNetworkMode:      *dockerNetwork,
		Volumes:                 *dockerVolumes,
		InheritUserIDs:          *dockerInheritUserIDs,
	}
	return NewDockerContainer(p.env, p.imageCacheAuth, p.client, props.ContainerImage, p.buildRoot, opts), nil
}

type DockerOptions struct {
	Socket                  string
	EnableSiblingContainers bool
	UseHostNetwork          bool
	ForceRoot               bool
	DockerInit              bool
	DockerUser              string
	DockerMountMode         string
	InheritUserIDs          bool
	DockerNetwork           string
	DefaultNetworkMode      string
	DockerCapAdd            string
	DockerDevices           []container.DockerDeviceMapping
	Volumes                 []string
}

// dockerCommandContainer containerizes a command's execution using a Docker container.
type dockerCommandContainer struct {
	env            environment.Env
	imageCacheAuth *container.ImageCacheAuthenticator

	image string
	// hostRootDir is the path on the _host_ machine ("node", in k8s land) of the
	// root data dir for builds. We need this information because we are interfacing
	// with the docker daemon on the host machine, which doesn't know about the
	// directories inside this container.
	hostRootDir string
	client      *dockerclient.Client
	options     *DockerOptions

	// id is the Docker container ID, which is available after creating the
	// container.
	id string
	// workDir is the path to the workspace directory mounted to the container.
	workDir string
	// removed is a flag that is set once Remove is called (before actually
	// removing the container).
	removed bool
}

func NewDockerContainer(env environment.Env, imageCacheAuth *container.ImageCacheAuthenticator, client *dockerclient.Client, image, hostRootDir string, options *DockerOptions) *dockerCommandContainer {
	return &dockerCommandContainer{
		env:            env,
		imageCacheAuth: imageCacheAuth,
		image:          image,
		hostRootDir:    hostRootDir,
		client:         client,
		options:        options,
	}
}

func (r *dockerCommandContainer) Run(ctx context.Context, command *repb.Command, workDir string, creds container.PullCredentials) *interfaces.CommandResult {
	result := &interfaces.CommandResult{
		CommandDebugString: fmt.Sprintf("(docker) %s", command.GetArguments()),
		ExitCode:           commandutil.NoExitCode,
	}

	containerName, err := generateContainerName()
	if err != nil {
		result.Error = status.UnavailableErrorf("failed to generate docker container name: %s", err)
		return result
	}

	// explicitly pull the image before running to avoid the
	// pull output logs spilling into the execution logs.
	if err := container.PullImageIfNecessary(ctx, r.env, r.imageCacheAuth, r, creds, r.image); err != nil {
		result.Error = wrapDockerErr(err, fmt.Sprintf("failed to pull docker image %q", r.image))
		return result
	}

	containerCfg, err := r.containerConfig(
		command.GetArguments(),
		commandutil.EnvStringList(command),
		workDir,
	)
	if err != nil {
		result.Error = err
		return result
	}
	createResponse, err := r.client.ContainerCreate(
		ctx,
		containerCfg,
		r.hostConfig(workDir),
		/*networkingConfig=*/ nil,
		/*platform=*/ nil,
		containerName,
	)
	if err != nil {
		result.Error = wrapDockerErr(err, "failed to create docker container")
		return result
	}
	cid := createResponse.ID

	hijackedResp, err := r.client.ContainerAttach(ctx, cid, dockertypes.ContainerAttachOptions{
		Stream: true,
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		result.Error = wrapDockerErr(err, "failed to attach to docker container")
		return result
	}
	defer hijackedResp.Close()

	err = r.client.ContainerStart(ctx, cid, dockertypes.ContainerStartOptions{})
	if err != nil {
		result.Error = wrapDockerErr(err, "failed to start docker container")
		return result
	}

	exitedCleanly := false
	defer func() {
		// Clean up the container in the background.
		// TODO: Add this removal as a job to a centralized queue.
		go func() {
			ctx := context.Background()
			if !exitedCleanly {
				if err := r.client.ContainerKill(ctx, cid, "SIGKILL"); err != nil {
					log.Errorf("Failed to kill docker container: %s", err)
				}
			}
			if err := r.client.ContainerRemove(ctx, cid, dockertypes.ContainerRemoveOptions{}); err != nil {
				log.Errorf("Failed to remove docker container: %s", err)
			}
		}()
	}()

	eg := &errgroup.Group{}
	eg.Go(func() error {
		var stdout, stderr bytes.Buffer
		_, err := stdcopy.StdCopy(&stdout, &stderr, hijackedResp.Reader)
		result.Stdout = stdout.Bytes()
		result.Stderr = stderr.Bytes()
		return wrapDockerErr(err, "failed to copy docker container output")
	})
	eg.Go(func() error {
		statusCh, errCh := r.client.ContainerWait(ctx, cid, dockercontainer.WaitConditionNotRunning)
		select {
		case err := <-errCh:
			return wrapDockerErr(err, "container did not exit cleanly")
		case s := <-statusCh:
			exitedCleanly = true
			if s.Error != nil {
				return wrapDockerErr(status.UnknownError(s.Error.Message), "failed to get container status")
			}
			result.ExitCode = int(s.StatusCode)
			return nil
		}
	})
	if err := eg.Wait(); err != nil {
		result.Error = err
	}

	return result
}

func wrapDockerErr(err error, contextMsg string) error {
	if err == nil {
		return nil
	}
	return gstatus.Errorf(errCode(err), "%s: %s", contextMsg, errMsg(err))
}

func (r *dockerCommandContainer) getUser() (string, error) {
	if r.options.ForceRoot {
		return "root", nil
	}
	if r.options.InheritUserIDs {
		user, err := user.Current()
		if err != nil {
			return "", status.InternalErrorf("Failed to get user: %s", err)
		}
		return fmt.Sprintf("%s:%s", user.Uid, user.Gid), nil
	}
	return r.options.DockerUser, nil
}

func (r *dockerCommandContainer) containerConfig(args, env []string, workDir string) (*dockercontainer.Config, error) {
	u, err := r.getUser()
	if err != nil {
		return nil, err
	}
	return &dockercontainer.Config{
		Image:      r.image,
		Env:        env,
		Cmd:        args,
		WorkingDir: workDir,
		User:       u,
	}, nil
}

func (r *dockerCommandContainer) hostConfig(workDir string) *dockercontainer.HostConfig {
	networkMode := dockercontainer.NetworkMode(r.options.DefaultNetworkMode)
	// Support the legacy `executor.docker_net_host` config option.
	if r.options.UseHostNetwork {
		networkMode = dockercontainer.NetworkMode("host")
	}
	// Translate network platform prop to the equivalent Docker network mode, to
	// allow overriding the default configured mode.
	switch strings.ToLower(r.options.DockerNetwork) {
	case "off":
		networkMode = dockercontainer.NetworkMode("none")
	case "bridge": // use Docker default (bridge)
		networkMode = dockercontainer.NetworkMode("")
	default: // ignore other values for now, sticking to the configured default.
	}
	capAdd := make([]string, 0)
	if r.options.DockerCapAdd != "" {
		capAdd = append(capAdd, strings.Split(r.options.DockerCapAdd, ",")...)
	}
	devices := make([]dockercontainer.DeviceMapping, 0)
	for _, device := range r.options.DockerDevices {
		devices = append(devices, dockercontainer.DeviceMapping{
			PathOnHost:        device.PathOnHost,
			PathInContainer:   device.PathInContainer,
			CgroupPermissions: device.CgroupPermissions,
		})
	}
	mountMode := ""
	if r.options.DockerMountMode != "" {
		mountMode = fmt.Sprintf(":%s", r.options.DockerMountMode)
	}
	binds := []string{
		fmt.Sprintf(
			"%s:%s%s",
			// Source path here needs to point to the host machine (*not* a path in this
			// executor's FS), since we spawn child actions via the docker daemon
			// running on the host.
			filepath.Join(r.hostRootDir, filepath.Base(workDir)),
			workDir,
			mountMode,
		),
	}
	if r.options.EnableSiblingContainers {
		binds = append(binds, fmt.Sprintf("%s:%s%s", r.options.Socket, r.options.Socket, mountMode))
	}
	binds = append(binds, r.options.Volumes...)
	var initPtr *bool
	if r.options.DockerInit {
		initPtr = &r.options.DockerInit
		// If dockerInit platform prop is false/unspecified, then leave the Init
		// option nil, which means "use dockerd configured settings"
	}
	return &dockercontainer.HostConfig{
		NetworkMode: networkMode,
		Binds:       binds,
		CapAdd:      capAdd,
		Init:        initPtr,
		Resources: dockercontainer.Resources{
			Devices: devices,
			Ulimits: []*units.Ulimit{
				{Name: "nofile", Soft: defaultDockerUlimit, Hard: defaultDockerUlimit},
			},
		},
	}
}

func copyOutputs(reader io.Reader, result *interfaces.CommandResult, stdio *container.Stdio) error {
	var stdoutBuf, stderrBuf bytes.Buffer
	stdout, stderr := io.Writer(&stdoutBuf), io.Writer(&stderrBuf)
	// Note: stdout and stderr aren't buffered in the command result when
	// providing an explicit writer.
	if stdio.Stdout != nil {
		stdout = stdio.Stdout
	}
	if stdio.Stderr != nil {
		stderr = stdio.Stderr
	}

	if *commandutil.DebugStreamCommandOutputs {
		stdout, stderr = io.MultiWriter(stdout, os.Stdout), io.MultiWriter(stderr, os.Stderr)
	}

	_, err := stdcopy.StdCopy(stdout, stderr, reader)
	result.Stdout = stdoutBuf.Bytes()
	result.Stderr = stderrBuf.Bytes()
	return err
}

func errCode(err error) codes.Code {
	if err == context.DeadlineExceeded {
		return codes.DeadlineExceeded
	}
	return codes.Unavailable
}

func errMsg(err error) string {
	if err == nil {
		return ""
	}
	if s, ok := gstatus.FromError(err); ok {
		return s.Message()
	}
	return err.Error()
}

func (r *dockerCommandContainer) IsImageCached(ctx context.Context) (bool, error) {
	_, _, err := r.client.ImageInspectWithRaw(ctx, r.image)
	if err == nil {
		return true, nil
	}
	if !dockerclient.IsErrNotFound(err) {
		return false, err
	}
	return false, nil
}

func (r *dockerCommandContainer) PullImage(ctx context.Context, creds container.PullCredentials) error {
	return PullImage(ctx, r.client, r.image, creds)
}

func PullImage(ctx context.Context, client *dockerclient.Client, image string, creds container.PullCredentials) error {
	if !creds.IsEmpty() {
		authCfg := dockertypes.AuthConfig{
			Username: creds.Username,
			Password: creds.Password,
		}
		auth, err := encodeAuthToBase64(authCfg)
		if err != nil {
			return err
		}
		rc, err := client.ImagePull(ctx, image, dockertypes.ImagePullOptions{
			RegistryAuth: auth,
		})
		if err != nil {
			return err
		}
		defer rc.Close()
		if _, err := io.Copy(io.Discard, rc); err != nil {
			return err
		}
		return nil
	}

	// TODO: find a way to implement this without calling the Docker CLI.
	// Currently it's a bit involved to replicate the exact protocols that the
	// CLI uses to pull images.
	cmd := exec.CommandContext(ctx, "docker", "pull", image)
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return wrapDockerErr(
			err,
			fmt.Sprintf("docker pull %q: %s -- stderr:\n%s", image, err, string(stderr.Bytes())),
		)
	}
	return nil
}

// SaveImage saves an image from the Docker cache to a tarball file at the given
// path. The image must have already been pulled, otherwise the operation will
// fail.
func SaveImage(ctx context.Context, client *dockerclient.Client, imageRef, path string) error {
	r, err := client.ImageSave(ctx, []string{imageRef})
	if err != nil {
		return err
	}
	defer r.Close()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func generateContainerName() (string, error) {
	suffix, err := random.RandomString(20)
	if err != nil {
		return "", err
	}
	return "buildbuddy-exec-" + suffix, nil
}

func (r *dockerCommandContainer) Create(ctx context.Context, workDir string) error {
	return commandutil.RetryIfTextFileBusy(func() error {
		return r.create(ctx, workDir)
	})
}

func (r *dockerCommandContainer) create(ctx context.Context, workDir string) error {
	containerName, err := generateContainerName()
	if err != nil {
		return status.UnavailableErrorf("failed to generate docker container name: %s", err)
	}

	containerConfig, err := r.containerConfig([]string{"sleep", "infinity"}, []string{}, workDir)
	if err != nil {
		return err
	}
	createResponse, err := r.client.ContainerCreate(
		ctx,
		// Top-level container process just sleeps forever so that the container
		// stays alive until explicitly killed.
		containerConfig,
		r.hostConfig(workDir),
		/*networkingConfig=*/ nil,
		/*platform=*/ nil,
		containerName,
	)
	if err != nil {
		return wrapDockerErr(err, "failed to create container")
	}
	r.id = createResponse.ID
	if err := r.client.ContainerStart(ctx, r.id, dockertypes.ContainerStartOptions{}); err != nil {
		return wrapDockerErr(err, "failed to start container")
	}
	r.workDir = workDir
	return nil
}

func (r *dockerCommandContainer) Exec(ctx context.Context, command *repb.Command, stdio *container.Stdio) *interfaces.CommandResult {
	var res *interfaces.CommandResult
	// Ignore error from this function; it is returned as part of res.
	commandutil.RetryIfTextFileBusy(func() error {
		res = r.exec(ctx, command, stdio)
		return res.Error
	})
	return res
}

func (r *dockerCommandContainer) exec(ctx context.Context, command *repb.Command, stdio *container.Stdio) *interfaces.CommandResult {
	result := &interfaces.CommandResult{
		CommandDebugString: fmt.Sprintf("(docker) %s", command.GetArguments()),
		ExitCode:           commandutil.NoExitCode,
	}
	u, err := r.getUser()
	if err != nil {
		result.Error = err
		return result
	}
	cfg := dockertypes.ExecConfig{
		Cmd:          command.GetArguments(),
		Env:          commandutil.EnvStringList(command),
		WorkingDir:   r.workDir,
		AttachStdout: true,
		AttachStderr: true,
		AttachStdin:  stdio.Stdin != nil,
		User:         u,
	}
	exec, err := r.client.ContainerExecCreate(ctx, r.id, cfg)
	if err != nil {
		result.Error = wrapDockerErr(err, "docker exec create failed")
		return result
	}
	attachResp, err := r.client.ContainerExecAttach(ctx, exec.ID, dockertypes.ExecStartCheck{})
	if err != nil {
		result.Error = wrapDockerErr(err, "docker exec attach failed")
		return result
	}

	if stdio.Stdin != nil {
		go func() {
			// Ignoring errors from the copy here since they're most likely due to the
			// process being intentionally terminated.
			io.Copy(attachResp.Conn, stdio.Stdin)
			attachResp.CloseWrite()
		}()
	}

	// note: Close() doesn't return an error, and can be safely called more than once.
	defer attachResp.Close()
	go func() {
		// If the context times out before we return from this func, close the
		// attachResp -- otherwise copyOutputs() hangs.
		<-ctx.Done()
		attachResp.Close()
	}()
	if err := copyOutputs(attachResp.Reader, result, stdio); err != nil {
		// If we timed out, ignore the "closed connection" error from copying
		// outputs
		if ctx.Err() == context.DeadlineExceeded {
			result.Error = status.DeadlineExceededError("command timed out")
			return result
		}
		result.Error = wrapDockerErr(err, "failed to get output of exec process")
		return result
	}
	info, err := r.client.ContainerExecInspect(ctx, exec.ID)
	if err != nil {
		result.Error = wrapDockerErr(err, "failed to get exec process info")
		return result
	}
	// Docker does not provide a direct way to get the exit signal from an exec
	// process in the case where the process terminated due to being signaled.
	// Instead, it uses a (somewhat common) convention of returning an exit code
	// of `128 + signal`. See:
	// https://cs.github.com/containerd/containerd/blob/590ef88c7181eb6601fdd2f08afe6f29551c6fb3/sys/reaper/reaper_unix.go?q=repo%3Acontainerd%2Fcontainerd+exitSignalOffset#L278-L283
	//
	// This convention is not 100% reliable since these conventional codes are
	// also valid exit code values. A command could run `exit 137`, for example.
	//
	// So we are overly cautious here and only convert the exit code values to
	// signals for the signals we care about, particularly SIGKILL, and only in
	// the case where we are expecting a SIGKILL due to the container being
	// removed.
	if r.removed && info.ExitCode == dockerExecSIGKILLExitCode {
		result.ExitCode = commandutil.KilledExitCode
		result.Error = commandutil.ErrSIGKILL
		return result
	}

	result.ExitCode = info.ExitCode
	return result
}

func (r *dockerCommandContainer) Unpause(ctx context.Context) error {
	if err := r.client.ContainerUnpause(ctx, r.id); err != nil {
		return wrapDockerErr(err, "failed to unpause container")
	}
	return nil
}

func (r *dockerCommandContainer) Pause(ctx context.Context) error {
	if err := r.client.ContainerPause(ctx, r.id); err != nil {
		return wrapDockerErr(err, "failed to pause container")
	}
	return nil
}

func (r *dockerCommandContainer) Remove(ctx context.Context) error {
	r.removed = true
	if err := r.client.ContainerRemove(ctx, r.id, dockertypes.ContainerRemoveOptions{Force: true}); err != nil {
		return wrapDockerErr(err, fmt.Sprintf("failed to remove docker container %s", r.id))
	}
	return nil
}

func (r *dockerCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	stats, err := r.client.ContainerStatsOneShot(ctx, r.id)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := stats.Body.Close(); err != nil {
			log.Printf("error closing docker stats response body: %s", err)
		}
	}()
	body, err := io.ReadAll(stats.Body)
	if err != nil {
		return nil, err
	}
	var response statsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	return &repb.UsageStats{
		// See formula here: https://docs.docker.com/engine/api/v1.41/#operation/ContainerStats
		MemoryBytes: response.MemoryStats.Usage - response.MemoryStats.Stats.Cache,
	}, nil
}

func (r *dockerCommandContainer) State(ctx context.Context) (*rnpb.ContainerState, error) {
	return nil, status.UnimplementedError("not implemented")
}

// See https://docs.docker.com/engine/api/v1.41/#operation/ContainerStats
type statsResponse struct {
	MemoryStats struct {
		Stats struct {
			Cache int64 `json:"cache"`
		} `json:"stats"`
		Usage int64 `json:"usage"`
	} `json:"memory_stats"`
}

// encodeAuthToBase64 serializes the auth configuration as JSON base64 payload
func encodeAuthToBase64(authConfig dockertypes.AuthConfig) (string, error) {
	buf, err := json.Marshal(authConfig)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(buf), nil
}
