package docker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"os/user"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/docker/docker/pkg/stdcopy"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	dockertypes "github.com/docker/docker/api/types"
	dockercontainer "github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	units "github.com/docker/go-units"
	gstatus "google.golang.org/grpc/status"
)

var (
	dockerDaemonErrorCode        = 125
	containerFinalizationTimeout = 10 * time.Second
	defaultDockerUlimit          = int64(65535)
)

type DockerOptions struct {
	Socket                  string
	EnableSiblingContainers bool
	UseHostNetwork          bool
	ForceRoot               bool
	DockerMountMode         string
	InheritUserIDs          bool
	EnableCASFS             bool
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

	statusCh, errCh := r.client.ContainerWait(ctx, cid, dockercontainer.WaitConditionNotRunning)

	select {
	case err := <-errCh:
		result.Error = wrapDockerErr(err, "container did not exit cleanly")
	case s := <-statusCh:
		exitedCleanly = true
		if s.Error != nil {
			result.Error = wrapDockerErr(status.UnknownError(s.Error.Message), "failed to get container status")
			return result
		}
		result.ExitCode = int(s.StatusCode)
		logOptions := dockertypes.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
		}
		logs, err := r.client.ContainerLogs(ctx, cid, logOptions)
		if err != nil {
			result.Error = wrapDockerErr(err, "failed to get docker container logs")
			return result
		}
		err = copyOutputs(logs, result)
		if closeErr := logs.Close(); closeErr != nil {
			log.Warningf("Failed to close docker logs: %s", closeErr)
		}
		if err != nil {
			result.Error = wrapDockerErr(err, "failed to read docker container logs")
			return result
		}
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
	return "", nil
}

func (r *dockerCommandContainer) containerConfig(args, env []string, workDir string) (*dockercontainer.Config, error) {
	u, err := r.getUser()
	if err != nil {
		return nil, err
	}
	return &dockercontainer.Config{
		Image:      r.image,
		Hostname:   "localhost",
		Env:        env,
		Cmd:        args,
		WorkingDir: workDir,
		User:       u,
	}, nil
}

func (r *dockerCommandContainer) hostConfig(workDir string) *dockercontainer.HostConfig {
	networkMode := dockercontainer.NetworkMode("")
	if r.options.UseHostNetwork {
		networkMode = dockercontainer.NetworkMode("host")
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
	return &dockercontainer.HostConfig{
		NetworkMode: networkMode,
		Binds:       binds,
		Resources: dockercontainer.Resources{
			Ulimits: []*units.Ulimit{
				&units.Ulimit{Name: "nofile", Soft: defaultDockerUlimit, Hard: defaultDockerUlimit},
			},
		},
	}
}

func copyOutputs(reader io.Reader, result *interfaces.CommandResult) error {
	var stdout, stderr bytes.Buffer
	_, err := stdcopy.StdCopy(&stdout, &stderr, reader)
	result.Stdout = stdout.Bytes()
	result.Stderr = stderr.Bytes()
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
	if !creds.IsEmpty() {
		authCfg := dockertypes.AuthConfig{
			Username: creds.Username,
			Password: creds.Password,
		}
		auth, err := encodeAuthToBase64(authCfg)
		if err != nil {
			return err
		}
		rc, err := r.client.ImagePull(ctx, r.image, dockertypes.ImagePullOptions{
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
	cmd := exec.CommandContext(ctx, "docker", "pull", r.image)
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return wrapDockerErr(
			err,
			fmt.Sprintf("docker pull %q: %s -- stderr:\n%s", r.image, err, string(stderr.Bytes())),
		)
	}
	return nil
}

func generateContainerName() (string, error) {
	suffix, err := random.RandomString(20)
	if err != nil {
		return "", err
	}
	return "buildbuddy_exec_" + suffix, nil
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

func (r *dockerCommandContainer) Exec(ctx context.Context, command *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
	var res *interfaces.CommandResult
	// Ignore error from this function; it is returned as part of res.
	commandutil.RetryIfTextFileBusy(func() error {
		res = r.exec(ctx, command, stdin, stdout)
		return res.Error
	})
	return res
}

func (r *dockerCommandContainer) exec(ctx context.Context, command *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
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
		AttachStdin:  stdin != nil,
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

	if stdin != nil {
		go io.Copy(attachResp.Conn, stdin)
	}

	var responseReader io.Reader
	responseReader = attachResp.Reader
	if stdout != nil {
		r, w := io.Pipe()
		defer w.Close()
		responseReader = io.TeeReader(responseReader, w)
		// TODO(siggisim): Pipe stderr to the action result's stdout.
		go stdcopy.StdCopy(stdout, ioutil.Discard, r)
	}

	defer attachResp.Close() // note: Close() doesn't return an error.
	if err := copyOutputs(responseReader, result); err != nil {
		result.Error = wrapDockerErr(err, "failed to get output of exec process")
		return result
	}
	info, err := r.client.ContainerExecInspect(ctx, exec.ID)
	if err != nil {
		result.Error = wrapDockerErr(err, "failed to get exec process info")
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
	if err := r.client.ContainerRemove(ctx, r.id, dockertypes.ContainerRemoveOptions{Force: true}); err != nil {
		return wrapDockerErr(err, fmt.Sprintf("failed to remove docker container %s", r.id))
	}
	return nil
}

func (r *dockerCommandContainer) Stats(ctx context.Context) (*container.Stats, error) {
	stats, err := r.client.ContainerStatsOneShot(ctx, r.id)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := stats.Body.Close(); err != nil {
			log.Printf("error closing docker stats response body: %s", err)
		}
	}()
	body, err := ioutil.ReadAll(stats.Body)
	if err != nil {
		return nil, err
	}
	var response statsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	return &container.Stats{
		// See formula here: https://docs.docker.com/engine/api/v1.41/#operation/ContainerStats
		MemoryUsageBytes: response.MemoryStats.Usage - response.MemoryStats.Stats.Cache,
	}, nil
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
