package containerd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	dockerconfig "github.com/containerd/containerd/remotes/docker/config"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	// namespace is the containerd namespace that the executor creates
	// all containers under. The main advantage of using the same namespace
	// is containers can share cached images, even across different executor
	// instances running on the same node.
	namespace = "buildbuddy"
	// containerFinalizationTimeout is the amount of time the executor
	// allows for the action process and container task to be killed. This
	// cleanup is done in the background after the task is executed, and
	// does not block the execution response from being returned to the client.
	containerFinalizationTimeout = 30 * time.Second
	// killedProcessExitCode is returned by containerd when it kills a process,
	// as opposed to the process exiting on its own.
	killedProcessExitCode = 137
)

var imageRefHashSuffixPattern = regexp.MustCompile("@.*")

type AuthConfig struct {
	CredHelpers map[string]string `json:"credHelpers"`
}

// ReadAuthConfig reads the existing Docker config at ~/.docker/config.json
// for use with containerd.
func readAuthConfig() (*AuthConfig, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadFile(filepath.Join(home, ".docker/config.json"))
	if err != nil {
		return nil, err
	}
	cfg := &AuthConfig{}
	if err := json.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// containerdCommandContainer runs a command in an OCI container via containerd.
type containerdCommandContainer struct {
	socket string
	image  string
	// hostRootDir is the path on the _host_ machine ("node", in k8s land) of the
	// root data dir for builds. We need this information because we are interfacing
	// with the containerd instance on the host machine, which doesn't know about the
	// directories inside this container.
	hostRootDir string
}

func NewContainerdContainer(socket, image, hostRootDir string) container.CommandContainer {
	return &containerdCommandContainer{socket, image, hostRootDir}
}

func formatCommand(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, fmt.Sprintf("%q", arg))
	}
	return strings.Join(quoted, " ")
}

func (r *containerdCommandContainer) Run(ctx context.Context, command *repb.Command, workDir string) *interfaces.CommandResult {
	result := &interfaces.CommandResult{
		ExitCode:           commandutil.NoExitCode,
		CommandDebugString: "(containerd) " + formatCommand(command.Arguments),
	}
	client, err := containerd.New(r.socket)
	if err != nil {
		result.Error = err
		return result
	}
	cleanupFns := []func(context.Context) error{}
	defer func() {
		ctx, cancel := background.ExtendContextForFinalization(ctx, containerFinalizationTimeout)
		go func() {
			defer cancel()
			// Clean up in LIFO order, since higher-level containerd objects
			// need to be cleaned up last.
			for i := len(cleanupFns) - 1; i >= 0; i-- {
				if err := cleanupFns[i](ctx); err != nil {
					log.Error(err.Error())
				}
			}
			client.Close()
		}()
	}()

	ctx = namespaces.WithNamespace(ctx, namespace)

	image, err := getOrPullImage(ctx, client, r.image)
	if err != nil {
		result.Error = err
		return result
	}
	env := make([]string, 0, len(command.GetEnvironmentVariables()))
	for _, envVar := range command.GetEnvironmentVariables() {
		env = append(env, fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}

	executionID := randomToken()
	container, err := client.NewContainer(
		ctx,
		objectName("container", executionID),
		containerd.WithImage(image),
		containerd.WithNewSnapshot(objectName("snapshot", executionID), image),
		containerd.WithNewSpec(
			oci.WithImageConfig(image),
			oci.WithMounts([]specs.Mount{{
				// Source path here needs to point to the host machine (*not* a path in this
				// executor's FS), since we spawn child actions via the containerd instance
				// running on the host.
				Source:      filepath.Join(r.hostRootDir, filepath.Base(workDir)),
				Destination: workDir,
				// Here we use the same mount options that k8s uses when mounting the emptyDir
				// volume to the executor container. This can be determined by running the command
				// `ctr --namespace k8s.io containers info $(basename $(cat /proc/1/cpuset))`
				// within a running executor container (search for "~empty-dir" in the output).
				Type:    "bind",
				Options: []string{"rbind", "rprivate", "rw"},
			}}),
			oci.WithProcessCwd(workDir),
			oci.WithEnv(env),
			oci.WithProcessArgs(command.Arguments...),
		),
	)
	if err != nil {
		result.Error = err
		return result
	}
	cleanupFns = append(cleanupFns, func(ctx context.Context) error {
		if err := container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil {
			return status.InternalErrorf("Failed to delete container: %s", err)
		}
		return nil
	})

	// Create and run the container task.
	var stdout, stderr bytes.Buffer
	task, err := container.NewTask(ctx, cio.NewCreator(cio.WithStreams(nil, &stdout, &stderr)))
	if err != nil {
		result.Error = err
		return result
	}
	cleanupFns = append(cleanupFns, func(ctx context.Context) error {
		if err := cleanupProcess(ctx, task); err != nil {
			return status.InternalErrorf("Failed to clean up process: %s", err)
		}
		return nil
	})
	log.Debugf("Starting command in container: %s", result.CommandDebugString)
	start := time.Now()
	result.ExitCode, result.Error = run(ctx, task)
	log.Debugf("Command completed in %s", time.Since(start))
	result.Stdout = stdout.Bytes()
	result.Stderr = stderr.Bytes()
	return result
}

func objectName(objType, executionID string) string {
	return fmt.Sprintf("%s_%s_%s", "buildbuddy_executor", objType, executionID)
}

func run(ctx context.Context, proc containerd.Process) (int, error) {
	exitStatusC, err := proc.Wait(ctx)
	if err != nil {
		return commandutil.NoExitCode, status.UnavailableErrorf("Error calling proc.Wait(): %s", err)
	}
	if err := proc.Start(ctx); err != nil {
		return commandutil.NoExitCode, status.UnavailableErrorf("Error starting the process: %s", err)
	}
	code, _, err := (<-exitStatusC).Result()
	if !isPastDeadline(ctx) && code == killedProcessExitCode {
		err = status.ResourceExhaustedErrorf("Container was killed (maybe due to OOM): %s", err)
	}
	return int(code), err
}

func isPastDeadline(ctx context.Context) bool {
	dl, ok := ctx.Deadline()
	return ok && time.Now().After(dl)
}

func randomToken() string {
	if token, err := random.RandomString(20); err != nil {
		return token
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func getOrPullImage(ctx context.Context, client *containerd.Client, ref string) (containerd.Image, error) {
	cachedImg, err := client.GetImage(ctx, ref)
	if err != nil && !errdefs.IsNotFound(err) {
		return nil, err
	}
	if err == nil {
		return cachedImg, nil
	}

	authConfig, err := readAuthConfig()
	if err != nil {
		return nil, status.InternalErrorf("Failed to read container auth config: %s", err)
	}
	remoteOpts := []containerd.RemoteOpt{containerd.WithPullUnpack}
	if authConfig != nil {
		if opt := withCredentialHelpers(ctx, authConfig, ref); opt != nil {
			remoteOpts = append(remoteOpts, opt)
		}
	}
	log.Debugf("Pulling %q", ref)
	startTime := time.Now()
	img, err := client.Pull(ctx, ref, remoteOpts...)
	if err == nil {
		log.Debugf("Pulled %q (took %s)", ref, time.Since(startTime))
	}
	return img, err
}

// withCredentialHelpers returns a RemoteOpt that authorizes remote image pull requests
// with credentials generated by "helper" binaries.
//
// Helper binaries are configured per-host. For compatibility with Docker, we source this
// configuration from the local Docker config. An example `~/.docker/config.json` looks
// like this:
//
//     { "credHelpers": { "marketplace.gcr.io": "gcr" } }
//
// In this case, when pulling images from marketplace.gcr.io, we'll return a `RemoteOpt`
// which basically runs a command like this:
//
//     $ echo "marketplace.gcr.io" | docker-credential-gcr get
//     # Output: { "Username": "...", "Secret": "..." }
//
// The returned JSON is parsed and the credentials are attached to the request.
func withCredentialHelpers(ctx context.Context, cfg *AuthConfig, ref string) containerd.RemoteOpt {
	if !(strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://")) {
		ref = "https://" + ref
	}
	// Hash suffixes like "@sha256:abc123..." prevent the URL from being parsed; strip if present.
	ref = imageRefHashSuffixPattern.ReplaceAllLiteralString(ref, "")
	u, err := url.Parse(ref)
	if err != nil {
		log.Warningf("Could not parse URL from %q: %s", ref, err)
		return nil
	}
	helper := cfg.CredHelpers[u.Host]
	if helper == "" {
		// No credential helper configured -- if the registry is public, this is OK.
		// If not, we'll return the 401/403 that we get from the registry.
		return nil
	}
	log.Debugf("Using credential helper %q for %q", helper, u.Host)
	return containerd.WithResolver(newCredentialHelperResolver(ctx, u, helper))
}

func newCredentialHelperResolver(ctx context.Context, ref *url.URL, helper string) remotes.Resolver {
	hostOptions := dockerconfig.HostOptions{
		Credentials: func(host string) (string, string, error) {
			c, err := runCredentialHelper(ctx, host, helper)
			if err != nil {
				return "", "", err
			}
			return c.Username, c.Secret, nil
		},
	}
	return docker.NewResolver(docker.ResolverOptions{
		Hosts: dockerconfig.ConfigureHosts(ctx, hostOptions),
	})
}

type credentials struct {
	Secret   string `json:"Secret"`
	Username string `json:"Username"`
}

func runCredentialHelper(ctx context.Context, host, helper string) (*credentials, error) {
	cmd := exec.CommandContext(ctx, "sh", "-c", fmt.Sprintf("echo %q | docker-credential-%s get", host, helper))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		log.Warningf("Credential helper %q failed for hostname %q: %s. Command output: %q", helper, host, err, string(out)+string(stderr.Bytes()))
		return nil, err
	}

	c := &credentials{}
	if err := json.Unmarshal(out, c); err != nil {
		return nil, err
	}
	return c, nil
}

func cleanupProcess(ctx context.Context, proc containerd.Process) error {
	exitStatusC, err := proc.Wait(ctx)
	if err != nil {
		return err
	}
	err = proc.Kill(ctx, syscall.SIGKILL)
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	_, _, err = (<-exitStatusC).Result()
	if err != nil {
		return err
	}
	exitStatus, err := proc.Delete(ctx)
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	_, _, err = exitStatus.Result()
	return err
}

func (r *containerdCommandContainer) Create(ctx context.Context, workDir string) error {
	return status.UnimplementedError("not implemented")
}
func (r *containerdCommandContainer) Exec(ctx context.Context, cmd *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
	res := &interfaces.CommandResult{}
	res.Error = status.UnimplementedError("not implemented")
	return res
}
func (r *containerdCommandContainer) PullImageIfNecessary(ctx context.Context) error {
	return status.UnimplementedError("not implemented")
}
func (r *containerdCommandContainer) Start(ctx context.Context) error {
	return status.UnimplementedError("not implemented")
}
func (r *containerdCommandContainer) Remove(ctx context.Context) error {
	return status.UnimplementedError("not implemented")
}
func (r *containerdCommandContainer) Stats(ctx context.Context) (*container.Stats, error) {
	return nil, status.UnimplementedError("not implemented")
}
func (r *containerdCommandContainer) Pause(ctx context.Context) error {
	return status.UnimplementedError("not implemented")
}
func (r *containerdCommandContainer) Unpause(ctx context.Context) error {
	return status.UnimplementedError("not implemented")
}
