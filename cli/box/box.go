// Package box implements the "bb box" command for starting remote Firecracker
// VMs with an SSH server.
package box

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Cap the allowed grace period and idle timeout to prevent resource
	// hogging.
	maxGracePeriod = 5 * time.Minute
	maxIdleTimeout = 5 * time.Minute
	actionTimeout  = 24 * time.Hour

	// Used for cache uploads and remotely executed action.
	digestFunction = repb.DigestFunction_BLAKE3

	// defaultImage is the container image used for box VMs. This image is
	// pinned by digest and is likely already cached on BuildBuddy executors.
	defaultImage = platform.DockerPrefix + platform.Ubuntu22_04Image

	// githubReleaseURL is the download URL for the linux/amd64 bb binary,
	// used when box create is run from a non-linux-amd64 host.
	githubReleaseURL = "https://github.com/buildbuddy-io/bazel/releases/download/%s/bazel-%s-linux-x86_64"

	usage = `
usage: bb box create [options] [name]

Starts an SSH server inside a remote Firecracker VM. Once the VM is
running, prints the command to connect via SSH.

If a name is given, the runner is recycled after the session ends so
that the next invocation resumes the same VM. Without a name the VM
is ephemeral.

`
)

var (
	createFlags = flag.NewFlagSet("box create", flag.ContinueOnError)

	imageFlag   = createFlags.String("image", defaultImage, "Container image for the VM")
	gracePeriod = createFlags.Duration("grace_period", 1*time.Minute, "How long the VM stays alive after all SSH connections close (max 5m)")
	idleTimeout = createFlags.Duration("idle_timeout", 5*time.Minute, "Close idle SSH sessions after this duration of inactivity (max 5m)")

	targetFlag  = createFlags.String("remote_executor", login.DefaultApiTarget, "Remote executor gRPC target")
	gatewayFlag = createFlags.String("gateway", "grpcs://gateway.buildbuddy.io", "Gateway gRPC target")
	apiKeyFlag  = createFlags.String("api_key", "", "Override the API key")
)

func HandleBox(args []string) (int, error) {
	if len(args) == 0 || args[0] == "help" {
		log.Print(usage)
		createFlags.SetOutput(os.Stderr)
		createFlags.PrintDefaults()
		return 1, nil
	}
	switch args[0] {
	case "create":
		return handleCreate(args[1:])
	default:
		log.Printf("unknown box subcommand %q", args[0])
		log.Print(usage)
		createFlags.SetOutput(os.Stderr)
		createFlags.PrintDefaults()
		return 1, nil
	}
}

func handleCreate(args []string) (int, error) {
	if err := arg.ParseFlagSet(createFlags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			createFlags.SetOutput(os.Stderr)
			createFlags.PrintDefaults()
			return 1, nil
		}
		return -1, err
	}
	// Cap grace period and idle timeout.
	if *gracePeriod > maxGracePeriod {
		*gracePeriod = maxGracePeriod
	}
	if *idleTimeout > maxIdleTimeout {
		*idleTimeout = maxIdleTimeout
	}

	// Determine the name. If the user didn't provide one, the VM will
	// be ephemeral (no runner recycling).
	var boxName string
	if positional := createFlags.Args(); len(positional) > 0 {
		boxName = positional[0]
	}
	recycleable := boxName != ""

	// Resolve API key.
	key := *apiKeyFlag
	if key == "" {
		var err error
		key, err = login.GetAPIKey()
		if err != nil {
			return -1, fmt.Errorf("getting API key: %w", err)
		}
	}

	// Get a linux/amd64 bb binary to use as the action input.
	bbPath, cleanupBB, err := getBBBinary()
	if err != nil {
		return -1, fmt.Errorf("getting bb binary: %w", err)
	}
	defer cleanupBB()

	// Build the input root: a temp directory containing just the bb binary.
	inputDir, err := os.MkdirTemp("", "bb-box-input-*")
	if err != nil {
		return -1, err
	}
	defer os.RemoveAll(inputDir)

	if err := copyFile(bbPath, filepath.Join(inputDir, "bb"), 0755); err != nil {
		return -1, fmt.Errorf("staging bb binary: %w", err)
	}

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", key)

	conn, err := grpc_client.DialSimple(*targetFlag)
	if err != nil {
		return -1, fmt.Errorf("dialing executor: %w", err)
	}
	defer conn.Close()

	env := real_environment.NewBatchEnv()
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))

	// Build platform exec properties.
	execProps := []string{
		platform.WorkloadIsolationPropertyName + "=firecracker",
		"network=external",
		"container-image=" + ensureDockerPrefix(*imageFlag),
		platform.DockerUserPropertyName + "=buildbuddy",
	}
	if recycleable {
		execProps = append(execProps,
			"recycle-runner=true",
			platform.RunnerRecyclingKey+"="+boxName,
		)
	}
	plat, err := rexec.MakePlatform(execProps...)
	if err != nil {
		return -1, fmt.Errorf("building platform: %w", err)
	}

	// Pre-generate the invocation ID so that bb record inside the VM
	// publishes to a known invocation that box create can poll.
	iid := uuid.New()
	log.Printf("Box: https://app.buildbuddy.io/invocation/%s", iid)

	cmdArgs := []string{
		"./bb", "record",
		"--invocation_id=" + iid,
		"--bes_backend=" + *targetFlag,
		"./bb", "ssh-server",
		"--gateway=" + *gatewayFlag,
		fmt.Sprintf("--grace_period=%s", gracePeriod.String()),
		fmt.Sprintf("--idle_timeout=%s", idleTimeout.String()),
	}
	if boxName != "" {
		cmdArgs = append(cmdArgs, boxName)
	}

	cmd := &repb.Command{
		Arguments: cmdArgs,
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			// Pass the API key via env so it doesn't appear in the
			// command-line arguments shown in the BuildBuddy UI.
			{Name: "BUILDBUDDY_API_KEY", Value: key},
			{Name: "HOME", Value: "/home/buildbuddy"},
			{Name: "USER", Value: "buildbuddy"},
		},
		Platform: plat,
	}
	action := &repb.Action{
		DoNotCache: true,
		Timeout:    durationpb.New(actionTimeout),
	}

	arn, err := rexec.Prepare(ctx, env, "bb-cli-box", digestFunction, action, cmd, inputDir)
	if err != nil {
		return -1, fmt.Errorf("preparing action: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := rexec.Start(ctx, env, arn, rexec.WithSkipCacheLookup(true))
	if err != nil {
		return -1, fmt.Errorf("starting action: %w", err)
	}

	// Watch the operation stream for failures and cancel the context so the
	// BES poll below unblocks immediately.
	streamErrCh := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				streamErrCh <- fmt.Errorf("executor: %w", err)
				cancel()
				return
			}
			if msg.Err != nil {
				streamErrCh <- fmt.Errorf("VM failed to start: %w", msg.Err)
				cancel()
				return
			}
			if msg.Done {
				result := msg.ExecuteResponse.GetResult()
				exitCode := result.GetExitCode()
				errMsg := fmt.Sprintf("VM exited before becoming ready (exit code %d)", exitCode)
				streamErrCh <- fmt.Errorf("%s", errMsg)
				cancel()
				return
			}
		}
	}()

	// Poll the BES event log until the ssh-server writes its READY line.
	log.Printf("Waiting for VM to start...")
	bbClient := bbspb.NewBuildBuddyServiceClient(conn)
	type readyResult struct {
		*url.URL
		err error
	}
	readyCh := make(chan readyResult, 1)
	go func() {
		u, err := waitForReady(ctx, bbClient, iid)
		readyCh <- readyResult{u, err}
	}()

	select {
	case err := <-streamErrCh:
		return -1, err
	case r := <-readyCh:
		if r.err != nil {
			// Prefer the stream error if the context was cancelled by it.
			select {
			case err := <-streamErrCh:
				return -1, err
			default:
				return -1, r.err
			}
		}
		nameOrIP := r.Query().Get("name")
		if nameOrIP == "" {
			nameOrIP = r.Hostname()
		}

		fmt.Printf("Box %q is ready.\n", nameOrIP)
		fmt.Printf("  URL:     %s\n", r.URL)
		fmt.Printf("  Connect: bb ssh %s\n", nameOrIP)
		return 0, nil
	}
}

// waitForReady polls GetEventLogChunk (BUILD_LOG) for the given invocation ID
// until bb ssh-server writes a "READY bb-ssh://..." line to stdout (which bb
// record streams as a BES Progress event), then returns the parsed URL.
func waitForReady(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, iid string) (*url.URL, error) {
	chunkID := ""
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := bbClient.GetEventLogChunk(ctx, &elpb.GetEventLogChunkRequest{
			InvocationId: iid,
			ChunkId:      chunkID,
			Type:         elpb.LogType_BUILD_LOG,
		})
		if err != nil {
			// Invocation likely doesn't exist yet; keep polling.
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if u, ok := parseReadyLine(string(resp.GetBuffer())); ok {
			return u, nil
		}

		nextID := resp.GetNextChunkId()
		if nextID == "" || nextID == chunkID {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		chunkID = nextID
	}
}

// parseReadyLine scans log output for a bb-ssh:// URL written by bb
// ssh-server (e.g. embedded in the "SSH server listening on ..." log line)
// and returns the parsed URL if found.
func parseReadyLine(buf string) (*url.URL, bool) {
	for line := range strings.SplitSeq(buf, "\n") {
		i := strings.Index(line, "bb-ssh://")
		if i < 0 {
			continue
		}
		u, err := url.Parse(strings.TrimSpace(line[i:]))
		if err != nil || u.Host == "" {
			continue
		}
		return u, true
	}
	return nil, false
}

// ensureDockerPrefix prepends "docker://" to an image reference if it isn't
// already present, so that users can pass plain image names like
// "ubuntu:22.04" or "alpine:latest".
func ensureDockerPrefix(image string) string {
	if strings.HasPrefix(image, platform.DockerPrefix) {
		return image
	}
	return platform.DockerPrefix + image
}

// getBBBinary returns the path to a linux/amd64 bb binary along with a
// cleanup function to remove any temp files. On a linux/amd64 host the
// current executable is used directly; on other platforms the release binary
// is downloaded from GitHub.
func getBBBinary() (path string, cleanup func(), err error) {
	noop := func() {}
	if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
		path, err = os.Executable()
		return path, noop, err
	}

	ver := version.String()
	url := fmt.Sprintf(githubReleaseURL, ver, ver)
	log.Printf("Downloading linux/amd64 bb binary (%s)...", ver)

	f, err := os.CreateTemp("", "bb-linux-amd64-*")
	if err != nil {
		return "", noop, err
	}
	cleanup = func() { os.Remove(f.Name()) }

	resp, err := http.Get(url)
	if err != nil {
		cleanup()
		return "", noop, fmt.Errorf("downloading bb: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		cleanup()
		return "", noop, fmt.Errorf("downloading bb from %s: HTTP %d", url, resp.StatusCode)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		cleanup()
		return "", noop, fmt.Errorf("writing bb binary: %w", err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", noop, err
	}
	if err := os.Chmod(f.Name(), 0755); err != nil {
		cleanup()
		return "", noop, err
	}
	return f.Name(), cleanup, nil
}

// copyFile copies src to dst with the given file mode.
func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
