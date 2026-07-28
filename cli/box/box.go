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
	"regexp"
	"runtime"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/error_util"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
	gwsvcpb "github.com/buildbuddy-io/buildbuddy/proto/gateway_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
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

	// remoteInstanceName is the remote instance name for box actions. The
	// "bb-devbox" prefix (snaputil.DevboxPartitionPrefix) opts box VMs into
	// remote snapshot sharing on the executor, so that a named box can be
	// resumed by any executor rather than only the one that last ran it, and
	// routes cache artifacts to the devbox cache partition. The
	// "<prefix>/<name>" form matches the convention used by the other devbox
	// producers (hosted runners, workflows).
	remoteInstanceName = "bb-devbox/box"

	// defaultImage is the container image used for box VMs. This image is
	// pinned by digest and is likely already cached on BuildBuddy executors.
	defaultImage = platform.DockerPrefix + platform.Ubuntu22_04Image

	// githubReleaseURL is the download URL for the linux/amd64 bb binary,
	// used when box create is run from a non-linux-amd64 host.
	githubReleaseURL = "https://github.com/buildbuddy-io/bazel/releases/download/%s/bazel-%s-linux-x86_64"

	usage = `
usage: bb box create [options] [name]
       bb box list [options]

create: Starts an SSH server inside a remote Firecracker VM. Once the VM is
running, prints the command to connect via SSH.

If a name is given, the runner is recycled after the session ends so that the
next invocation resumes the same VM. Without a name the VM is ephemeral.

list: Lists the named boxes currently available for your group.

`
)

// boxNameRE restricts box names to a safe character set: the name is used as
// a runner recycling key, an output path component (so path traversal
// characters must be rejected), and a gateway DNS name.
var boxNameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)

var (
	createFlags = flag.NewFlagSet("box create", flag.ContinueOnError)
	Flags       = createFlags

	imageFlag   = createFlags.String("image", defaultImage, "Container image for the VM")
	gracePeriod = createFlags.Duration("grace_period", 1*time.Minute, "How long the VM stays alive after all SSH connections close (max 5m)")
	idleTimeout = createFlags.Duration("idle_timeout", 5*time.Minute, "Close idle SSH sessions after this duration of inactivity (max 5m)")

	targetFlag              = createFlags.String("remote_executor", login.DefaultApiTarget, "Remote executor gRPC target")
	gatewayFlag, apiKeyFlag = registerGatewayFlags(createFlags)

	listFlags                       = flag.NewFlagSet("box list", flag.ContinueOnError)
	listGatewayFlag, listAPIKeyFlag = registerGatewayFlags(listFlags)
)

// registerGatewayFlags registers the gateway connection flags shared by the box
// subcommands on fs, so their defaults and descriptions are defined once.
func registerGatewayFlags(fs *flag.FlagSet) (gateway, apiKey *string) {
	gateway = fs.String("gateway", "grpcs://gateway.buildbuddy.io", "Gateway gRPC target")
	apiKey = fs.String("api_key", "", "Override the API key")
	return gateway, apiKey
}

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
	case "list":
		return handleList(args[1:])
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
		// The name is used as a runner recycling key, an output path
		// component, and the gateway DNS name, so restrict it to a safe
		// character set.
		if !boxNameRE.MatchString(boxName) {
			log.Printf("Invalid box name %q: names must start with an alphanumeric character and contain only alphanumerics, '_', '.', or '-' (max 64 characters)", boxName)
			return 1, nil
		}
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

	ctx := context.Background()

	// Get a linux/amd64 bb binary to use as the action input.
	bbPath, cleanupBB, err := getBBBinary(ctx)
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
			// Give the executor that last ran this box a head start when
			// scheduling, so that resumes hit its warm local snapshot. (The
			// server caps this via remote_execution.max_scheduling_delay.)
			platform.RunnerRecyclingMaxWaitPropertyName+"=5s",
			// By default, firecracker only saves a snapshot for non-CI
			// actions if none exists yet, which would freeze the box's state
			// at its first session. Always save so that changes made in each
			// session persist to the next one.
			platform.SnapshotSavePolicyPropertyName+"="+platform.AlwaysSaveSnapshot,
			// The always-save policy above only covers the remote snapshot:
			// the local manifest on a warm executor can lag one session
			// behind, and the default read policy prefers it, which would
			// roll the box back a session. Always read the newest (remote)
			// manifest instead. Chunks referenced by it that are already in
			// the executor's filecache are still reused, so most of the
			// warm-start benefit is retained.
			platform.SnapshotReadPolicyPropertyName+"="+platform.AlwaysReadNewestSnapshot,
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
	if recycleable {
		// Declare a stable output path per box name. This activates the
		// scheduler's affinity routing (which routes on the first output
		// path), so that resumes prefer the executor holding the warm local
		// snapshot. The path is never actually produced by the action, which
		// is fine.
		cmd.OutputPaths = []string{"bb-box/" + boxName}
	}
	action := &repb.Action{
		DoNotCache: true,
		Timeout:    durationpb.New(actionTimeout),
	}

	arn, err := rexec.Prepare(ctx, env, remoteInstanceName, digestFunction, action, cmd, inputDir)
	if err != nil {
		return -1, fmt.Errorf("preparing action: %w", err)
	}

	bbClient := bbspb.NewBuildBuddyServiceClient(conn)

	// If a named box's snapshot disappears between the executor's initial
	// existence check and the VM load (e.g. local filecache eviction), the
	// action fails with a SNAPSHOT_NOT_FOUND error. Retry once: on the next
	// attempt the executor re-checks for the snapshot, misses, and does a
	// normal cold boot. The failed attempt never ran the command inside the
	// VM, so the same action and invocation ID can be reused.
	var code int
	r := retry.New(ctx, &retry.Options{MaxRetries: 2})
	for r.Next() {
		code, err = startAndAwaitReady(ctx, env, arn, bbClient, iid)
		if err != nil && error_util.IsSnapshotNotFoundError(err) {
			log.Printf("Box snapshot is no longer available; starting a fresh VM... (%s)", err)
			continue
		}
		break
	}
	return code, err
}

// startAndAwaitReady starts the box action and polls the BES event log until
// bb ssh-server writes its READY line, or the action fails.
func startAndAwaitReady(ctx context.Context, env environment.Env, arn *rspb.ResourceName, bbClient bbspb.BuildBuddyServiceClient, iid string) (int, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := rexec.Start(ctx, env, arn, rexec.WithSkipCacheLookup(true))
	if err != nil {
		return -1, fmt.Errorf("starting action: %w", err)
	}

	// Watch the operation stream for failures and cancel the context so the
	// readiness polls below unblock immediately.
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
				streamErrCh <- fmt.Errorf("VM exited before becoming ready (exit code %d)", exitCode)
				cancel()
				return
			}
		}
	}()

	log.Printf("Waiting for VM to start...")
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

func handleList(args []string) (int, error) {
	if err := arg.ParseFlagSet(listFlags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			listFlags.SetOutput(os.Stderr)
			listFlags.PrintDefaults()
			return 1, nil
		}
		return -1, err
	}

	if *listGatewayFlag == "" {
		log.Printf("A non-empty --gateway must be specified")
		return 1, nil
	}

	// Resolve API key.
	key := *listAPIKeyFlag
	if key == "" {
		var err error
		key, err = login.GetAPIKey()
		if err != nil {
			return -1, fmt.Errorf("getting API key: %w", err)
		}
	}

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", key)

	conn, err := grpc_client.DialSimple(*listGatewayFlag)
	if err != nil {
		return -1, fmt.Errorf("dialing gateway: %w", err)
	}
	defer conn.Close()

	gwClient := gwsvcpb.NewGatewayServiceClient(conn)
	resp, err := gwClient.List(ctx, &gwpb.ListRequest{})
	if err != nil {
		return -1, fmt.Errorf("listing boxes: %w", err)
	}

	peers := resp.GetPeers()
	if len(peers) == 0 {
		fmt.Println("No boxes available.")
		return 0, nil
	}

	rows := make([][]string, 0, len(peers))
	for _, p := range peers {
		rows = append(rows, []string{p.GetName(), p.GetIp()})
	}

	headerStyle := lipgloss.NewStyle().Bold(true).Padding(0, 1)
	cellStyle := lipgloss.NewStyle().Padding(0, 1)
	t := table.New().
		Border(lipgloss.RoundedBorder()).
		BorderStyle(lipgloss.NewStyle().Foreground(lipgloss.Color("240"))).
		Headers("NAME", "ADDRESS").
		Rows(rows...).
		StyleFunc(func(row, col int) lipgloss.Style {
			if row == table.HeaderRow {
				return headerStyle
			}
			return cellStyle
		})
	fmt.Println(t)
	return 0, nil
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
func getBBBinary(ctx context.Context) (path string, cleanup func(), err error) {
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cleanup()
		return "", noop, fmt.Errorf("downloading bb: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
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
