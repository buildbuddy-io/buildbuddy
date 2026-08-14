// Package box implements the "bb box" command for starting remote Firecracker
// VMs with an SSH server.
package box

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/ssh"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/error_util"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	petname "github.com/dustinkirkland/golang-petname"
	"golang.org/x/term"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
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
	// used when bb box is run from a non-linux-amd64 host.
	githubReleaseURL = "https://github.com/buildbuddy-io/bazel/releases/download/%s/bazel-%s-linux-x86_64"

	usage = `
usage: bb box [options] [name] [command...]
       bb box list [options]

Starts a remote Firecracker VM running an SSH server and opens a session in
it. Anything after the name is run in the box instead of a login shell.

Boxes are recycled after the session ends, so running bb box with the same
name again resumes the same VM, and a box that is still running is connected
to rather than started again. A box with no name given is assigned one.

Passing --run_from_snapshot resumes the VM from an existing snapshot.
The snapshot key is a JSON object that can be obtained from the "Execution metadata"
section of a previous invocation's Execution page.

list: Lists your group's connected peers: boxes (with their names) and
transient clients like bb ssh sessions (identified by session ID only).
`
)

// boxNameRE restricts box names to a safe character set: the name is used as
// a runner recycling key, an output path component (so path traversal
// characters must be rejected), and a gateway DNS name.
var boxNameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)

var (
	createFlags = flag.NewFlagSet("box", flag.ContinueOnError)
	Flags       = createFlags

	imageFlag   = createFlags.String("image", defaultImage, "Container image for the VM")
	gracePeriod = createFlags.Duration("grace_period", 1*time.Minute, "How long the VM stays alive after all SSH connections close (max 5m)")
	idleTimeout = createFlags.Duration("idle_timeout", 5*time.Minute, "Log out interactive SSH sessions after this duration without user input (max 5m)")
	trace       = createFlags.Bool("trace", false, "Force server-side tracing for this box's execution and print the execution ID")

	detach    = createFlags.Bool("detach", false, "Print the connect command instead of opening a session (the default when stdin is not a terminal)")
	forceTTY  = createFlags.Bool("t", false, "Force pseudo-terminal allocation for a command run in the box")
	noCommand = createFlags.Bool("N", false, "Do not run a command in the box; useful when only forwarding ports")

	localForwards  = flag.New(createFlags, "L", []string{}, "Forward a port on this machine to one reachable from the box: [bind:]port:host:hostport (repeatable)")
	remoteForwards = flag.New(createFlags, "R", []string{}, "Forward a port on the box to one reachable from this machine: [bind:]port:host:hostport (repeatable)")

	// From a shell, pass the JSON in single quotes.
	// Ex. --run_from_snapshot='{"snapshotId":"XXX","instanceName":""}'
	runFromSnapshot = createFlags.String("run_from_snapshot", "", "JSON for a snapshot key that the remote runner should be resumed from.")
	runnerExecProps = flag.New(createFlags, "runner_exec_properties", []string{}, "Exec properties that apply to the remote runner. Key-value pairs should be separated by '=' (for example, --runner_exec_properties=Pool=my-pool). Can be specified more than once.")

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

// reservedNames are the `bb box` subcommands, which therefore cannot be used
// as box names: `bb box list` has to mean "list boxes", not "attach to the box
// called list".
var reservedNames = map[string]bool{"create": true, "list": true, "help": true}

func HandleBox(args []string) (int, error) {
	// `bb box [flags] [name] [command...]` is the primary form; the
	// subcommands are recognized only as the first argument, so flags must
	// precede the name (as with ssh's `ssh [options] destination [command]`).
	if len(args) > 0 {
		switch args[0] {
		case "help":
			log.Print(usage)
			createFlags.SetOutput(os.Stderr)
			createFlags.PrintDefaults()
			return 1, nil
		case "create":
			return handleCreate(args[1:])
		case "list":
			return handleList(args[1:])
		}
	}
	return handleCreate(args)
}

func handleCreate(args []string) (int, error) {
	// Plain Parse (rather than arg.ParseFlagSet) so parsing stops at the box
	// name: everything after it belongs to the remote command, including its
	// own flags.
	createFlags.SetOutput(io.Discard)
	if err := createFlags.Parse(args); err != nil {
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

	// If a specific snapshot key is provided, check that it's valid JSON.
	if *runFromSnapshot != "" {
		if err := protojson.Unmarshal([]byte(*runFromSnapshot), &fcpb.SnapshotKey{}); err != nil {
			return -1, fmt.Errorf("parsing --run_from_snapshot: %w", err)
		}
	}

	// Anything after the name is a command to run in the box instead of a
	// shell. A box with no name given is assigned a generated one below.
	var boxName, remoteCmd string
	if positional := createFlags.Args(); len(positional) > 0 {
		boxName = positional[0]
		// The name is used as a runner recycling key, an output path
		// component, and the gateway DNS name, so restrict it to a safe
		// character set.
		if !boxNameRE.MatchString(boxName) {
			log.Printf("Invalid box name %q: names must start with an alphanumeric character and contain only alphanumerics, '_', '.', or '-' (max 64 characters)", boxName)
			return 1, nil
		}
		if reservedNames[boxName] {
			log.Printf("Invalid box name %q: it names a `bb box` subcommand", boxName)
			return 1, nil
		}
		remoteCmd = ssh.JoinRemoteCommand(positional[1:])
	}

	if err := checkDetachConflicts(remoteCmd); err != nil {
		log.Print(err.Error())
		return 1, nil
	}

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
	gwCtx := metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", key)

	// The gateway is dialed up front for readiness detection below, and to
	// check whether this box is already running.
	gwConn, err := grpc_client.DialSimple(*gatewayFlag)
	if err != nil {
		return -1, fmt.Errorf("dialing gateway: %w", err)
	}
	defer gwConn.Close()
	gwClient := gwsvcpb.NewGatewayServiceClient(gwConn)

	peers, err := listPeers(gwCtx, gwClient)
	if err != nil {
		return -1, fmt.Errorf("listing boxes: %w", err)
	}
	if boxName == "" {
		boxName = generateName(peers)
		log.Printf("Starting box %q", boxName)
	} else if p := findPeer(peers, boxName); p != nil {
		// A box registers with the gateway for as long as it is running, so a
		// name found here is live: attach to it rather than starting a second
		// VM (which would fail on the duplicate name anyway).
		return attach(p, key, remoteCmd)
	}

	// Get a linux/amd64 bb binary to use as the action input.
	bbPath, cleanupBB, err := getBBBinary(ctx)
	if err != nil {
		return -1, fmt.Errorf("getting bb binary: %w", err)
	}
	defer cleanupBB()

	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", key)
	if *trace {
		// Force server-side OTel sampling for every RPC in this session,
		// including the remote execution itself.
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-trace", "force")
	}

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
		platform.AllowRemoteSnapshotsPropertyName + "=true",
	}
	execProps = append(execProps,
		"recycle-runner=true",
		platform.RunnerRecyclingKey+"="+boxName,
		// Give the executor that last ran this box a head start when
		// scheduling, so that resumes hit its warm local snapshot. (The
		// server caps this via remote_execution.max_scheduling_delay.)
		platform.RunnerRecyclingMaxWaitPropertyName+"=5s",
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
	if *runFromSnapshot != "" {
		execProps = append(execProps, platform.SnapshotKeyOverridePropertyName+"="+*runFromSnapshot)
	}
	// Explicitly supplied runner exec properties override the command's defaults.
	execProps = append(execProps, (*runnerExecProps)...)
	plat, err := rexec.MakePlatform(execProps...)
	if err != nil {
		return -1, fmt.Errorf("building platform: %w", err)
	}

	// Pre-generate the invocation ID so that bb record inside the VM
	// publishes to a known invocation that bb box can poll.
	iid := uuid.New()
	log.Printf("Box: https://app.buildbuddy.io/invocation/%s", iid)

	ctx, err = bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
		ToolInvocationId: iid,
		ActionMnemonic:   "BuildBuddyBox",
	})
	if err != nil {
		return 0, status.WrapError(err, "add request metadata to ctx")
	}

	cmdArgs := []string{
		"./bb", "record",
		"--invocation_id=" + iid,
		"--bes_backend=" + *targetFlag,
		"./bb", "ssh-server",
		"--gateway=" + *gatewayFlag,
		// Use the invocation ID as the gateway session ID so that the
		// gateway's peer listing can be correlated with the action running
		// the VM.
		"--session_id=" + iid,
		fmt.Sprintf("--grace_period=%s", gracePeriod.String()),
		fmt.Sprintf("--idle_timeout=%s", idleTimeout.String()),
	}
	cmdArgs = append(cmdArgs, boxName)

	cmd := &repb.Command{
		Arguments: cmdArgs,
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			// Pass the API key via env so it doesn't appear in the
			// command-line arguments shown in the BuildBuddy UI.
			{Name: "BUILDBUDDY_API_KEY", Value: key},
			{Name: "HOME", Value: "/home/buildbuddy"},
			{Name: "USER", Value: "buildbuddy"},
			// The action is given no PATH, leaving processes that don't
			// supply their own default (anything but a shell) unable to find
			// system binaries.
			{Name: "PATH", Value: "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			// Tells bb ssh-server that it is running inside a remote
			// action, where writing executor marker files (e.g. the
			// do-not-recycle marker) to the working directory is
			// meaningful.
			{Name: "BUILDBUDDY_REMOTE_ACTION", Value: "1"},
		},
		Platform: plat,
	}
	// Declare a stable output path per box name. This activates the
	// scheduler's affinity routing (which routes on the first output path), so
	// that resumes prefer the executor holding the warm local snapshot. The
	// path is never actually produced by the action, which is fine.
	cmd.OutputPaths = []string{"bb-box/" + boxName}

	// Build the input root — just the bb binary — from its real path, rather
	// than staging a copy in a temp dir.
	inputRootDigest, err := uploadInputRoot(ctx, env, bbPath)
	if err != nil {
		return -1, fmt.Errorf("uploading bb binary: %w", err)
	}
	action := &repb.Action{
		DoNotCache:      true,
		Timeout:         durationpb.New(actionTimeout),
		InputRootDigest: inputRootDigest,
	}

	arn, err := rexec.Prepare(ctx, env, remoteInstanceName, digestFunction, action, cmd, "" /*=inputRootDir*/)
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
	var peer *gwpb.Peer
	r := retry.New(ctx, &retry.Options{MaxRetries: 2})
	for r.Next() {
		peer, err = startAndAwaitReady(ctx, env, arn, bbClient, gwClient, iid)
		if err != nil && error_util.IsSnapshotNotFoundError(err) {
			log.Printf("Box snapshot is no longer available; starting a fresh VM... (%s)", err)
			continue
		}
		break
	}
	if err != nil {
		return -1, err
	}
	if peer == nil {
		return -1, fmt.Errorf("box never became ready")
	}

	return attach(peer, key, remoteCmd)
}

// attach connects to a ready box, or prints how to connect if this process
// can't usefully hold an interactive session.
func attach(peer *gwpb.Peer, key, remoteCmd string) (int, error) {
	nameOrIP := peer.GetName()
	if nameOrIP == "" {
		nameOrIP = peer.GetIp()
	}
	// Attaching an interactive shell only makes sense with a terminal, so
	// without one behave as --detach and print how to connect.
	if *detach || (remoteCmd == "" && !*noCommand && !term.IsTerminal(int(os.Stdin.Fd()))) {
		fmt.Printf("Box %q is ready.\n", nameOrIP)
		fmt.Printf("  Address: %s\n", peer.GetIp())
		fmt.Printf("  Connect: bb ssh %s\n", nameOrIP)
		return 0, nil
	}
	// The caller's context carries the API key; ssh.Run adds its own, so give
	// it a clean context.
	return ssh.Run(context.Background(), ssh.Options{
		Gateway:        *gatewayFlag,
		APIKey:         key,
		Host:           nameOrIP,
		Command:        remoteCmd,
		ForceTTY:       *forceTTY,
		NoCommand:      *noCommand,
		LocalForwards:  *localForwards,
		RemoteForwards: *remoteForwards,
	})
}

// listPeers returns the gateway's registrations for this group: running boxes
// and transient clients.
func listPeers(ctx context.Context, gwClient gwsvcpb.GatewayServiceClient) ([]*gwpb.Peer, error) {
	rsp, err := gwClient.List(ctx, &gwpb.ListRequest{})
	if err != nil {
		return nil, err
	}
	return rsp.GetPeers(), nil
}

func findPeer(peers []*gwpb.Peer, name string) *gwpb.Peer {
	for _, p := range peers {
		if p.GetName() == name {
			return p
		}
	}
	return nil
}

// generateName names an unnamed box after a pet, avoiding the names of boxes
// that are currently running.
func generateName(peers []*gwpb.Peer) string {
	taken := make(map[string]bool, len(peers))
	for _, p := range peers {
		taken[p.GetName()] = true
	}
	for i := 0; i < 10; i++ {
		if name := petname.Generate(3, "-"); !taken[name] {
			return name
		}
	}
	return petname.Generate(4, "-")
}

// checkDetachConflicts reports flag combinations that --detach cannot honor.
func checkDetachConflicts(remoteCmd string) error {
	if !*detach {
		return nil
	}
	// Forwards live in this process, so they cannot outlive it.
	if len(*localForwards) > 0 || len(*remoteForwards) > 0 {
		return fmt.Errorf("--detach cannot be combined with -L/-R: the forwards would close as this command exits (use -N to hold them open)")
	}
	// Detaching returns as soon as the box is ready, so a command would never
	// run — and its exit code would be reported as success.
	if remoteCmd != "" {
		return fmt.Errorf("--detach cannot be combined with a command: the command would never run (start the box detached, then `bb box %s`)", "<name> <command>")
	}
	return nil
}

// startAndAwaitReady starts the box action and polls the gateway until the
// VM's peer registration appears (matched by session ID, which is the
// invocation ID), or the action fails.
func startAndAwaitReady(ctx context.Context, env environment.Env, arn *rspb.ResourceName, bbClient bbspb.BuildBuddyServiceClient, gwClient gwsvcpb.GatewayServiceClient, iid string) (*gwpb.Peer, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := rexec.Start(ctx, env, arn, rexec.WithSkipCacheLookup(true))
	if err != nil {
		return nil, fmt.Errorf("starting action: %w", err)
	}

	// Watch the operation stream for failures and cancel the context so the
	// readiness polls below unblock immediately.
	streamErrCh := make(chan error, 1)
	var logExecutionID sync.Once
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				streamErrCh <- fmt.Errorf("executor: %w", err)
				cancel()
				return
			}
			if name := msg.GetName(); name != "" {
				logExecutionID.Do(func() {
					if *trace {
						log.Printf("Execution: %s", name)
					} else {
						log.Debugf("Execution: %s", name)
					}
				})
			}
			if msg.Err != nil {
				streamErrCh <- fmt.Errorf("VM failed to start: %w", msg.Err)
				cancel()
				return
			}
			if msg.Done {
				result := msg.ExecuteResponse.GetResult()
				exitCode := result.GetExitCode()
				// Surface the VM's own output (e.g. "peer name already in
				// use") rather than just the exit code.
				if tail := fetchLogTail(ctx, bbClient, iid); tail != "" {
					streamErrCh <- fmt.Errorf("VM exited before becoming ready (exit code %d):\n%s", exitCode, tail)
				} else {
					streamErrCh <- fmt.Errorf("VM exited before becoming ready (exit code %d)", exitCode)
				}
				cancel()
				return
			}
		}
	}()

	log.Printf("Waiting for VM to start...")
	readyCh := make(chan *gwpb.Peer, 1)
	go func() {
		if p, err := waitForPeer(ctx, gwClient, iid); err == nil {
			readyCh <- p
		}
		// On error the context was canceled; the stream watcher above
		// reports the underlying failure.
	}()

	select {
	case err := <-streamErrCh:
		return nil, err
	case p := <-readyCh:
		return p, nil
	}
}

func waitForPeer(ctx context.Context, gwClient gwsvcpb.GatewayServiceClient, sessionID string) (*gwpb.Peer, error) {
	stream, err := gwClient.Watch(ctx, &gwpb.WatchRequest{SessionId: sessionID})
	if err != nil {
		return nil, err
	}
	for {
		rsp, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		// Require a completed WireGuard handshake, not just a gateway
		// registration: the VM registers before it validates its tunnel, and
		// a box with a dark tunnel will exit shortly afterwards (see
		// ssh-server's --wg_health_timeout).
		if p := rsp.GetPeer(); p.GetLastHandshakeTime() != nil {
			return p, nil
		}
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
		// A live peer re-handshakes every couple of minutes, so "never" or
		// an old value here means the peer's tunnel is dark even though it
		// is still registered.
		lastHandshake := "never"
		if t := p.GetLastHandshakeTime(); t != nil {
			lastHandshake = fmt.Sprintf("%s ago", time.Since(t.AsTime()).Truncate(time.Second))
		}
		rows = append(rows, []string{p.GetName(), p.GetIp(), p.GetSessionId(), lastHandshake})
	}

	cellStyle := lipgloss.NewStyle().Padding(0, 2, 0, 0)
	t := table.New().
		BorderTop(false).
		BorderBottom(false).
		BorderLeft(false).
		BorderRight(false).
		BorderColumn(false).
		BorderHeader(false).
		Headers("NAME", "ADDRESS", "SESSION", "LAST HANDSHAKE").
		Rows(rows...).
		StyleFunc(func(_, _ int) lipgloss.Style {
			return cellStyle
		})
	fmt.Println(t)
	return 0, nil
}

// fetchLogTail returns the last few lines of the invocation's build log, so
// that the VM's failure output can be shown in the user's terminal when the
// action exits before becoming ready. Returns "" if no log output could be
// fetched. Retries briefly since the action's final log lines may still be
// in flight to BES when the execution completes.
func fetchLogTail(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, iid string) string {
	for attempt := 0; attempt < 3; attempt++ {
		resp, err := bbClient.GetEventLogChunk(ctx, &elpb.GetEventLogChunkRequest{
			InvocationId: iid,
			// Empty ChunkId fetches the last chunk.
			MinLines: 10,
			Type:     elpb.LogType_BUILD_LOG,
		})
		if err == nil && len(resp.GetBuffer()) > 0 {
			return strings.TrimSpace(string(resp.GetBuffer()))
		}
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(500 * time.Millisecond):
		}
	}
	return ""
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

// uploadInputRoot uploads the bb binary at the given path and returns a digest of an
// input root containing it, executable, as "bb".
func uploadInputRoot(ctx context.Context, env environment.Env, path string) (*repb.Digest, error) {
	ul := cachetools.NewBatchCASUploader(ctx, env, remoteInstanceName, digestFunction, nil /*=chunkingParams*/)
	d, err := ul.UploadFile(path)
	if err != nil {
		return nil, err
	}
	root, err := ul.UploadProto(&repb.Directory{
		Files: []*repb.FileNode{{Name: "bb", Digest: d, IsExecutable: true}},
	})
	if err != nil {
		return nil, err
	}
	if err := ul.Wait(); err != nil {
		return nil, err
	}
	return root, nil
}
