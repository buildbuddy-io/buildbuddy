// Package ssh_server starts an SSH server with userspace networking.
package ssh_server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/buildbuddy-io/buildbuddy/server/util/wgkeys"
	"github.com/creack/pty"
	"github.com/gliderlabs/ssh"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
	"google.golang.org/grpc/metadata"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
	gwsvcpb "github.com/buildbuddy-io/buildbuddy/proto/gateway_service"
	gossh "golang.org/x/crypto/ssh"
)

var (
	flags = flag.NewFlagSet("ssh_server", flag.ContinueOnError)
	Flags = flags

	gateway     = flags.String("gateway", "grpcs://gateway.buildbuddy.io", "Gateway gRPC target")
	network     = flags.String("network", "", "Network name (default is blank)")
	apiKey      = flags.String("api_key", "", "Optionally override the API key with this value")
	gracePeriod = flags.Duration("grace_period", 1*time.Minute, "How long the VM will remain alive when no users are connected")
	idleTimeout = flags.Duration("idle_timeout", 5*time.Minute, "Log out interactive sessions after this duration without user input")

	sshPort     = flags.Int("port", 22, "SSH listen port on the tunnel interface")
	shellPath   = flags.String("shell", "", "Shell to use for interactive sessions (auto-detected if unset)")
	hostKeyFile = flags.String("host_key_file", "", "SSH host private key file (generates an ephemeral key if empty)")
	sessionID   = flags.String("session_id", "", "Unique identifier for this gateway connection, shown in gateway listings (generated if empty)")

	// The default covers two handshake attempts: wireguard-go retransmits an
	// unanswered handshake initiation after 5s (the protocol's REKEY_TIMEOUT,
	// plus jitter), so a smaller value would tolerate zero packet loss.
	wgHealthTimeout = flags.Duration("wg_health_timeout", 12*time.Second, "Exit if the WireGuard tunnel has not completed a handshake with the gateway within this duration after coming up. 0 disables the check.")

	usage string
)

// waitForHandshake polls the WireGuard device until its peer (the gateway)
// completes a handshake, or the timeout elapses.
func waitForHandshake(dev *device.Device, timeout time.Duration) error {
	var lastErr error
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cfg, err := dev.IpcGet()
		if err != nil {
			lastErr = err
		} else {
			for line := range strings.SplitSeq(cfg, "\n") {
				if v, ok := strings.CutPrefix(line, "last_handshake_time_sec="); ok && v != "0" {
					return nil
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return status.UnavailableErrorf("no WireGuard handshake with the gateway within %s (last device error: %s)", timeout, lastErr)
	}
	return status.UnavailableErrorf("no WireGuard handshake with the gateway within %s", timeout)
}

// remoteActionEnvVar signals that this process is running inside a remote
// action. It is set by `bb box` on the action's environment (and
// inherited through bb record).
const remoteActionEnvVar = "BUILDBUDDY_REMOTE_ACTION"

// doNotRecycleMarkerFile, when present in the workspace root, tells the
// executor not to recycle the runner or save a VM snapshot for it. Mirrors
// the constant in enterprise/server/remote_execution/runner.
const doNotRecycleMarkerFile = ".BUILDBUDDY_DO_NOT_RECYCLE"

// writeDoNotRecycleMarker writes the executor's do-not-recycle marker to the
// current working directory — which the executor guarantees is the workspace
// root for remote actions — so the VM is neither recycled nor snapshotted.
// No-op when running outside a remote action.
func writeDoNotRecycleMarker() {
	if os.Getenv(remoteActionEnvVar) == "" {
		return
	}
	// Log the absolute path so a violated workspace-root assumption is
	// visible in the invocation log.
	path, _ := filepath.Abs(doNotRecycleMarkerFile)
	if err := os.WriteFile(doNotRecycleMarkerFile, nil, 0644); err != nil {
		log.Warnf("write %s: %v", path, err)
	} else {
		log.Printf("Wrote %s", path)
	}
}

func init() {
	var buf strings.Builder
	fmt.Fprintf(&buf, "usage: bb %s [flags] [name]\n\nRun an SSH server on a user-mode wireguard network connected to\nthe gateway server.\n\nFlags:\n", flags.Name())
	flags.SetOutput(&buf)
	flags.PrintDefaults()
	usage = buf.String()
}

// setHostname renames the VM after the box so shell prompts and logs
// identify it. No-op outside a remote action: this command also runs on
// developer machines, which must not be renamed. The box image runs as a
// non-root user with passwordless sudo. Best effort: failures are cosmetic.
func setHostname(name string) {
	if name == "" || os.Getenv(remoteActionEnvVar) == "" {
		return
	}
	// `bb box` validates the name, but this command can also be run
	// directly, and the name reaches both sethostname(2) and an /etc/hosts
	// line.
	if len(name) > 64 || strings.ContainsAny(name, " \t\n#") {
		log.Debugf("not renaming to invalid hostname %q", name)
		return
	}
	if h, err := os.Hostname(); err == nil && h == name {
		return // already set, e.g. on a resumed VM
	}
	if out, err := exec.Command("sudo", "-n", "hostname", name).CombinedOutput(); err != nil {
		log.Debugf("set hostname to %s: %v: %s", name, err, out)
		return
	}
	// Keep the new name resolvable, which sudo warns about otherwise. Only
	// after the rename succeeded, so a failure leaves no stray entry. The
	// leading newline covers an /etc/hosts written without a trailing one.
	hosts := exec.Command("sudo", "-n", "tee", "-a", "/etc/hosts")
	hosts.Stdin = strings.NewReader(fmt.Sprintf("\n127.0.0.1 %s\n", name))
	if out, err := hosts.CombinedOutput(); err != nil {
		log.Debugf("add %s to /etc/hosts: %v: %s", name, err, out)
	}
}

func getShell() string {
	if *shellPath != "" {
		return *shellPath
	}
	if s := os.Getenv("SHELL"); s != "" {
		return s
	}
	if _, err := os.Stat("/bin/bash"); err == nil {
		return "/bin/bash"
	}
	return "/bin/sh"
}

// resolveEndpoint resolves the hostname in a host:port endpoint string to an
// IP address. WireGuard's IPC parser requires an IP address, not a hostname.
func resolveEndpoint(endpoint string) (string, error) {
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", err
	}
	if net.ParseIP(host) != nil {
		return endpoint, nil
	}
	addrs, err := net.LookupHost(host)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(addrs[0], port), nil
}

// loadOrCreateHostKey returns the PEM-encoded ed25519 host key at path,
// generating and persisting a new one if the file does not yet exist.
// Reusing the same key across restarts prevents SSH clients from seeing a
// host-key-changed warning when reconnecting to a resumed VM.
func loadOrCreateHostKey(path string) ([]byte, error) {
	if data, err := os.ReadFile(path); err == nil {
		return data, nil
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	block, err := gossh.MarshalPrivateKey(privateKey, "")
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(block)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, pemBytes, 0600); err != nil {
		return nil, err
	}
	return pemBytes, nil
}

// connIdleTimeout closes connections whose client has stopped sending
// traffic entirely — bb ssh sends keepalives every 15s (keepaliveInterval in
// cli/ssh), so only a dead client (host asleep, network gone) goes quiet
// this long. Clients that don't send keepalives (older bb versions, plain
// ssh) stay connected only as long as some traffic flows within this
// window: if they sit completely silent, they are dropped here at 3m and
// never reach the (5m default) --idle_timeout logout banner. Distinct from
// --idle_timeout, which logs out live-but-inactive interactive sessions.
const connIdleTimeout = 3 * time.Minute

// countedConn runs onClose exactly once, when the connection is closed.
type countedConn struct {
	net.Conn
	once    sync.Once
	onClose func()
}

func (c *countedConn) Close() error {
	c.once.Do(c.onClose)
	return c.Conn.Close()
}

// activityReader resets an inactivity timer on every read that carries data.
type activityReader struct {
	r     io.Reader
	reset func()
}

func (a *activityReader) Read(p []byte) (int, error) {
	n, err := a.r.Read(p)
	if n > 0 {
		a.reset()
	}
	return n, err
}

// killGroup kills cmd's process group. Process.Kill fails once cmd.Wait has
// reaped, and an unreaped pid can't be recycled, so it guards the group kill
// against signaling an unrelated group.
func killGroup(cmd *exec.Cmd) {
	if err := cmd.Process.Kill(); err != nil {
		return
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		log.Debugf("logout kill: %v", err)
	}
}

func setWinsize(f *os.File, w, h int) {
	syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), uintptr(syscall.TIOCSWINSZ),
		uintptr(unsafe.Pointer(&struct{ h, w, x, y uint16 }{uint16(h), uint16(w), 0, 0})))
}

func handleSession(s ssh.Session) {
	ptyReq, winCh, isPty := s.Pty()
	raw := s.RawCommand()

	// Remote commands run through the shell (like sshd's `$SHELL -c`) so
	// quoting, pipes, and expansions behave as users expect; the raw command
	// string is used because ssh.Session.Command() pre-splits it. With no
	// command, start a login shell.
	var cmd *exec.Cmd
	if raw != "" {
		log.Printf("SSH exec: user=%s remote=%s pty=%v cmd=%q", s.User(), s.RemoteAddr(), isPty, raw)
		cmd = exec.Command(getShell(), "-c", raw)
	} else {
		log.Printf("SSH session opened: user=%s remote=%s pty=%v", s.User(), s.RemoteAddr(), isPty)
		cmd = exec.Command(getShell(), "-l")
	}
	defer log.Printf("SSH session closed: user=%s remote=%s", s.User(), s.RemoteAddr())

	// Bound Wait when a grandchild inherits the non-pty stdout/stderr pipes
	// and outlives the child.
	cmd.WaitDelay = 5 * time.Second

	// Interactive sessions (a PTY, or a shell with no command) are logged
	// out after --idle_timeout without user input; program output and client
	// keepalives don't count, and plain command execution is exempt.
	// cmd.Process.Kill is a no-op once cmd.Wait has reaped, so a logout
	// racing the command's own exit can't signal an unrelated process.
	var logout *time.Timer
	if isPty {
		cmd.Env = append(os.Environ(), "TERM="+ptyReq.Term)
		f, err := pty.Start(cmd)
		if err != nil {
			fmt.Fprintf(s.Stderr(), "start command: %v\n", err)
			s.Exit(1)
			return
		}
		defer f.Close()

		var input io.Reader = s
		if *idleTimeout > 0 {
			logout = time.AfterFunc(*idleTimeout, func() {
				fmt.Fprintf(s, "\r\nLogged out: no input for %s.\r\n", *idleTimeout)
				// Closing the pty unblocks the copy below; killing the group
				// takes the shell's children with it. A tmux server survives:
				// it daemonizes into its own session.
				f.Close()
				killGroup(cmd)
			})
			defer logout.Stop()
			input = &activityReader{r: s, reset: func() { logout.Reset(*idleTimeout) }}
		}
		setWinsize(f, ptyReq.Window.Width, ptyReq.Window.Height)
		go func() {
			for win := range winCh {
				if logout != nil {
					logout.Reset(*idleTimeout)
				}
				setWinsize(f, win.Width, win.Height)
			}
		}()
		go io.Copy(f, input)
		io.Copy(s, f)
	} else {
		wantLogout := *idleTimeout > 0 && raw == ""
		cmd.Env = os.Environ()
		cmd.Stdout = s
		cmd.Stderr = s.Stderr()
		if wantLogout {
			// Without a pty there is no Setsid; give the shell its own group
			// so the logout kill reaches its children and not the server.
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		}
		// Pump stdin through a real pipe ourselves: handing the session to
		// os/exec would add a copier goroutine that cmd.Wait must join but
		// that can block forever reading from the session (WaitDelay cannot
		// interrupt reads from a non-File).
		stdin, err := cmd.StdinPipe()
		if err != nil {
			fmt.Fprintf(s.Stderr(), "start command: %v\n", err)
			s.Exit(1)
			return
		}
		if err := cmd.Start(); err != nil {
			fmt.Fprintf(s.Stderr(), "start command: %v\n", err)
			s.Exit(1)
			return
		}
		var input io.Reader = s
		if wantLogout {
			logout = time.AfterFunc(*idleTimeout, func() {
				fmt.Fprintf(s, "\nLogged out: no input for %s.\n", *idleTimeout)
				killGroup(cmd)
			})
			defer logout.Stop()
			input = &activityReader{r: s, reset: func() { logout.Reset(*idleTimeout) }}
		}
		go func() {
			io.Copy(stdin, input)
			stdin.Close()
		}()
	}

	// ErrWaitDelay means the process exited cleanly but a pipe copier was
	// still blocked (e.g. a client holding stdin open); treat as success.
	if err := cmd.Wait(); err != nil && !errors.Is(err, exec.ErrWaitDelay) {
		if exitErr, ok := err.(*exec.ExitError); ok {
			// ExitCode is -1 for a signaled process (including the logout
			// kill); report 255 rather than -1 wrapping around on the wire.
			if code := exitErr.ExitCode(); code >= 0 {
				s.Exit(code)
			} else {
				s.Exit(255)
			}
			return
		}
		fmt.Fprintf(s.Stderr(), "wait for command: %v\n", err)
		s.Exit(1)
		return
	}
	s.Exit(0)
}

func HandleSSHServer(args []string) (int, error) {
	start := time.Now()
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return 1, err
	}

	var name string
	if positional := flags.Args(); len(positional) > 0 {
		name = positional[0]
	}

	if *gateway == "" {
		log.Printf("A non-empty --gateway must be specified")
		return 1, nil
	}

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	} else if apiKey, err := login.GetAPIKey(); err == nil && apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}

	// Generate a local WireGuard keypair — the private key never leaves this process.
	privKey, err := wgkeys.GeneratePrivateKey()
	if err != nil {
		return 1, status.WrapError(err, "generating wg private key")
	}

	// Register with the gateway.
	grpcConn, err := grpc_client.DialSimple(*gateway)
	if err != nil {
		return 1, status.WrapError(err, "dialing gateway")
	}
	defer grpcConn.Close()

	gwClient := gwsvcpb.NewGatewayServiceClient(grpcConn)

	// Renaming shells out to sudo; overlap it with gateway and tunnel setup
	// rather than adding to startup latency. Waited on before serving, since
	// shells read the hostname when they start.
	hostnameDone := make(chan struct{})
	go func() {
		defer close(hostnameDone)
		setHostname(name)
	}()

	sid := *sessionID
	if sid == "" {
		sid = uuid.New()
	}

	// Connect to the gateway. The registration is leased to this stream: the
	// gateway frees the peer's IP and DNS name as soon as the stream closes,
	// so there is no explicit Deregister on shutdown — canceling connectCtx
	// (deferred below) is the clean-shutdown path.
	connectCtx, cancelConnect := context.WithCancel(ctx)
	defer cancelConnect()
	stream, err := gwClient.Connect(connectCtx, &gwpb.ConnectRequest{
		NetworkName: *network,
		PeerName:    name,
		PublicKey:   privKey.PublicKey().Hex(),
		SessionId:   sid,
	})
	if err != nil {
		// Note: for a server-streaming RPC, grpc-go surfaces most status
		// errors (including ALREADY_EXISTS from the gateway) on the first
		// Recv rather than here; this branch only catches immediate
		// connection failures.
		return 1, status.WrapError(err, "connecting to gateway")
	}
	rsp, err := stream.Recv()
	if err != nil {
		if status.IsAlreadyExistsError(err) {
			// The gateway refused the registration because the peer name,
			// session ID, or public key is already in use — the server's
			// message says which. Exit without doing any work, and drop the
			// do-not-recycle marker so this VM is not snapshotted: otherwise
			// its empty session could race the winner's snapshot save for
			// the same recycling key.
			log.Printf("Gateway registration refused: %s; exiting.", status.Message(err))
			writeDoNotRecycleMarker()
			return 1, nil
		}
		return 1, status.WrapError(err, "connecting to gateway")
	}
	log.Printf("Connected: assigned_ip=%s gateway_ip=%s cidr=%s endpoint=%s name=%s session=%s",
		rsp.GetAssignedIp(), rsp.GetGatewayIp(), rsp.GetNetworkCidr(), rsp.GetServerEndpoint(), name, sid)
	registeredIn := time.Since(start)

	// Bring up the userspace WireGuard tunnel.
	assignedAddr := netip.MustParseAddr(rsp.GetAssignedIp())
	tunDev, tnet, err := netstack.CreateNetTUN(
		[]netip.Addr{assignedAddr},
		[]netip.Addr{netip.MustParseAddr(rsp.GetGatewayIp())},
		1420,
	)
	if err != nil {
		return 1, status.WrapError(err, "creating netstack TUN")
	}
	wgLogger := &device.Logger{
		Verbosef: func(format string, args ...any) { log.Debugf("wg: "+format, args...) },
		Errorf:   func(format string, args ...any) { log.Warnf("wg: "+format, args...) },
	}
	dev := device.NewDevice(tunDev, conn.NewDefaultBind(), wgLogger)
	endpoint, err := resolveEndpoint(rsp.GetServerEndpoint())
	if err != nil {
		return 1, status.WrapError(err, "resolving wg endpoint")
	}
	ipc := fmt.Sprintf(
		"private_key=%s\npublic_key=%s\nallowed_ip=%s\nendpoint=%s\npersistent_keepalive_interval=25\n",
		privKey.Hex(), rsp.GetServerPublicKey(), rsp.GetNetworkCidr(), endpoint,
	)
	if err := dev.IpcSet(ipc); err != nil {
		return 1, status.WrapError(err, "configuring wg")
	}
	if err := dev.Up(); err != nil {
		return 1, status.WrapError(err, "bringing up wg")
	}
	defer dev.Close()

	// Fail fast if the tunnel never comes up. Persistent keepalives make the
	// first handshake begin immediately, so a missing handshake means this
	// host's UDP path to the gateway is broken (e.g. a misprogrammed
	// executor node). Exiting promptly surfaces the failure in the create
	// log instead of leaving behind a registered but unreachable server.
	if *wgHealthTimeout > 0 {
		handshakeStart := time.Now()
		if err := waitForHandshake(dev, *wgHealthTimeout); err != nil {
			writeDoNotRecycleMarker()
			return 1, err
		}
		log.Printf("WireGuard handshake completed in %s", time.Since(handshakeStart))
	}

	// Build the SSH server. WireGuard membership is the auth boundary; no SSH
	// credential checking is required. gliderlabs/ssh automatically sets
	// NoClientAuth=true when no auth handlers are configured.
	forwards := &ssh.ForwardedTCPHandler{}
	sshServer := &ssh.Server{
		IdleTimeout: connIdleTimeout,
		ChannelHandlers: map[string]ssh.ChannelHandler{
			"session": ssh.DefaultSessionHandler,
			// `bb ssh -L`: the box dials the destination and splices.
			"direct-tcpip": ssh.DirectTCPIPHandler,
		},
		RequestHandlers: map[string]ssh.RequestHandler{
			// `bb ssh -R`: the box listens and opens a channel per connection.
			"tcpip-forward":        forwards.HandleSSHRequest,
			"cancel-tcpip-forward": forwards.HandleSSHRequest,
		},
		// The WireGuard tunnel is the authentication boundary, and anyone
		// forwarding a port here can already run commands here.
		LocalPortForwardingCallback:   func(ssh.Context, string, uint32) bool { return true },
		ReversePortForwardingCallback: func(ssh.Context, string, uint32) bool { return true },
	}
	hostKeyPath := *hostKeyFile
	if hostKeyPath == "" {
		cacheDir, err := os.UserCacheDir()
		if err != nil {
			return 1, status.WrapError(err, "getting cache dir for host key")
		}
		// Key filename is scoped to the peer name so that a VM resuming with
		// the same name reuses the same key, preventing SSH warnings. Falls
		// back to the assigned IP for peers registered without a name (unique
		// per peer, though not stable across restarts).
		keyID := name
		if keyID == "" {
			keyID = strings.ReplaceAll(rsp.GetAssignedIp(), ":", "_")
		}
		hostKeyPath = filepath.Join(cacheDir, "buildbuddy", "ssh_host_key_"+keyID)
	}
	pemBytes, err := loadOrCreateHostKey(hostKeyPath)
	if err != nil {
		return 1, status.WrapError(err, "loading host key")
	}
	if err := sshServer.SetOption(ssh.HostKeyPEM(pemBytes)); err != nil {
		return 1, status.WrapError(err, "setting host key")
	}

	listener, err := tnet.ListenTCP(&net.TCPAddr{Port: *sshPort})
	if err != nil {
		return 1, status.WrapError(err, "listening on tunnel port")
	}
	defer listener.Close()

	hostPort := net.JoinHostPort(rsp.GetAssignedIp(), fmt.Sprintf("%d", *sshPort))
	q := url.Values{}
	if name != "" {
		q.Set("name", name)
	}
	sshURL := &url.URL{Scheme: "bb-ssh", Host: hostPort, RawQuery: q.Encode()}
	log.Printf("Listening on %s (registered with gateway in %s, startup took %s)", sshURL, registeredIn, time.Since(start))
	connectTarget := name
	if connectTarget == "" {
		connectTarget = rsp.GetAssignedIp()
	}
	if *sshPort != 22 {
		log.Printf("Connect with: bb ssh -p %d %s", *sshPort, connectTarget)
	} else {
		log.Printf("Connect with: bb ssh %s", connectTarget)
	}

	// Idle-shutdown: call sshServer.Shutdown once the grace period elapses with
	// no connected clients. The timer starts immediately to cover the case
	// where no client ever connects. Connections are counted rather than
	// sessions, so that a client holding only port forwards (bb ssh -N) keeps
	// the VM alive, and a client whose command outlives it does not.
	var (
		mu          sync.Mutex
		activeConns int
		idleTimer   *time.Timer
	)
	resetIdleTimer := func() {
		// Must be called with mu held.
		if idleTimer != nil {
			idleTimer.Stop()
		}
		var t *time.Timer
		t = time.AfterFunc(*gracePeriod, func() {
			mu.Lock()
			// Stop doesn't unschedule a timer that already fired, so this
			// callback may belong to a grace period that has been superseded
			// by a client connecting and disconnecting again.
			if idleTimer != t || activeConns != 0 {
				mu.Unlock()
				return
			}
			idleTimer = nil
			// Released before Shutdown: it waits for connections to drain,
			// and both connection callbacks take this lock.
			mu.Unlock()

			log.Printf("No clients connected for %s; shutting down.", *gracePeriod)
			shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sshServer.Shutdown(shutCtx)
		})
		idleTimer = t
	}
	mu.Lock()
	resetIdleTimer()
	mu.Unlock()

	sshServer.Handler = handleSession
	sshServer.ConnCallback = func(_ ssh.Context, conn net.Conn) net.Conn {
		mu.Lock()
		if idleTimer != nil {
			idleTimer.Stop()
			idleTimer = nil
		}
		activeConns++
		mu.Unlock()
		return &countedConn{Conn: conn, onClose: func() {
			mu.Lock()
			activeConns--
			if activeConns == 0 {
				resetIdleTimer()
			}
			mu.Unlock()
		}}
	}

	// The gateway registration is leased to the Connect stream. If the stream
	// ends for any reason other than local shutdown (gateway restart, network
	// partition, eviction), this server is unreachable through the tunnel:
	// shut down so the VM suspends cleanly and can be resumed.
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				if connectCtx.Err() != nil {
					// Local shutdown already in progress.
					return
				}
				log.Printf("Gateway connection lost (%v); shutting down.", err)
				shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				sshServer.Shutdown(shutCtx)
				return
			}
		}
	}()

	// Catch SIGINT/SIGTERM so the process shuts down via Shutdown() rather than
	// being killed abruptly, ensuring deferred cleanup (closing the gateway
	// stream, dev.Close) runs on Ctrl-C or a normal kill signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		sig, ok := <-sigCh
		if !ok {
			return
		}
		log.Printf("Received %s; shutting down.", sig)
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		sshServer.Shutdown(shutCtx)
	}()

	<-hostnameDone

	if err := sshServer.Serve(listener); err != nil && err != ssh.ErrServerClosed {
		return 1, status.WrapError(err, "ssh server")
	}

	return 0, nil
}
