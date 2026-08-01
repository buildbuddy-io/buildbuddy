// Package ssh dials an SSH server with userspace networking.
package ssh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/buildbuddy-io/buildbuddy/server/util/wgkeys"
	"golang.org/x/term"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
	"google.golang.org/grpc/metadata"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
	gwsvcpb "github.com/buildbuddy-io/buildbuddy/proto/gateway_service"
	gossh "golang.org/x/crypto/ssh"
)

var (
	flags = flag.NewFlagSet("ssh", flag.ContinueOnError)
	Flags = flags

	gatewayTarget = flags.String("gateway", "grpcs://gateway.buildbuddy.io", "Gateway gRPC target")
	network       = flags.String("network", "", "Network name (default is blank)")
	apiKey        = flags.String("api_key", "", "Optionally override the API key with this value")
	port          = flags.Int("p", 22, "SSH port to dial on the remote host")
	user          = flags.String("l", "", "SSH login name (used when the target has no user@ prefix)")
	forceTTY      = flags.Bool("t", false, "Force pseudo-terminal allocation, e.g. to run an interactive program remotely")

	usage string
)

const (
	// keepaliveInterval is how often the client pings the server over the
	// SSH transport. 
	keepaliveInterval = 15 * time.Second

	// keepaliveTimeout bounds how long an unanswered keepalive is allowed
	// to hang before the transport is declared dead.
	keepaliveTimeout = 3 * keepaliveInterval
)

func init() {
	var buf strings.Builder
	fmt.Fprintf(&buf, "usage: bb %s [flags] [user@]<host> [command ...]\n\nConnect to an SSH server reachable via the BuildBuddy gateway.\n\nFlags:\n", flags.Name())
	flags.SetOutput(&buf)
	flags.PrintDefaults()
	usage = buf.String()
	flags.SetOutput(io.Discard)
}

// parseTarget splits a target argument — [user@]host, host:port, or a
// bb-ssh:// URL — into its components. userFlag (-l) and portFlag (-p)
// supply the defaults; a user@ prefix in the target takes precedence over
// -l, and a port in the target takes precedence over -p.
func parseTarget(target, userFlag string, portFlag int) (host, loginUser string, port int) {
	loginUser = userFlag
	if before, after, ok := strings.Cut(target, "@"); ok {
		loginUser, target = before, after
	}
	port = portFlag
	if u, err := url.Parse(target); err == nil && u.Scheme == "bb-ssh" {
		target = u.Hostname()
		if p, err := strconv.Atoi(u.Port()); err == nil {
			port = p
		}
	} else if h, portStr, err := net.SplitHostPort(target); err == nil {
		target = h
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}
	return target, loginUser, port
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

func HandleSSH(args []string) (int, error) {
	// Parse with the standard library directly (not arg.ParseFlagSet, which
	// re-parses flags after each positional arg): parsing must stop at the
	// first positional so that flags in the remote command, e.g.
	// `bb ssh box claude --continue`, are passed through rather than
	// interpreted by bb. `--` also terminates flag parsing, as usual.
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return 1, err
	}

	if *gatewayTarget == "" {
		log.Printf("A non-empty --gateway must be specified")
		return 1, nil
	}

	positional := flags.Args()
	if len(positional) < 1 {
		log.Print(usage)
		return 1, nil
	}

	// The first positional argument is the target; any remaining arguments
	// are joined as the remote command to execute.
	target, loginUser, dialPort := parseTarget(positional[0], *user, *port)
	if loginUser == "" {
		loginUser = os.Getenv("USER")
	}
	var remoteCmd string
	if len(positional) > 1 {
		remoteCmd = strings.Join(positional[1:], " ")
	}

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	} else if key, err := login.GetAPIKey(); err == nil && key != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", key)
	}

	// Generate a local WireGuard keypair — the private key never leaves this process.
	privKey, err := wgkeys.GeneratePrivateKey()
	if err != nil {
		return 1, status.WrapError(err, "generating wg private key")
	}

	// Register with the gateway.
	grpcConn, err := grpc_client.DialSimple(*gatewayTarget)
	if err != nil {
		return 1, status.WrapError(err, "dialing gateway")
	}
	defer grpcConn.Close()

	gwClient := gwsvcpb.NewGatewayServiceClient(grpcConn)

	// Connect to the gateway. The registration is leased to this stream: the
	// gateway frees the peer's IP as soon as the stream closes, so canceling
	// connectCtx (deferred below) is the clean-shutdown path.
	connectCtx, cancelConnect := context.WithCancel(ctx)
	defer cancelConnect()
	stream, err := gwClient.Connect(connectCtx, &gwpb.ConnectRequest{
		NetworkName: *network,
		PublicKey:   privKey.PublicKey().Hex(),
		SessionId:   uuid.New(),
	})
	if err != nil {
		// Note: for a server-streaming RPC, grpc-go surfaces most status
		// errors on the first Recv rather than here; this branch only
		// catches immediate connection failures.
		return 1, status.WrapError(err, "connecting to gateway")
	}
	rsp, err := stream.Recv()
	if err != nil {
		return 1, status.WrapError(err, "connecting to gateway")
	}
	// Hold the stream open in the background for the lifetime of the SSH
	// session; if it ends, the tunnel is (or is about to be) dead. gwLostErr
	// is written before gwLost is closed, so it is safe to read after
	// receiving from gwLost.
	var gwLostErr error
	gwLost := make(chan struct{})
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				if connectCtx.Err() != nil {
					// Normal local shutdown.
					return
				}
				gwLostErr = err
				close(gwLost)
				return
			}
		}
	}()

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

	// Dial the SSH server through the WireGuard tunnel.
	addr := net.JoinHostPort(target, fmt.Sprintf("%d", dialPort))
	tcpConn, err := tnet.Dial("tcp", addr)
	if err != nil {
		return 1, status.WrapError(err, "dialing ssh server")
	}

	sshConfig := &gossh.ClientConfig{
		User: loginUser,
		// Host key verification is intentionally skipped: the WireGuard tunnel
		// provides mutual authentication (only a peer that registered the correct
		// public key with the gateway can receive traffic), so the SSH layer does
		// not need an additional TOFU/known_hosts check.
		HostKeyCallback: gossh.InsecureIgnoreHostKey(),
		Timeout:         15 * time.Second,
	}
	sshConn, chans, reqs, err := gossh.NewClientConn(tcpConn, addr, sshConfig)
	if err != nil {
		return 1, status.WrapError(err, "ssh handshake")
	}
	client := gossh.NewClient(sshConn, chans, reqs)
	defer client.Close()

	// If the gateway connection is lost mid-session, the tunnel is dead:
	// close the SSH client so the session below unblocks immediately.
	// The close message is printed on the normal exit path, after the
	// terminal is restored from raw mode.
	go func() {
		select {
		case <-gwLost:
			client.Close()
		case <-connectCtx.Done():
		}
	}()

	// Send keepalives to keep connection up. The server will close the
	// connection after detecting no activity (data sent from the client)
	// for some time.
	var connDeadErr error
	connDead := make(chan struct{})
	go func() {
		ticker := time.NewTicker(keepaliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				replied := make(chan error, 1)
				go func() {
					// client.SendRequest blocks until reply.
					_, _, err := client.SendRequest("keepalive@openssh.com", true, nil)
					replied <- err
				}()
				var err error
				select {
				case err = <-replied:
				case <-time.After(keepaliveTimeout):
					err = fmt.Errorf("no keepalive reply within %s", keepaliveTimeout)
				case <-connectCtx.Done():
					return
				}
				if err != nil {
					connDeadErr = err
					close(connDead)
					client.Close()
					return
				}
			case <-connectCtx.Done():
				return
			}
		}
	}()

	session, err := client.NewSession()
	if err != nil {
		return 1, status.WrapError(err, "opening ssh session")
	}
	defer session.Close()

	session.Stdout = os.Stdout
	session.Stderr = os.Stderr

	// For remote commands without -t, only wire up stdin when it is being
	// piped (e.g. echo data | bb ssh host cat) — a program that doesn't read
	// stdin shouldn't steal keystrokes typed while it runs. Interactive
	// sessions and -t commands always get stdin.
	if remoteCmd == "" || *forceTTY || !term.IsTerminal(int(os.Stdin.Fd())) {
		session.Stdin = os.Stdin
	}

	// rawRestore, if set, restores the terminal from raw mode. We call it
	// explicitly before printing the close message so the \n lands correctly;
	// the deferred call is a safety net for early returns.
	var rawRestore func()
	defer func() {
		if rawRestore != nil {
			rawRestore()
		}
	}()

	// Request a PTY for interactive sessions (no explicit remote command) and
	// for remote commands run with -t, matching standard ssh behaviour.
	wantPTY := remoteCmd == "" || *forceTTY
	if *forceTTY && !term.IsTerminal(int(os.Stdin.Fd())) {
		fmt.Fprintln(os.Stderr, "Pseudo-terminal will not be allocated because stdin is not a terminal.")
	}
	if wantPTY && term.IsTerminal(int(os.Stdin.Fd())) {
		w, h, err := term.GetSize(int(os.Stdin.Fd()))
		if err != nil {
			w, h = 80, 24
		}
		termName := os.Getenv("TERM")
		if termName == "" {
			termName = "xterm-256color"
		}
		modes := gossh.TerminalModes{gossh.ECHO: 1}
		if err := session.RequestPty(termName, h, w, modes); err != nil {
			return 1, status.WrapError(err, "requesting pty")
		}

		// Put local terminal into raw mode so control sequences pass through.
		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			return 1, status.WrapError(err, "setting raw terminal mode")
		}
		rawRestore = func() { term.Restore(int(os.Stdin.Fd()), oldState) }

		// Forward SIGWINCH to the remote PTY.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGWINCH)
		defer signal.Stop(sigCh)
		go func() {
			for range sigCh {
				w, h, err := term.GetSize(int(os.Stdin.Fd()))
				if err == nil {
					session.WindowChange(h, w)
				}
			}
		}()
	}

	if remoteCmd != "" {
		if err := session.Run(remoteCmd); err != nil {
			if gwErr := chanError(gwLost, &gwLostErr); gwErr != nil {
				return 1, status.UnavailableErrorf("gateway connection lost: %s", gwErr)
			}
			if kaErr := chanError(connDead, &connDeadErr); kaErr != nil {
				if rawRestore != nil {
					rawRestore()
					rawRestore = nil
				}
				return 1, status.UnavailableErrorf("connection to %s lost: no response from server (%s)", target, kaErr)
			}
			var exitErr *gossh.ExitError
			if errors.As(err, &exitErr) {
				return exitErr.ExitStatus(), nil
			}
			return 1, status.WrapError(err, "running remote command")
		}
		return 0, nil
	}
	if err := session.Shell(); err != nil {
		return 1, status.WrapError(err, "starting shell")
	}

	err = session.Wait()
	if gwErr := chanError(gwLost, &gwLostErr); gwErr != nil {
		// Restore the terminal before printing so the message lands at
		// column 0 (the goroutine that noticed the loss can't print safely
		// while the terminal is in raw mode).
		if rawRestore != nil {
			rawRestore()
			rawRestore = nil
		}
		fmt.Fprintf(os.Stderr, "Connection to %s closed: gateway connection lost: %v\n", target, gwErr)
		return 1, nil
	}
	if kaErr := chanError(connDead, &connDeadErr); kaErr != nil {
		if rawRestore != nil {
			rawRestore()
			rawRestore = nil
		}
		fmt.Fprintf(os.Stderr, "Connection to %s closed: no response from server (%v).\n", target, kaErr)
		return 1, nil
	}
	if err != nil {
		var exitErr *gossh.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitStatus(), nil
		}
		// Server closed without an exit status (e.g. its idle timeout or
		// shutdown closed the connection); fall through to print the close
		// message.
		var missingErr *gossh.ExitMissingError
		if !errors.As(err, &missingErr) && !errors.Is(err, io.EOF) {
			return 1, err
		}
	}
	// Restore the terminal before printing so the message lands at column 0.
	if rawRestore != nil {
		rawRestore()
		rawRestore = nil // prevent double-restore from the deferred call
	}
	fmt.Fprintf(os.Stderr, "Connection to %s closed.\n", target)
	return 0, nil
}

// chanError returns *err if ch has fired, or nil otherwise. err must point
// to a variable that is written only before ch is closed: the pointer
// indirection ensures the read happens after the channel synchronizes,
// rather than when the argument is evaluated at the call site.
func chanError(ch chan struct{}, err *error) error {
	select {
	case <-ch:
		return *err
	default:
		return nil
	}
}
