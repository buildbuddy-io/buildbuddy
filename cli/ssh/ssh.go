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
	noCommand     = flags.Bool("N", false, "Do not run a remote command; useful when only forwarding ports")

	localForwards  = flag.New(flags, "L", []string{}, "Forward a port on this machine to one reachable from the box: [bind:]port:host:hostport (repeatable)")
	remoteForwards = flag.New(flags, "R", []string{}, "Forward a port on the box to one reachable from this machine: [bind:]port:host:hostport (repeatable)")

	usage string
)

// keepaliveInterval is how often the client pings the server over the SSH
// transport. Well under the server's connection-level dead-client timeout
// (connIdleTimeout in cli/ssh_server, 3m), so a live connection is never
// reaped — though the server still logs out interactive sessions that go
// --idle_timeout without user input; keepalives don't count as input.
const keepaliveInterval = 15 * time.Second

func init() {
	var buf strings.Builder
	fmt.Fprintf(&buf, "usage: bb %s [flags] [user@]<host> [command ...]\n\nConnect to an SSH server reachable via the BuildBuddy gateway.\n\nFlags:\n", flags.Name())
	flags.SetOutput(&buf)
	flags.PrintDefaults()
	usage = buf.String()
	// The buffer above only exists to build the usage string; route any
	// later parse-error output to /dev/null (errors are returned instead).
	flags.SetOutput(io.Discard)
}

// parseTarget splits a target argument — [user@]host, host:port, or a
// bb-ssh://[user@]host URL — into host, login user, and port. userFlag (-l)
// and portFlag (-p) supply the defaults; user and port in the target take
// precedence.
func parseTarget(target, userFlag string, portFlag int) (string, string, int) {
	loginUser := userFlag
	port := portFlag
	// Try the URL form first: cutting at "@" before parsing would mangle
	// bb-ssh://user@host into user "bb-ssh://user".
	if u, err := url.Parse(target); err == nil && u.Scheme == "bb-ssh" {
		if name := u.User.Username(); name != "" {
			loginUser = name
		}
		if p, err := strconv.Atoi(u.Port()); err == nil {
			port = p
		}
		return u.Hostname(), loginUser, port
	}
	if before, after, ok := strings.Cut(target, "@"); ok {
		loginUser, target = before, after
	}
	// Only honor a :port suffix when the port is numeric; otherwise keep the
	// whole string as the host so the resulting dial error names it, rather
	// than silently connecting to the default port.
	if h, portStr, err := net.SplitHostPort(target); err == nil {
		if p, err := strconv.Atoi(portStr); err == nil {
			target, port = h, p
		}
	}
	return target, loginUser, port
}

// parseForward parses an OpenSSH-style forward spec,
// [bind_address:]port:host:hostport, into the address to listen on and the
// address to dial. The bind address defaults to 127.0.0.1, so a forwarded
// port is not exposed to the network. (OpenSSH defaults to every loopback
// address, but net.Listen binds only the first address a name resolves to,
// which would make the family served depend on the resolver.)
func parseForward(spec string) (listen, dial string, err error) {
	// Split on colons outside of brackets, which delimit IPv6 literals.
	var parts []string
	var cur strings.Builder
	depth := 0
	for _, r := range spec {
		switch r {
		case '[':
			depth++
		case ']':
			if depth == 0 {
				return "", "", status.InvalidArgumentErrorf("invalid forward %q: unbalanced brackets", spec)
			}
			depth--
		case ':':
			if depth == 0 {
				parts = append(parts, cur.String())
				cur.Reset()
			} else {
				cur.WriteRune(r)
			}
		default:
			cur.WriteRune(r)
		}
	}
	parts = append(parts, cur.String())

	bind := "127.0.0.1"
	if len(parts) == 4 {
		bind, parts = parts[0], parts[1:]
	}
	if len(parts) != 3 {
		return "", "", status.InvalidArgumentErrorf("invalid forward %q: want [bind:]port:host:hostport", spec)
	}
	for _, p := range []string{parts[0], parts[2]} {
		if n, err := strconv.Atoi(p); err != nil || n < 1 || n > 65535 {
			return "", "", status.InvalidArgumentErrorf("invalid port %q in forward %q", p, spec)
		}
	}
	if parts[1] == "" {
		return "", "", status.InvalidArgumentErrorf("invalid forward %q: missing host", spec)
	}
	return net.JoinHostPort(bind, parts[0]), net.JoinHostPort(parts[1], parts[2]), nil
}

// forward accepts connections on ln and splices each one to a connection
// opened by dial. It returns when ln is closed.
func forward(ln net.Listener, dial func() (net.Conn, error), spec string) {
	for {
		in, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer in.Close()
			out, err := dial()
			if err != nil {
				log.Printf("Forward %s: %s", spec, err)
				return
			}
			defer out.Close()
			go io.Copy(out, in)
			io.Copy(in, out)
		}()
	}
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
	// session; if it ends, the tunnel is (or is about to be) dead.
	gwLost := make(chan struct{})
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				if connectCtx.Err() != nil {
					// Normal local shutdown.
					return
				}
				log.Debugf("gateway stream ended: %v", err)
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
	// close the SSH client so the session below unblocks immediately instead
	// of hanging until TCP gives up. The close message is printed on the
	// normal exit path, after the terminal is restored from raw mode.
	go func() {
		select {
		case <-gwLost:
			client.Close()
		case <-connectCtx.Done():
		}
	}()

	// Keepalives are the traffic that keeps a live connection from being
	// reaped by the server's connection-level dead-client timeout (they do
	// NOT prevent --idle_timeout logout, which counts only user input). They
	// also detect a dead transport (server suspended, network gone): the
	// client is closed so the session below unblocks with a clean message
	// instead of hanging indefinitely. Detection is not fast — SendRequest
	// only fails once the netstack TCP layer gives up on the peer, which can
	// take minutes without a RST — but it is what turns "hangs forever" into
	// "eventually exits cleanly".
	connDead := make(chan struct{})
	go func() {
		ticker := time.NewTicker(keepaliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if _, _, err := client.SendRequest("keepalive@openssh.com", true, nil); err != nil {
					log.Debugf("keepalive failed: %v", err)
					close(connDead)
					client.Close()
					return
				}
			case <-connectCtx.Done():
				return
			}
		}
	}()

	// -L: listen here, dial from the box.
	for _, spec := range *localForwards {
		listenAddr, dialAddr, err := parseForward(spec)
		if err != nil {
			return 1, err
		}
		ln, err := net.Listen("tcp", listenAddr)
		if err != nil {
			return 1, status.WrapErrorf(err, "listen for -L %s", spec)
		}
		defer ln.Close()
		go forward(ln, func() (net.Conn, error) { return client.Dial("tcp", dialAddr) }, spec)
	}
	// -R: the box listens, we dial from here.
	for _, spec := range *remoteForwards {
		listenAddr, dialAddr, err := parseForward(spec)
		if err != nil {
			return 1, err
		}
		ln, err := client.Listen("tcp", listenAddr)
		if err != nil {
			return 1, status.WrapErrorf(err, "listen on %s for -R %s", target, spec)
		}
		defer ln.Close()
		go forward(ln, func() (net.Conn, error) { return net.Dial("tcp", dialAddr) }, spec)
	}

	if *noCommand {
		if len(*localForwards) == 0 && len(*remoteForwards) == 0 {
			log.Printf("-N was given with no port forwards; nothing to do")
			return 1, nil
		}
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(sigCh)
		select {
		case <-sigCh:
			return 0, nil
		case <-gwLost:
			fmt.Fprintf(os.Stderr, "Connection to %s closed: gateway connection lost.\n", target)
			return 1, nil
		case <-connDead:
			fmt.Fprintf(os.Stderr, "Connection to %s closed: no response from server.\n", target)
			return 1, nil
		}
	}

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
			if closed(gwLost) {
				return 1, status.UnavailableError("gateway connection lost")
			}
			if closed(connDead) {
				return 1, status.UnavailableErrorf("connection to %s lost: no response from server", target)
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
	if closed(gwLost) {
		// Restore the terminal before printing so the message lands at
		// column 0 (the goroutine that noticed the loss can't print safely
		// while the terminal is in raw mode).
		if rawRestore != nil {
			rawRestore()
			rawRestore = nil
		}
		fmt.Fprintf(os.Stderr, "Connection to %s closed: gateway connection lost.\n", target)
		return 1, nil
	}
	if closed(connDead) {
		if rawRestore != nil {
			rawRestore()
			rawRestore = nil
		}
		fmt.Fprintf(os.Stderr, "Connection to %s closed: no response from server.\n", target)
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

// closed reports whether ch has been closed. The underlying error, if any,
// is logged at debug level by whoever closed the channel.
func closed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
