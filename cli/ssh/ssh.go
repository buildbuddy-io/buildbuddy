// Package ssh dials an SSH server with userspace networking.
package ssh

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/keys"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/crypto/ssh/knownhosts"
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

	gatewayTarget = flags.String("gateway", "grpcs://gateway.buildbuddy.dev", "Gateway gRPC target")
	network       = flags.String("network", "", "Network name (default is blank)")
	apiKey        = flags.String("api_key", "", "Optionally override the API key with this value")
	port          = flags.Int("p", 22, "SSH port to dial on the remote host")
	user          = flags.String("l", "", "SSH login name (overrides user@host syntax)")

	usage = `
usage: bb ` + flags.Name() + ` [flags] [user@]<host>

Connect to an SSH server reachable via the BuildBuddy gateway.
`
)

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

// hostKeyCallback returns a callback that checks the server's host key against
// ~/.ssh/known_hosts, prompting the user to accept unknown keys and persisting
// accepted keys so future connections are verified silently.
func hostKeyCallback(knownHostsPath string) gossh.HostKeyCallback {
	return func(hostname string, remote net.Addr, key gossh.PublicKey) error {
		cb, err := knownhosts.New(knownHostsPath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("reading known_hosts: %w", err)
		}
		if err == nil {
			err = cb(hostname, remote, key)
			if err == nil {
				return nil // known and verified
			}
			var keyErr *knownhosts.KeyError
			if errors.As(err, &keyErr) && len(keyErr.Want) > 0 {
				// Key mismatch — always hard-reject.
				fmt.Fprintf(os.Stderr, "WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!\n")
				return err
			}
			// err != nil but no Want entries → key not found, fall through to prompt.
		}

		fingerprint := gossh.FingerprintSHA256(key)
		fmt.Fprintf(os.Stderr, "The authenticity of host %q can't be established.\n", hostname)
		fmt.Fprintf(os.Stderr, "%s key fingerprint is %s.\n", key.Type(), fingerprint)
		fmt.Fprintf(os.Stderr, "Are you sure you want to continue connecting (yes/no)? ")

		var response string
		fmt.Scanln(&response)
		if strings.TrimSpace(strings.ToLower(response)) != "yes" {
			return fmt.Errorf("host key verification rejected by user")
		}

		// Persist to known_hosts.
		if err := os.MkdirAll(filepath.Dir(knownHostsPath), 0700); err != nil {
			return fmt.Errorf("creating known_hosts dir: %w", err)
		}
		f, err := os.OpenFile(knownHostsPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("opening known_hosts: %w", err)
		}
		defer f.Close()
		line := knownhosts.Line([]string{knownhosts.Normalize(hostname)}, key)
		_, err = fmt.Fprintln(f, line)
		return err
	}
}


func HandleSSH(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
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

	// Parse [user@]host from the first positional argument; any remaining
	// arguments are joined as the remote command to execute.
	target := positional[0]
	var remoteCmd string
	if len(positional) > 1 {
		remoteCmd = strings.Join(positional[1:], " ")
	}
	loginUser := *user
	if before, after, ok := strings.Cut(target, "@"); ok {
		loginUser, target = before, after
	}
	if loginUser == "" {
		loginUser = os.Getenv("USER")
	}

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	} else if key, err := login.GetAPIKey(); err == nil && key != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", key)
	}

	// Generate a local WireGuard keypair — the private key never leaves this process.
	privKey, err := keys.GeneratePrivateKey()
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

	// Deregister runs before grpcConn.Close() (LIFO).
	defer func() {
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := gwClient.Deregister(dctx, &gwpb.DeregisterRequest{PublicKey: privKey.PublicKey().Hex()}); err != nil {
			log.Warnf("deregister: %v", err)
		}
	}()

	rsp, err := gwClient.Register(ctx, &gwpb.RegisterRequest{
		NetworkName: *network,
		PublicKey:   privKey.PublicKey().Hex(),
	})
	if err != nil {
		return 1, status.WrapError(err, "registering with gateway")
	}

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
	addr := net.JoinHostPort(target, fmt.Sprintf("%d", *port))
	tcpConn, err := tnet.Dial("tcp", addr)
	if err != nil {
		return 1, status.WrapError(err, "dialing ssh server")
	}

	home, _ := os.UserHomeDir()
	knownHostsPath := filepath.Join(home, ".ssh", "known_hosts")

	sshConfig := &gossh.ClientConfig{
		User:            loginUser,
		HostKeyCallback: hostKeyCallback(knownHostsPath),
		Timeout:         15 * time.Second,
	}
	sshConn, chans, reqs, err := gossh.NewClientConn(tcpConn, addr, sshConfig)
	if err != nil {
		return 1, status.WrapError(err, "ssh handshake")
	}
	client := gossh.NewClient(sshConn, chans, reqs)
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return 1, status.WrapError(err, "opening ssh session")
	}
	defer session.Close()

	session.Stdout = os.Stdout
	session.Stderr = os.Stderr
	// For remote commands, only wire up stdin when it is being piped — if
	// stdin is a terminal, golang.org/x/crypto/ssh's Wait() would block on
	// the stdin copy goroutine until the user presses Enter after the command
	// exits. Piped stdin still works (e.g. echo data | bb ssh host cat).
	if remoteCmd == "" || !term.IsTerminal(int(os.Stdin.Fd())) {
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

	// Request a PTY only for interactive sessions (no explicit remote command),
	// matching standard ssh behaviour.
	if remoteCmd == "" && term.IsTerminal(int(os.Stdin.Fd())) {
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
	if err != nil {
		var exitErr *gossh.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitStatus(), nil
		}
		// Server closed without an exit status (e.g. idle timeout); fall
		// through to print the close message.
		var missingErr *gossh.ExitMissingError
		if !errors.As(err, &missingErr) {
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
