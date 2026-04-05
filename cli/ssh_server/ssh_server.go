// Package ssh_server starts an SSH server with userspace networking.
package ssh_server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/netip"
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
	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/keys"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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

	gateway     = flags.String("gateway", "grpcs://gateway.buildbuddy.dev", "Gateway gRPC target")
	network     = flags.String("network", "", "Network name (default is blank)")
	apiKey      = flags.String("api_key", "", "Optionally override the API key with this value")
	gracePeriod = flags.Duration("grace_period", 1*time.Minute, "How long the VM will remain alive when no users are connected")
	idleTimeout = flags.Duration("idle_timeout", 0, "Close idle SSH sessions after this duration of inactivity (0 means no timeout)")

	name        = flags.String("name", "", "Name for this peer; reachable at <name>.internal on the tunnel network (auto-generated if unset)")
	sshPort     = flags.Int("ssh_port", 22, "SSH listen port on the tunnel interface")
	shellPath   = flags.String("shell", "", "Shell to use for interactive sessions (auto-detected if unset)")
	hostKeyFile = flags.String("host_key_file", "", "SSH host private key file (generates an ephemeral key if empty)")

	usage = `
usage: bb ` + flags.Name() + ` [--grace_period=1m]

Run an SSH server on a user-mode wireguard network connected to
the gateway server.
`
)

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

func setWinsize(f *os.File, w, h int) {
	syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), uintptr(syscall.TIOCSWINSZ),
		uintptr(unsafe.Pointer(&struct{ h, w, x, y uint16 }{uint16(h), uint16(w), 0, 0})))
}

func handleSession(s ssh.Session) {
	ptyReq, winCh, isPty := s.Pty()
	if isPty {
		log.Printf("SSH session opened: user=%s remote=%s pty=%s", s.User(), s.RemoteAddr(), ptyReq.Term)
		defer log.Printf("SSH session closed: user=%s remote=%s", s.User(), s.RemoteAddr())

		cmd := exec.Command(getShell(), "-l")
		cmd.Env = append(os.Environ(), "TERM="+ptyReq.Term)
		f, err := pty.Start(cmd)
		if err != nil {
			fmt.Fprintf(s.Stderr(), "start shell: %v\n", err)
			s.Exit(1)
			return
		}
		defer f.Close()
		go func() {
			for win := range winCh {
				setWinsize(f, win.Width, win.Height)
			}
		}()
		go io.Copy(f, s)
		io.Copy(s, f)
		if err := cmd.Wait(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				s.Exit(exitErr.ExitCode())
				return
			}
		}
		s.Exit(0)
	} else {
		// No PTY: run the provided command, or a non-interactive shell if none given.
		args := s.Command()
		var cmd *exec.Cmd
		if len(args) > 0 {
			log.Printf("SSH exec: user=%s remote=%s cmd=%q", s.User(), s.RemoteAddr(), args)
			cmd = exec.Command(args[0], args[1:]...)
		} else {
			log.Printf("SSH session opened: user=%s remote=%s (no pty)", s.User(), s.RemoteAddr())
			cmd = exec.Command(getShell(), "-l")
		}
		defer log.Printf("SSH session closed: user=%s remote=%s", s.User(), s.RemoteAddr())
		cmd.Env = os.Environ()
		cmd.Stdout = s
		cmd.Stderr = s.Stderr()
		cmd.Stdin = s
		if err := cmd.Run(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				s.Exit(exitErr.ExitCode())
				return
			}
			s.Exit(1)
			return
		}
		s.Exit(0)
	}
}

func HandleSSHServer(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return 1, err
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
	privKey, err := keys.GeneratePrivateKey()
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

	// Deregister runs before grpcConn.Close() (LIFO), freeing the IP and DNS
	// name on the gateway immediately rather than waiting for stale-peer cleanup.
	defer func() {
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := gwClient.Deregister(dctx, &gwpb.DeregisterRequest{PublicKey: privKey.PublicKey().Hex()}); err != nil {
			log.Warnf("deregister: %v", err)
		} else {
			log.Printf("Deregistered from gateway.")
		}
	}()

	rsp, err := gwClient.Register(ctx, &gwpb.RegisterRequest{
		NetworkName: *network,
		PeerName:    *name,
		PublicKey:   privKey.PublicKey().Hex(),
	})
	if err != nil {
		return 1, status.WrapError(err, "registering with gateway")
	}
	log.Printf("Registered: assigned_ip=%s gateway_ip=%s cidr=%s endpoint=%s name=%s",
		rsp.GetAssignedIp(), rsp.GetGatewayIp(), rsp.GetNetworkCidr(), rsp.GetServerEndpoint(), rsp.GetAssignedPeerName())

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

	// Build the SSH server. WireGuard membership is the auth boundary; no SSH
	// credential checking is required. gliderlabs/ssh automatically sets
	// NoClientAuth=true when no auth handlers are configured.
	sshServer := &ssh.Server{IdleTimeout: *idleTimeout}
	hostKeyPath := *hostKeyFile
	if hostKeyPath == "" {
		cacheDir, err := os.UserCacheDir()
		if err != nil {
			return 1, status.WrapError(err, "getting cache dir for host key")
		}
		// Key filename is scoped to the assigned peer name so that a VM
		// resuming with the same name reuses the same key, preventing SSH warnings.
		// A different assigned name (e.g. "myvm-1" due to a conflict) gets a
		// fresh key. Falls back to the assigned IP for peers registered without
		// a name (unique per peer, though not stable across restarts).
		keyID := rsp.GetAssignedPeerName()
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

	listeningMsg := fmt.Sprintf("SSH server listening on %s:%d", rsp.GetAssignedIp(), *sshPort)
	if name := rsp.GetAssignedPeerName(); name != "" {
		listeningMsg += fmt.Sprintf(" (%s.internal)", name)
	}
	log.Print(listeningMsg)
	
	// Idle-shutdown: call sshServer.Shutdown once the grace period elapses with
	// no active sessions. The timer starts immediately to cover the case where
	// no client ever connects.
	var (
		mu             sync.Mutex
		activeSessions int
		idleTimer      *time.Timer
	)
	resetIdleTimer := func() {
		// Must be called with mu held.
		if idleTimer != nil {
			idleTimer.Stop()
		}
		idleTimer = time.AfterFunc(*gracePeriod, func() {
			log.Printf("No active sessions for %s; shutting down.", *gracePeriod)
			shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sshServer.Shutdown(shutCtx)
		})
	}
	mu.Lock()
	resetIdleTimer()
	mu.Unlock()

	sshServer.Handler = func(s ssh.Session) {
		mu.Lock()
		if idleTimer != nil {
			idleTimer.Stop()
			idleTimer = nil
		}
		activeSessions++
		mu.Unlock()
		defer func() {
			mu.Lock()
			activeSessions--
			if activeSessions == 0 {
				resetIdleTimer()
			}
			mu.Unlock()
		}()
		handleSession(s)
	}

	// Catch SIGINT/SIGTERM so the process shuts down via Shutdown() rather than
	// being killed abruptly, ensuring deferred cleanup (Deregister, dev.Close)
	// runs on Ctrl-C or a normal kill signal.
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

	if err := sshServer.Serve(listener); err != nil && err != ssh.ErrServerClosed {
		return 1, status.WrapError(err, "ssh server")
	}

	return 0, nil
}
