// SSH server that registers with the WireGuard gateway and serves an
// interactive shell on the tunnel IP.
//
// Usage:
//
//	sshvm --gateway_target=grpc://localhost:1985 --api_key=KEY [--network_name=NAME]
//
// Then connect from another peer on the same network:
//
//	ssh -p 22 <assigned-ip>
package main

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
	"syscall"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/server/util/wgkeys"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/creack/pty"
	"github.com/gliderlabs/ssh"
	gossh "golang.org/x/crypto/ssh"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
	"google.golang.org/grpc/metadata"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
	gwsvcpb "github.com/buildbuddy-io/buildbuddy/proto/gateway_service"
)

var (
	gatewayTarget = flag.String("gateway_target", "grpc://localhost:1985", "gRPC address of the gateway server")
	apiKey        = flag.String("api_key", "", "BuildBuddy API key")
	networkName   = flag.String("network_name", "", "Optional network name (must match across peers)")
	peerName      = flag.String("peer_name", "", "Optional name for this peer; reachable at <name>.internal on the tunnel network (last-write-wins)")
	sshPort       = flag.Int("ssh_port", 22, "SSH listen port on the tunnel interface")
	shellPath     = flag.String("shell", "bash", "Shell binary for interactive sessions")
	hostKeyFile   = flag.String("host_key_file", "", "SSH host private key file (generates an ephemeral key if empty)")
)

func setWinsize(f *os.File, w, h int) {
	syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), uintptr(syscall.TIOCSWINSZ),
		uintptr(unsafe.Pointer(&struct{ h, w, x, y uint16 }{uint16(h), uint16(w), 0, 0})))
}

func handleSession(s ssh.Session) {
	ptyReq, winCh, isPty := s.Pty()
	if isPty {
		log.Infof("SSH session opened: user=%s remote=%s pty=%s", s.User(), s.RemoteAddr(), ptyReq.Term)
		defer log.Infof("SSH session closed: user=%s remote=%s", s.User(), s.RemoteAddr())

		cmd := exec.Command(*shellPath, "-l")
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
			log.Infof("SSH exec: user=%s remote=%s cmd=%q", s.User(), s.RemoteAddr(), args)
			cmd = exec.Command(args[0], args[1:]...)
		} else {
			log.Infof("SSH session opened: user=%s remote=%s (no pty)", s.User(), s.RemoteAddr())
			cmd = exec.Command(*shellPath, "-l")
		}
		defer log.Infof("SSH session closed: user=%s remote=%s", s.User(), s.RemoteAddr())
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

func main() {
	flag.Parse()
	if *apiKey == "" {
		log.Fatal("--api_key is required")
	}

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)

	// Generate a local WireGuard keypair — the private key never leaves this process.
	privKey, err := wgkeys.GeneratePrivateKey()
	if err != nil {
		log.Fatalf("generate WireGuard key: %s", err)
	}

	// Register with the gateway.
	grpcConn, err := grpc_client.DialSimple(*gatewayTarget)
	if err != nil {
		log.Fatalf("dial gateway: %s", err)
	}
	defer grpcConn.Close()

	gwClient := gwsvcpb.NewGatewayServiceClient(grpcConn)
	rsp, err := gwClient.Register(ctx, &gwpb.RegisterRequest{
		NetworkName: *networkName,
		PeerName:    *peerName,
		PublicKey:   privKey.PublicKey().Hex(),
	})
	if err != nil {
		log.Fatalf("Register: %s", err)
	}
	log.Infof("Registered: assigned_ip=%s gateway_ip=%s cidr=%s endpoint=%s name=%s",
		rsp.GetAssignedIp(), rsp.GetGatewayIp(), rsp.GetNetworkCidr(), rsp.GetServerEndpoint(), rsp.GetAssignedPeerName())

	// Bring up the userspace WireGuard tunnel.
	assignedAddr := netip.MustParseAddr(rsp.GetAssignedIp())
	tunDev, tnet, err := netstack.CreateNetTUN(
		[]netip.Addr{assignedAddr},
		[]netip.Addr{netip.MustParseAddr(rsp.GetGatewayIp())},
		1420,
	)
	if err != nil {
		log.Fatalf("create netstack TUN: %s", err)
	}

	wgLogger := &device.Logger{
		Verbosef: func(format string, args ...any) { log.Debugf("wg: "+format, args...) },
		Errorf:   func(format string, args ...any) { log.Errorf("wg: "+format, args...) },
	}
	dev := device.NewDevice(tunDev, conn.NewDefaultBind(), wgLogger)
	endpoint, err := resolveEndpoint(rsp.GetServerEndpoint())
	if err != nil {
		log.Fatalf("resolve WireGuard endpoint: %s", err)
	}
	ipc := fmt.Sprintf(
		"private_key=%s\npublic_key=%s\nallowed_ip=%s\nendpoint=%s\npersistent_keepalive_interval=25\n",
		privKey.Hex(), rsp.GetServerPublicKey(), rsp.GetNetworkCidr(), endpoint,
	)
	if err := dev.IpcSet(ipc); err != nil {
		log.Fatalf("configure WireGuard: %s", err)
	}
	if err := dev.Up(); err != nil {
		log.Fatalf("bring up WireGuard: %s", err)
	}
	defer dev.Close()

	// Configure the SSH server.
	var sshOpts []ssh.Option
	if *hostKeyFile != "" {
		sshOpts = append(sshOpts, ssh.HostKeyFile(*hostKeyFile))
	} else {
		pemBytes, err := generateEphemeralHostKeyPEM()
		if err != nil {
			log.Fatalf("generate host key: %s", err)
		}
		sshOpts = append(sshOpts, ssh.HostKeyPEM(pemBytes))
	}
	// WireGuard is the auth boundary; do not configure any password handler.

	listener, err := tnet.ListenTCP(&net.TCPAddr{Port: *sshPort})
	if err != nil {
		log.Fatalf("listen on tunnel port %d: %s", *sshPort, err)
	}
	log.Infof("SSH server listening on %s:%d", rsp.GetAssignedIp(), *sshPort)
	if name := rsp.GetAssignedPeerName(); name != "" {
		fmt.Printf("Assigned DNS name: %s (connect with: ssh -p %d %s)\n", name, *sshPort, name)
	}

	if err := ssh.Serve(listener, handleSession, sshOpts...); err != nil {
		log.Fatalf("ssh serve: %s", err)
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

// generateEphemeralHostKeyPEM generates a fresh ed25519 key pair and returns
// the private key as an OpenSSH PEM block. The key is not persisted, so clients
// will see a new host key fingerprint on each restart unless --host_key_file is used.
func generateEphemeralHostKeyPEM() ([]byte, error) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	block, err := gossh.MarshalPrivateKey(privateKey, "")
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(block), nil
}
