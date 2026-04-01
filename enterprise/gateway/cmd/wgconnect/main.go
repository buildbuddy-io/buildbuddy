// wg-connect is an SSH ProxyCommand helper that dials a target host:port
// through the WireGuard gateway tunnel and proxies stdin/stdout.
//
// Peers that registered with a peer_name can be addressed by name directly.
// Add to ~/.ssh/config:
//
//	Host *.wg
//	    ProxyCommand wg-connect --api_key=KEY --network_name=NAME --target=%h:%p
//
// Then connect normally:
//
//	ssh myvm.wg
package main

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
	"google.golang.org/grpc/metadata"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
)

var (
	gatewayTarget = flag.String("gateway_target", "grpc://localhost:9473", "gRPC address of the gateway server")
	apiKey        = flag.String("api_key", "", "BuildBuddy API key")
	networkName   = flag.String("network_name", "", "Network name (must match the target peer)")
	target        = flag.String("target", "", "Target address as host:port; host may be a peer name or IP")
)

func main() {
	flag.Parse()
	if *apiKey == "" {
		log.Fatal("--api_key is required")
	}
	if *target == "" {
		log.Fatal("--target is required")
	}

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)

	// Register with the gateway.
	grpcConn, err := grpc_client.DialSimple(*gatewayTarget)
	if err != nil {
		log.Fatalf("dial gateway: %s", err)
	}
	defer grpcConn.Close()

	gwClient := gwpb.NewGatewayServiceClient(grpcConn)
	rsp, err := gwClient.Register(ctx, &gwpb.RegisterRequest{NetworkName: *networkName})
	if err != nil {
		log.Fatalf("Register: %s", err)
	}

	// Bring up the userspace WireGuard tunnel.
	// Pass the gateway IP as DNS so peer names registered via peer_name resolve.
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
		Verbosef: func(format string, args ...any) {}, // discard routine WireGuard noise
		Errorf:   func(format string, args ...any) { fmt.Fprintf(os.Stderr, "wg: "+format+"\n", args...) },
	}
	dev := device.NewDevice(tunDev, conn.NewDefaultBind(), wgLogger)
	ipc := fmt.Sprintf(
		"private_key=%s\npublic_key=%s\nallowed_ip=%s\nendpoint=%s\npersistent_keepalive_interval=25\n",
		rsp.GetPrivateKey(), rsp.GetServerPublicKey(), rsp.GetNetworkCidr(), rsp.GetServerEndpoint(),
	)
	if err := dev.IpcSet(ipc); err != nil {
		log.Fatalf("configure WireGuard: %s", err)
	}
	if err := dev.Up(); err != nil {
		log.Fatalf("bring up WireGuard: %s", err)
	}
	defer dev.Close()

	// Dial the target through the tunnel. DialContext resolves hostnames via
	// the tunnel's DNS server (gateway_ip:53) so peer names work directly.
	tcpConn, err := tnet.DialContext(ctx, "tcp", *target)
	if err != nil {
		log.Fatalf("dial %s: %s", *target, err)
	}
	defer tcpConn.Close()

	// Proxy stdin/stdout ↔ tcpConn. When the server closes the connection,
	// copy-to-stdout returns and we close tcpConn, which unblocks copy-from-stdin.
	go func() {
		io.Copy(tcpConn, os.Stdin)
		if tc, ok := tcpConn.(interface{ CloseWrite() error }); ok {
			tc.CloseWrite()
		}
	}()
	io.Copy(os.Stdout, tcpConn)
}
