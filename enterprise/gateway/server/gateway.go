// Package server implements the BuildBuddy WireGuard gateway.
//
// A single WireGuard device listens on one UDP port and serves all groups.
// Each (groupID, networkName) pair is assigned a unique /48 IPv6 prefix,
// derived from a monotonically increasing index:
//
//	fd00:bb:N::/48  — network N's prefix
//	fd00:bb:N::1    — network N's hub (DNS)
//	fd00:bb:N::2+   — network N's clients, assigned sequentially
//
// Group isolation is enforced inside muxTUN.Write(): packets whose source
// and destination belong to different networks are silently dropped.
package server

import (
	"context"
	"fmt"
	"net/netip"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/keys"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
)

var (
	udpListenPort = flag.Int("gateway.udp_listen_port", 51820, "UDP port for the WireGuard device")
	publicHost = flag.String("gateway.public_host", "localhost", "Public hostname returned to clients as the WireGuard endpoint")
)

// networkState holds IP allocation and peer name state for one
// (groupID, networkName) pair.
type networkState struct {
	index    int      // assigned network index; determines the /48 prefix
	names    sync.Map // peer_name (string) → assigned IP (netip.Addr)
	nextHost int      // next host number to assign; starts at 2 (.1 is the hub)
}

// Gateway manages a single WireGuard device shared across all groups.
// Network isolation is enforced in the muxTUN layer.
type Gateway struct {
	mu         sync.Mutex
	env        environment.Env
	dev        *device.Device
	tun        *muxTUN
	pubKey     string // server's base64 public key
	networks   map[string]*networkState
	nextIndex  int // monotonically increasing network index
	publicHost string
	udpListenPort int
}

// New creates a Gateway with a single shared WireGuard device.
func New(env environment.Env) (*Gateway, error) {
	serverPrivKey, err := keys.GeneratePrivateKey()
	if err != nil {
		return nil, status.InternalErrorf("generate server private key: %s", err)
	}

	tunDev := newMuxTUN(1420)

	logger := &device.Logger{
		Verbosef: func(format string, args ...any) { log.Debugf("wg: "+format, args...) },
		Errorf:   func(format string, args ...any) { log.Errorf("wg: "+format, args...) },
	}
	dev := device.NewDevice(tunDev, conn.NewDefaultBind(), logger)

	ipc := fmt.Sprintf("private_key=%s\nlisten_port=%d\n", serverPrivKey.Hex(), *udpListenPort)
	if err := dev.IpcSet(ipc); err != nil {
		dev.Close()
		return nil, status.InternalErrorf("configure WireGuard device: %s", err)
	}
	if err := dev.Up(); err != nil {
		dev.Close()
		return nil, status.InternalErrorf("bring up WireGuard device: %s", err)
	}

	pubKey := serverPrivKey.PublicKey().Hex()
	log.Infof("gateway: WireGuard device up on port %d (pubkey %s...)", *udpListenPort, pubKey[:8])

	return &Gateway{
		env:        env,
		dev:        dev,
		tun:        tunDev,
		pubKey:     pubKey,
		networks:   make(map[string]*networkState),
		publicHost: *publicHost,
		udpListenPort: *udpListenPort,
	}, nil
}

// Register authenticates the caller, assigns them an IP within their network,
// registers the client as a peer on the shared device, and returns the config.
// The client is responsible for generating its own WireGuard keypair and
// supplying its public key in the request.
func (g *Gateway) Register(ctx context.Context, req *gwpb.RegisterRequest) (*gwpb.RegisterResponse, error) {
	claims, err := g.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := claims.GetGroupID()

	if req.GetPublicKey() == "" {
		return nil, status.InvalidArgumentError("public_key is required")
	}
	clientPubKey, err := keys.ParseHexKey(req.GetPublicKey())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid public_key: %s", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	ns, err := g.getOrCreateNetwork(groupID, req.GetNetworkName())
	if err != nil {
		return nil, err
	}

	if ns.nextHost > 0xFFFF {
		return nil, status.ResourceExhaustedError("IP pool exhausted for this network")
	}
	assignedIP := networkClientIP(ns.index, ns.nextHost)
	ns.nextHost++

	// Register IP→network mapping in the TUN so Write() can enforce isolation.
	g.tun.registerIP(assignedIP, ns.index)

	// Add peer to the shared WireGuard device.
	ipc := fmt.Sprintf("public_key=%s\nallowed_ip=%s/128\n", clientPubKey.Hex(), assignedIP)
	if err := g.dev.IpcSet(ipc); err != nil {
		g.tun.unregisterIP(assignedIP)
		ns.nextHost--
		return nil, status.InternalErrorf("add WireGuard peer: %s", err)
	}

	if name := req.GetPeerName(); name != "" {
		ns.names.Store(name, assignedIP)
	}

	log.Infof("gateway: registered peer %s in group %q network %q, assigned %s (name=%q)",
		clientPubKey.String()[:8]+"...", groupID, req.GetNetworkName(), assignedIP, req.GetPeerName())

	return &gwpb.RegisterResponse{
		ServerPublicKey: g.pubKey,
		ServerEndpoint:  fmt.Sprintf("%s:%d", g.publicHost, g.udpListenPort),
		AssignedIp:      assignedIP.String(),
		GatewayIp:       networkHubIP(ns.index).String(),
		NetworkCidr:     networkPrefix(ns.index).String(),
	}, nil
}

// getOrCreateNetwork returns the networkState for (groupID, networkName),
// creating it if it doesn't exist. Must be called with g.mu held.
func (g *Gateway) getOrCreateNetwork(groupID, networkName string) (*networkState, error) {
	key := groupID + "/" + networkName
	if ns, ok := g.networks[key]; ok {
		return ns, nil
	}

	index := g.nextIndex
	g.nextIndex++

	ns := &networkState{
		index:    index,
		nextHost: 2,
	}

	nameLookup := func(name string) (netip.Addr, bool) {
		v, ok := ns.names.Load(name)
		if !ok {
			return netip.Addr{}, false
		}
		return v.(netip.Addr), true
	}
	if err := g.tun.startNetworkDNS(index, key, nameLookup); err != nil {
		return nil, status.InternalErrorf("start DNS for network %q: %s", key, err)
	}

	g.networks[key] = ns
	log.Infof("gateway: created network %q at index %d (prefix %s)", key, index, networkPrefix(index))
	return ns, nil
}

// ---------------------------------------------------------------------------
// IP helpers — network index N is encoded into bytes [4:6] of fd00:bb::
// ---------------------------------------------------------------------------

// networkPrefix returns fd00:bb:N::/48 for network index N.
// Address layout (each pair of bytes = one IPv6 group):
//
//	[0:2]  = fd00
//	[2:4]  = 00bb  (printed as "bb")
//	[4:6]  = index (the network index, printed as hex)
//	[6:16] = 0
func networkPrefix(index int) netip.Prefix {
	var a [16]byte
	a[0], a[1] = 0xfd, 0x00
	a[3] = 0xbb
	a[4] = byte(index >> 8)
	a[5] = byte(index)
	return netip.PrefixFrom(netip.AddrFrom16(a), 48)
}

// networkHubIP returns fd00:bb:N::1 for network index N.
func networkHubIP(index int) netip.Addr {
	var a [16]byte
	a[0], a[1] = 0xfd, 0x00
	a[3] = 0xbb
	a[4] = byte(index >> 8)
	a[5] = byte(index)
	a[15] = 0x01
	return netip.AddrFrom16(a)
}

// networkClientIP returns the address for host hostNum within network index N.
// hostNum is encoded into bytes [14:16], giving 65534 usable addresses per network.
func networkClientIP(index, hostNum int) netip.Addr {
	var a [16]byte
	a[0], a[1] = 0xfd, 0x00
	a[3] = 0xbb
	a[4] = byte(index >> 8)
	a[5] = byte(index)
	a[14] = byte(hostNum >> 8)
	a[15] = byte(hostNum)
	return netip.AddrFrom16(a)
}
