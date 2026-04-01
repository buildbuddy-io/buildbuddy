package server

import (
	"context"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
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
	hubIP       = flag.String("gateway.hub_ip", "fd00:bb::1", "Hub IP address for each group's WireGuard network")
	cidr        = flag.String("gateway.cidr", "fd00:bb::/64", "IP pool CIDR (reused per group network)")
	listenPorts = flag.String("gateway.listen_ports", "20000-20002", "UDP port range for WireGuard devices")
	publicHost  = flag.String("gateway.public_host", "localhost", "Public hostname returned to clients as the WireGuard endpoint")
)

// groupNetwork holds the WireGuard device and IP allocation state for a single
// (groupID, networkName) pair.
type groupNetwork struct {
	dev        *device.Device
	names      sync.Map // peer_name (string) → assigned IP (netip.Addr)
	nextHost   int      // next host number to assign; starts at 2 (.1 is the hub)
	listenPort int
	pubKeyHex  string
}

// Gateway manages per-group WireGuard hub devices. Each (groupID, networkName)
// pair gets its own isolated device, CIDR, and port. Peers within the same
// network can reach each other through the hub via IP-level forwarding;
// peers in different networks are completely isolated.
type Gateway struct {
	mu         sync.Mutex
	env        environment.Env
	networks   map[string]*groupNetwork // key: "<groupID>/<networkName>"
	portPool   []int
	hubIP      string
	cidr       string
	publicHost string
}

// New creates a Gateway using configuration from flags. WireGuard devices are
// created lazily on first registration for each (groupID, networkName).
func New(env environment.Env) (*Gateway, error) {
	portPool, err := parsePortRange(*listenPorts)
	if err != nil {
		return nil, err
	}
	return &Gateway{
		env:        env,
		networks:   make(map[string]*groupNetwork),
		portPool:   portPool,
		hubIP:      *hubIP,
		cidr:       *cidr,
		publicHost: *publicHost,
	}, nil
}

// Register authenticates the caller via API key, assigns them a fresh WireGuard
// keypair and IP within their group's network, and returns the full config.
// Every call to Register creates a new peer — idempotency is the caller's
// responsibility (cache the response).
func (g *Gateway) Register(ctx context.Context, req *gwpb.RegisterRequest) (*gwpb.RegisterResponse, error) {
	claims, err := g.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := claims.GetGroupID()

	// Generate the client keypair before taking the lock.
	clientPrivKey, err := keys.GeneratePrivateKey()
	if err != nil {
		return nil, status.InternalErrorf("generate client private key: %s", err)
	}
	clientPubKey := clientPrivKey.PublicKey()

	g.mu.Lock()
	defer g.mu.Unlock()

	gn, err := g.getOrCreateNetwork(groupID, req.GetNetworkName())
	if err != nil {
		return nil, err
	}

	if gn.nextHost > 1<<32-1 {
		return nil, status.ResourceExhaustedError("WireGuard IP pool exhausted for this network")
	}
	assignedIP := networkClientIP(g.cidr, gn.nextHost)
	gn.nextHost++

	// Add the peer to the WireGuard device. Sending only public_key + allowed_ip
	// (no private_key line) updates just this peer without touching the interface
	// config or other existing peers.
	bits := 32
	if netip.MustParseAddr(assignedIP).Is6() {
		bits = 128
	}
	ipc := fmt.Sprintf("public_key=%s\nallowed_ip=%s/%d\n", clientPubKey.String(), assignedIP, bits)
	if err := gn.dev.IpcSet(ipc); err != nil {
		gn.nextHost-- // roll back on failure
		return nil, status.InternalErrorf("add WireGuard peer: %s", err)
	}

	// Register peer name (last-write-wins).
	if name := req.GetPeerName(); name != "" {
		gn.names.Store(name, netip.MustParseAddr(assignedIP))
	}

	log.Infof("gateway: registered peer %s in group %q network %q, assigned %s (name=%q)",
		clientPubKey.String()[:8]+"...", groupID, req.GetNetworkName(), assignedIP, req.GetPeerName())

	return &gwpb.RegisterResponse{
		PrivateKey:      clientPrivKey.String(),
		ServerPublicKey: gn.pubKeyHex,
		ServerEndpoint:  fmt.Sprintf("%s:%d", g.publicHost, gn.listenPort),
		AssignedIp:      assignedIP,
		GatewayIp:       g.hubIP,
		NetworkCidr:     g.cidr,
	}, nil
}

// getOrCreateNetwork returns the groupNetwork for (groupID, networkName),
// creating it if it doesn't exist. Must be called with g.mu held.
func (g *Gateway) getOrCreateNetwork(groupID, networkName string) (*groupNetwork, error) {
	key := groupID + "/" + networkName
	if gn, ok := g.networks[key]; ok {
		return gn, nil
	}

	if len(g.portPool) == 0 {
		return nil, status.ResourceExhaustedError("WireGuard port pool exhausted; all group networks are in use")
	}
	port := g.portPool[0]
	g.portPool = g.portPool[1:]

	serverPrivKey, err := keys.GeneratePrivateKey()
	if err != nil {
		return nil, status.InternalErrorf("generate server private key: %s", err)
	}

	tunDev, err := createForwardingTUN(g.hubIP, 1420)
	if err != nil {
		return nil, status.InternalErrorf("create forwarding TUN: %s", err)
	}

	gn := &groupNetwork{
		dev:        nil, // set below
		nextHost:   2,
		listenPort: port,
		pubKeyHex:  serverPrivKey.PublicKey().String(),
	}
	if err := tunDev.StartDNS(func(name string) (netip.Addr, bool) {
		v, ok := gn.names.Load(name)
		if !ok {
			return netip.Addr{}, false
		}
		return v.(netip.Addr), true
	}); err != nil {
		return nil, status.InternalErrorf("start DNS server: %s", err)
	}

	logger := &device.Logger{
		Verbosef: func(format string, args ...any) { log.Debugf("wg["+key+"]: "+format, args...) },
		Errorf:   func(format string, args ...any) { log.Errorf("wg["+key+"]: "+format, args...) },
	}
	dev := device.NewDevice(tunDev, conn.NewDefaultBind(), logger)

	ipc := fmt.Sprintf("private_key=%s\nlisten_port=%d\n", serverPrivKey.String(), port)
	if err := dev.IpcSet(ipc); err != nil {
		dev.Close()
		return nil, status.InternalErrorf("configure WireGuard device: %s", err)
	}
	if err := dev.Up(); err != nil {
		dev.Close()
		return nil, status.InternalErrorf("bring up WireGuard device: %s", err)
	}

	gn.dev = dev
	g.networks[key] = gn
	log.Infof("gateway: created network %q on port %d (pubkey %s...)", key, port, gn.pubKeyHex[:8])
	return gn, nil
}

// networkClientIP returns the IP address for the given host number within cidr.
// Works for both IPv4 (e.g. "10.0.0.0/24", hostNum=5 → "10.0.0.5") and IPv6
// (e.g. "fd00:bb::/64", hostNum=5 → "fd00:bb::5"). For IPv6, hostNum is
// encoded into the low-order bytes of the host portion.
func networkClientIP(cidr string, hostNum int) string {
	prefix := netip.MustParsePrefix(cidr)
	addr := prefix.Addr()
	if addr.Is4() {
		base := addr.As4()
		base[3] = byte(hostNum)
		return netip.AddrFrom4(base).String()
	}
	base := addr.As16()
	base[15] = byte(hostNum)
	base[14] = byte(hostNum >> 8)
	base[13] = byte(hostNum >> 16)
	base[12] = byte(hostNum >> 24)
	return netip.AddrFrom16(base).String()
}

// parsePortRange parses a "start-end" port range string into a slice of ports.
func parsePortRange(s string) ([]int, error) {
	startStr, endStr, ok := strings.Cut(s, "-")
	if !ok {
		return nil, fmt.Errorf("invalid port range %q: expected \"start-end\"", s)
	}
	start, err := strconv.Atoi(startStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port range %q: %s", s, err)
	}
	end, err := strconv.Atoi(endStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port range %q: %s", s, err)
	}
	if start > end {
		return nil, fmt.Errorf("invalid port range %q: start > end", s)
	}
	ports := make([]int, 0, end-start+1)
	for p := start; p <= end; p++ {
		ports = append(ports, p)
	}
	return ports, nil
}
