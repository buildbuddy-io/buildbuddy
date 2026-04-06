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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/wgkeys"
	"github.com/miekg/dns"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
)

var (
	udpListenPort    = flag.Int("gateway.udp_listen_port", 51820, "UDP port for the WireGuard device")
	publicHost       = flag.String("gateway.public_host", "localhost", "Public hostname returned to clients as the WireGuard endpoint")
	stalePeerTimeout = flag.Duration("gateway.stale_peer_timeout", 5*time.Minute, "Time after the last WireGuard handshake before a peer is removed. WireGuard re-handshakes every 3 minutes, so this should be at least that.")
	cleanupInterval  = flag.Duration("gateway.cleanup_interval", time.Minute, "How often to scan for and remove stale peers.")
)

// networkState holds IP allocation and peer name state for one
// (groupID, networkName) pair.
type networkState struct {
	index    int                   // assigned network index; determines the /48 prefix
	namesMu  sync.Mutex            // protects names
	names    map[string]netip.Addr // peer_name → assigned IP
	nextHost int                   // next host number to assign; starts at 2 (.1 is the hub)
}

// peerInfo tracks per-peer state needed for cleanup.
type peerInfo struct {
	ip           netip.Addr
	networkState *networkState
	assignedName string    // empty if peer registered without a name
	registeredAt time.Time // used as last-seen baseline if peer never completed a handshake
}

// Gateway manages a single WireGuard device shared across all groups.
// Network isolation is enforced in the muxTUN layer.
type Gateway struct {
	mu            sync.Mutex
	env           environment.Env
	dev           *device.Device
	tun           *muxTUN
	pubKey        string // server's base64 public key
	networks      map[string]*networkState
	peers         map[string]*peerInfo // WireGuard public key hex → peer info
	nextIndex     int                  // monotonically increasing network index
	publicHost    string
	udpListenPort int
	done          chan struct{}
}

// New creates a Gateway with a single shared WireGuard device.
func New(env environment.Env) (*Gateway, error) {
	serverPrivKey, err := wgkeys.GeneratePrivateKey()
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
	log.Infof("WireGuard device up on port %d (pubkey %s...)", *udpListenPort, pubKey[:8])

	gw := &Gateway{
		env:           env,
		dev:           dev,
		tun:           tunDev,
		pubKey:        pubKey,
		networks:      make(map[string]*networkState),
		peers:         make(map[string]*peerInfo),
		publicHost:    *publicHost,
		udpListenPort: *udpListenPort,
		done:          make(chan struct{}),
	}
	go gw.cleanupLoop()
	return gw, nil
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
	clientPubKey, err := wgkeys.ParseHexKey(req.GetPublicKey())
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

	var assignedName string
	if requested := req.GetPeerName(); requested != "" {
		if labels, ok := dns.IsDomainName(requested); !ok || labels != 1 {
			return nil, status.InvalidArgumentErrorf("peer_name %q is not a valid DNS label", requested)
		}
		// Find an available name: try the requested name first, then append
		// numeric suffixes until we find a free slot.
		assignedName = requested
		ns.namesMu.Lock()
		for i := 1; ; i++ {
			if _, taken := ns.names[assignedName]; !taken {
				break
			}
			assignedName = fmt.Sprintf("%s-%d", requested, i)
		}
		ns.names[assignedName] = assignedIP
		ns.namesMu.Unlock()
	}

	g.peers[clientPubKey.Hex()] = &peerInfo{
		ip:           assignedIP,
		networkState: ns,
		assignedName: assignedName,
		registeredAt: time.Now(),
	}

	log.Infof("Registered peer %s in group %q network %q, assigned %s (name=%q)",
		clientPubKey.String()[:8]+"...", groupID, req.GetNetworkName(), assignedIP, assignedName)

	return &gwpb.RegisterResponse{
		ServerPublicKey:  g.pubKey,
		ServerEndpoint:   fmt.Sprintf("%s:%d", g.publicHost, g.udpListenPort),
		AssignedIp:       assignedIP.String(),
		GatewayIp:        networkHubIP(ns.index).String(),
		NetworkCidr:      networkPrefix(ns.index).String(),
		AssignedPeerName: assignedName,
	}, nil
}

// Deregister removes the calling peer from the gateway immediately. Well-behaved
// clients should call this on clean shutdown rather than waiting for the
// stale-peer cleanup cycle to reclaim the IP and DNS name.
func (g *Gateway) Deregister(ctx context.Context, req *gwpb.DeregisterRequest) (*gwpb.DeregisterResponse, error) {
	if _, err := g.env.GetAuthenticator().AuthenticatedUser(ctx); err != nil {
		return nil, err
	}

	if req.GetPublicKey() == "" {
		return nil, status.InvalidArgumentError("public_key is required")
	}
	pubKeyHex := req.GetPublicKey()
	if _, err := wgkeys.ParseHexKey(pubKeyHex); err != nil {
		return nil, status.InvalidArgumentErrorf("invalid public_key: %s", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	info, ok := g.peers[pubKeyHex]
	if !ok {
		return nil, status.NotFoundErrorf("peer %s... not registered", pubKeyHex[:8])
	}
	g.removePeerLocked(pubKeyHex, info)
	return &gwpb.DeregisterResponse{}, nil
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
		names:    make(map[string]netip.Addr),
		nextHost: 2,
	}

	nameLookup := func(name string) (netip.Addr, bool) {
		ns.namesMu.Lock()
		addr, ok := ns.names[name]
		ns.namesMu.Unlock()
		return addr, ok
	}
	if err := g.tun.startNetworkDNS(index, key, nameLookup); err != nil {
		return nil, status.InternalErrorf("start DNS for network %q: %s", key, err)
	}

	g.networks[key] = ns
	log.Infof("Created network %q at index %d (prefix %s)", key, index, networkPrefix(index))
	return ns, nil
}

// Close stops the cleanup goroutine and shuts down the WireGuard device.
func (g *Gateway) Close() {
	close(g.done)
	g.dev.Close()
}

func (g *Gateway) cleanupLoop() {
	ticker := time.NewTicker(*cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			g.cleanupStalePeers()
		case <-g.done:
			return
		}
	}
}

// cleanupStalePeers removes peers whose last WireGuard handshake (or
// registration time, if they never completed one) is older than
// stalePeerTimeout.
func (g *Gateway) cleanupStalePeers() {
	ipc, err := g.dev.IpcGet()
	if err != nil {
		log.Errorf("Cleanup: IpcGet failed: %s", err)
		return
	}

	// Parse last handshake time per peer from the WireGuard IPC output.
	// Each peer section starts with a "public_key=" line.
	handshakeTimes := make(map[string]time.Time)
	var currentKey string
	for line := range strings.SplitSeq(ipc, "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			switch k {
			case "public_key":
				currentKey = v
			case "last_handshake_time_sec":
				if currentKey != "" {
					if sec, err := strconv.ParseInt(v, 10, 64); err == nil && sec > 0 {
						handshakeTimes[currentKey] = time.Unix(sec, 0)
					}
				}
			}
		}
	}

	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()

	for pubKeyHex, info := range g.peers {
		lastSeen, ok := handshakeTimes[pubKeyHex]
		if !ok {
			// Peer never completed a handshake; use registration time as baseline.
			lastSeen = info.registeredAt
		}
		if now.Sub(lastSeen) >= *stalePeerTimeout {
			g.removePeerLocked(pubKeyHex, info)
		}
	}
}

// removePeerLocked removes a peer from the WireGuard device, the TUN, and the
// DNS name map. Must be called with g.mu held.
func (g *Gateway) removePeerLocked(pubKeyHex string, info *peerInfo) {
	if err := g.dev.IpcSet(fmt.Sprintf("public_key=%s\nremove=true\n", pubKeyHex)); err != nil {
		log.Errorf("Remove WireGuard peer %s...: %s", pubKeyHex[:8], err)
	}
	g.tun.unregisterIP(info.ip)
	if info.assignedName != "" {
		info.networkState.namesMu.Lock()
		delete(info.networkState.names, info.assignedName)
		info.networkState.namesMu.Unlock()
	}
	delete(g.peers, pubKeyHex)
	log.Infof("Removed stale peer %s... (ip=%s name=%q)", pubKeyHex[:8], info.ip, info.assignedName)
}

// ---------------------------------------------------------------------------
// IP helpers — network index N is encoded into bytes [4:6] of fd00:bb::
// ---------------------------------------------------------------------------

// networkIP returns the IPv6 address fd00:bb:N::host for network index N and
// host number host. Address layout (each pair of bytes = one IPv6 group):
//
//	[0:2]  = fd00
//	[2:4]  = 00bb  (printed as "bb")
//	[4:6]  = network index, printed as hex
//	[6:14] = 0
//	[14:16] = host, giving 65534 usable addresses per network
func networkIP(network, host int) netip.Addr {
	var a [16]byte
	a[0], a[1] = 0xfd, 0x00
	a[3] = 0xbb
	a[4] = byte(network >> 8)
	a[5] = byte(network)
	a[14] = byte(host >> 8)
	a[15] = byte(host)
	return netip.AddrFrom16(a)
}

func networkPrefix(index int) netip.Prefix       { return netip.PrefixFrom(networkIP(index, 0), 48) }
func networkHubIP(index int) netip.Addr          { return networkIP(index, 1) }
func networkClientIP(index, host int) netip.Addr { return networkIP(index, host) }
