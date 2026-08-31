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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
	gwsvcpb "github.com/buildbuddy-io/buildbuddy/proto/gateway_service"
)

var (
	udpListenPort             = flag.Int("gateway.udp_listen_port", 51820, "UDP port for the WireGuard device")
	publicHost                = flag.String("gateway.public_host", "localhost", "Public hostname returned to clients as the WireGuard endpoint")
	stalePeerTimeout          = flag.Duration("gateway.stale_peer_timeout", 5*time.Minute, "Time after the last WireGuard handshake before a peer is removed. WireGuard re-handshakes every 3 minutes, so this should be at least that.")
	cleanupInterval           = flag.Duration("gateway.cleanup_interval", time.Minute, "How often to scan for and remove stale peers.")
	heartbeatInterval         = flag.Duration("gateway.connect_heartbeat_interval", 30*time.Second, "How often Connect streams send an empty heartbeat message. Should be well under typical intermediary idle timeouts (usually 60s+).")
	watchFallbackPollInterval = flag.Duration("gateway.watch_fallback_poll_interval", time.Second, "How often Watch streams re-check the watched peer's state without being woken by an event. A backstop: registration changes and WireGuard handshake activity wake watchers directly.")
)

// networkState holds IP allocation and peer name state for one
// (groupID, networkName) pair.
type networkState struct {
	groupID  string                // group that owns this network
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
	sessionID    string    // uniquely identifies this connection
	registeredAt time.Time // used as last-seen baseline if peer never completed a handshake

	// cancel, if set, closes the Connect stream that owns this registration.
	// removePeerLocked calls it so that a peer removed by another path (e.g.
	// the stale-peer sweep) doesn't leave its stream dangling.
	cancel context.CancelFunc
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
	hubServices   []HubService
	nextIndex     int // monotonically increasing network index
	publicHost    string
	udpListenPort int
	done          chan struct{}

	// watchMu guards stateChanged. Separate from mu so that notifyWatchers is
	// safe to call from anywhere: under mu (peer add/remove) and from the
	// WireGuard device's own goroutines (via the logger hook).
	watchMu sync.Mutex
	// stateChanged is closed and replaced whenever peer state may have
	// changed, waking all Watch streams (see watchSignal).
	stateChanged chan struct{}
}

// New creates a Gateway with a single shared WireGuard device.
func New(env environment.Env, hubServices ...HubService) (*Gateway, error) {
	serverPrivKey, err := wgkeys.GeneratePrivateKey()
	if err != nil {
		return nil, status.InternalErrorf("generate server private key: %s", err)
	}

	tunDev := newMuxTUN(1420)

	gw := &Gateway{
		env:           env,
		tun:           tunDev,
		networks:      make(map[string]*networkState),
		peers:         make(map[string]*peerInfo),
		hubServices:   hubServices,
		publicHost:    *publicHost,
		udpListenPort: *udpListenPort,
		done:          make(chan struct{}),
		stateChanged:  make(chan struct{}),
	}

	logger := &device.Logger{
		Verbosef: func(format string, args ...any) {
			log.Debugf("wg: "+format, args...)
			// Handshake activity wakes Watch streams. The device logs a
			// keepalive/handshake line at the moment a peer's last-handshake
			// time is stamped (wireguard-go receive.go: timersHandshakeComplete
			// runs, then "Receiving keepalive packet" is logged), so watchers
			// learn of a completed handshake immediately. The match is
			// deliberately loose: Watch re-reads real state on every wake, so a
			// spurious wake is free and a missed one only costs latency (see
			// watchFallbackPollInterval).
			if strings.Contains(format, "handshake") || strings.Contains(format, "keepalive") {
				gw.notifyWatchers()
			}
		},
		Errorf: func(format string, args ...any) { log.Errorf("wg: "+format, args...) },
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

	gw.dev = dev
	gw.pubKey = pubKey
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
		ns.namesMu.Unlock()
	}

	// Note: session_id is left empty for Register peers — it is a Connect
	// concept, and leaving it unset lets List callers distinguish legacy
	// registrations.
	assignedIP, err := g.addPeerLocked(ns, clientPubKey.Hex(), assignedName, "" /*=sessionID*/, nil /*=cancel*/)
	if err != nil {
		return nil, err
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

// Connect implements the streaming registration API. The peer's registration
// is leased to the stream: the first response carries the tunnel
// configuration, and the peer stays registered exactly as long as the stream
// remains open. Unlike the deprecated Register RPC, peer names are unique
// within a network: a name held by a connected peer causes ALREADY_EXISTS.
func (g *Gateway) Connect(req *gwpb.ConnectRequest, stream gwsvcpb.GatewayService_ConnectServer) error {
	claims, err := g.env.GetAuthenticator().AuthenticatedUser(stream.Context())
	if err != nil {
		return err
	}
	groupID := claims.GetGroupID()

	if req.GetPublicKey() == "" {
		return status.InvalidArgumentError("public_key is required")
	}
	clientPubKey, err := wgkeys.ParseHexKey(req.GetPublicKey())
	if err != nil {
		return status.InvalidArgumentErrorf("invalid public_key: %s", err)
	}
	name := req.GetPeerName()
	if name != "" {
		if labels, ok := dns.IsDomainName(name); !ok || labels != 1 {
			return status.InvalidArgumentErrorf("peer_name %q is not a valid DNS label", name)
		}
	}
	sessionID := req.GetSessionId()
	if sessionID == "" {
		return status.InvalidArgumentError("session_id is required")
	}

	// ctx is canceled when the client goes away (stream closed, connection
	// lost, gateway shutdown). removePeerLocked also cancels it when the peer
	// is removed by another path (e.g. the stale-peer sweep), which closes
	// this stream and lets the client observe its own eviction.
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	g.mu.Lock()
	ns, err := g.getOrCreateNetwork(groupID, req.GetNetworkName())
	if err != nil {
		g.mu.Unlock()
		return err
	}
	if name != "" {
		ns.namesMu.Lock()
		_, taken := ns.names[name]
		ns.namesMu.Unlock()
		if taken {
			g.mu.Unlock()
			return status.AlreadyExistsErrorf("peer name %q is already in use", name)
		}
	}
	// Session IDs uniquely identify connections within a group, so that
	// removal below (and deregistration by session ID, eventually) is
	// unambiguous. Note: peers registered via the deprecated Register RPC
	// have an empty session ID and can never conflict.
	for _, info := range g.peers {
		if info.sessionID == sessionID && info.networkState.groupID == groupID {
			g.mu.Unlock()
			return status.AlreadyExistsErrorf("session_id %q is already in use", sessionID)
		}
	}
	assignedIP, err := g.addPeerLocked(ns, clientPubKey.Hex(), name, sessionID, cancel)
	g.mu.Unlock()
	if err != nil {
		return err
	}

	// Remove the peer, by (key, session_id), when the stream ends.
	defer func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if info, ok := g.peers[clientPubKey.Hex()]; ok && info.sessionID == sessionID {
			g.removePeerLocked(clientPubKey.Hex(), info)
		}
	}()

	if err := stream.Send(&gwpb.ConnectResponse{
		ServerPublicKey: g.pubKey,
		ServerEndpoint:  fmt.Sprintf("%s:%d", g.publicHost, g.udpListenPort),
		AssignedIp:      assignedIP.String(),
		GatewayIp:       networkHubIP(ns.index).String(),
		NetworkCidr:     networkPrefix(ns.index).String(),
	}); err != nil {
		return err
	}

	log.Infof("Connected peer %s in group %q network %q, assigned %s (name=%q session=%s)",
		clientPubKey.String()[:8]+"...", groupID, req.GetNetworkName(), assignedIP, name, sessionID)

	// Send periodic (empty) heartbeat messages. These keep the stream from
	// being closed by idle-sensitive intermediaries while the tunnel is
	// otherwise quiet on the gRPC connection, and surface half-open TCP
	// connections whose peer is long gone. Clients ignore them.
	heartbeat := time.NewTicker(*heartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-heartbeat.C:
			if err := stream.Send(&gwpb.ConnectResponse{}); err != nil {
				return err
			}
		case <-ctx.Done():
			// If the registration is no longer this session's, another path
			// (the stale-peer sweep, Deregister) evicted this peer; tell the
			// client so it can distinguish eviction from a clean shutdown.
			g.mu.Lock()
			info, ok := g.peers[clientPubKey.Hex()]
			evicted := !ok || info.sessionID != sessionID
			g.mu.Unlock()
			if evicted {
				return status.AbortedErrorf("peer evicted: session %s is no longer registered", sessionID)
			}
			return nil
		}
	}
}

// Watch streams state changes for the peer connection with the requested
// session ID in the caller's group. A message is sent when the peer first
// appears (which may be after the watch starts) and whenever its state
// changes, e.g. when its first WireGuard handshake completes. Once a
// previously-reported peer is removed, the stream ends with NotFound.
// Watching a session that never appears blocks until the caller gives up;
// sessions in other groups are indistinguishable from nonexistent ones.
// While the watched peer is quiet the stream carries periodic empty
// heartbeat messages (as Connect does); clients ignore them.
func (g *Gateway) Watch(req *gwpb.WatchRequest, stream gwsvcpb.GatewayService_WatchServer) error {
	claims, err := g.env.GetAuthenticator().AuthenticatedUser(stream.Context())
	if err != nil {
		return err
	}
	groupID := claims.GetGroupID()
	sessionID := req.GetSessionId()
	if sessionID == "" {
		return status.InvalidArgumentError("session_id is required")
	}

	ticker := time.NewTicker(*watchFallbackPollInterval)
	defer ticker.Stop()
	heartbeat := time.NewTicker(*heartbeatInterval)
	defer heartbeat.Stop()

	var last *gwpb.Peer
	loggedSnapshotErr := false
	for {
		// Arm the signal before snapshotting: a state change landing after
		// the snapshot closes the already-obtained channel, so the select
		// below wakes instead of missing it.
		signal := g.watchSignal()
		peer, err := g.snapshotPeer(groupID, sessionID)
		if err != nil && !loggedSnapshotErr {
			// Log once per stream: this runs on every wake, and a
			// persistently failing IpcGet would otherwise flood the log.
			log.Errorf("Watch: %s", err)
			loggedSnapshotErr = true
		}
		if peer == nil && last != nil {
			return status.NotFoundErrorf("session %q is no longer registered", sessionID)
		}
		if peer != nil && !proto.Equal(peer, last) {
			if err := stream.Send(&gwpb.WatchResponse{Peer: peer}); err != nil {
				return err
			}
			last = peer
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-signal:
		case <-ticker.C:
		case <-heartbeat.C:
			// Keep the stream alive through idle-sensitive intermediaries
			// while the watched peer is quiet or not yet registered.
			if err := stream.Send(&gwpb.WatchResponse{}); err != nil {
				return err
			}
		}
	}
}

// notifyWatchers wakes all Watch streams so they re-check peer state.
// Closing the channel broadcasts to every waiter; a fresh channel re-arms
// the signal. Unlike sync.Cond, a channel can be waited on in a select
// alongside the stream context and the fallback ticker.
func (g *Gateway) notifyWatchers() {
	g.watchMu.Lock()
	close(g.stateChanged)
	g.stateChanged = make(chan struct{})
	g.watchMu.Unlock()
}

// watchSignal returns a channel that is closed on the next state change.
// Callers must obtain the channel before reading the state they act on, so
// that a change landing between the read and the wait still wakes them.
func (g *Gateway) watchSignal() <-chan struct{} {
	g.watchMu.Lock()
	defer g.watchMu.Unlock()
	return g.stateChanged
}

// snapshotPeer returns the current state of the peer with the given session
// ID in groupID, or nil if no such peer is registered. A non-nil error
// reports a failure to read handshake state; the returned peer is still
// valid, with last_handshake_time left unset.
func (g *Gateway) snapshotPeer(groupID, sessionID string) (*gwpb.Peer, error) {
	// Locate the peer before touching the WireGuard device: dumping device
	// state (lastHandshakeTimes) is O(all peers), and the common case for a
	// watch on a still-booting session is that the peer doesn't exist yet.
	var p *gwpb.Peer
	var pubKeyHex string
	g.mu.Lock()
	for k, info := range g.peers {
		if info.sessionID != sessionID || info.networkState.groupID != groupID {
			continue
		}
		pubKeyHex = k
		p = &gwpb.Peer{
			Name:      info.assignedName,
			Ip:        info.ip.String(),
			SessionId: info.sessionID,
		}
		break
	}
	g.mu.Unlock()
	if p == nil {
		return nil, nil
	}
	handshakeTimes, err := g.lastHandshakeTimes()
	if err != nil {
		return p, err
	}
	if t, ok := handshakeTimes[pubKeyHex]; ok {
		p.LastHandshakeTime = timestamppb.New(t)
	}
	return p, nil
}

// addPeerLocked allocates an IP in ns, registers it with the TUN and the
// WireGuard device, and records the peer. assignedName and sessionID must
// already be validated (and checked for conflicts) by the caller. Must be
// called with g.mu held.
func (g *Gateway) addPeerLocked(ns *networkState, pubKeyHex, assignedName, sessionID string, cancel context.CancelFunc) (netip.Addr, error) {
	if _, ok := g.peers[pubKeyHex]; ok {
		return netip.Addr{}, status.AlreadyExistsErrorf("public key %s... is already registered", pubKeyHex[:8])
	}
	if ns.nextHost > 0xFFFF {
		return netip.Addr{}, status.ResourceExhaustedError("IP pool exhausted for this network")
	}
	assignedIP := networkClientIP(ns.index, ns.nextHost)
	ns.nextHost++

	// Register IP→network mapping in the TUN so Write() can enforce isolation.
	g.tun.registerIP(assignedIP, ns.index)

	// Add peer to the shared WireGuard device.
	ipc := fmt.Sprintf("public_key=%s\nallowed_ip=%s/128\n", pubKeyHex, assignedIP)
	if err := g.dev.IpcSet(ipc); err != nil {
		g.tun.unregisterIP(assignedIP)
		ns.nextHost--
		return netip.Addr{}, status.InternalErrorf("add WireGuard peer: %s", err)
	}

	if assignedName != "" {
		ns.namesMu.Lock()
		ns.names[assignedName] = assignedIP
		ns.namesMu.Unlock()
	}
	g.peers[pubKeyHex] = &peerInfo{
		ip:           assignedIP,
		networkState: ns,
		assignedName: assignedName,
		sessionID:    sessionID,
		registeredAt: time.Now(),
		cancel:       cancel,
	}
	g.notifyWatchers()
	return assignedIP, nil
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

// List returns the peers currently registered by the caller's group. Every
// Connect-registered peer has a session ID and is included, named or not;
// peers from the deprecated Register RPC have no session ID and are omitted.
func (g *Gateway) List(ctx context.Context, req *gwpb.ListRequest) (*gwpb.ListResponse, error) {
	claims, err := g.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := claims.GetGroupID()

	handshakeTimes, err := g.lastHandshakeTimes()
	if err != nil {
		log.Errorf("List: %s", err)
		// Proceed without handshake info; last_handshake_time is left unset.
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	peers := make([]*gwpb.Peer, 0)
	for pubKeyHex, info := range g.peers {
		if info.sessionID == "" {
			continue
		}
		ns := info.networkState
		if ns.groupID != groupID {
			continue
		}
		p := &gwpb.Peer{
			Name:      info.assignedName,
			Ip:        info.ip.String(),
			SessionId: info.sessionID,
		}
		if t, ok := handshakeTimes[pubKeyHex]; ok {
			p.LastHandshakeTime = timestamppb.New(t)
		}
		peers = append(peers, p)
	}
	return &gwpb.ListResponse{Peers: peers}, nil
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
		groupID:  groupID,
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
	if err := g.tun.startNetworkServices(index, key, g.hubServices, nameLookup); err != nil {
		return nil, status.InternalErrorf("start services for network %q: %s", key, err)
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
// stalePeerTimeout. For Connect-based peers this is a backstop: their
// registrations are normally removed when their stream closes, but this
// catches peers whose stream is somehow alive while their tunnel is dark.
func (g *Gateway) cleanupStalePeers() {
	handshakeTimes, err := g.lastHandshakeTimes()
	if err != nil {
		log.Errorf("Cleanup: %s", err)
		return
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
			log.Infof("Found STALE peer %s... (ip=%s name=%q)", pubKeyHex[:8], info.ip, info.assignedName)
			g.removePeerLocked(pubKeyHex, info)
		}
	}
}

// lastHandshakeTimes parses the last handshake time per peer public key from
// the WireGuard IPC output. Each peer section starts with a "public_key="
// line. Peers that never completed a handshake are absent from the map.
func (g *Gateway) lastHandshakeTimes() (map[string]time.Time, error) {
	ipc, err := g.dev.IpcGet()
	if err != nil {
		return nil, status.InternalErrorf("IpcGet failed: %s", err)
	}
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
	return handshakeTimes, nil
}

// removePeerLocked removes a peer from the WireGuard device, the TUN, and the
// DNS name map, and closes the peer's Connect stream if it has one. Must be
// called with g.mu held.
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
	if info.cancel != nil {
		info.cancel()
	}
	g.notifyWatchers()
	log.Infof("Removed peer %s... (ip=%s name=%q session=%s)", pubKeyHex[:8], info.ip, info.assignedName, info.sessionID)
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
