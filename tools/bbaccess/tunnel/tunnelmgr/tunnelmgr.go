// Package tunnelmgr brings up userspace WireGuard tunnels to gateways on
// demand and dials targets through them.
package tunnelmgr

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/relaywire"
	"github.com/buildbuddy-io/buildbuddy/server/util/wgkeys"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
	gstatus "google.golang.org/grpc/status"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
	gwsvcpb "github.com/buildbuddy-io/buildbuddy/proto/gateway_service"
)

// dialTimeout bounds a single relay dial, including gateway-side resolution.
const dialTimeout = 20 * time.Second

// handshakeStaleAfter is how long without a completed WireGuard handshake we
// treat a tunnel as dead. WireGuard re-handshakes every ~2 minutes while
// traffic flows, so anything past ~3.5 minutes means the gateway forgot us.
// This is a backstop as the Connect stream ending usually reports eviction well
// before the handshakes go quiet.
const handshakeStaleAfter = 210 * time.Second

// failureBackoff is how long a failed bring-up is remembered.
const failureBackoff = 5 * time.Second

// Credentials authenticates gateway RPCs.
//
// Attach is given the WireGuard public key the call is about, because a tunnel
// certificate credential is only valid for the key it names; see
// server/util/relayauth.
type Credentials interface {
	Attach(ctx context.Context, zone tunnelconfig.Zone, wgPublicKey string) (context.Context, error)
}

// Manager owns one tunnel per gateway target.
type Manager struct {
	creds       Credentials
	idleTimeout time.Duration

	mu       sync.Mutex
	tunnels  map[string]*tunnel      // keyed by gateway target
	failures map[string]*failedSetup // recent bring-up failures, keyed the same way
}

type failedSetup struct {
	err   error
	until time.Time
}

func New(creds Credentials, idleTimeout time.Duration) *Manager {
	return &Manager{
		creds:       creds,
		idleTimeout: idleTimeout,
		tunnels:     make(map[string]*tunnel),
		failures:    make(map[string]*failedSetup),
	}
}

// tunnel is a live (or pending) WireGuard connection to one gateway.
type tunnel struct {
	target string
	zone   tunnelconfig.Zone
	creds  Credentials
	// dropSelf removes this tunnel from its manager, so the stream watcher can
	// retire an evicted tunnel and the next use registers a fresh one.
	dropSelf func(*tunnel)
	// failSelf retires this tunnel after a failed bring-up and records the
	// failure, so that callers back off.
	failSelf func(*tunnel, error)

	mu           sync.Mutex
	ready        chan struct{} // closed when up or failed
	up           bool
	err          error
	privKey      wgkeys.Key
	dev          *device.Device
	tnet         *netstack.Net
	grpcConn     *grpc_client.ClientConnPool
	cancelStream context.CancelFunc // ends the Connect stream = deregisters
	hubIP        netip.Addr
	assigned     netip.Addr

	activeConns int
	lastUsed    time.Time
	closed      bool // set by close, so an in-flight bring-up does not publish

	closeOnce sync.Once
}

// Dial opens a connection to host:port through the gateway serving zone,
// bringing up the tunnel first if needed.
//
// The name is resolved by the gateway.
func (m *Manager) Dial(ctx context.Context, zone tunnelconfig.Zone, host string, port int) (net.Conn, error) {
	t, err := m.get(ctx, zone)
	if err != nil {
		return nil, err
	}

	conn, err := t.dial(ctx, host, port)
	if err == nil {
		return conn, nil
	}
	// A tunnel that was up but is no longer usable is almost always a gateway
	// restart: it has a new server key and has forgotten our peer, so nothing
	// short of a fresh Register will work. Rebuild once and retry.
	if !isRetriable(err) {
		return nil, err
	}
	// Only rebuild if the tunnel really looks dead. Tearing down a tunnel that
	// is merely slow would kill the established sessions riding it.
	if !t.handshakeStale() && !t.idle(0) {
		return nil, err
	}
	log.Debugf("tunnel to %s looks stale (%s); re-registering", zone.Gateway, err)
	m.drop(zone.Gateway, t)
	t2, err2 := m.get(ctx, zone)
	if err2 != nil {
		return nil, err2
	}
	return t2.dial(ctx, host, port)
}

// Prewarm brings up the tunnel for a zone without dialing anything, so that the
// handshake overlaps with whatever the application does between resolving a
// name and connecting to it.
func (m *Manager) Prewarm(ctx context.Context, zone tunnelconfig.Zone) {
	if _, err := m.get(ctx, zone); err != nil {
		log.Debugf("prewarm of %s failed: %s", zone.Gateway, err)
	}
}

// get returns a ready tunnel for the zone, starting one if necessary.
// Concurrent callers wait on the same bring-up rather than racing to register
// several peers.
func (m *Manager) get(ctx context.Context, zone tunnelconfig.Zone) (*tunnel, error) {
	for attempt := 0; ; attempt++ {
		m.mu.Lock()
		if f, ok := m.failures[zone.Gateway]; ok {
			if time.Now().Before(f.until) {
				m.mu.Unlock()
				return nil, f.err
			}
			delete(m.failures, zone.Gateway)
		}
		t, ok := m.tunnels[zone.Gateway]
		if !ok {
			t = &tunnel{
				target:   zone.Gateway,
				zone:     zone,
				creds:    m.creds,
				dropSelf: func(tt *tunnel) { m.drop(tt.target, tt) },
				failSelf: func(tt *tunnel, err error) { m.dropFailed(tt.target, tt, err) },
				ready:    make(chan struct{}),
				lastUsed: time.Now(),
			}
			m.tunnels[zone.Gateway] = t
			go t.bringUp()
		}
		m.mu.Unlock()

		select {
		case <-t.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		t.mu.Lock()
		up, bringUpErr := t.up, t.err
		t.mu.Unlock()
		if up {
			return t, nil
		}
		if bringUpErr != nil {
			// The bring-up retired the tunnel and armed the backoff itself.
			return nil, bringUpErr
		}
		// The tunnel came up and was torn down again between the map lookup
		// and here — an idle reap or another caller's retry. Its replacement
		// is one loop away.
		if attempt == 2 {
			return nil, fmt.Errorf("tunnel to %s keeps being torn down while connecting", zone.Gateway)
		}
	}
}

// drop removes t from the manager (if it is still the current tunnel) and
// closes it.
func (m *Manager) drop(target string, t *tunnel) {
	m.mu.Lock()
	if m.tunnels[target] == t {
		delete(m.tunnels, target)
	}
	m.mu.Unlock()
	t.close()
}

// dropFailed retires a tunnel whose bring-up failed and records the failure
// so that callers back off, in one step: the record can never land on a
// replacement that another caller has since brought up.
func (m *Manager) dropFailed(target string, t *tunnel, err error) {
	m.mu.Lock()
	if m.tunnels[target] == t {
		delete(m.tunnels, target)
		m.failures[target] = &failedSetup{err: err, until: time.Now().Add(failureBackoff)}
	}
	m.mu.Unlock()
	t.close()
}

// session holds the resources of one registration with a gateway. They are
// created together and torn down together.
type session struct {
	privKey      wgkeys.Key
	grpcConn     *grpc_client.ClientConnPool
	stream       gwsvcpb.GatewayService_ConnectClient
	cancelStream context.CancelFunc
	dev          *device.Device
	tnet         *netstack.Net
	assigned     netip.Addr
	hubIP        netip.Addr
}

// bringUp establishes a session and publishes it, then unblocks everyone
// waiting on this tunnel. It always closes t.ready.
func (t *tunnel) bringUp() {
	s, err := t.connect()

	// Publish under the lock: Manager.Close can be tearing the daemon down
	// while this is still running.
	t.mu.Lock()
	abandoned := t.closed
	switch {
	case err != nil:
		t.err = err
	case abandoned:
		// The tunnel was closed while we were connecting. Publishing now would
		// leave a live, registered WireGuard device that nothing owns and
		// nothing will ever close.
		t.err = fmt.Errorf("tunnel to %s was closed while connecting", t.target)
	default:
		t.privKey, t.grpcConn, t.cancelStream = s.privKey, s.grpcConn, s.cancelStream
		t.dev, t.tnet = s.dev, s.tnet
		t.assigned, t.hubIP = s.assigned, s.hubIP
		t.up = true
	}
	close(t.ready)
	t.mu.Unlock()

	switch {
	case abandoned && err == nil:
		// Canceling the stream is the deregistration.
		s.close()
	case err != nil:
		// Retire the failed tunnel here rather than in a waiting caller, which
		// may have given up: the entry must not outlive the attempt, or the
		// next caller is served this failure long after the gateway recovered.
		t.failSelf(t, err)
	default:
		go t.watchStream(s.stream)
	}
}

// watchStream drains the Connect stream's heartbeats. The registration is
// leased to the stream, so the stream ending means the gateway no longer knows
// this peer — it evicted us (credential expiry, stale-peer sweep, restart) or
// the connection died. Either way the tunnel is retired so the next use
// registers afresh; established connections riding it are already dead.
func (t *tunnel) watchStream(stream gwsvcpb.GatewayService_ConnectClient) {
	for {
		if _, err := stream.Recv(); err != nil {
			t.mu.Lock()
			closed := t.closed
			t.mu.Unlock()
			// Our own close cancels the stream; only an end we didn't ask for
			// is an eviction.
			if !closed {
				log.Debugf("gateway %s ended our session (%s); will re-register on next use", t.target, err)
				if t.dropSelf != nil {
					t.dropSelf(t)
				}
			}
			return
		}
	}
}

// connect registers with the gateway and starts a userspace WireGuard device.
// On failure it releases whatever it had already created, since nothing else
// has a reference to it yet.
func (t *tunnel) connect() (_ *session, err error) {
	start := time.Now()
	s := &session{}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	s.privKey, err = wgkeys.GeneratePrivateKey()
	if err != nil {
		return nil, fmt.Errorf("generating wireguard key: %w", err)
	}

	s.grpcConn, err = grpc_client.DialSimple(t.target)
	if err != nil {
		return nil, fmt.Errorf("dialing gateway %s: %w", t.target, err)
	}
	client := gwsvcpb.NewGatewayServiceClient(s.grpcConn)

	authCtx, err := t.authContext(context.Background(), s.privKey.PublicKey().Hex())
	if err != nil {
		return nil, fmt.Errorf("authenticating with gateway %s: %w", t.target, err)
	}
	// The registration is leased to this stream, so its context lives as long
	// as the session — canceling it later is how the tunnel deregisters.
	streamCtx, cancelStream := context.WithCancel(authCtx)
	s.cancelStream = cancelStream
	s.stream, err = client.Connect(streamCtx, &gwpb.ConnectRequest{
		PublicKey: s.privKey.PublicKey().Hex(),
		SessionId: newSessionID(),
	})
	if err != nil {
		return nil, fmt.Errorf("registering with gateway %s: %w", t.target, err)
	}

	// The first message carries the tunnel config; don't wait on it forever.
	rsp, err := recvWithTimeout(s.stream, 30*time.Second, cancelStream)
	if err != nil {
		return nil, fmt.Errorf("registering with gateway %s: %w", t.target, err)
	}

	s.assigned, err = netip.ParseAddr(rsp.GetAssignedIp())
	if err != nil {
		return nil, fmt.Errorf("gateway returned an unparseable address %q: %w", rsp.GetAssignedIp(), err)
	}
	s.hubIP, err = netip.ParseAddr(rsp.GetGatewayIp())
	if err != nil {
		return nil, fmt.Errorf("gateway returned an unparseable hub address %q: %w", rsp.GetGatewayIp(), err)
	}

	tunDev, tnet, err := netstack.CreateNetTUN([]netip.Addr{s.assigned}, []netip.Addr{s.hubIP}, 1420)
	if err != nil {
		return nil, fmt.Errorf("creating userspace network stack: %w", err)
	}
	s.tnet = tnet

	wgLogger := &device.Logger{
		Verbosef: func(format string, args ...any) { log.Debugf("wg: "+format, args...) },
		Errorf:   func(format string, args ...any) { log.Debugf("wg: "+format, args...) },
	}
	s.dev = device.NewDevice(tunDev, conn.NewDefaultBind(), wgLogger)

	endpoint, err := resolveEndpoint(rsp.GetServerEndpoint())
	if err != nil {
		return nil, fmt.Errorf("resolving gateway endpoint %q: %w", rsp.GetServerEndpoint(), err)
	}
	ipc := fmt.Sprintf(
		"private_key=%s\npublic_key=%s\nallowed_ip=%s\nendpoint=%s\npersistent_keepalive_interval=25\n",
		s.privKey.Hex(), rsp.GetServerPublicKey(), rsp.GetNetworkCidr(), endpoint,
	)
	if err := s.dev.IpcSet(ipc); err != nil {
		return nil, fmt.Errorf("configuring wireguard: %w", err)
	}
	if err := s.dev.Up(); err != nil {
		return nil, fmt.Errorf("bringing up wireguard: %w", err)
	}

	log.Debugf("tunnel to %s up in %s (assigned %s, hub %s)",
		t.target, time.Since(start).Round(time.Millisecond), s.assigned, s.hubIP)
	return s, nil
}

// close releases a session's resources. Canceling the Connect stream is what
// releases the gateway-side registration; there is no separate deregister
// call, so cleanup cannot be blocked by an expired credential.
func (s *session) close() {
	if s.cancelStream != nil {
		s.cancelStream()
	}
	if s.dev != nil {
		s.dev.Close()
	}
	if s.grpcConn != nil {
		s.grpcConn.Close()
	}
}

// newSessionID returns a session ID unique enough across one employee's
// daemons. The hostname prefix makes `bbaccess tunnel status` and the gateway's
// List output readable.
func newSessionID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "daemon"
	}
	if i := strings.IndexByte(host, '.'); i > 0 {
		host = host[:i]
	}
	b := make([]byte, 6)
	rand.Read(b)
	return fmt.Sprintf("%s-%x", host, b)
}

// recvWithTimeout waits for one stream message for at most d. On timeout it
// cancels the stream, which unblocks the pending Recv.
func recvWithTimeout(stream gwsvcpb.GatewayService_ConnectClient, d time.Duration, cancel context.CancelFunc) (*gwpb.ConnectResponse, error) {
	type result struct {
		rsp *gwpb.ConnectResponse
		err error
	}
	ch := make(chan result, 1)
	go func() {
		rsp, err := stream.Recv()
		ch <- result{rsp, err}
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.rsp, r.err
	case <-timer.C:
		cancel()
		return nil, fmt.Errorf("timed out waiting for the tunnel config")
	}
}

// resolveEndpoint resolves the hostname in a host:port endpoint. WireGuard's
// IPC parser requires an IP address, not a hostname.
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
	if len(addrs) == 0 {
		return "", fmt.Errorf("no addresses for %q", host)
	}
	return net.JoinHostPort(addrs[0], port), nil
}

func (t *tunnel) authContext(ctx context.Context, wgPublicKey string) (context.Context, error) {
	if t.creds == nil {
		return ctx, nil
	}
	return t.creds.Attach(ctx, t.zone, wgPublicKey)
}

// dial opens a relay connection to host:port through this tunnel.
func (t *tunnel) dial(ctx context.Context, host string, port int) (net.Conn, error) {
	t.mu.Lock()
	tnet, hubIP := t.tnet, t.hubIP
	t.activeConns++
	t.lastUsed = time.Now()
	t.mu.Unlock()

	release := func() {
		t.mu.Lock()
		t.activeConns--
		t.lastUsed = time.Now()
		t.mu.Unlock()
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	relayAddr := net.JoinHostPort(hubIP.String(), strconv.Itoa(relaywire.DefaultPort))
	relayConn, err := tnet.DialContext(dialCtx, "tcp", relayAddr)
	if err != nil {
		release()
		return nil, fmt.Errorf("connecting to the relay on %s: %w", t.target, err)
	}

	// Bound the handshake too: a tunnel whose peer has forgotten us accepts the
	// SYN into gVisor's queue and then goes quiet.
	if deadline, ok := dialCtx.Deadline(); ok {
		relayConn.SetDeadline(deadline)
	}
	if err := relaywire.Connect(relayConn, host, port); err != nil {
		relayConn.Close()
		release()
		return nil, err
	}
	relayConn.SetDeadline(time.Time{})

	return &trackedConn{Conn: relayConn, release: release}, nil
}

// trackedConn decrements the tunnel's active connection count exactly once,
// when the connection closes, so idle teardown does not cut a live session.
type trackedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// CloseWrite and CloseRead must be forwarded explicitly: embedding net.Conn
// hides them, and a wrapper that silently drops CloseWrite turns a half-close
// into a full close. That truncates every protocol that says "I'm done sending,
// now send me the rest" — `git fetch`, `ssh host cmd`, HTTP request bodies.
func (c *trackedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return fmt.Errorf("underlying connection does not support half-close")
}

func (c *trackedConn) CloseRead() error {
	if cr, ok := c.Conn.(interface{ CloseRead() error }); ok {
		return cr.CloseRead()
	}
	return fmt.Errorf("underlying connection does not support half-close")
}

// close deregisters from the gateway and tears down the WireGuard device.
// Deregistration is just ending the Connect stream: the gateway frees the
// address and DNS name the moment the stream closes, with no extra RPC and no
// fresh credential needed.
//
// Several callers can race to drop the same tunnel — multiple waiters on a
// failed bring-up, or the idle reaper and a dial-time retry — so the work runs
// exactly once.
func (t *tunnel) close() {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		dev, grpcConn, cancelStream := t.dev, t.grpcConn, t.cancelStream
		t.up = false
		t.closed = true
		t.mu.Unlock()

		if cancelStream != nil {
			cancelStream()
		}
		if dev != nil {
			dev.Close()
		}
		if grpcConn != nil {
			grpcConn.Close()
		}
	})
}

// idle reports whether the tunnel has had no connections for at least d.
func (t *tunnel) idle(d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.activeConns == 0 && time.Since(t.lastUsed) >= d
}

// lastHandshake returns the most recent WireGuard handshake time across peers,
// parsed out of the device's IPC state.
func (t *tunnel) lastHandshake() (time.Time, bool) {
	t.mu.Lock()
	dev := t.dev
	t.mu.Unlock()
	if dev == nil {
		return time.Time{}, false
	}
	ipc, err := dev.IpcGet()
	if err != nil {
		return time.Time{}, false
	}
	var latest time.Time
	for line := range strings.SplitSeq(ipc, "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok || k != "last_handshake_time_sec" {
			continue
		}
		var sec int64
		if _, err := fmt.Sscanf(v, "%d", &sec); err != nil || sec <= 0 {
			continue
		}
		if ts := time.Unix(sec, 0); ts.After(latest) {
			latest = ts
		}
	}
	return latest, !latest.IsZero()
}

// ReapIdle tears down tunnels nobody is using, and tunnels whose peer has
// stopped handshaking. Call it periodically.
func (m *Manager) ReapIdle() {
	m.mu.Lock()
	snapshot := maps.Clone(m.tunnels)
	m.mu.Unlock()

	for target, t := range snapshot {
		t.mu.Lock()
		up := t.up
		t.mu.Unlock()
		if !up {
			continue
		}

		reason := ""
		switch {
		case m.idleTimeout > 0 && t.idle(m.idleTimeout):
			reason = "idle"
		case t.handshakeStale():
			// Deliberately NOT gated on having no active connections. When a
			// gateway restarts it forgets our peer, and no FIN ever crosses the
			// dead tunnel — so the connections riding it hang forever and their
			// counts never drop. Waiting for them to finish means waiting
			// forever, and the tunnel plus its device leak for the life of the
			// daemon. Those connections are already dead; closing the tunnel is
			// what turns them into errors the application can see.
			reason = "peer stopped handshaking"
		}
		if reason == "" {
			continue
		}
		log.Debugf("tearing down tunnel to %s (%s)", target, reason)
		m.drop(target, t)
	}
}

// handshakeStale reports whether the tunnel has gone too long without a
// completed WireGuard handshake, which means the gateway no longer knows us.
func (t *tunnel) handshakeStale() bool {
	ts, ok := t.lastHandshake()
	if !ok {
		// Never handshaked. Give a tunnel that was just created time to finish
		// connecting before declaring it dead.
		t.mu.Lock()
		defer t.mu.Unlock()
		return time.Since(t.lastUsed) > handshakeStaleAfter
	}
	return time.Since(ts) > handshakeStaleAfter
}

// Close tears down every tunnel, deregistering from each gateway.
func (m *Manager) Close() {
	m.mu.Lock()
	tunnels := m.tunnels
	m.tunnels = make(map[string]*tunnel)
	m.mu.Unlock()
	for _, t := range tunnels {
		t.close()
	}
}

// Status describes a live tunnel, for `bbaccess tunnel status`.
type Status struct {
	Gateway       string
	AssignedIP    string
	HubIP         string
	ActiveConns   int
	IdleFor       time.Duration
	LastHandshake time.Time
}

func (m *Manager) Status() []Status {
	m.mu.Lock()
	tunnels := maps.Clone(m.tunnels)
	m.mu.Unlock()

	out := make([]Status, 0, len(tunnels))
	for target, t := range tunnels {
		t.mu.Lock()
		if !t.up {
			t.mu.Unlock()
			continue
		}
		s := Status{
			Gateway:     target,
			AssignedIP:  t.assigned.String(),
			HubIP:       t.hubIP.String(),
			ActiveConns: t.activeConns,
		}
		if t.activeConns == 0 {
			s.IdleFor = time.Since(t.lastUsed)
		}
		t.mu.Unlock()
		if ts, ok := t.lastHandshake(); ok {
			s.LastHandshake = ts
		}
		out = append(out, s)
	}
	return out
}

// isRetriable reports whether an error is the kind that a fresh registration
// would plausibly fix. A relay refusal arrives as a gRPC status error (see
// relaywire.Connect); it means the relay answered us, so the tunnel is fine
// and the target or the policy is at fault — re-registering would just
// produce a second identical failure and a second audit log line. Errors from
// the tunnel itself (dial failures, timeouts, resets) carry no status.
func isRetriable(err error) bool {
	var se interface{ GRPCStatus() *gstatus.Status }
	return !errors.As(err, &se)
}
