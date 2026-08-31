// Package relay implements a gateway hub service that allows connections to
// destinations that the gateway itself can reach, such as internal k8s
// resources.
//
// The client requests a relay connection either by name or by IP. If the
// client sends a name, the gateway uses its own resolver to resolve the IP.
// This allows the client to resolve IPs for DNS entries that are internal
// to the cluster.
//
// The relay listens on the hub IP and speaks the protocol implemented in the
// relaywire package. The client opens a connection, sends the relay request
// proto inline, the relay service attempts to open the connection and sends a
// proto response back to the client. If the connection is successful, the
// connection then switches to shuffling raw bytes between the client and the
// destination.
package relay

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/server"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/relaywire"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	relayAllowedTargetSuffixes = flag.Slice("gateway.relay.allowed_target_suffixes", []string{}, "DNS suffixes the relay may connect to, e.g. 'svc.cluster.local'. Empty means any target.")
	relayDialTimeout           = flag.Duration("gateway.relay.dial_timeout", 10*time.Second, "Timeout for the gateway's outbound dial on behalf of a relay client.")
)

// New returns the egress relay hub service.
func New() server.HubService { return relayService{} }

type relayService struct{}

func (relayService) Start(hub *server.HubNetwork) (io.Closer, error) {
	ln, err := hub.ListenTCP(relaywire.DefaultPort)
	if err != nil {
		return nil, err
	}
	go serveRelay(ln, hub.NetworkKey)
	return ln, nil
}

// serveRelay accepts relay handshakes on the hub listener until it is closed.
func serveRelay(ln net.Listener, networkKey string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // stack or listener closed
		}
		go handleRelayConn(conn, networkKey)
	}
}

func handleRelayConn(conn net.Conn, networkKey string) {
	defer conn.Close()

	srcAddr, _ := netip.ParseAddrPort(conn.RemoteAddr().String())

	req, err := relaywire.ReadRequest(conn)
	if err != nil {
		log.Warningf("relay[%s]: handshake with %s failed: %s", networkKey, srcAddr.Addr(), err)
		return
	}
	target := net.JoinHostPort(req.GetHost(), strconv.Itoa(int(req.GetPort())))

	if !relayTargetAllowed(req.GetHost()) {
		log.Warningf("relay[%s]: src=%s target=%s is not in the allowed suffix list",
			networkKey, srcAddr.Addr(), target)
		relaywire.Refuse(conn, status.PermissionDeniedErrorf(
			"target %q is not in this gateway's allowed suffix list", req.GetHost()))
		return
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), *relayDialTimeout)
	defer cancel()
	var dialer net.Dialer
	upstream, err := dialer.DialContext(ctx, "tcp", target)
	if err != nil {
		log.Warningf("relay[%s]: src=%s target=%s dial failed after %s: %s",
			networkKey, srcAddr.Addr(), target, time.Since(start).Round(time.Millisecond), err)
		relaywire.Refuse(conn, refusalForDialError(err, target))
		return
	}
	defer upstream.Close()

	resolved := upstream.RemoteAddr().String()
	if err := relaywire.Accept(conn, resolved); err != nil {
		return
	}

	log.Infof("relay[%s]: OPEN src=%s target=%s resolved=%s",
		networkKey, srcAddr.Addr(), target, resolved)

	sent, received := splice(conn, upstream)
	log.Infof("relay[%s]: CLOSE src=%s target=%s resolved=%s sent=%d received=%d duration=%s",
		networkKey, srcAddr.Addr(), target, resolved, sent, received,
		time.Since(start).Round(time.Millisecond))
}

// refusalForDialError turns the gateway's failed outbound dial into a status
// error for the client.
func refusalForDialError(err error, target string) error {
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &dnsErr):
		return status.NotFoundErrorf("%q does not resolve at the gateway: %s", dnsErr.Name, dnsErr.Err)
	case errors.Is(err, context.DeadlineExceeded):
		return status.DeadlineExceededErrorf("dialing %s from the gateway timed out after %s", target, *relayDialTimeout)
	default:
		return status.UnavailableErrorf("dialing %s from the gateway: %s", target, err)
	}
}

// closeWriter is implemented by both *net.TCPConn and gVisor's *gonet.TCPConn.
type closeWriter interface{ CloseWrite() error }

// splice copies bytes in both directions until both halves are done, and
// returns the number of bytes sent to and received from upstream.
func splice(client, upstream net.Conn) (sent, received int64) {
	var toUpstream, fromUpstream atomic.Int64
	done := make(chan struct{}, 2)

	go func() {
		n, _ := io.Copy(upstream, client)
		toUpstream.Store(n)
		if cw, ok := upstream.(closeWriter); ok {
			cw.CloseWrite()
		} else {
			upstream.Close()
		}
		done <- struct{}{}
	}()
	go func() {
		n, _ := io.Copy(client, upstream)
		fromUpstream.Store(n)
		if cw, ok := client.(closeWriter); ok {
			cw.CloseWrite()
		} else {
			client.Close()
		}
		done <- struct{}{}
	}()

	<-done
	<-done
	return toUpstream.Load(), fromUpstream.Load()
}

// relayTargetAllowed reports whether host is covered by the configured suffix
// allowlist. An exact match counts, as does any name ending in ".<suffix>";
// "foo.evil-cluster.local" must not match the suffix "cluster.local".
func relayTargetAllowed(host string) bool {
	suffixes := *relayAllowedTargetSuffixes
	if len(suffixes) == 0 {
		return true
	}
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	for _, s := range suffixes {
		s = strings.ToLower(strings.TrimSuffix(strings.TrimPrefix(s, "."), "."))
		if s == "" {
			continue
		}
		if h == s || strings.HasSuffix(h, "."+s) {
			return true
		}
	}
	return false
}
