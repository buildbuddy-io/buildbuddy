// Package interceptor turns ordinary TCP connections to fake IPs into relayed
// connections through the tunnel.
//
// This is what lets a developer type `ssh sjc-prod-abc.prod.buildbuddy.io` with
// no proxy settings, no ProxyCommand, and no per-tool configuration. The fake
// range is routed to a TUN device the daemon owns; a gVisor network stack reads
// that device and sees every SYN sent to it.
//
// The SYN is held, not accepted: the endpoint is only created once the relayed
// connection is actually established. That is deliberate. Accepting first would
// make every port on every fake IP look open, turn a failure to reach the
// target into a connection that opens and immediately closes, and leave no way
// to report "refused". Holding the SYN instead means TCP's own retransmission
// covers the tunnel setup time, and real errors surface as real errors.
package interceptor

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/fakeip"
	"golang.zx2c4.com/wireguard/tun"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"
)

// maxInFlight bounds how many connections can be waiting on tunnel setup at
// once. Held SYNs count against it, and gVisor's forwarder dedupes
// retransmissions of a request already in flight.
const maxInFlight = 1024

// nicID is the only NIC on our stack.
const nicID = 1

const (
	maxConsecutiveReadErrors = 10
	readErrorBackoff         = 100 * time.Millisecond
)

// Dialer opens a connection to host:port through the gateway for a zone.
type Dialer interface {
	Dial(ctx context.Context, zone config.Zone, host string, port int) (net.Conn, error)
}

type Interceptor struct {
	dev   tun.Device
	cfg   *config.Config
	table *fakeip.Table
	dial  Dialer

	stack *stack.Stack
	ep    *channel.Endpoint

	closeOnce sync.Once
	done      chan struct{}

	activeConns atomic.Int64
	totalConns  atomic.Int64
}

func New(dev tun.Device, cfg *config.Config, table *fakeip.Table, dialer Dialer) (*Interceptor, error) {
	mtu, err := dev.MTU()
	if err != nil {
		return nil, fmt.Errorf("reading TUN MTU: %w", err)
	}

	i := &Interceptor{
		dev:   dev,
		cfg:   cfg,
		table: table,
		dial:  dialer,
		done:  make(chan struct{}),
	}

	s := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol, ipv6.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol},
	})
	i.stack = s
	i.ep = channel.New(256, uint32(mtu), "")

	if err := s.CreateNIC(nicID, i.ep); err != nil {
		return nil, fmt.Errorf("creating NIC: %v", err)
	}
	// The stack has to answer for every address in the fake range, not just its
	// own, and reply from whatever address the client dialed.
	if err := s.SetPromiscuousMode(nicID, true); err != nil {
		return nil, fmt.Errorf("enabling promiscuous mode: %v", err)
	}
	if err := s.SetSpoofing(nicID, true); err != nil {
		return nil, fmt.Errorf("enabling spoofing: %v", err)
	}
	s.SetRouteTable([]tcpip.Route{
		{Destination: header.IPv4EmptySubnet, NIC: nicID},
		{Destination: header.IPv6EmptySubnet, NIC: nicID},
	})

	fwd := tcp.NewForwarder(s, 0 /* default receive window */, maxInFlight, i.handleRequest)
	s.SetTransportProtocolHandler(tcp.ProtocolNumber, fwd.HandlePacket)

	i.ep.AddNotify(i)
	return i, nil
}

// Start pumps packets between the TUN device and the stack until Close.
func (i *Interceptor) Start() {
	go i.readLoop()
}

// WriteNotify implements channel.Notification: the stack has a packet to send,
// so write it to the TUN device.
func (i *Interceptor) WriteNotify() {
	pkt := i.ep.Read()
	if pkt == nil {
		return
	}
	view := pkt.ToView()
	pkt.DecRef()

	buf := make([]byte, view.Size())
	if _, err := view.Read(buf); err != nil {
		view.Release()
		return
	}
	view.Release()

	// wireguard-go's TUN write API takes a batch with a reserved offset prefix.
	packet := make([]byte, offset+len(buf))
	copy(packet[offset:], buf)
	if _, err := i.dev.Write([][]byte{packet}, offset); err != nil {
		select {
		case <-i.done:
		default:
			log.Debugf("tunnel: writing to TUN: %s", err)
		}
	}
}

// offset is the space wireguard-go's TUN implementations expect before the
// packet, so they can prepend headers without reallocating.
const offset = 16

func (i *Interceptor) readLoop() {
	mtu, err := i.dev.MTU()
	if err != nil {
		mtu = 1500
	}
	batch := i.dev.BatchSize()
	bufs := make([][]byte, batch)
	for j := range bufs {
		bufs[j] = make([]byte, offset+mtu+64)
	}
	sizes := make([]int, batch)

	consecutiveErrors := 0
	for {
		n, err := i.dev.Read(bufs, sizes, offset)
		if err != nil {
			select {
			case <-i.done:
				return
			default:
			}
			// Giving up here stops interception entirely while the daemon keeps
			// running and DNS keeps handing out addresses, so every covered
			// name silently becomes a black hole. Transient errors are worth
			// riding out; a persistent one is worth saying loudly.
			consecutiveErrors++
			if consecutiveErrors > maxConsecutiveReadErrors {
				log.Warningf("tunnel: giving up reading from the TUN device after %d consecutive errors (%s). "+
					"Interception has stopped — restart the daemon.", consecutiveErrors, err)
				return
			}
			log.Debugf("tunnel: reading from TUN: %s (retrying)", err)
			time.Sleep(readErrorBackoff)
			continue
		}
		consecutiveErrors = 0
		for j := range n {
			pkt := bufs[j][offset : offset+sizes[j]]
			if len(pkt) == 0 {
				continue
			}
			pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{
				Payload: buffer.MakeWithData(pkt),
			})
			switch pkt[0] >> 4 {
			case 4:
				i.ep.InjectInbound(header.IPv4ProtocolNumber, pkb)
			case 6:
				i.ep.InjectInbound(header.IPv6ProtocolNumber, pkb)
			}
			pkb.DecRef()
		}
	}
}

// handleRequest is called for every SYN to an address the stack accepts.
func (i *Interceptor) handleRequest(r *tcp.ForwarderRequest) {
	id := r.ID()
	dstAddr, ok := netip.AddrFromSlice(id.LocalAddress.AsSlice())
	if !ok {
		r.Complete(true /* send RST */)
		return
	}
	dstAddr = dstAddr.Unmap()
	port := int(id.LocalPort)

	name, ok := i.table.Name(dstAddr)
	if !ok {
		// An address in our range that we never handed out: nothing to
		// connect to, so refuse rather than hang.
		log.Debugf("tunnel: connection to unknown address %s:%d", dstAddr, port)
		r.Complete(true)
		return
	}
	zone, ok := i.cfg.MatchZone(name)
	if !ok {
		log.Warningf("tunnel: %s is no longer covered by any zone", name)
		r.Complete(true)
		return
	}
	target := zone.TargetName(name)

	go i.connect(r, zone, name, target, port)
}

func (i *Interceptor) connect(r *tcp.ForwarderRequest, zone config.Zone, name, target string, port int) {
	start := time.Now()

	// The dial happens before the endpoint is created, so the client stays in
	// SYN_SENT — and retransmits — until we know whether this will work.
	upstream, err := i.dial.Dial(context.Background(), zone, target, port)
	if err != nil {
		log.Printf("tunnel: %s:%d — %s", name, port, err)
		r.Complete(true /* send RST → "connection refused" */)
		return
	}

	var wq waiter.Queue
	ep, tcpipErr := r.CreateEndpoint(&wq)
	if tcpipErr != nil {
		upstream.Close()
		log.Debugf("tunnel: creating endpoint for %s:%d: %v", name, port, tcpipErr)
		r.Complete(true)
		return
	}
	r.Complete(false)

	local := gonet.NewTCPConn(&wq, ep)
	active := i.activeConns.Add(1)
	i.totalConns.Add(1)
	log.Debugf("tunnel: %s:%d connected via %s in %s (%d active)",
		name, port, zone.Gateway, time.Since(start).Round(time.Millisecond), active)

	splice(local, upstream)
	i.activeConns.Add(-1)
	local.Close()
	upstream.Close()
}

type closeWriter interface{ CloseWrite() error }

// splice copies in both directions and propagates half-close, so that
// `ssh host cmd` and `git fetch` — which send EOF and then wait for the reply —
// terminate instead of hanging.
func splice(local, upstream net.Conn) {
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(upstream, local)
		if cw, ok := upstream.(closeWriter); ok {
			cw.CloseWrite()
		} else {
			upstream.Close()
		}
		done <- struct{}{}
	}()
	go func() {
		io.Copy(local, upstream)
		if cw, ok := local.(closeWriter); ok {
			cw.CloseWrite()
		} else {
			local.Close()
		}
		done <- struct{}{}
	}()
	<-done
	<-done
}

// Stats returns connection counters for `bbaccess tunnel status`.
func (i *Interceptor) Stats() (active, total int64) {
	return i.activeConns.Load(), i.totalConns.Load()
}

func (i *Interceptor) Close() {
	i.closeOnce.Do(func() {
		close(i.done)
		i.ep.Close()
		i.stack.Close()
		i.dev.Close()
	})
}
