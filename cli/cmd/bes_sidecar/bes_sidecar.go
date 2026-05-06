// bes_sidecar is an experimental minimal BES-only sidecar.
//
// It exists to measure how small the BuildBuddy CLI sidecar could be if it
// stopped reusing broad server packages. It intentionally duplicates a small
// amount of gRPC/BES glue rather than importing server/environment,
// server/interfaces, server/build_event_protocol, server/util/grpc_client,
// server/util/grpc_server, server/rpc/interceptors, or server/util/log.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/sidecar"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	listenAddr        = flag.String("listen_addr", "localhost:1991", "Local address to listen on. Use unix:///path for a Unix socket.")
	besBackends       = flag.String("bes_backend", "", "Real BES backend(s) to proxy to. Multiple servers may be comma-separated.")
	synchronous       = flag.Bool("bes_synchronous", false, "If true, wait for backend forwarding and surface forwarding errors to Bazel.")
	inactivityTimeout = flag.Duration("inactivity_timeout", 5*time.Minute, "Exit after this much inactivity.")
	bufferSize        = flag.Int("build_event_proxy.buffer_size", 500_000, "Number of build events to buffer per backend when proxying asynchronously.")
	maxRecvMsgSize    = flag.Int("grpc_max_recv_msg_size_bytes", 50_000_000, "Max gRPC receive message size.")
)

type sidecarServer struct {
	scpb.UnimplementedSidecarServer
	pepb.UnimplementedPublishBuildEventServer

	mu      sync.Mutex
	lastUse time.Time
	quit    chan struct{}
	once    sync.Once
}

func newSidecarServer() *sidecarServer {
	s := &sidecarServer{
		lastUse: time.Now(),
		quit:    make(chan struct{}),
	}
	go s.watchInactivity()
	return s
}

func (s *sidecarServer) Ping(ctx context.Context, req *scpb.PingRequest) (*scpb.PingResponse, error) {
	s.markUsed()
	return &scpb.PingResponse{}, nil
}

func (s *sidecarServer) PublishLifecycleEvent(ctx context.Context, req *pepb.PublishLifecycleEventRequest) (*emptypb.Empty, error) {
	s.markUsed()
	clients := dialBackends(ctx)
	if *synchronous {
		for _, c := range clients {
			if _, err := c.client.PublishLifecycleEvent(ctx, req); err != nil {
				return nil, err
			}
		}
		return &emptypb.Empty{}, nil
	}
	for _, c := range clients {
		c := c
		go func() {
			defer c.close()
			if _, err := c.client.PublishLifecycleEvent(context.Background(), req); err != nil {
				log.Printf("warning: async lifecycle event forward to %s failed: %s", c.target, err)
			}
		}()
	}
	return &emptypb.Empty{}, nil
}

func (s *sidecarServer) PublishBuildToolEventStream(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	s.markUsed()
	ctx := stream.Context()
	forwarders, err := openForwarders(ctx)
	if err != nil && *synchronous {
		return err
	}
	defer closeForwarders(forwarders)

	acks := make([]int64, 0)
	var invocationID string
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if *synchronous {
				if err := waitForwarders(forwarders); err != nil {
					return err
				}
			}
			return sendACKs(stream, invocationID, acks)
		}
		if err != nil {
			return err
		}
		s.markUsed()
		obe := req.GetOrderedBuildEvent()
		if obe == nil {
			continue
		}
		streamID := obe.GetStreamId()
		if invocationID == "" {
			invocationID = streamID.GetInvocationId()
		}
		acks = append(acks, obe.GetSequenceNumber())
		for _, f := range forwarders {
			if err := f.send(req); err != nil && *synchronous {
				return err
			}
		}
	}
}

func sendACKs(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, invocationID string, acks []int64) error {
	sort.Slice(acks, func(i, j int) bool { return acks[i] < acks[j] })
	for _, seq := range acks {
		rsp := &pepb.PublishBuildToolEventStreamResponse{
			StreamId:       streamID(invocationID),
			SequenceNumber: seq,
		}
		if err := stream.Send(rsp); err != nil {
			return err
		}
	}
	return nil
}

func streamID(invocationID string) *bepb.StreamId {
	return &bepb.StreamId{InvocationId: invocationID}
}

type backendClient struct {
	target string
	conn   *grpc.ClientConn
	client pepb.PublishBuildEventClient
}

func (c *backendClient) close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func dialBackends(ctx context.Context) []*backendClient {
	targets := splitTargets(*besBackends)
	clients := make([]*backendClient, 0, len(targets))
	for _, target := range targets {
		conn, err := dial(ctx, target)
		if err != nil {
			log.Printf("warning: failed to dial BES backend %s: %s", target, err)
			continue
		}
		clients = append(clients, &backendClient{target: target, conn: conn, client: pepb.NewPublishBuildEventClient(conn)})
	}
	return clients
}

type forwarder struct {
	backend *backendClient
	stream  pepb.PublishBuildEvent_PublishBuildToolEventStreamClient
	ch      chan *pepb.PublishBuildToolEventStreamRequest
	done    chan error
}

func openForwarders(ctx context.Context) ([]*forwarder, error) {
	clients := dialBackends(ctx)
	forwarders := make([]*forwarder, 0, len(clients))
	var firstErr error
	for _, c := range clients {
		stream, err := c.client.PublishBuildToolEventStream(ctx)
		if err != nil {
			c.close()
			log.Printf("warning: failed to open BES stream to %s: %s", c.target, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		f := &forwarder{
			backend: c,
			stream:  stream,
			ch:      make(chan *pepb.PublishBuildToolEventStreamRequest, *bufferSize),
			done:    make(chan error, 1),
		}
		go f.run()
		forwarders = append(forwarders, f)
	}
	return forwarders, firstErr
}

func (f *forwarder) send(req *pepb.PublishBuildToolEventStreamRequest) error {
	if *synchronous {
		return f.stream.Send(req)
	}
	select {
	case f.ch <- req:
	default:
		log.Printf("warning: dropped BES event while forwarding to %s", f.backend.target)
	}
	return nil
}

func (f *forwarder) run() {
	defer f.backend.close()
	for req := range f.ch {
		if err := f.stream.Send(req); err != nil {
			f.done <- err
			return
		}
	}
	if err := f.stream.CloseSend(); err != nil {
		f.done <- err
		return
	}
	for {
		_, err := f.stream.Recv()
		if err == io.EOF {
			f.done <- nil
			return
		}
		if err != nil {
			f.done <- err
			return
		}
	}
}

func closeForwarders(forwarders []*forwarder) {
	for _, f := range forwarders {
		if !*synchronous {
			close(f.ch)
			continue
		}
		f.backend.close()
	}
}

func waitForwarders(forwarders []*forwarder) error {
	var firstErr error
	for _, f := range forwarders {
		if *synchronous {
			if err := f.stream.CloseSend(); err != nil && firstErr == nil {
				firstErr = err
			}
			for {
				_, err := f.stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					break
				}
			}
			continue
		}
		if err := <-f.done; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func splitTargets(s string) []string {
	var targets []string
	for t := range strings.SplitSeq(s, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			targets = append(targets, normalizeTarget(t))
		}
	}
	return targets
}

func normalizeTarget(target string) string {
	if strings.Contains(target, "://") {
		return target
	}
	return "grpcs://" + target
}

func dial(ctx context.Context, target string) (*grpc.ClientConn, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	dialTarget := target
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<31 - 1)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 20 * time.Second, PermitWithoutStream: true}),
	}
	if u.Scheme == "grpcs" {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(nil)))
		if u.Port() == "" {
			u.Host += ":443"
		}
		dialTarget = u.Host
	} else if u.Scheme == "grpc" {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		dialTarget = u.Host
	} else if u.Scheme == "unix" {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		return nil, fmt.Errorf("unsupported BES backend scheme %q", u.Scheme)
	}
	return grpc.DialContext(ctx, dialTarget, opts...)
}

func (s *sidecarServer) markUsed() {
	s.mu.Lock()
	s.lastUse = time.Now()
	s.mu.Unlock()
}

func (s *sidecarServer) watchInactivity() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.mu.Lock()
			inactive := time.Since(s.lastUse) > *inactivityTimeout
			s.mu.Unlock()
			if inactive {
				log.Printf("exiting after %s of inactivity", *inactivityTimeout)
				s.stop()
				return
			}
		}
	}
}

func (s *sidecarServer) stop() {
	s.once.Do(func() { close(s.quit) })
}

func listen(address string) (net.Listener, error) {
	if path, ok := strings.CutPrefix(address, "unix://"); ok {
		_ = os.Remove(path)
		return net.Listen("unix", path)
	}
	return net.Listen("tcp", address)
}

func main() {
	flag.Parse()
	if *besBackends == "" {
		log.Fatal("--bes_backend is required")
	}
	lis, err := listen(*listenAddr)
	if err != nil {
		log.Fatalf("listen %q: %s", *listenAddr, err)
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(*maxRecvMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)
	s := newSidecarServer()
	scpb.RegisterSidecarServer(server, s)
	pepb.RegisterPublishBuildEventServer(server, s)
	go func() {
		<-s.quit
		server.GracefulStop()
	}()
	log.Printf("BES sidecar listening on %s; forwarding to %s", lis.Addr(), strings.Join(splitTargets(*besBackends), ", "))
	if err := server.Serve(lis); err != nil {
		log.Fatalf("serve: %s", err)
	}
}
