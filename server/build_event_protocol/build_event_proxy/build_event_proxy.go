package build_event_proxy

import (
	"context"
	"flag"
	"io"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

var (
	hosts      = flagutil.StringSlice("build_event_proxy.hosts", []string{}, "The list of hosts to pass build events onto.")
	bufferSize = flag.Int("build_event_proxy.buffer_size", 100, "The number of build events to buffer locally when proxying build events.")
)

type BuildEventProxyClient struct {
	client    pepb.PublishBuildEventClient
	rootCtx   context.Context
	target    string
	clientMux sync.Mutex // PROTECTS(client)
}

func (c *BuildEventProxyClient) reconnectIfNecessary() {
	if c.client != nil {
		return
	}
	c.clientMux.Lock()
	defer c.clientMux.Unlock()
	conn, err := grpc_client.DialTarget(c.target)
	if err != nil {
		log.Warningf("Unable to connect to proxy host '%s': %s", c.target, err)
		c.client = nil
		return
	}
	c.client = pepb.NewPublishBuildEventClient(conn)
}

func Register(env environment.Env) error {
	buildEventProxyClients := make([]pepb.PublishBuildEventClient, len(*hosts))
	for i, target := range *hosts {
		// NB: This can block for up to a second on connecting. This would be a
		// great place to have our health checker and mark these as optional.
		buildEventProxyClients[i] = NewBuildEventProxyClient(env, target)
		log.Printf("Proxy: forwarding build events to: %s", target)
	}
	env.SetBuildEventProxyClients(buildEventProxyClients)
	return nil
}

func NewBuildEventProxyClient(env environment.Env, target string) *BuildEventProxyClient {
	c := &BuildEventProxyClient{
		target:  target,
		rootCtx: env.GetServerContext(),
	}
	c.reconnectIfNecessary()
	return c
}

func (c *BuildEventProxyClient) PublishLifecycleEvent(_ context.Context, req *pepb.PublishLifecycleEventRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	c.reconnectIfNecessary()
	go func() {
		_, err := c.client.PublishLifecycleEvent(c.rootCtx, req)
		if err != nil {
			log.Warningf("Error publishing lifecycle event: %s", err.Error())
		}
	}()
	return &emptypb.Empty{}, nil
}

type asyncStreamProxy struct {
	pepb.PublishBuildEvent_PublishBuildToolEventStreamClient
	ctx    context.Context
	events chan *pepb.PublishBuildToolEventStreamRequest
}

func (c *BuildEventProxyClient) newAsyncStreamProxy(ctx context.Context, opts ...grpc.CallOption) *asyncStreamProxy {
	asp := &asyncStreamProxy{
		ctx:    ctx,
		events: make(chan *pepb.PublishBuildToolEventStreamRequest, *bufferSize),
	}
	// Start a goroutine that will open the stream and pass along events.
	go func() {
		stream, err := c.client.PublishBuildToolEventStream(ctx, opts...)
		if err != nil {
			log.Warningf("Error opening BES stream to proxy: %s", err.Error())
			return
		}
		asp.PublishBuildEvent_PublishBuildToolEventStreamClient = stream

		// Receive all responses (ACKs) from the proxy, but ignore those.
		// Without this step the channel maybe blocked with outstanding messages.
		go func() {
			for {
				_, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Warningf("Got error while getting response from proxy: %s", err.Error())
					break
				}
			}
		}()

		// `range` *copies* the values it returns into the loopvar, and
		// copies of protos are not permitted, so rather than range over the
		// channel we read from the channel inside of an outer loop.
		for {
			req, ok := <-asp.events
			if !ok {
				break
			}
			err := stream.Send(req)
			if err != nil {
				log.Warningf("Error sending req on stream: %s", err.Error())
				break
			}
		}
		stream.CloseSend()
	}()
	return asp
}

func (asp *asyncStreamProxy) Send(req *pepb.PublishBuildToolEventStreamRequest) error {
	select {
	case asp.events <- req:
		// does not fallthrough.
	default:
		log.Warningf("BuildEventProxy dropped message.")
	}
	return nil
}

func (asp *asyncStreamProxy) Recv() (*pepb.PublishBuildToolEventStreamResponse, error) {
	return nil, nil
}

func (asp *asyncStreamProxy) CloseSend() error {
	close(asp.events)
	return nil
}

func (c *BuildEventProxyClient) PublishBuildToolEventStream(_ context.Context, opts ...grpc.CallOption) (pepb.PublishBuildEvent_PublishBuildToolEventStreamClient, error) {
	c.reconnectIfNecessary()
	return c.newAsyncStreamProxy(c.rootCtx, opts...), nil
}
