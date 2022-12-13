package build_event_proxy

import (
	"context"
	"flag"
	"io"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/besutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

var (
	hosts      = flagutil.New("build_event_proxy.hosts", []string{}, "The list of hosts to pass build events onto.")
	bufferSize = flag.Int("build_event_proxy.buffer_size", 100, "The number of build events to buffer locally when proxying build events.")
)

type BuildEventProxyClient struct {
	rootCtx context.Context
	target  string

	bytestreamPrefixToReplace   string
	bytestreamPrefixReplacement string

	clientMux sync.Mutex // PROTECTS(client)
	client    pepb.PublishBuildEventClient
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

func (c *BuildEventProxyClient) SetBytestreamURISubstitution(oldTarget, newTarget string) {
	c.bytestreamPrefixToReplace = bytestreamPrefixFromTarget(oldTarget)
	c.bytestreamPrefixReplacement = bytestreamPrefixFromTarget(newTarget)
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
			c.prepareForSend(req)
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

func (c *BuildEventProxyClient) prepareForSend(req *pepb.PublishBuildToolEventStreamRequest) {
	// Short-circuit if there are no modifications to be made, to avoid
	// unmarshaling unnecessarily.
	if c.bytestreamPrefixReplacement == "" {
		return
	}

	bazelEventAny := req.GetOrderedBuildEvent().GetEvent().GetBazelEvent()
	if bazelEventAny == nil {
		return
	}
	bazelEvent := &bespb.BuildEvent{}
	if err := bazelEventAny.UnmarshalTo(bazelEvent); err != nil {
		log.Warningf("Failed to unmarshal bazel event: %s", err)
		return
	}
	changed := false
	visitFiles := func(files ...*bespb.File) {
		changed = true
		c.replaceAllFileURIs(files...)
	}
	besutil.VisitFiles(bazelEvent, visitFiles)
	// Only re-pack the bazel event if it was changed.
	if !changed {
		return
	}
	bazelEventAny, err := anypb.New(bazelEvent)
	if err != nil {
		log.Warningf("Failed to marshal modified bazel event: %s", err)
		return
	}
	req.GetOrderedBuildEvent().Event.Event = &bepb.BuildEvent_BazelEvent{BazelEvent: bazelEventAny}
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

func (c *BuildEventProxyClient) replaceAllFileURIs(files ...*bespb.File) {
	for _, f := range files {
		switch f.File.(type) {
		case *bespb.File_Uri:
			replacement := c.replaceFileURI(f.GetUri())
			f.File = &bespb.File_Uri{Uri: replacement}
		}
	}
}

func (c *BuildEventProxyClient) replaceFileURI(uri string) string {
	if strings.HasPrefix(uri, c.bytestreamPrefixToReplace) {
		return c.bytestreamPrefixReplacement + strings.TrimPrefix(uri, c.bytestreamPrefixToReplace)
	}
	return uri
}

// Returns the expected "bytestream://DOMAIN" prefix that would appear in BES
// file URIs for a given remote cache target. Example: for a target of
// "grpcs://remote.buildbuddy.io", this returns
// "bytestream://remote.buildbuddy.io".
func bytestreamPrefixFromTarget(target string) string {
	const bytestreamPrefix = "bytestream://"
	if strings.HasPrefix(target, "unix:") {
		// If the target is a unix socket like "unix:///tmp/foo.sock", then
		// bazel will determine the bystream target as
		// "bytestream://///tmp/foo.sock". Presumably, there are 5 slashes
		// because bazel is stripping just "unix:" instead of "unix://", so we
		// replicate that here (even though it seems inconsistent with how
		// grpc:// and grpcs:// are handled).
		return bytestreamPrefix + strings.TrimPrefix(target, "unix:")
	}
	for _, scheme := range []string{"grpc://", "grpcs://", "http://", "https://"} {
		if strings.HasPrefix(target, scheme) {
			return bytestreamPrefix + strings.TrimPrefix(target, scheme)
		}
	}
	return bytestreamPrefix + target
}
