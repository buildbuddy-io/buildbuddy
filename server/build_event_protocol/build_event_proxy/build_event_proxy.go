package build_event_proxy

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"strings"
	"sync"
	"time"

	bpb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

type BuildEventProxyClient struct {
	target    string
	clientMux sync.Mutex // PROTECTS(client)
	client    bpb.PublishBuildEventClient
}

func (c *BuildEventProxyClient) reconnectIfNecessary() {
	if c.client != nil {
		return
	}
	c.clientMux.Lock()
	defer c.clientMux.Unlock()
	conn, err := grpc.Dial(c.target, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
	if err != nil {
		log.Printf("Unable to connect to proxy host '%s': %s", c.target, err)
		c.client = nil
		return
	}
	c.client = bpb.NewPublishBuildEventClient(conn)
}

func NewBuildEventProxyClient(target string) *BuildEventProxyClient {
	if strings.HasPrefix(target, "grpc://") {
		target = strings.TrimPrefix(target, "grpc://")
	}
	c := &BuildEventProxyClient{
		target: target,
	}
	c.reconnectIfNecessary()
	return c
}

func (c *BuildEventProxyClient) PublishLifecycleEvent(ctx context.Context, req *bpb.PublishLifecycleEventRequest) {
	c.reconnectIfNecessary()
	newContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	c.client.PublishLifecycleEvent(newContext, req)
}

func (c *BuildEventProxyClient) PublishBuildToolEventStream(ctx context.Context, opts ...grpc.CallOption) (bpb.PublishBuildEvent_PublishBuildToolEventStreamClient, error) {
	c.reconnectIfNecessary()
	newContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.client.PublishBuildToolEventStream(newContext, opts...)
}
