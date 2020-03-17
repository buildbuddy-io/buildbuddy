package build_event_proxy

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"strings"
	"sync"
	"time"

	bpb "proto"
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

func proxyContext(ctx context.Context) context.Context {
	// Your price is way too high, you need to cut it.
	newContext, _ := context.WithTimeout(ctx, 10*time.Second)
	return newContext
}

func (c *BuildEventProxyClient) PublishLifecycleEvent(ctx context.Context, req *bpb.PublishLifecycleEventRequest) {
	c.reconnectIfNecessary()
	c.client.PublishLifecycleEvent(proxyContext(ctx), req)
}

func (c *BuildEventProxyClient) PublishBuildToolEventStream(ctx context.Context, opts ...grpc.CallOption) (bpb.PublishBuildEvent_PublishBuildToolEventStreamClient, error) {
	c.reconnectIfNecessary()
	return c.client.PublishBuildToolEventStream(proxyContext(ctx), opts...)
}
