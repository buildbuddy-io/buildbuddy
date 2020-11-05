package build_event_proxy

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"

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
	conn, err := grpc_client.DialTarget(c.target)
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

func (c *BuildEventProxyClient) PublishLifecycleEvent(ctx context.Context, req *bpb.PublishLifecycleEventRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	c.reconnectIfNecessary()
	newContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	c.client.PublishLifecycleEvent(newContext, req)
	return nil, nil
}

func (c *BuildEventProxyClient) PublishBuildToolEventStream(ctx context.Context, opts ...grpc.CallOption) (bpb.PublishBuildEvent_PublishBuildToolEventStreamClient, error) {
	c.reconnectIfNecessary()
	newContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.client.PublishBuildToolEventStream(newContext, opts...)
}
