package devnull

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

type nullEventChannel struct {
	ctx context.Context
}

func (c *nullEventChannel) MarkInvocationDisconnected(ctx context.Context, iid string) error {
	return nil
}
func (c *nullEventChannel) FinalizeInvocation(iid string) error { return nil }
func (c *nullEventChannel) HandleEvent(event *pepb.PublishBuildToolEventStreamRequest) error {
	return nil
}
func (c *nullEventChannel) GetNumDroppedEvents() uint64 {
	return 0
}
func (c *nullEventChannel) Close() {}

func (c *nullEventChannel) Context() context.Context {
	return c.ctx
}

type BuildEventHandler struct{}

func (h *BuildEventHandler) OpenChannel(ctx context.Context, iid string) interfaces.BuildEventChannel {
	return &nullEventChannel{ctx: ctx}
}
