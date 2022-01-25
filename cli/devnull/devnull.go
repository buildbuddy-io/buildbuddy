package devnull

import (
	"context"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

type nullEventChannel struct{}

func (c *nullEventChannel) MarkInvocationDisconnected(ctx context.Context, iid string) error {
	return nil
}
func (c *nullEventChannel) FinalizeInvocation(iid string) error { return nil }
func (c *nullEventChannel) HandleEvent(event *pepb.PublishBuildToolEventStreamRequest) error {
	return nil
}
func (c *nullEventChannel) Close() {}

type BuildEventHandler struct{}

func (h *BuildEventHandler) OpenChannel(ctx context.Context, iid string) interfaces.BuildEventChannel {
	return &nullEventChannel{}
}
