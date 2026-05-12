// Package devnull provides a no-op implementation of BuildEventChannel and
// BuildEventHandler. It's used when build event publishing needs to be disabled
// or mocked out, such as in testing scenarios or when running commands that
// don't require event streaming.
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
func (c *nullEventChannel) GetInitialSequenceNumber() int64 {
	return 1
}
func (c *nullEventChannel) Close() {}

func (c *nullEventChannel) Context() context.Context {
	return c.ctx
}

type BuildEventHandler struct{}

func (h *BuildEventHandler) OpenChannel(ctx context.Context, iid string) (interfaces.BuildEventChannel, error) {
	return &nullEventChannel{ctx: ctx}, nil
}
