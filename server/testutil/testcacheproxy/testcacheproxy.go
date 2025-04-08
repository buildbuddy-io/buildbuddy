package testcacheproxy

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type NoOpAtimeUpdater struct{}

func (a *NoOpAtimeUpdater) Enqueue(_ context.Context, _ string, _ []*remote_execution.Digest, _ remote_execution.DigestFunction_Value) {
}
func (a *NoOpAtimeUpdater) EnqueueByResourceName(_ context.Context, _ string) {}
func (a *NoOpAtimeUpdater) EnqueueByFindMissingRequest(_ context.Context, _ *remote_execution.FindMissingBlobsRequest) {
}
