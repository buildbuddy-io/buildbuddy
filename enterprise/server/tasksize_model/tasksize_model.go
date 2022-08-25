package tasksize_model

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

type Model interface {
	Predict(task *repb.ExecutionTask) *scpb.TaskSize
}

var New = func() (Model, error) {
	return nil, status.UnimplementedError("task size prediction is unsupported on this platform")
}
