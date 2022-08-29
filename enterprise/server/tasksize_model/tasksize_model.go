package tasksize_model

import (

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

type Model interface {
	Predict(task *repb.ExecutionTask) *scpb.TaskSize
}
