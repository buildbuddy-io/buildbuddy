package tasksize_model

import (
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

type Model interface {
	Predict(task *repb.ExecutionTask) *scpb.TaskSize
}

func isPredictionEnabled(task *repb.ExecutionTask) bool {
	// Don't use predicted task sizes for Firecracker tasks for now, since task
	// sizes are used as hard limits on allowed resources.
	props := platform.ParseProperties(task)
	// If a task size is explicitly requested, measured task size is not used.
	if props.EstimatedComputeUnits != 0 {
		return false
	}
	// Don't use predicted task sizes for Firecracker tasks for now, since task
	// sizes are used as hard limits on allowed resources.
	if props.WorkloadIsolationType == string(platform.FirecrackerContainerType) {
		return false
	}
	// TODO(bduffany): Implement this platform prop and re-enable
	// if props.DisablePredictedTaskSize {
	// 	return false
	// }
	return true
}
