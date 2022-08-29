//go:build darwin && !ios
// +build darwin,!ios

package task_size_model

const (
	fixedMemEstimateBytes = 51_000_000
	fixedMilliCPUEstimate = 318
)

type macTaskSizeModel struct{}

func (m *macTaskSizeModel) Predict(task *repb.ExecutionTask) (*scpb.TaskSize, error) {
	return &scpb.TaskSize{
		EstimatedMemoryBytes: fixedMemEstimateBytes,
		EstimatedMilliCpu:    fixedMilliCPUEstimate,
	}
}

func New() (Model, error) {
	return &macTaskSizeModel{}
}
