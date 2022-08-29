//go:build darwin && !ios
// +build darwin,!ios

package tasksize_model

// We don't yet support model-based prediction on Mac apps, so when model-based
// prediction is enabled just return these constants that are loosely based on
// task sizes we have seen in practice.
const (
	fixedMemEstimateBytes = 51_000_000
	fixedMilliCPUEstimate = 318
)

type macTaskSizeModel struct{}

func New() (Model, error) {
	return &macTaskSizeModel{}
}

func (m *macTaskSizeModel) Predict(task *repb.ExecutionTask) *scpb.TaskSize {
	if !isPredictionEnabled(task) {
		return nil
	}
	return &scpb.TaskSize{
		EstimatedMemoryBytes: fixedMemEstimateBytes,
		EstimatedMilliCpu:    fixedMilliCPUEstimate,
	}
}
