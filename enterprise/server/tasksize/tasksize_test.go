package tasksize_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/stretchr/testify/assert"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestEstimate_EmptyTask_DefaultEstimate(t *testing.T) {
	ts := tasksize.Estimate(&repb.ExecutionTask{})

	assert.Equal(t, tasksize.DefaultMemEstimate, ts.EstimatedMemoryBytes)
	assert.Equal(t, tasksize.DefaultCPUEstimate, ts.EstimatedMilliCpu)
	assert.Equal(t, tasksize.DefaultFreeDiskEstimate, ts.EstimatedFreeDiskBytes)
}

func TestEstimate_TestTask_RespectsTestSize(t *testing.T) {
	for _, testCase := range []struct {
		size                                  string
		expectedMemoryBytes, expectedMilliCPU int64
	}{
		{"small", 20 * 1e6, 600},
		{"enormous", 800 * 1e6, 1000},
	} {
		ts := tasksize.Estimate(&repb.ExecutionTask{
			Command: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "TEST_SIZE", Value: testCase.size},
				},
			},
		})

		assert.Equal(t, testCase.expectedMemoryBytes, ts.EstimatedMemoryBytes)
		assert.Equal(t, testCase.expectedMilliCPU, ts.EstimatedMilliCpu)
		assert.Equal(t, tasksize.DefaultFreeDiskEstimate, ts.EstimatedFreeDiskBytes)
	}
}

func TestEstimate_TestTask_Firecracker_AddsAdditionalResources(t *testing.T) {
	for _, testCase := range []struct {
		size                string
		isolationType       platform.ContainerType
		initDockerd         bool
		expectedMemoryBytes int64
		expectedMilliCPU    int64
		expectedDiskBytes   int64
	}{
		{"small", platform.BareContainerType, false /*=initDockerd*/, 20 * 1e6, 600, tasksize.DefaultFreeDiskEstimate},
		{"enormous", platform.BareContainerType, false /*=initDockerd*/, 800 * 1e6, 1000, tasksize.DefaultFreeDiskEstimate},
		{"small", platform.FirecrackerContainerType, false /*=initDockerd*/, 20*1e6 + tasksize.FirecrackerAdditionalMemEstimateBytes, 600, tasksize.DefaultFreeDiskEstimate},
		{"enormous", platform.FirecrackerContainerType, true /*=initDockerd*/, 800*1e6 + tasksize.FirecrackerAdditionalMemEstimateBytes + tasksize.DockerInFirecrackerAdditionalMemEstimateBytes, 1000, tasksize.DefaultFreeDiskEstimate + tasksize.DockerInFirecrackerAdditionalDiskEstimateBytes},
	} {
		ts := tasksize.Estimate(&repb.ExecutionTask{
			Command: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{Name: "workload-isolation-type", Value: string(testCase.isolationType)},
						{Name: "init-dockerd", Value: fmt.Sprintf("%v", testCase.initDockerd)},
					},
				},
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "TEST_SIZE", Value: testCase.size},
				},
			},
		})

		assert.Equal(t, testCase.expectedMemoryBytes, ts.EstimatedMemoryBytes)
		assert.Equal(t, testCase.expectedMilliCPU, ts.EstimatedMilliCpu)
		assert.Equal(t, testCase.expectedDiskBytes, ts.EstimatedFreeDiskBytes)
	}

}

func TestEstimate_BCUPlatformProps_ConvertsBCUToTaskSize(t *testing.T) {
	ts := tasksize.Estimate(&repb.ExecutionTask{
		Command: &repb.Command{
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{
					{Name: "estimatedcomputeunits", Value: " 2 "},
				},
			},
		},
	})

	assert.Equal(t, int64(2*2.5*1e9), ts.EstimatedMemoryBytes)
	assert.Equal(t, int64(2*1000), ts.EstimatedMilliCpu)
	assert.Equal(t, tasksize.DefaultFreeDiskEstimate, ts.EstimatedFreeDiskBytes)
}

func TestEstimate_DiskSizePlatformProp_UsesPropValueForDiskSize(t *testing.T) {
	const disk = tasksize.DefaultFreeDiskEstimate * 10
	ts := tasksize.Estimate(&repb.ExecutionTask{
		Command: &repb.Command{
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{
					{Name: "EstimatedFreeDiskBytes", Value: fmt.Sprintf("%d", disk)},
				},
			},
		},
	})

	assert.Equal(t, tasksize.DefaultMemEstimate, ts.EstimatedMemoryBytes)
	assert.Equal(t, tasksize.DefaultCPUEstimate, ts.EstimatedMilliCpu)
	assert.Equal(t, disk, ts.EstimatedFreeDiskBytes)
}
