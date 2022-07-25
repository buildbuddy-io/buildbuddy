package tasksize_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

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

func TestSizer_Get_ShouldReturnRecordedUsageStats(t *testing.T) {
	flags.Set(t, "remote_execution.use_measured_task_sizes", true)

	env := testenv.GetTestEnv(t)
	rdb := testredis.Start(t).Client()
	env.SetRemoteExecutionRedisClient(rdb)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers())
	env.SetAuthenticator(auth)
	sizer, err := tasksize.NewSizer(env)
	require.NoError(t, err)

	ctx := context.Background()
	task := &repb.ExecutionTask{
		Command: &repb.Command{
			Arguments: []string{"/usr/bin/clang", "foo.c", "-o", "foo.o"},
		},
	}
	ts := sizer.Get(ctx, task)

	require.Nil(t, ts, "should not return a task size initially")

	execStart := time.Now()
	md := &repb.ExecutedActionMetadata{
		UsageStats: &repb.UsageStats{
			// Intentionally using weird numbers here to make sure we aren't
			// just returning the default estimates.
			CpuNanos:        7.13 * 1e9,
			PeakMemoryBytes: 917 * 1e6,
		},
		ExecutionStartTimestamp: timestamppb.New(execStart),
		// Set the completed timestamp so that the exec duration is 2 seconds.
		ExecutionCompletedTimestamp: timestamppb.New(execStart.Add(2 * time.Second)),
	}
	err = sizer.Update(ctx, task.GetCommand(), md)

	require.NoError(t, err)

	ts = sizer.Get(ctx, task)

	assert.Equal(
		t, int64(917*1e6), ts.GetEstimatedMemoryBytes(),
		"subsequent mem estimate should equal recorded peak mem usage")
	assert.Equal(
		t, int64(7.13/2*1000), ts.GetEstimatedMilliCpu(),
		"subsequent milliCPU estimate should equal recorded milliCPU")
}
