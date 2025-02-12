package tasksize_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

func TestDefault_EmptyTask_DefaultEstimate(t *testing.T) {
	ts := tasksize.Default(&repb.ExecutionTask{})

	assert.Equal(t, tasksize.DefaultMemEstimate, ts.EstimatedMemoryBytes)
	assert.Equal(t, tasksize.DefaultCPUEstimate, ts.EstimatedMilliCpu)
	assert.Equal(t, tasksize.DefaultFreeDiskEstimate, ts.EstimatedFreeDiskBytes)
}

func TestDefault_TestTask_RespectsTestSize(t *testing.T) {
	for _, testCase := range []struct {
		size                                  string
		expectedMemoryBytes, expectedMilliCPU int64
	}{
		{"small", 20 * 1e6, 1000},
		{"enormous", 800 * 1e6, 1000},
	} {
		ts := tasksize.Default(&repb.ExecutionTask{
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

func TestDefault_TestTask_Firecracker_AddsAdditionalResources(t *testing.T) {
	for _, testCase := range []struct {
		size                string
		isolationType       platform.ContainerType
		initDockerd         bool
		expectedMemoryBytes int64
		expectedMilliCPU    int64
		expectedDiskBytes   int64
	}{
		{"small", platform.BareContainerType, false /*=initDockerd*/, 20 * 1e6, 1000, tasksize.DefaultFreeDiskEstimate},
		{"enormous", platform.BareContainerType, false /*=initDockerd*/, 800 * 1e6, 1000, tasksize.DefaultFreeDiskEstimate},
		{"small", platform.FirecrackerContainerType, false /*=initDockerd*/, 20*1e6 + tasksize.FirecrackerAdditionalMemEstimateBytes, 1000, tasksize.DefaultFreeDiskEstimate},
		{"enormous", platform.FirecrackerContainerType, true /*=initDockerd*/, 800*1e6 + tasksize.FirecrackerAdditionalMemEstimateBytes + tasksize.DockerInFirecrackerAdditionalMemEstimateBytes, 1000, tasksize.DefaultFreeDiskEstimate + tasksize.DockerInFirecrackerAdditionalDiskEstimateBytes},
	} {
		ts := tasksize.Default(&repb.ExecutionTask{
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

func TestRequested_ViolatingLimits(t *testing.T) {
	const disk = tasksize.MaxEstimatedFreeDisk * 10
	ts := tasksize.Requested(&repb.ExecutionTask{
		Command: &repb.Command{
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{
					{Name: "EstimatedCPU", Value: " 100m "},
					{Name: "EstimatedMemory", Value: " 100 "},
					{Name: "EstimatedFreeDiskBytes", Value: fmt.Sprintf("%d", disk)},
				},
			},
		},
	})

	assert.Equal(t, int64(100), ts.EstimatedMemoryBytes)
	assert.Equal(t, int64(100), ts.EstimatedMilliCpu)
	assert.Equal(t, disk, ts.EstimatedFreeDiskBytes)
}

func TestRequested_BCUPlatformProps_ConvertsBCUToTaskSize(t *testing.T) {
	ts := tasksize.Requested(&repb.ExecutionTask{
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
	assert.Equal(t, int64(0), ts.EstimatedFreeDiskBytes)
}

func TestRequested_BCUPlatformProps_Overriden(t *testing.T) {
	// only override ram
	ts := tasksize.Requested(&repb.ExecutionTask{
		Command: &repb.Command{
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{
					{Name: "EstimatedMemory", Value: "60000000"},
					{Name: "estimatedcomputeunits", Value: "2"},
				},
			},
		},
	})

	assert.Equal(t, int64(60000000), ts.EstimatedMemoryBytes)
	assert.Equal(t, int64(2*1000), ts.EstimatedMilliCpu)
	assert.Equal(t, int64(0), ts.EstimatedFreeDiskBytes)

	// only override cpu
	ts = tasksize.Requested(&repb.ExecutionTask{
		Command: &repb.Command{
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{
					{Name: "EstimatedCPU", Value: "1"},
					{Name: "estimatedcomputeunits", Value: "2"},
				},
			},
		},
	})

	assert.Equal(t, int64(2*2.5*1e9), ts.EstimatedMemoryBytes)
	assert.Equal(t, int64(1000), ts.EstimatedMilliCpu)
	assert.Equal(t, int64(0), ts.EstimatedFreeDiskBytes)
}

func TestOverride(t *testing.T) {
	sz := tasksize.Override(
		&scpb.TaskSize{
			EstimatedMemoryBytes:   10_000_000_000,
			EstimatedMilliCpu:      300,
			EstimatedFreeDiskBytes: 30_000_000_000,
			CustomResources:        []*scpb.CustomResource{{Name: "foo", Value: 0.5}},
		},
		&scpb.TaskSize{
			EstimatedMemoryBytes:   20_000_000_000,
			EstimatedMilliCpu:      100,
			EstimatedFreeDiskBytes: 15_000_000_000,
			CustomResources:        []*scpb.CustomResource{{Name: "bar", Value: 1}},
		})
	assert.Equal(t, int64(20_000_000_000), sz.EstimatedMemoryBytes)
	assert.Equal(t, int64(100), sz.EstimatedMilliCpu)
	assert.Equal(t, int64(15_000_000_000), sz.EstimatedFreeDiskBytes)
	assert.Equal(t, []*scpb.CustomResource{{Name: "bar", Value: 1}}, sz.GetCustomResources())
}

func TestOverride_EmptyOver(t *testing.T) {
	sz := tasksize.Override(
		&scpb.TaskSize{
			EstimatedMemoryBytes:   1,
			EstimatedMilliCpu:      2,
			EstimatedFreeDiskBytes: 3,
		},
		&scpb.TaskSize{})
	assert.Equal(t, int64(1), sz.EstimatedMemoryBytes)
	assert.Equal(t, int64(2), sz.EstimatedMilliCpu)
	assert.Equal(t, int64(3), sz.EstimatedFreeDiskBytes)
}

func TestApplyLimits(t *testing.T) {
	sz := tasksize.ApplyLimits(
		&repb.ExecutionTask{},
		&scpb.TaskSize{
			EstimatedMemoryBytes:   10,
			EstimatedMilliCpu:      10,
			EstimatedFreeDiskBytes: tasksize.MaxEstimatedFreeDisk * 10,
		})
	assert.Equal(t, tasksize.MinimumMemoryBytes, sz.EstimatedMemoryBytes)
	assert.Equal(t, tasksize.MinimumMilliCPU, sz.EstimatedMilliCpu)
	assert.Equal(t, tasksize.MaxEstimatedFreeDisk, sz.EstimatedFreeDiskBytes)
}

func TestApplyLimits_LargeTest(t *testing.T) {
	sz := tasksize.ApplyLimits(
		&repb.ExecutionTask{
			Command: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "TEST_SIZE", Value: "large"},
				},
			},
		},
		&scpb.TaskSize{
			EstimatedMemoryBytes:   10,
			EstimatedMilliCpu:      10,
			EstimatedFreeDiskBytes: tasksize.MaxEstimatedFreeDisk * 10,
		})
	assert.Equal(t, int64(300_000_000), sz.EstimatedMemoryBytes)
	assert.Equal(t, int64(1000), sz.EstimatedMilliCpu)
	assert.Equal(t, tasksize.MaxEstimatedFreeDisk, sz.EstimatedFreeDiskBytes)
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
			// The *sum* of all of the full-stall total durations, multiplied by
			// the PSI correction factor flag, should be subtracted from the
			// execution duration for the purposes of the milliCPU calculation.
			CpuPressure: &repb.PSI{
				Full: &repb.PSI_Metrics{Total: 0.33 * 1e6 /*usec*/},
			},
			IoPressure: &repb.PSI{
				Full: &repb.PSI_Metrics{Total: 0.21 * 1e6 /*usec*/},
			},
			MemoryPressure: &repb.PSI{
				Full: &repb.PSI_Metrics{Total: 0.07 * 1e6 /*usec*/},
			},
		},
		ExecutionStartTimestamp: timestamppb.New(execStart),
		// Set the completed timestamp so that the exec duration is 2 seconds.
		ExecutionCompletedTimestamp: timestamppb.New(execStart.Add(2 * time.Second)),
	}
	err = sizer.Update(ctx, task.GetAction(), task.GetCommand(), md)

	require.NoError(t, err)

	ts = sizer.Get(ctx, task)

	assert.Equal(
		t, int64(917*1e6), ts.GetEstimatedMemoryBytes(),
		"subsequent mem estimate should equal recorded peak mem usage")
	assert.Equal(
		t, int64(math.Ceil(7.13/(2.0-1.0*(0.33+0.21+0.07))*1000.0)), ts.GetEstimatedMilliCpu(),
		"subsequent milliCPU estimate should equal recorded milliCPU")
}

func TestSizer_RespectsMilliCPULimit(t *testing.T) {
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

	execStart := time.Now()
	md := &repb.ExecutedActionMetadata{
		UsageStats: &repb.UsageStats{
			CpuNanos:        8000 * 1e9, // 8000 seconds
			PeakMemoryBytes: 1e9,
		},
		// Set a 1s execution duration
		ExecutionStartTimestamp:     timestamppb.New(execStart),
		ExecutionCompletedTimestamp: timestamppb.New(execStart.Add(1 * time.Second)),
	}
	err = sizer.Update(ctx, task.GetAction(), task.GetCommand(), md)
	require.NoError(t, err)

	ts := sizer.Get(ctx, task)
	assert.Equal(
		t, int64(1e9), ts.GetEstimatedMemoryBytes(),
		"mem estimate should equal recorded peak mem usage")
	assert.Equal(
		t, int64(7500), ts.GetEstimatedMilliCpu(),
		"subsequent milliCPU estimate should equal recorded milliCPU")
}

func TestSizer_RespectsMinimumSize(t *testing.T) {
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

	execStart := time.Now()
	md := &repb.ExecutedActionMetadata{
		UsageStats: &repb.UsageStats{
			CpuNanos:        1,
			PeakMemoryBytes: 1,
		},
		ExecutionStartTimestamp: timestamppb.New(execStart),
		// Set the completed timestamp so that the exec duration is 1 second.
		ExecutionCompletedTimestamp: timestamppb.New(execStart.Add(1 * time.Second)),
	}

	err = sizer.Update(ctx, task.GetAction(), task.GetCommand(), md)
	require.NoError(t, err)

	ts := sizer.Get(ctx, task)
	assert.Equal(t, tasksize.MinimumMilliCPU, ts.GetEstimatedMilliCpu())
	assert.Equal(t, tasksize.MinimumMemoryBytes, ts.GetEstimatedMemoryBytes())

	// Test actions have different minimums.
	task = &repb.ExecutionTask{
		Command: &repb.Command{
			Arguments: []string{"test.sh"},
			EnvironmentVariables: []*repb.Command_EnvironmentVariable{
				{Name: "TEST_SIZE", Value: "enormous"},
			},
		},
	}
	err = sizer.Update(ctx, task.GetAction(), task.GetCommand(), md)
	require.NoError(t, err)

	ts = sizer.Get(ctx, task)
	assert.Equal(t, int64(1000), ts.GetEstimatedMilliCpu())
	assert.Equal(t, int64(800*1e6), ts.GetEstimatedMemoryBytes())
}

func TestCgroupSettings(t *testing.T) {
	// Basic settings should be applied
	{
		size := &scpb.TaskSize{
			EstimatedMilliCpu:    1000,
			EstimatedMemoryBytes: 800e6,
		}
		actual := tasksize.GetCgroupSettings(size)
		expected := &scpb.CgroupSettings{
			CpuWeight:          proto.Int64(39),
			CpuQuotaLimitUsec:  proto.Int64(30 * 100 * 1e3),
			CpuQuotaPeriodUsec: proto.Int64(1 * 100 * 1e3),
			PidsMax:            proto.Int64(2048),
			MemoryOomGroup:     proto.Bool(true),
		}
		assert.Empty(t, cmp.Diff(expected, actual, protocmp.Transform()))
	}

	// CPU weight should be roughly proportional to task size
	for mcpu, weight := range map[int64]int64{
		1000:    39,
		10_000:  391,
		100_000: 3906,
	} {
		size := &scpb.TaskSize{EstimatedMilliCpu: mcpu}
		settings := tasksize.GetCgroupSettings(size)
		assert.Equal(t, int64(weight), settings.GetCpuWeight())
	}
}
