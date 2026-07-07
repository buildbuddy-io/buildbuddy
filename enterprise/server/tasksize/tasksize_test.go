package tasksize_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
	openfeatureTesting "github.com/open-feature/go-sdk/openfeature/testing"
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
		context.Background(),
		nil,
		&repb.Command{},
		&platform.Properties{},
		&scpb.TaskSize{
			EstimatedMemoryBytes:   10,
			EstimatedMilliCpu:      10,
			EstimatedFreeDiskBytes: tasksize.MaxEstimatedFreeDisk * 10,
		})
	assert.Equal(t, tasksize.MinimumMemoryBytes, sz.EstimatedMemoryBytes)
	assert.Equal(t, tasksize.MinimumMilliCPU, sz.EstimatedMilliCpu)
	assert.Equal(t, tasksize.MaxEstimatedFreeDiskRecycleFalse, sz.EstimatedFreeDiskBytes)
}

func TestApplyLimitsNonRecyleableLargeDisk(t *testing.T) {
	sz := tasksize.ApplyLimits(
		context.Background(),
		nil,
		&repb.Command{},
		&platform.Properties{
			RecycleRunner: false,
		},
		&scpb.TaskSize{
			EstimatedMemoryBytes:   10,
			EstimatedMilliCpu:      10,
			EstimatedFreeDiskBytes: tasksize.MaxEstimatedFreeDiskRecycleFalse * 10,
		},
	)
	assert.Equal(t, tasksize.MinimumMemoryBytes, sz.EstimatedMemoryBytes)
	assert.Equal(t, tasksize.MinimumMilliCPU, sz.EstimatedMilliCpu)
	assert.Equal(t, tasksize.MaxEstimatedFreeDiskRecycleFalse, sz.EstimatedFreeDiskBytes)
}

func TestApplyLimits_LargeTest(t *testing.T) {
	sz := tasksize.ApplyLimits(
		context.Background(),
		nil,
		&repb.Command{
			EnvironmentVariables: []*repb.Command_EnvironmentVariable{
				{Name: "TEST_SIZE", Value: "large"},
			},
		},
		&platform.Properties{
			RecycleRunner: true,
		},
		&scpb.TaskSize{
			EstimatedMemoryBytes:   10,
			EstimatedMilliCpu:      10,
			EstimatedFreeDiskBytes: tasksize.MaxEstimatedFreeDisk * 10,
		},
	)
	assert.Equal(t, int64(300_000_000), sz.EstimatedMemoryBytes)
	assert.Equal(t, int64(1000), sz.EstimatedMilliCpu)
	assert.Equal(t, tasksize.MaxEstimatedFreeDisk, sz.EstimatedFreeDiskBytes)
}

func TestApplyLimits_MaxDiskLimitDisabled(t *testing.T) {
	testProvider := openfeatureTesting.NewTestProvider()
	testProvider.UsingFlags(t, map[string]memprovider.InMemoryFlag{
		"disable-task-sizing-disk-limit": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(testProvider))
	defer testProvider.Cleanup()

	fp, err := experiments.NewFlagProvider("")
	require.NoError(t, err)

	sz := tasksize.ApplyLimits(
		context.Background(),
		fp,
		&repb.Command{},
		&platform.Properties{
			RecycleRunner: true,
		},
		&scpb.TaskSize{
			EstimatedFreeDiskBytes: tasksize.MaxEstimatedFreeDisk * 10,
		},
	)
	assert.Equal(t, tasksize.MaxEstimatedFreeDisk*10, sz.EstimatedFreeDiskBytes)
}

func TestSizer_Get_ShouldReturnRecordedUsageStats(t *testing.T) {
	flags.Set(t, "remote_execution.use_measured_task_sizes", true)

	env := testenv.GetTestEnv(t)
	rdb := testredis.Start(t).Client()
	env.SetRemoteExecutionRedisClient(rdb)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers())
	env.SetAuthenticator(auth)
	sizer, err := tasksize.NewSizer(env)
	require.NoError(t, err)

	ctx := context.Background()
	cmd := &repb.Command{
		Arguments: []string{"/usr/bin/clang", "foo.c", "-o", "foo.o"},
	}
	props := &platform.Properties{}
	ts := sizer.Get(ctx, cmd, props)

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
	err = sizer.Update(ctx, cmd, props, md)

	require.NoError(t, err)

	ts = sizer.Get(ctx, cmd, props)

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
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers())
	env.SetAuthenticator(auth)
	sizer, err := tasksize.NewSizer(env)
	require.NoError(t, err)

	ctx := context.Background()
	cmd := &repb.Command{
		Arguments: []string{"/usr/bin/clang", "foo.c", "-o", "foo.o"},
	}
	props := &platform.Properties{}

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
	err = sizer.Update(ctx, cmd, props, md)
	require.NoError(t, err)

	ts := sizer.Get(ctx, cmd, props)
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
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers())
	env.SetAuthenticator(auth)
	sizer, err := tasksize.NewSizer(env)
	require.NoError(t, err)

	ctx := context.Background()
	cmd := &repb.Command{
		Arguments: []string{"/usr/bin/clang", "foo.c", "-o", "foo.o"},
	}
	props := &platform.Properties{}

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

	err = sizer.Update(ctx, cmd, props, md)
	require.NoError(t, err)

	ts := sizer.Get(ctx, cmd, props)
	assert.Equal(t, tasksize.MinimumMilliCPU, ts.GetEstimatedMilliCpu())
	assert.Equal(t, tasksize.MinimumMemoryBytes, ts.GetEstimatedMemoryBytes())

	// Test actions have different minimums.
	cmd = &repb.Command{
		Arguments: []string{"test.sh"},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "TEST_SIZE", Value: "enormous"},
		},
	}
	props = &platform.Properties{}
	err = sizer.Update(ctx, cmd, props, md)
	require.NoError(t, err)

	ts = sizer.Get(ctx, cmd, props)
	assert.Equal(t, int64(1000), ts.GetEstimatedMilliCpu())
	assert.Equal(t, int64(800*1e6), ts.GetEstimatedMemoryBytes())
}

func TestSizer_P90CPUExperiment(t *testing.T) {
	flags.Set(t, "remote_execution.use_measured_task_sizes", true)
	env := testenv.GetTestEnv(t)
	rdb := testredis.Start(t).Client()
	env.SetRemoteExecutionRedisClient(rdb)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers())
	env.SetAuthenticator(auth)

	tmp := testfs.MakeTempDir(t)
	// Enable p90 experiment
	offlineFlagPath := testfs.WriteFile(t, tmp, "config.flagd.json", `
{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "remote_execution.task_size_use_p90_cpu": {
      "state": "ENABLED",
      "variants": { "default": true },
      "defaultVariant": "default"
    }
  }
}
`)
	provider, err := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(offlineFlagPath))
	require.NoError(t, err)
	openfeature.SetProviderAndWait(provider)
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	// Simulate a task with a burst of 4000m CPU (4 cores) for 1 second, then
	// 1000m CPU (1 core) for 9 seconds.
	execStartTimestamp := time.Now()
	usageStats := &repb.UsageStats{
		CpuNanos:        0,   // Updated in loop below
		PeakMemoryBytes: 1e9, // Arbitrary
		Timeline: &repb.UsageTimeline{
			// Set initial samples for delta-encoding - these are arbitrary;
			// we only care about the deltas.
			Timestamps: []int64{777},
			CpuSamples: []int64{999},
		},
	}
	md := &repb.ExecutedActionMetadata{
		ExecutionStartTimestamp:     timestamppb.New(execStartTimestamp),
		ExecutionCompletedTimestamp: timestamppb.New(execStartTimestamp), // Updated in loop below
		UsageStats:                  usageStats,
	}
	// Simulate CPU usage as described above
	timeline := usageStats.Timeline
	for i := 0; i < 10; i++ {
		elapsedDuration := 1 * time.Second
		timeline.Timestamps = append(timeline.Timestamps, elapsedDuration.Milliseconds())
		md.ExecutionCompletedTimestamp = timestamppb.New(md.ExecutionCompletedTimestamp.AsTime().Add(elapsedDuration))
		if i == 0 {
			timeline.CpuSamples = append(timeline.CpuSamples, 4000)
		} else {
			timeline.CpuSamples = append(timeline.CpuSamples, 1000)
		}
		usageStats.CpuNanos += timeline.CpuSamples[len(timeline.CpuSamples)-1] * 1e6 // ms to nanos
	}

	// Update task sizer
	sizer, err := tasksize.NewSizer(env)
	require.NoError(t, err)
	ctx := context.Background()
	cmd := &repb.Command{
		Arguments: []string{"big_linker_action", "<blah>"},
	}
	props := &platform.Properties{}
	err = sizer.Update(ctx, cmd, props, md)
	require.NoError(t, err)

	// Get task size and make sure it reports the p90 CPU usage
	ts := sizer.Get(ctx, cmd, props)
	assert.Equal(t, int64(4000), ts.GetEstimatedMilliCpu())
}

func TestSizer_UpdateForOOM(t *testing.T) {
	for _, test := range []struct {
		name string
		// resizeMultiplier is the remote_execution.oom_resize_multiplier flag
		// value.
		resizeMultiplier float64
		// scheduledSize is the size the task was scheduled with.
		scheduledSize *scpb.TaskSize
		// observedMemoryBytes is the task's memory usage observed by the OOM
		// killer.
		observedMemoryBytes int64
		// expectedSize is the size expected to be recorded after the OOM
		// update, or nil if no size should be recorded.
		expectedSize *scpb.TaskSize
	}{
		{
			// The task exceeded its 1GB estimate, so the new estimate should be
			// the observed usage times the resize multiplier, with the CPU
			// estimate carried over from the scheduled size.
			name:                "usage above estimate records a resized estimate",
			resizeMultiplier:    1.5,
			scheduledSize:       &scpb.TaskSize{EstimatedMemoryBytes: 1e9, EstimatedMilliCpu: 2000},
			observedMemoryBytes: 2e9,
			expectedSize:        &scpb.TaskSize{EstimatedMemoryBytes: 3e9, EstimatedMilliCpu: 2000},
		},
		{
			// The task was OOM-killed while using less memory than its estimate
			// (it was killed for some other reason), so nothing should be
			// recorded.
			name:                "usage within estimate records nothing",
			resizeMultiplier:    1.5,
			scheduledSize:       &scpb.TaskSize{EstimatedMemoryBytes: 1e9, EstimatedMilliCpu: 2000},
			observedMemoryBytes: 800e6,
			expectedSize:        nil,
		},
		{
			// Without a scheduled memory estimate, there is no baseline to
			// compare the observed usage against, so nothing should be recorded.
			name:                "missing memory estimate records nothing",
			resizeMultiplier:    1.5,
			scheduledSize:       &scpb.TaskSize{EstimatedMilliCpu: 2000},
			observedMemoryBytes: 2e9,
			expectedSize:        nil,
		},
		{
			// Without a scheduled CPU estimate, the recorded size should fall
			// back to the default CPU estimate, since a recorded size needs a
			// non-zero CPU estimate to be read back.
			name:                "missing CPU estimate falls back to the default CPU estimate",
			resizeMultiplier:    1.5,
			scheduledSize:       &scpb.TaskSize{EstimatedMemoryBytes: 1e9},
			observedMemoryBytes: 2e9,
			expectedSize:        &scpb.TaskSize{EstimatedMemoryBytes: 3e9, EstimatedMilliCpu: tasksize.DefaultCPUEstimate},
		},
		{
			// A resize multiplier of 0 disables OOM resizing entirely.
			name:                "resizing disabled records nothing",
			resizeMultiplier:    0,
			scheduledSize:       &scpb.TaskSize{EstimatedMemoryBytes: 1e9, EstimatedMilliCpu: 2000},
			observedMemoryBytes: 2e9,
			expectedSize:        nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags.Set(t, "remote_execution.use_measured_task_sizes", true)
			flags.Set(t, "remote_execution.oom_resize_multiplier", test.resizeMultiplier)

			env := testenv.GetTestEnv(t)
			rdb := testredis.Start(t).Client()
			env.SetRemoteExecutionRedisClient(rdb)
			auth := testauth.NewTestAuthenticator(t, testauth.TestUsers())
			env.SetAuthenticator(auth)
			sizer, err := tasksize.NewSizer(env)
			require.NoError(t, err)

			ctx := t.Context()
			cmd := &repb.Command{Arguments: []string{"some_memory_hungry_command"}}
			props := &platform.Properties{}

			// Report that the OOM killer killed the task while it was using
			// observedMemoryBytes of memory.
			err = sizer.UpdateForOOM(ctx, cmd, props, test.scheduledSize, test.observedMemoryBytes)
			require.NoError(t, err)

			// Read back the size that would be used to schedule the next
			// attempt of the same command.
			ts := sizer.Get(ctx, cmd, props)
			if test.expectedSize == nil {
				require.Nil(t, ts, "no task size should be recorded")
			} else {
				require.NotNil(t, ts, "expected a task size to be recorded")
				assert.Equal(t, test.expectedSize.GetEstimatedMemoryBytes(), ts.GetEstimatedMemoryBytes())
				assert.Equal(t, test.expectedSize.GetEstimatedMilliCpu(), ts.GetEstimatedMilliCpu())
			}
		})
	}
}

func TestSizer_UpdateForOOM_DoesNotDecreaseExistingEstimate(t *testing.T) {
	flags.Set(t, "remote_execution.use_measured_task_sizes", true)
	flags.Set(t, "remote_execution.oom_resize_multiplier", 1.5)

	env := testenv.GetTestEnv(t)
	rdb := testredis.Start(t).Client()
	env.SetRemoteExecutionRedisClient(rdb)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers())
	env.SetAuthenticator(auth)
	sizer, err := tasksize.NewSizer(env)
	require.NoError(t, err)

	ctx := t.Context()
	cmd := &repb.Command{Arguments: []string{"some_memory_hungry_command"}}
	props := &platform.Properties{}

	// Record a measured size of 5GB memory and 4000 milliCPU (8 CPU-seconds
	// over a 2 second execution).
	execStart := time.Now()
	md := &repb.ExecutedActionMetadata{
		UsageStats: &repb.UsageStats{
			CpuNanos:        8 * 1e9,
			PeakMemoryBytes: 5 * 1e9,
		},
		ExecutionStartTimestamp:     timestamppb.New(execStart),
		ExecutionCompletedTimestamp: timestamppb.New(execStart.Add(2 * time.Second)),
	}
	err = sizer.Update(ctx, cmd, props, md)
	require.NoError(t, err)

	// Report an OOM kill at 2GB of observed usage for a task scheduled with a
	// 1GB estimate. The resized estimate (1.5 * 2GB = 3GB) is below the
	// recorded 5GB estimate, so the recorded size should be unchanged.
	scheduledSize := &scpb.TaskSize{EstimatedMemoryBytes: 1e9, EstimatedMilliCpu: 2000}
	err = sizer.UpdateForOOM(ctx, cmd, props, scheduledSize, 2e9)
	require.NoError(t, err)

	ts := sizer.Get(ctx, cmd, props)
	require.NotNil(t, ts)
	assert.Equal(t, int64(5*1e9), ts.GetEstimatedMemoryBytes())
	assert.Equal(t, int64(4000), ts.GetEstimatedMilliCpu())
}

func TestNewSizer_InvalidOOMResizeMultiplier(t *testing.T) {
	// A multiplier below 1 (other than 0, which disables resizing) would
	// resize the estimate to below the observed usage, so NewSizer should
	// reject it.
	flags.Set(t, "remote_execution.oom_resize_multiplier", 0.5)
	env := testenv.GetTestEnv(t)
	_, err := tasksize.NewSizer(env)
	require.Error(t, err)
}

func TestCgroupSettings(t *testing.T) {
	ctx := t.Context()
	env := testenv.GetTestEnv(t)

	// Basic settings should be applied
	{
		size := &scpb.TaskSize{
			EstimatedMilliCpu:    1000,
			EstimatedMemoryBytes: 800e6,
		}
		actual := tasksize.GetCgroupSettings(ctx, env.GetExperimentFlagProvider(), size, &scpb.SchedulingMetadata{})
		expected := &scpb.CgroupSettings{
			CpuWeight:          proto.Int64(39),
			CpuQuotaLimitUsec:  proto.Int64(30 * 100 * 1e3),
			CpuQuotaPeriodUsec: proto.Int64(1 * 100 * 1e3),
			PidsMax:            proto.Int64(4096 + 4096), // 4096 base limit + (1*4096) CPU-based limit
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
		settings := tasksize.GetCgroupSettings(ctx, env.GetExperimentFlagProvider(), size, &scpb.SchedulingMetadata{})
		assert.Equal(t, int64(weight), settings.GetCpuWeight())
	}
}

func TestCgroupSettings_AdditionalPIDsLimit(t *testing.T) {
	ctx := t.Context()
	env := testenv.GetTestEnv(t)

	flags.Set(t, "remote_execution.pids_limit", 10_000)
	flags.Set(t, "remote_execution.additional_pids_limit_per_cpu", 200)
	size := &scpb.TaskSize{EstimatedMilliCpu: 1500}
	settings := tasksize.GetCgroupSettings(ctx, env.GetExperimentFlagProvider(), size, &scpb.SchedulingMetadata{})
	const wantPidsMax = 10_300 // 10K base limit + (1.5*200 = 300) CPU-based limit
	assert.Equal(t, int64(10_300), settings.GetPidsMax())
}
