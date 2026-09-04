//go:build linux && !android && cgo && !static

package gpu

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/NVIDIA/go-nvml/pkg/nvml/mock"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestConfigure_GPUMemoryTrackingDisabled_DoesNotFail(t *testing.T) {
	flags.Set(t, "executor.gpu_memory_tracking_enabled", false)
	flags.Set(t, "executor.gpu_memory_poll_interval", time.Duration(0))

	err := Configure()
	require.NoError(t, err)
}

func TestConfigure_InvalidPollInterval_Fails(t *testing.T) {
	flags.Set(t, "executor.gpu_memory_tracking_enabled", true)
	flags.Set(t, "executor.gpu_memory_poll_interval", time.Duration(0))

	err := Configure()
	require.ErrorContains(t, err, "must be at least 1ms")
}

func TestNewMemoryMonitor_NVMLFailsToInitialize_ReturnsNVMLError(t *testing.T) {
	library := &mock.Interface{
		InitFunc: func() nvml.Return { return nvml.ERROR_LIBRARY_NOT_FOUND },
	}

	_, err := newMemoryMonitor(library)
	require.ErrorContains(t, err, "initialize NVML")
}

func TestNewMemoryMonitor_NoGPUsAvailable_ReturnsError(t *testing.T) {
	library := &mock.Interface{
		InitFunc:           func() nvml.Return { return nvml.SUCCESS },
		DeviceGetCountFunc: func() (int, nvml.Return) { return 0, nvml.SUCCESS },
		ShutdownFunc:       func() nvml.Return { return nvml.SUCCESS },
	}

	_, err := newMemoryMonitor(library)
	require.ErrorContains(t, err, "reported no NVIDIA GPUs")
	require.Len(t, library.ShutdownCalls(), 1)
}

func TestDeviceProcessMemory_UnavailableValueReported_ValueIsIgnored(t *testing.T) {
	device := gpuDevice{device: &mock.Device{
		GetComputeRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
			return []nvml.ProcessInfo{
				{Pid: 123, UsedGpuMemory: 2 * 1024 * 1024},
				// Uint64 means "unavailable"
				{Pid: 456, UsedGpuMemory: math.MaxUint64},
			}, nvml.SUCCESS
		},
		GetGraphicsRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
			return nil, nvml.SUCCESS
		},
	}}

	memoryBytesByPID, err := device.processMemory()
	require.NoError(t, err)
	require.Equal(t, map[int]int64{123: 2 * 1024 * 1024}, memoryBytesByPID)
}

func TestDeviceProcessMemory_ProcessHoldingComputeAndGraphicsMemory_UsageIsCountedOnce(t *testing.T) {
	// PID 100 holds both a compute and a graphics context, so NVML reports it
	// in both lists with its total usage. PIDs 200 and 300 hold only one
	// context type each.
	device := gpuDevice{device: &mock.Device{
		GetComputeRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
			return []nvml.ProcessInfo{
				{Pid: 100, UsedGpuMemory: 5 * 1024 * 1024},
				{Pid: 200, UsedGpuMemory: 2 * 1024 * 1024},
			}, nvml.SUCCESS
		},
		GetGraphicsRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
			return []nvml.ProcessInfo{
				{Pid: 100, UsedGpuMemory: 5 * 1024 * 1024},
				{Pid: 300, UsedGpuMemory: 3 * 1024 * 1024},
			}, nvml.SUCCESS
		},
	}}

	memoryBytesByPID, err := device.processMemory()
	require.NoError(t, err)
	require.Equal(t, map[int]int64{
		100: 5 * 1024 * 1024,
		200: 2 * 1024 * 1024,
		300: 3 * 1024 * 1024,
	}, memoryBytesByPID)
}

func TestMemoryMonitorRead_MultipleReads_OnlyLatestReadingIsReported(t *testing.T) {
	processes := []nvml.ProcessInfo{
		{Pid: 123, UsedGpuMemory: 2 * 1024 * 1024},
		{Pid: 456, UsedGpuMemory: 3 * 1024 * 1024},
	}
	device := &mock.Device{
		GetComputeRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
			return processes, nvml.SUCCESS
		},
		GetGraphicsRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
			return nil, nvml.SUCCESS
		},
	}
	m := &memoryMonitor{devices: []gpuDevice{{device: device, uuid: "GPU-a"}}}

	first, err := m.read()
	require.NoError(t, err)
	require.Equal(t, memoryReading{
		"GPU-a": {
			123: 2 * 1024 * 1024,
			456: 3 * 1024 * 1024,
		},
	}, first)

	processes = []nvml.ProcessInfo{{Pid: 789, UsedGpuMemory: 7 * 1024 * 1024}}
	second, err := m.read()
	require.NoError(t, err)
	require.Equal(t, memoryReading{
		"GPU-a": {789: 7 * 1024 * 1024},
	}, second)
}

func TestMemoryMonitorRead_MultipleGPUsWithSingleGPUFailing_EntireReadingIsDiscarded(t *testing.T) {
	m := &memoryMonitor{devices: []gpuDevice{
		{uuid: "GPU-a", device: &mock.Device{
			GetComputeRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
				return []nvml.ProcessInfo{{Pid: 123, UsedGpuMemory: 2 * 1024 * 1024}}, nvml.SUCCESS
			},
			GetGraphicsRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
				return nil, nvml.SUCCESS
			},
		}},
		{uuid: "GPU-b", device: &mock.Device{
			GetComputeRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
				return nil, nvml.ERROR_GPU_IS_LOST
			},
		}},
	}}

	reading, err := m.read()
	require.ErrorContains(t, err, "GPU-b")
	require.Nil(t, reading)
}

func TestMemoryMonitorCgroupGPUUsage_CgroupAndUnrelatedProcesses_OnlyCgroupMemoryIsSummed(t *testing.T) {
	cgroupPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupPath, "cgroup.procs"), []byte("123\n456\n"), 0o644))

	m := &memoryMonitor{lastReading: memoryReading{
		"GPU-b": {
			123: 7 * 1024 * 1024,
		},
		"GPU-a": {
			123: 2 * 1024 * 1024,
			456: 3 * 1024 * 1024,
			// PID 789 is not in the cgroup.
			789: 100 * 1024 * 1024,
		},
	}}
	want := &repb.GPUUsage{
		TotalMemoryBytes: 12 * 1024 * 1024,
		DeviceUsage: []*repb.GPUDeviceUsage{
			{Id: "GPU-a", MemoryBytes: 5 * 1024 * 1024, Vendor: repb.GPUDeviceUsage_NVIDIA},
			{Id: "GPU-b", MemoryBytes: 7 * 1024 * 1024, Vendor: repb.GPUDeviceUsage_NVIDIA},
		},
	}
	usage := m.cgroupGPUUsage(cgroupPath)
	require.Empty(t, cmp.Diff(want, usage, protocmp.Transform()))
}

func TestMemoryMonitorCgroupGPUUsage_NoAvailableData_UsageIsNil(t *testing.T) {
	cgroupPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupPath, "cgroup.procs"), nil, 0o644))

	m := &memoryMonitor{}
	usage := m.cgroupGPUUsage(cgroupPath)
	require.Nil(t, usage)
}

func TestMemoryMonitorCgroupGPUUsage_EmptyReading_ZeroUsageIsReported(t *testing.T) {
	cgroupPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupPath, "cgroup.procs"), nil, 0o644))

	m := &memoryMonitor{lastReading: memoryReading{}}
	usage := m.cgroupGPUUsage(cgroupPath)
	require.NotNil(t, usage)
	require.Empty(t, cmp.Diff(&repb.GPUUsage{}, usage, protocmp.Transform()))
}
