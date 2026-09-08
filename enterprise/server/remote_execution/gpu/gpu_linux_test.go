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

func TestGetTotalGPUMemoryBytes_ConfiguredMonitor(t *testing.T) {
	previous := defaultMemoryMonitor
	t.Cleanup(func() { defaultMemoryMonitor = previous })
	defaultMemoryMonitor = &memoryMonitor{devices: []gpuDevice{
		{uuid: "GPU-a", device: &mock.Device{
			GetMemoryInfoFunc: func() (nvml.Memory, nvml.Return) {
				return nvml.Memory{Total: 8_000_000_000, Used: 3_000_000_000, Free: 5_000_000_000}, nvml.SUCCESS
			},
		}},
		{uuid: "GPU-b", device: &mock.Device{
			GetMemoryInfoFunc: func() (nvml.Memory, nvml.Return) {
				return nvml.Memory{Total: 24_000_000_000, Used: 4_000_000_000, Free: 20_000_000_000}, nvml.SUCCESS
			},
		}},
	}}

	// Reuse the monitor's devices without loading NVML or starting a poller.
	// Capacity includes memory that is already in use on either GPU.
	total, err := GetTotalGPUMemoryBytes()
	require.NoError(t, err)
	require.Equal(t, int64(32_000_000_000), total)
}

func TestGetTotalGPUMemoryBytes_WithoutMonitor(t *testing.T) {
	previous := defaultMemoryMonitor
	t.Cleanup(func() { defaultMemoryMonitor = previous })
	defaultMemoryMonitor = nil

	// Capacity queries cannot discover devices before Configure has run.
	total, err := GetTotalGPUMemoryBytes()
	require.ErrorContains(t, err, "monitor is unavailable")
	require.Zero(t, total)
}

func TestConfigure_GPUMemoryTrackingDisabled_CapacityIsAvailable(t *testing.T) {
	previousMonitor := defaultMemoryMonitor
	previousLibrary := nvmlLibrary
	t.Cleanup(func() {
		defaultMemoryMonitor = previousMonitor
		nvmlLibrary = previousLibrary
	})
	flags.Set(t, "executor.gpu_memory_tracking_enabled", false)
	flags.Set(t, "executor.gpu_memory_poll_interval", time.Duration(0))
	device := &mock.Device{
		GetUUIDFunc: func() (string, nvml.Return) { return "GPU-a", nvml.SUCCESS },
		GetMemoryInfoFunc: func() (nvml.Memory, nvml.Return) {
			return nvml.Memory{Total: 8_000_000_000}, nvml.SUCCESS
		},
	}
	library := &mock.Interface{
		InitFunc:           func() nvml.Return { return nvml.SUCCESS },
		DeviceGetCountFunc: func() (int, nvml.Return) { return 1, nvml.SUCCESS },
		DeviceGetHandleByIndexFunc: func(index int) (nvml.Device, nvml.Return) {
			return device, nvml.SUCCESS
		},
	}
	nvmlLibrary = library

	// Disabled tracking still discovers GPUs for capacity queries. The invalid
	// poll interval is irrelevant because neither query starts polling.
	require.NoError(t, Configure())
	require.NotNil(t, defaultMemoryMonitor)
	for range 2 {
		total, err := GetTotalGPUMemoryBytes()
		require.NoError(t, err)
		require.Equal(t, int64(8_000_000_000), total)
	}
	require.Nil(t, CgroupUsage(t.TempDir()))
	require.Len(t, library.InitCalls(), 1)
	require.Empty(t, library.ShutdownCalls())
	require.Empty(t, device.GetComputeRunningProcessesCalls())
}

func TestConfigure_NVMLErrors(t *testing.T) {
	flags.Set(t, "executor.gpu_memory_tracking_enabled", true)
	for _, testCase := range []struct {
		name        string
		initRet     nvml.Return
		count       int
		countRet    nvml.Return
		handleRet   nvml.Return
		uuidRet     nvml.Return
		shutdownRet nvml.Return
		wantErr     string
	}{
		{name: "NVML unavailable", initRet: nvml.ERROR_LIBRARY_NOT_FOUND},
		{name: "driver unavailable", initRet: nvml.ERROR_DRIVER_NOT_LOADED, wantErr: "initialize NVML"},
		{name: "device count", countRet: nvml.ERROR_UNKNOWN, wantErr: "get device count"},
		{name: "no GPUs", wantErr: "no NVIDIA GPUs"},
		{name: "device handle", count: 1, handleRet: nvml.ERROR_GPU_IS_LOST, wantErr: "get device 0"},
		{name: "device UUID", count: 1, uuidRet: nvml.ERROR_GPU_IS_LOST, wantErr: "get device 0 UUID"},
		{name: "discovery and shutdown", count: 1, uuidRet: nvml.ERROR_GPU_IS_LOST, shutdownRet: nvml.ERROR_UNKNOWN, wantErr: "get device 0 UUID"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			previousMonitor := defaultMemoryMonitor
			previousLibrary := nvmlLibrary
			t.Cleanup(func() {
				defaultMemoryMonitor = previousMonitor
				nvmlLibrary = previousLibrary
			})
			defaultMemoryMonitor = nil
			library := &mock.Interface{
				InitFunc:           func() nvml.Return { return testCase.initRet },
				ShutdownFunc:       func() nvml.Return { return testCase.shutdownRet },
				DeviceGetCountFunc: func() (int, nvml.Return) { return testCase.count, testCase.countRet },
				DeviceGetHandleByIndexFunc: func(index int) (nvml.Device, nvml.Return) {
					return &mock.Device{
						GetUUIDFunc: func() (string, nvml.Return) { return "GPU-a", testCase.uuidRet },
					}, testCase.handleRet
				},
			}
			nvmlLibrary = library

			// Missing NVML must not prevent startup, even with tracking enabled.
			// Other initialization failures are reported and release NVML when
			// needed, without hiding the original discovery error.
			err := Configure()
			if testCase.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, testCase.wantErr)
			}
			require.Nil(t, defaultMemoryMonitor)
			require.Nil(t, CgroupUsage(t.TempDir()))
			_, capacityErr := GetTotalGPUMemoryBytes()
			require.ErrorContains(t, capacityErr, "monitor is unavailable")
			if testCase.initRet != nvml.SUCCESS {
				require.Empty(t, library.ShutdownCalls())
			} else {
				require.Len(t, library.ShutdownCalls(), 1)
				if testCase.shutdownRet != nvml.SUCCESS {
					require.ErrorIs(t, err, testCase.shutdownRet)
				}
			}
		})
	}
}

func TestTotalGPUMemoryBytes_PartialQueryFails(t *testing.T) {
	devices := []gpuDevice{
		{uuid: "GPU-a", device: &mock.Device{
			GetMemoryInfoFunc: func() (nvml.Memory, nvml.Return) {
				return nvml.Memory{Total: 8_000_000_000}, nvml.SUCCESS
			},
		}},
		{uuid: "GPU-b", device: &mock.Device{
			GetMemoryInfoFunc: func() (nvml.Memory, nvml.Return) {
				return nvml.Memory{}, nvml.ERROR_GPU_IS_LOST
			},
		}},
	}

	// Reporting a partial sum would understate capacity without revealing
	// that a GPU could not be queried.
	total, err := totalGPUMemoryBytes(devices)
	require.ErrorContains(t, err, "GPU-b")
	require.ErrorIs(t, err, nvml.ERROR_GPU_IS_LOST)
	require.Zero(t, total)
}

func TestTotalGPUMemoryBytes_Overflow(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		totals []uint64
	}{
		{name: "device capacity", totals: []uint64{math.MaxUint64}},
		{name: "combined capacity", totals: []uint64{math.MaxInt64, 1}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var devices []gpuDevice
			for _, total := range testCase.totals {
				devices = append(devices, gpuDevice{device: &mock.Device{
					GetMemoryInfoFunc: func() (nvml.Memory, nvml.Return) {
						return nvml.Memory{Total: total}, nvml.SUCCESS
					},
				}})
			}

			// NVML reports uint64 capacities, but scheduling uses int64.
			// Reject either a device or a sum that cannot be represented.
			total, err := totalGPUMemoryBytes(devices)
			require.ErrorContains(t, err, "total GPU memory exceeds")
			require.Zero(t, total)
		})
	}
}

func TestConfigure_InvalidPollInterval_Fails(t *testing.T) {
	flags.Set(t, "executor.gpu_memory_tracking_enabled", true)
	flags.Set(t, "executor.gpu_memory_poll_interval", time.Duration(0))

	err := Configure()
	require.ErrorContains(t, err, "must be at least 1ms")
}

func TestCgroupUsage_StartsPollingOnceOnFirstMeasurement(t *testing.T) {
	flags.Set(t, "executor.gpu_memory_tracking_enabled", false)
	flags.Set(t, "executor.gpu_memory_poll_interval", time.Hour)
	previous := defaultMemoryMonitor
	t.Cleanup(func() { defaultMemoryMonitor = previous })
	cgroupPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cgroupPath, "cgroup.procs"), []byte("123\n"), 0o644))
	device := &mock.Device{
		GetUUIDFunc: func() (string, nvml.Return) { return "GPU-a", nvml.SUCCESS },
		GetComputeRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
			return []nvml.ProcessInfo{{Pid: 123, UsedGpuMemory: 8}}, nvml.SUCCESS
		},
		GetGraphicsRunningProcessesFunc: func() ([]nvml.ProcessInfo, nvml.Return) {
			return nil, nvml.SUCCESS
		},
	}
	library := &mock.Interface{
		InitFunc:           func() nvml.Return { return nvml.SUCCESS },
		DeviceGetCountFunc: func() (int, nvml.Return) { return 1, nvml.SUCCESS },
		DeviceGetHandleByIndexFunc: func(index int) (nvml.Device, nvml.Return) {
			return device, nvml.SUCCESS
		},
	}
	monitor, err := newMemoryMonitor(t.Context(), library)
	require.NoError(t, err)
	defaultMemoryMonitor = monitor

	// Constructing a monitor and requesting usage with tracking disabled must
	// leave the process poller idle.
	require.Nil(t, CgroupUsage(cgroupPath))
	require.Never(t, func() bool { return len(device.GetComputeRunningProcessesCalls()) > 0 }, 20*time.Millisecond, time.Millisecond)

	// Concurrent first measurements start a single poller. Its initial sample
	// is available without waiting for the hourly interval set above.
	flags.Set(t, "executor.gpu_memory_tracking_enabled", true)
	done := make(chan struct{}, 10)
	for range 10 {
		go func() {
			CgroupUsage(cgroupPath)
			done <- struct{}{}
		}()
	}
	for range 10 {
		<-done
	}
	require.Eventually(t, func() bool { return CgroupUsage(cgroupPath).GetTotalMemoryBytes() == 8 }, time.Second, time.Millisecond)
	require.Never(t, func() bool { return len(device.GetComputeRunningProcessesCalls()) > 1 }, 20*time.Millisecond, time.Millisecond)
	require.Len(t, device.GetGraphicsRunningProcessesCalls(), 1)
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
