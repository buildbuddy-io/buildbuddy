//go:build linux && !android && cgo && !static

package gpu

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	// defaultMemoryMonitor is the executor-wide monitor, set by configure when
	// GPU memory tracking is enabled.
	defaultMemoryMonitor *memoryMonitor
)

// memoryReading maps GPU ID to PID to GPU memory usage in bytes, as measured
// in a single polling pass over all GPUs.
type memoryReading map[string]map[int]int64

type readFunc func() (memoryReading, error)

// gpuDevice pairs an NVML device handle with its UUID, which is queried once
// at discovery time.
type gpuDevice struct {
	device nvml.Device
	uuid   string
}

// discoverDevices returns the accessible NVIDIA GPUs and their stable UUIDs.
// Zero accessible GPUs is reported as an error, since enabling GPU tracking
// on an executor without GPUs indicates a misconfiguration.
func discoverDevices(library nvml.Interface) ([]gpuDevice, error) {
	count, ret := library.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("get device count: %w", ret)
	}
	if count == 0 {
		return nil, errors.New("NVML reported no NVIDIA GPUs")
	}
	devices := make([]gpuDevice, 0, count)
	for index := range count {
		device, ret := library.DeviceGetHandleByIndex(index)
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("get device %d: %w", index, ret)
		}
		uuid, ret := device.GetUUID()
		if ret != nvml.SUCCESS {
			return nil, fmt.Errorf("get device %d UUID: %w", index, ret)
		}
		devices = append(devices, gpuDevice{device: device, uuid: uuid})
	}
	return devices, nil
}

// processMemory returns GPU memory usage in bytes by PID for the compute
// (e.g. CUDA) and graphics (e.g. OpenGL) processes using the device.
//
// Per the NVML docs, usedGpuMemory in each process list is "all of the memory
// used by the application", so a process holding both a compute and a
// graphics context appears in both lists with its total usage (confirmed
// empirically). Such processes are deduplicated by PID rather than summed.
func (d gpuDevice) processMemory() (map[int]int64, error) {
	compute, ret := d.device.GetComputeRunningProcesses()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("query compute processes: %w", ret)
	}
	graphics, ret := d.device.GetGraphicsRunningProcesses()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("query graphics processes: %w", ret)
	}
	memoryBytesByPID := make(map[int]int64, len(compute)+len(graphics))
	for _, info := range slices.Concat(compute, graphics) {
		// NVML reports "value not available" as all ones, e.g. under WSL and
		// on some virtualized GPUs. Skip such processes since there is no
		// memory value to attribute.
		if info.UsedGpuMemory == math.MaxUint64 {
			continue
		}
		if uint64(info.Pid) > uint64(math.MaxInt) {
			return nil, fmt.Errorf("PID is out of range (%d)", info.Pid)
		}
		if info.UsedGpuMemory > math.MaxInt64 {
			return nil, fmt.Errorf("memory is out of range (%d bytes)", info.UsedGpuMemory)
		}
		// The lists are sampled at slightly different times, so take the max
		// of the samples to better approximate the peak.
		memoryBytesByPID[int(info.Pid)] = max(memoryBytesByPID[int(info.Pid)], int64(info.UsedGpuMemory))
	}
	return memoryBytesByPID, nil
}

// memoryMonitor samples NVIDIA GPU memory usage and attributes it to cgroups.
type memoryMonitor struct {
	library nvml.Interface
	devices []gpuDevice

	// mu guards lastReading.
	mu sync.Mutex
	// lastReading is the most recent complete poll. A nil map means that no
	// successful reading is available.
	lastReading memoryReading
}

// newMemoryMonitor initializes NVML, discovers GPUs once so polling only
// queries process memory, and starts the background poller. If discovery
// fails, NVML is shut down before returning the error.
func newMemoryMonitor(library nvml.Interface) (*memoryMonitor, error) {
	if ret := library.Init(); ret != nvml.SUCCESS {
		return nil, fmt.Errorf("initialize NVML: %w", ret)
	}
	devices, err := discoverDevices(library)
	if err != nil {
		err = fmt.Errorf("discover GPUs: %w", err)
		if ret := library.Shutdown(); ret != nvml.SUCCESS {
			err = errors.Join(err, fmt.Errorf("shutdown NVML: %w", ret))
		}
		return nil, err
	}
	m := &memoryMonitor{library: library, devices: devices}
	go m.monitor(context.Background(), m.read)
	return m, nil
}

// cgroupGPUUsage returns the most recent GPU memory usage for processes in
// the given cgroup, aggregated by GPU. It returns nil when no successful NVML
// reading is available or the cgroup's process list cannot be read.
func (m *memoryMonitor) cgroupGPUUsage(cgroupPath string) *repb.GPUUsage {
	pids, err := cgroup.ReadCgroupProcs(cgroupPath)
	if err != nil {
		// The cgroup may be deleted while a final stats poll is in flight, so
		// don't warn about a missing cgroup.
		if !errors.Is(err, os.ErrNotExist) {
			log.Warningf("Could not read NVIDIA GPU usage for cgroup %q because reading its process list returned %s", cgroupPath, err)
		}
		return nil
	}

	m.mu.Lock()
	lastReading := m.lastReading
	m.mu.Unlock()

	if lastReading == nil {
		return nil
	}

	memoryBytesByGPU := make(map[string]int64)
	for gpuID, memoryBytesByPID := range lastReading {
		for pid, memoryBytes := range memoryBytesByPID {
			if _, ok := pids[pid]; ok {
				memoryBytesByGPU[gpuID] += memoryBytes
			}
		}
	}

	gpuIDs := make([]string, 0, len(memoryBytesByGPU))
	for gpuID := range memoryBytesByGPU {
		gpuIDs = append(gpuIDs, gpuID)
	}
	// Sort by ID so clients see per-GPU stats in a deterministic order.
	slices.Sort(gpuIDs)

	usage := &repb.GPUUsage{}
	for _, gpuID := range gpuIDs {
		memoryBytes := memoryBytesByGPU[gpuID]
		usage.TotalMemoryBytes += memoryBytes
		usage.DeviceUsage = append(usage.DeviceUsage, &repb.GPUDeviceUsage{
			Id:          gpuID,
			MemoryBytes: memoryBytes,
			Vendor:      repb.GPUDeviceUsage_NVIDIA,
		})
	}
	return usage
}

// monitor polls NVML and replaces the published reading after each poll. A
// failed poll clears the published reading rather than leaving stale data.
func (m *memoryMonitor) monitor(ctx context.Context, read readFunc) {
	ticker := time.NewTicker(*gpuMemoryPollInterval)
	defer ticker.Stop()
	var lastError string
	for {
		reading, err := read()
		if err != nil {
			m.setReading(nil)
			if err.Error() != lastError {
				log.Warningf("Could not query NVIDIA GPU memory usage with NVML: %s", err)
				lastError = err.Error()
			}
		} else {
			m.setReading(reading)
			lastError = ""
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// read returns a reading of the memory used by each process on each GPU. If
// any GPU query fails, the whole reading is discarded, so a partial reading
// is never presented as total usage.
func (m *memoryMonitor) read() (memoryReading, error) {
	reading := make(memoryReading)
	for _, device := range m.devices {
		memoryBytesByPID, err := device.processMemory()
		if err != nil {
			return nil, fmt.Errorf("query GPU %q processes: %w", device.uuid, err)
		}
		if len(memoryBytesByPID) > 0 {
			reading[device.uuid] = memoryBytesByPID
		}
	}
	return reading, nil
}

func (m *memoryMonitor) setReading(reading memoryReading) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastReading = reading
}

// configure creates the executor-wide memory monitor.
func configure() error {
	monitor, err := newMemoryMonitor(nvml.New())
	if err != nil {
		return err
	}
	defaultMemoryMonitor = monitor
	return nil
}

// cgroupUsage returns the latest reading from the executor-wide monitor.
func cgroupUsage(cgroupPath string) *repb.GPUUsage {
	if defaultMemoryMonitor == nil {
		// Configure was not called; usage is unknown.
		return nil
	}
	return defaultMemoryMonitor.cgroupGPUUsage(cgroupPath)
}
