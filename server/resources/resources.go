package resources

import (
	"flag"
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/elastic/gosigar"

)

const (
	memoryEnvVarName   = "SYS_MEMORY_BYTES"
	cpuEnvVarName      = "SYS_MILLICPU"
	nodeEnvVarName     = "MY_NODENAME"
	hostnameEnvVarName = "MY_HOSTNAME"
	portEnvVarName     = "MY_PORT"
	poolEnvVarName     = "MY_POOL"
)

var (
	allocatedRAMBytes  int64
	allocatedCPUMillis int64
	once               sync.Once
)

func init() {
	once.Do(func() {
		setSysRAMBytes()
		setSysMilliCPUCapacity()
	})
}

func setSysRAMBytes() {
	if v := os.Getenv(memoryEnvVarName); v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			allocatedRAMBytes = i
			return
		}
	}
	mem := gosigar.Mem{}
	mem.Get()
	allocatedRAMBytes = int64(mem.ActualFree)
	log.Debugf("Set allocatedRAMBytes to %d", allocatedRAMBytes)
}

func setSysMilliCPUCapacity() {
	if v := os.Getenv(cpuEnvVarName); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			allocatedCPUMillis = int64(f * 1000)
			return
		}
	}

	cpuList := gosigar.CpuList{}
	cpuList.Get()
	numCores := len(cpuList.List)
	allocatedCPUMillis = int64(numCores * 1000)
	log.Debugf("Set allocatedCPUMillis to %d", allocatedCPUMillis)
}

func GetSysFreeRAMBytes() int64 {
	mem := gosigar.Mem{}
	mem.Get()
	return int64(mem.ActualFree)
}

func GetAllocatedRAMBytes() int64 {
	return allocatedRAMBytes
}

func GetAllocatedCPUMillis() int64 {
	return allocatedCPUMillis
}

func GetNodeName() string {
	return os.Getenv(nodeEnvVarName)
}

func GetPoolName() string {
	return os.Getenv(poolEnvVarName)
}

func GetArch() string {
	return runtime.GOARCH
}

func GetOS() string {
	return runtime.GOOS
}

func GetMyHostname() (string, error) {
	if v := os.Getenv(hostnameEnvVarName); v != "" {
		return v, nil
	}
	return os.Hostname()
}

func GetMyPort() (int32, error) {
	portStr := ""
	if v := os.Getenv(portEnvVarName); v != "" {
		portStr = v
	} else {
		if v := flag.Lookup("grpc_port"); v != nil {
			portStr = v.Value.String()
		}
	}
	i, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(i), nil
}

type Tracker struct {
	capacity *interfaces.Resources

	mu       sync.Mutex // protects(assigned)
	assigned *interfaces.Resources
}

type TrackerOptions struct {
	RAMBytesCapacityOverride  int64
	CPUMillisCapacityOverride int64
}

func NewTracker(opts *TrackerOptions) *Tracker {
	capacity := &interfaces.Resources{
		MemoryBytes: opts.RAMBytesCapacityOverride,
		MilliCPU:    opts.CPUMillisCapacityOverride,
	}
	if capacity.MemoryBytes == 0 {
		capacity.MemoryBytes = int64(float64(GetAllocatedRAMBytes()) * .80)
	}
	if capacity.MilliCPU == 0 {
		capacity.MilliCPU = int64(float64(GetAllocatedCPUMillis()) * .80)
	}
	return &Tracker{
		capacity: &interfaces.Resources{
			MemoryBytes: GetAllocatedRAMBytes(),
			MilliCPU:    GetAllocatedCPUMillis(),
		},
	}
}

func (t *Tracker) Request(r *interfaces.Resources) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if r.MemoryBytes > t.capacity.MemoryBytes-t.assigned.MemoryBytes ||
		r.MilliCPU > t.capacity.MilliCPU-t.assigned.MilliCPU {
		return false
	}

	t.assigned.MemoryBytes += r.MemoryBytes
	t.assigned.MilliCPU += r.MilliCPU
	t.updateMetrics()
	return true
}

func (t *Tracker) Return(r *interfaces.Resources) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.assigned.MemoryBytes -= r.MemoryBytes
	t.assigned.MilliCPU -= r.MilliCPU
	t.updateMetrics()
}

func (t *Tracker) updateMetrics() {
	metrics.RemoteExecutionAssignedRAMBytes.Set(float64(t.assigned.MemoryBytes))
	metrics.RemoteExecutionAssignedMilliCPU.Set(float64(t.assigned.MilliCPU))
}
