package resources

import (
	"flag"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync"

	"cloud.google.com/go/compute/metadata"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/elastic/gosigar"
)

var (
	memoryBytes  = flag.Int64("executor.memory_bytes", 0, "Optional maximum memory to allocate to execution tasks (approximate). Cannot set both this option and the SYS_MEMORY_BYTES env var.")
	milliCPU     = flag.Int64("executor.millicpu", 0, "Optional maximum CPU milliseconds to allocate to execution tasks (approximate). Cannot set both this option and the SYS_MILLICPU env var.")
	zoneOverride = flag.String("zone_override", "", "A value that will override the auto-detected zone. Ignored if empty")
)

const (
	memoryEnvVarName   = "SYS_MEMORY_BYTES"
	cpuEnvVarName      = "SYS_MILLICPU"
	nodeEnvVarName     = "MY_NODENAME"
	hostnameEnvVarName = "MY_HOSTNAME"
	portEnvVarName     = "MY_PORT"
	poolEnvVarName     = "MY_POOL"
	podUIDVarName      = "K8S_POD_UID"
)

const (
	ZoneHeader = "zone"
)

var (
	allocatedRAMBytes  int64
	allocatedCPUMillis int64
	once               sync.Once
)

var (
	podIDFromCpusetRegexp = regexp.MustCompile(`/kubepods(/.*?)?/pod([a-z0-9\-]{36})/`)
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
	allocatedRAMBytes = int64(mem.Total)
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
}

func Configure() error {
	if *memoryBytes > 0 {
		if os.Getenv(memoryEnvVarName) != "" {
			return status.InvalidArgumentErrorf("Only one of the 'executor.memory_bytes' config option and 'SYS_MEMORY_BYTES' environment variable may be set")
		}
		allocatedRAMBytes = *memoryBytes
	}
	if *milliCPU > 0 {
		if os.Getenv(cpuEnvVarName) != "" {
			return status.InvalidArgumentErrorf("Only one of the 'executor.millicpu' config option and 'SYS_MILLICPU' environment variable may be set")
		}
		allocatedCPUMillis = *milliCPU
	}

	log.Debugf("Set allocatedRAMBytes to %d", allocatedRAMBytes)
	log.Debugf("Set allocatedCPUMillis to %d", allocatedCPUMillis)

	return nil
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
	} else if p, err := flagutil.GetDereferencedValue[int]("grpc_port"); err == nil {
		portStr = strconv.Itoa(p)
	}
	i, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(i), nil
}

func GetZone() (string, error) {
	if *zoneOverride != "" {
		return *zoneOverride, nil
	}
	if !metadata.OnGCE() {
		return "", status.UnavailableError("not running on GCE")
	}
	return metadata.Zone()
}

func GetK8sPodUID() (string, error) {
	if podID := os.Getenv(podUIDVarName); podID != "" {
		return podID, nil
	}
	if _, err := os.Stat("/proc/1/cpuset"); err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	buf, err := os.ReadFile("/proc/1/cpuset")
	if err != nil {
		return "", err
	}
	cpuset := string(buf)
	if m := podIDFromCpusetRegexp.FindStringSubmatch(cpuset); m != nil {
		return m[2], nil
	}
	return "", nil
}
