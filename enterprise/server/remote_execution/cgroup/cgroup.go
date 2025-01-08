//go:build linux && !android

package cgroup

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/block_io"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cpuset"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

const (
	// Standard path where cgroupfs is expected to be mounted.
	RootPath = "/sys/fs/cgroup"

	// Placeholder value representing the container ID in cgroup path templates.
	cidPlaceholder = "{{.ContainerID}}"
)

var (
	// ErrV1NotSupported is returned when a function does not support cgroup V1.
	ErrV1NotSupported = fmt.Errorf("cgroup v1 is not supported")
)

// GetCurrent returns the cgroup of which the current process is a member.
//
// The returned path is relative to the cgroupfs root. For example, if the
// current process is part of "/sys/fs/cgroup/foo", this returns "foo".
func GetCurrent() (string, error) {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", fmt.Errorf("read cgroup from procfs: %w", err)
	}
	s := strings.TrimRight(string(b), "\n")
	if s == "" {
		return "", nil
	}
	lines := strings.Split(s, "\n")
	// In cgroup v1, a process can be a member of multiple cgroup hierarchies.
	if len(lines) > 1 {
		return "", ErrV1NotSupported
	}
	parts := strings.Split(lines[0], ":")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid /proc/self/cgroup value %q", err)
	}
	if controllers := parts[1]; controllers != "" {
		return "", ErrV1NotSupported
	}
	// re-join in case the path itself contains ":"
	path := strings.Join(parts[2:], ":")
	// Strip leading "/"
	path = strings.TrimPrefix(path, string(os.PathSeparator))
	return path, nil
}

// Setup configures the cgroup at the given path with the given settings.
// Any settings for cgroup controllers that aren't enabled are ignored.
// IO limits are applied to the given block device, if specified.
func Setup(ctx context.Context, path string, s *scpb.CgroupSettings, blockDevice *block_io.Device) error {
	m, err := settingsMap(s, blockDevice)
	if err != nil {
		return err
	}
	if len(m) == 0 {
		return nil
	}
	enabledControllers, err := ParentEnabledControllers(path)
	if err != nil {
		return fmt.Errorf("read enabled controllers: %w", err)
	}
	for name, value := range m {
		controller, _, _ := strings.Cut(name, ".")
		if !enabledControllers[controller] {
			log.CtxWarningf(ctx, "Skipping cgroup %q setting for disabled cgroup controller %q", name, controller)
			continue
		}
		settingFilePath := filepath.Join(path, name)
		if err := os.WriteFile(settingFilePath, []byte(value), 0); err != nil {
			return fmt.Errorf("write %q to cgroup file %q: %w", value, name, err)
		}
	}
	return nil
}

// ParentEnabledControllers returns the cgroup controllers that are enabled for
// the parent cgroup of a given cgroup.
func ParentEnabledControllers(path string) (map[string]bool, error) {
	b, err := os.ReadFile(filepath.Join(path, "..", "cgroup.subtree_control"))
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(b))
	enabled := make(map[string]bool, len(fields))
	for _, f := range fields {
		enabled[f] = true
	}
	return enabled, nil
}

func settingsMap(s *scpb.CgroupSettings, blockDevice *block_io.Device) (map[string]string, error) {
	m := map[string]string{}
	if s == nil {
		return m, nil
	}
	if s.CpuWeight != nil {
		m["cpu.weight"] = strconv.Itoa(int(s.GetCpuWeight()))
	}
	if s.CpuQuotaLimitUsec == nil {
		if s.CpuQuotaPeriodUsec != nil {
			return nil, fmt.Errorf("cannot set CPU period without also setting quota")
		}
	} else {
		if s.CpuQuotaPeriodUsec == nil {
			// Keep current or default period (100ms) but update quota.
			m["cpu.max"] = strconv.Itoa(int(s.GetCpuQuotaLimitUsec()))
		} else {
			m["cpu.max"] = fmt.Sprintf("%d %d", s.GetCpuQuotaLimitUsec(), s.GetCpuQuotaPeriodUsec())
		}
	}
	if s.CpuMaxBurstUsec != nil {
		m["cpu.max.burst"] = strconv.Itoa(int(s.GetCpuMaxBurstUsec()))
	}
	if s.CpuUclampMin != nil {
		m["cpu.uclamp.min"] = fmtPercent(s.GetCpuUclampMin())
	}
	if s.CpuUclampMax != nil {
		m["cpu.uclamp.max"] = fmtPercent(s.GetCpuUclampMax())
	}
	if s.PidsMax != nil {
		m["pids.max"] = strconv.Itoa(int(s.GetPidsMax()))
	}
	if s.MemoryThrottleLimitBytes != nil {
		m["memory.high"] = strconv.Itoa(int(s.GetMemoryThrottleLimitBytes()))
	}
	if s.MemoryLimitBytes != nil {
		m["memory.max"] = strconv.Itoa(int(s.GetMemoryLimitBytes()))
	}
	if s.MemorySoftGuaranteeBytes != nil {
		m["memory.low"] = strconv.Itoa(int(s.GetMemorySoftGuaranteeBytes()))
	}
	if s.MemoryMinimumBytes != nil {
		m["memory.min"] = strconv.Itoa(int(s.GetMemoryMinimumBytes()))
	}
	if s.MemoryOomGroup != nil {
		m["memory.oom.group"] = fmtBool(s.GetMemoryOomGroup())
	}
	if s.SwapThrottleLimitBytes != nil {
		m["memory.swap.high"] = strconv.Itoa(int(s.GetSwapThrottleLimitBytes()))
	}
	if s.SwapLimitBytes != nil {
		m["memory.swap.max"] = strconv.Itoa(int(s.GetSwapLimitBytes()))
	}
	if blockDevice != nil {
		if s.BlockIoLatencyTargetUsec != nil {
			m["io.latency"] = fmt.Sprintf("%d:%d target=%d", blockDevice.Maj, blockDevice.Min, s.GetBlockIoLatencyTargetUsec())
		}
		if s.BlockIoWeight != nil {
			m["io.weight"] = fmt.Sprintf("%d:%d %d", blockDevice.Maj, blockDevice.Min, s.GetBlockIoWeight())
		}
		var limitFields []string
		limits := s.GetBlockIoLimit()
		if limits != nil {
			if limits.Riops != nil {
				limitFields = append(limitFields, fmt.Sprintf("riops=%d", limits.GetRiops()))
			}
			if limits.Wiops != nil {
				limitFields = append(limitFields, fmt.Sprintf("wiops=%d", limits.GetWiops()))
			}
			if limits.Rbps != nil {
				limitFields = append(limitFields, fmt.Sprintf("rbps=%d", limits.GetRbps()))
			}
			if limits.Wbps != nil {
				limitFields = append(limitFields, fmt.Sprintf("wbps=%d", limits.GetWbps()))
			}
		}
		if len(limitFields) > 0 {
			m["io.max"] = fmt.Sprintf("%d:%d %s", blockDevice.Maj, blockDevice.Min, strings.Join(limitFields, " "))
		}
	}
	if len(s.GetCpusetCpus()) > 0 {
		m["cpuset.cpus"] = cpuset.Format(s.GetCpusetCpus())
	}
	return m, nil
}

func fmtBool(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func fmtPercent(v float32) string {
	return fmt.Sprintf("%.2f", v)
}

// Paths holds cgroup path templates that map a container ID to their cgroupfs
// file paths.
//
// A single instance should be shared across all containers of the same
// isolation type, since the first call to Stats() walks the cgroupfs tree in
// order to discover the cgroup path locations.
//
// This struct supports both v1 and v2. If only v2 support is required and the
// full cgroup path is known, consider calling Stats() directly, passing in
// the cgroup v2 path.
type Paths struct {
	mu sync.RWMutex

	// V2DirTemplate is the unified cgroupfs dir template (cgroup v2 only).
	V2DirTemplate string

	// V1CPUTemplate is the CPU usage path template (cgroup v1 only).
	V1CPUTemplate string

	// V1MemoryTemplate is the memory usage path template (cgroup v1 only).
	V1MemoryTemplate string
}

func (p *Paths) CgroupVersion() int {
	if p.V1CPUTemplate != "" {
		return 1
	}
	if p.V2DirTemplate != "" {
		return 2
	}
	return 0 // unknown
}

// Stats returns cgroup stats for the cgroup matching the given name. If
// blockDevice is non-nil, IO stats are included for the device, otherwise IO
// stats are not reported.
func (p *Paths) Stats(ctx context.Context, name string, blockDevice *block_io.Device) (*repb.UsageStats, error) {
	if err := p.find(ctx, name); err != nil {
		return nil, err
	}

	if p.CgroupVersion() == 1 {
		return p.v1Stats(ctx, name)
	}

	// cgroup v2 has all cgroup files under a single dir.
	dir := strings.ReplaceAll(p.V2DirTemplate, cidPlaceholder, name)

	return Stats(ctx, dir, blockDevice)
}

// Stats reads all stats from the given cgroup2 directory. The directory should
// be an absolute path, including the /sys/fs/cgroup prefix.
func Stats(ctx context.Context, dir string, blockDevice *block_io.Device) (*repb.UsageStats, error) {
	// Read CPU usage.
	// cpu.stat file contains a line like "usage_usec <N>"
	// It contains other lines like user_usec, system_usec etc. but we just
	// report the total for now.
	cpuUsagePath := filepath.Join(dir, "cpu.stat")
	cpuMicros, err := readCgroupInt64Field(cpuUsagePath, "usage_usec")
	if err != nil {
		return nil, err
	}

	// Read memory usage
	memUsagePath := filepath.Join(dir, "memory.current")
	memoryBytes, err := readInt64FromFile(memUsagePath)
	if err != nil {
		return nil, err
	}

	// Read IO stats for the given block device
	var ioStats *repb.CgroupIOStats
	if blockDevice != nil {
		ioStatPath := filepath.Join(dir, "io.stat")
		stats, err := readIOStatFile(ioStatPath)
		if err != nil {
			return nil, err
		}
		// Find the stats for the block device we care about
		// NOTE: we may not actually find stats if no IO has happened yet!
		for _, stat := range stats {
			if stat.Maj == blockDevice.Maj && stat.Min == blockDevice.Min {
				ioStats = stat
				break
			}
		}
	}

	// Read PSI metrics.
	// Note that PSI may not be supported in all environments,
	// so ignore NotExist errors.

	cpuPressurePath := filepath.Join(dir, "cpu.pressure")
	cpuPressure, err := readPSIFile(cpuPressurePath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	memPressurePath := filepath.Join(dir, "memory.pressure")
	memPressure, err := readPSIFile(memPressurePath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	ioPressurePath := filepath.Join(dir, "io.pressure")
	ioPressure, err := readPSIFile(ioPressurePath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return &repb.UsageStats{
		CpuNanos:       cpuMicros * 1e3,
		MemoryBytes:    memoryBytes,
		CgroupIoStats:  ioStats,
		CpuPressure:    cpuPressure,
		MemoryPressure: memPressure,
		IoPressure:     ioPressure,
	}, nil
}

func (p *Paths) v1Stats(ctx context.Context, cid string) (*repb.UsageStats, error) {
	// cpuacct.usage file contains just the CPU usage in ns.
	cpuUsagePath := strings.ReplaceAll(p.V1CPUTemplate, cidPlaceholder, cid)
	cpuNanos, err := readInt64FromFile(cpuUsagePath)
	if err != nil {
		return nil, err
	}

	// memory.usage_in_bytes file contains the current memory usage.
	memUsagePath := strings.ReplaceAll(p.V1MemoryTemplate, cidPlaceholder, cid)
	memoryBytes, err := readInt64FromFile(memUsagePath)
	if err != nil {
		return nil, err
	}

	return &repb.UsageStats{
		CpuNanos:    cpuNanos,
		MemoryBytes: memoryBytes,
	}, nil
}

// find locates cgroup path templates. For this to work, the container must be
// started (i.e. `podman create` does not set up the cgroups; `podman start`
// does). We use this walking approach because the logic for figuring out the
// actual cgroup paths depends on the system setup and is pretty complicated.
//
// TODO: get rid of this lookup. Going forward, we're only supporting cgroup2
// and expect it to be mounted at /sys/fs/cgroup.
func (p *Paths) find(ctx context.Context, cid string) error {
	// If already initialized, do nothing.
	p.mu.RLock()
	v := p.CgroupVersion()
	p.mu.RUnlock()
	if v != 0 {
		return nil
	}

	// Walk the cgroupfs tree.
	p.mu.Lock()
	defer p.mu.Unlock()
	// Re-check whether we're initialized since we released the lock for a brief
	// period.
	if p.CgroupVersion() != 0 {
		return nil
	}
	start := time.Now()
	var v2DirTemplate, v1CPUTemplate, v1MemoryTemplate string
	err := filepath.WalkDir(RootPath, func(path string, dir fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !strings.Contains(path, cid) {
			return nil
		}
		basename := filepath.Base(path)
		if basename == "memory.current" {
			dir := filepath.Dir(path)
			v2DirTemplate = strings.ReplaceAll(dir, cid, cidPlaceholder)
		} else if basename == "cpuacct.usage" {
			v1CPUTemplate = strings.ReplaceAll(path, cid, cidPlaceholder)
		} else if basename == "memory.usage_in_bytes" {
			v1MemoryTemplate = strings.ReplaceAll(path, cid, cidPlaceholder)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Prioritize cgroup v1 paths. In a "hybrid" setup (both v1 and v2 mounted),
	// some v2 paths will be set up, but might be missing certain files like
	// memory.current, and the cpu.stat file will be missing some information.
	if v1CPUTemplate != "" && v1MemoryTemplate != "" {
		p.V1CPUTemplate = v1CPUTemplate
		p.V1MemoryTemplate = v1MemoryTemplate
		log.CtxInfof(ctx, "Initialized cgroup v1 paths: duration=%s, cpu=%s, mem=%s", time.Since(start), v1CPUTemplate, v1MemoryTemplate)
		return nil
	}

	if v2DirTemplate != "" {
		p.V2DirTemplate = v2DirTemplate
		log.CtxInfof(ctx, "Initialized cgroup v2 paths: duration=%s, template=%s", time.Since(start), v2DirTemplate)
		return nil
	}

	return status.InternalErrorf("failed to locate cgroup under %s", RootPath)
}

// readInt64FromFile reads a file expected to contain a single int64.
func readInt64FromFile(path string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// readCgroupInt64Field reads a cgroupfs file containing a list of lines like
// "field_name <int64_value>" and returns the value of the given field name.
func readCgroupInt64Field(path, fieldName string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || fields[0] != fieldName {
			continue
		}
		val, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, err
		}
		return val, nil
	}
	return 0, status.NotFoundErrorf("could not find field %q in %s", fieldName, path)
}

func readPSIFile(path string) (*repb.PSI, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readPSI(f)
}

// Parses PSI (Pressure Stall Information) metrics output.
func readPSI(r io.Reader) (*repb.PSI, error) {
	psi := &repb.PSI{}
	s := bufio.NewScanner(r)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 5 {
			return nil, fmt.Errorf("malformed PSI line %q (5 fields expected)", line)
		}
		m := &repb.PSI_Metrics{}
		// Parse (some|full)
		switch fields[0] {
		case "some":
			psi.Some = m
		case "full":
			psi.Full = m
		default:
			return nil, fmt.Errorf("unexpected string %q at field 0", fields[0])
		}
		// Parse avgs
		var avgs [3]float32
		for i := 0; i < len(avgs); i++ {
			field := fields[i+1]
			name, rawValue, ok := strings.Cut(field, "=")
			if !ok {
				return nil, fmt.Errorf("malformed avg field %q", field)
			}
			value, err := strconv.ParseFloat(rawValue, 32)
			if err != nil {
				return nil, fmt.Errorf("malformed avg value field %q", field)
			}
			switch name {
			case "avg10":
				m.Avg10 = float32(value)
			case "avg60":
				m.Avg60 = float32(value)
			case "avg300":
				m.Avg300 = float32(value)
			default:
				return nil, fmt.Errorf("unexpected field name %q", name)
			}
		}
		// Parse total
		field := fields[4]
		name, rawValue, ok := strings.Cut(field, "=")
		if !ok {
			return nil, fmt.Errorf("malformed total field %q", field)
		}
		total, err := strconv.ParseInt(rawValue, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("malformed total field %q", field)
		}
		if name != "total" {
			return nil, fmt.Errorf("unexpected field name %q", name)
		}
		m.Total = int64(total)
	}
	if s.Err() != nil {
		return nil, fmt.Errorf("read: %w", s.Err())
	}
	return psi, nil
}

// readIOStatFile reads the cgroup "io.stat" file from the given path.
func readIOStatFile(path string) ([]*repb.CgroupIOStats, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readIOStat(f)
}

func readIOStat(r io.Reader) ([]*repb.CgroupIOStats, error) {
	var stats []*repb.CgroupIOStats
	s := bufio.NewScanner(r)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			return nil, fmt.Errorf("fields unexpectedly empty")
		}
		devStr := fields[0]
		if devStr == "(unknown)" {
			// TODO(bduffany): figure out what these "(unknown)" devices are
			continue
		}
		major, minor, err := block_io.ParseMajMin(devStr)
		if err != nil {
			return nil, fmt.Errorf("parse block device: %w", err)
		}
		stat := &repb.CgroupIOStats{
			Maj: int64(major),
			Min: int64(minor),
		}
		for _, entry := range fields[1:] {
			name, valStr, ok := strings.Cut(entry, "=")
			if !ok {
				return nil, fmt.Errorf("malformed counter entry")
			}
			val, err := strconv.Atoi(valStr)
			if err != nil {
				return nil, fmt.Errorf("malformed counter value")
			}
			switch name {
			case "rbytes":
				stat.Rbytes = int64(val)
			case "wbytes":
				stat.Wbytes = int64(val)
			case "rios":
				stat.Rios = int64(val)
			case "wios":
				stat.Wios = int64(val)
			case "dbytes":
				stat.Dbytes = int64(val)
			case "dios":
				stat.Dios = int64(val)
			default:
			}
		}
		stats = append(stats, stat)
	}
	if s.Err() != nil {
		return nil, fmt.Errorf("read: %w", s.Err())
	}
	return stats, nil
}
