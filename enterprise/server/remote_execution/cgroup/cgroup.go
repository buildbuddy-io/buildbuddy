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

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// Standard path where cgroupfs is expected to be mounted.
	cgroupfsPath = "/sys/fs/cgroup"

	// Placeholder value representing the container ID in cgroup path templates.
	cidPlaceholder = "{{.ContainerID}}"
)

// Paths holds cgroup path templates that map a container ID to their cgroupfs
// file paths.
//
// A single instance should be shared across all containers of the same
// isolation type, since the first call to Stats() walks the cgroupfs tree in
// order to discover the cgroup path locations.
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

func (p *Paths) Stats(ctx context.Context, cid string) (*repb.UsageStats, error) {
	if err := p.find(ctx, cid); err != nil {
		return nil, err
	}

	if p.CgroupVersion() == 1 {
		return p.v1Stats(ctx, cid)
	}

	// cgroup v2 has all cgroup files under a single dir.
	dir := strings.ReplaceAll(p.V2DirTemplate, cidPlaceholder, cid)

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
	err := filepath.WalkDir(cgroupfsPath, func(path string, dir fs.DirEntry, err error) error {
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

	return status.InternalErrorf("failed to locate cgroup under %s", cgroupfsPath)
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
