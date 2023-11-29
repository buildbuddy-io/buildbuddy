package cgroup

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
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

	// MemoryTemplate is the path template for the cgroupfs file containing
	// current memory usage.
	MemoryTemplate string
	// CPUPathTemplate is the path template for the cgroupsfs file containing
	// cumulative CPU usage.
	CPUTemplate string
}

func (p *Paths) Stats(ctx context.Context, cid string) (*repb.UsageStats, error) {
	if err := p.find(ctx, cid); err != nil {
		return nil, err
	}

	memUsagePath := strings.ReplaceAll(p.MemoryTemplate, cidPlaceholder, cid)
	cpuUsagePath := strings.ReplaceAll(p.CPUTemplate, cidPlaceholder, cid)

	memUsageBytes, err := readInt64FromFile(memUsagePath)
	if err != nil {
		return nil, err
	}
	var cpuNanos int64
	switch p.CgroupVersion() {
	case 1:
		// cgroup v1: /cpuacct.usage file contains just the CPU usage in ns.
		cpuNanos, err = readInt64FromFile(cpuUsagePath)
		if err != nil {
			return nil, err
		}
	case 2:
		// cgroup v2: /cpu.stat file contains a line like "usage_usec <N>" It
		// contains other lines like user_usec, system_usec etc. but we just
		// report the total for now.
		cpuMicros, err := readCgroupInt64Field(cpuUsagePath, "usage_usec")
		if err != nil {
			return nil, err
		}
		cpuNanos = cpuMicros * 1e3
	default:
		return nil, status.FailedPreconditionErrorf("invalid cgroup version %d", p.CgroupVersion())
	}
	return &repb.UsageStats{
		MemoryBytes: memUsageBytes,
		CpuNanos:    cpuNanos,
	}, nil
}

func (p *Paths) CgroupVersion() int {
	if p.CPUTemplate == "" || p.MemoryTemplate == "" {
		// Not fully initialized.
		return 0
	} else if strings.HasSuffix(p.CPUTemplate, "/cpuacct.usage") {
		return 1
	} else {
		return 2
	}
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
	// Sentinel error value to short-circuit the walk
	stop := fmt.Errorf("stop walk")
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
		if strings.HasSuffix(path, "/memory.usage_in_bytes") || strings.HasSuffix(path, "/memory.current") {
			p.MemoryTemplate = strings.ReplaceAll(path, cid, cidPlaceholder)
		} else if strings.HasSuffix(path, "/cpu.stat") || strings.HasSuffix(path, "/cpuacct.usage") {
			p.CPUTemplate = strings.ReplaceAll(path, cid, cidPlaceholder)
		}
		if p.MemoryTemplate != "" && p.CPUTemplate != "" {
			return stop
		}
		return nil
	})
	if err != nil && err != stop {
		return err
	}
	if p.MemoryTemplate == "" {
		return status.InternalErrorf("failed to locate memory cgroup file under %s", cgroupfsPath)
	}
	if p.CPUTemplate == "" {
		return status.InternalErrorf("failed to locate CPU cgroup file under %s", cgroupfsPath)
	}
	log.CtxInfof(ctx, "Initialized cgroup path templates (%s): mem=%s, cpu=%s", time.Since(start), p.MemoryTemplate, p.CPUTemplate)
	return nil
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
