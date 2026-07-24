//go:build linux && !android

package oomkiller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// NewMemoryMonitor returns the default executor memory monitor. cgroupPath is
// the executor's starting cgroup path relative to the cgroupfs root. The
// monitor measures the cgroup's memory usage against its effective memory
// limit. If cgroup-based accounting is unavailable (cgroup v1, the real
// cgroup v2 root, no memory controller, or no memory limit), the monitor
// measures system memory usage instead, since the executor is then bounded by
// system memory.
func NewMemoryMonitor(cgroupPath string) (MemoryMonitor, error) {
	if cgroupPath == "" {
		// The executor is at the cgroupfs root. Under a cgroup namespace
		// (e.g. when the executor runs in a container), the root directory is
		// really a non-root cgroup on the host and can be monitored like any
		// other cgroup. The real cgroup v2 root (and cgroup v1) has no
		// memory.current file, and system memory is the right measurement.
		if _, err := os.Stat(filepath.Join(cgroup.RootPath, "memory.current")); err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}
			log.Infof("Executor is in the root cgroup; the OOM killer will measure system memory usage.")
			return newSystemMemoryMonitor()
		}
	}
	dir := filepath.Join(cgroup.RootPath, cgroupPath)
	controllers, err := cgroup.EnabledControllers(dir)
	if err != nil {
		return nil, fmt.Errorf("read enabled controllers for %q: %w", dir, err)
	}
	if !controllers["memory"] {
		log.Infof("Executor cgroup does not have the memory controller enabled; the OOM killer will measure system memory usage.")
		return newSystemMemoryMonitor()
	}
	limitBytes, err := cgroup.ReadEffectiveMemoryLimit(dir)
	if err != nil {
		return nil, fmt.Errorf("read cgroup memory limit: %w", err)
	}
	if limitBytes == nil {
		log.Infof("Executor cgroup has no memory limit; the OOM killer will measure system memory usage.")
		return newSystemMemoryMonitor()
	}
	log.Infof("Executor OOM killer is measuring memory usage of cgroup %q with effective memory limit %d bytes", cgroupPath, *limitBytes)
	return &cgroupMemoryMonitor{
		dir:        dir,
		limitBytes: *limitBytes,
	}, nil
}

// cgroupMemoryMonitor measures executor memory usage from a cgroup.
type cgroupMemoryMonitor struct {
	// dir is the absolute cgroupfs directory of the monitored cgroup.
	dir string
	// limitBytes is the effective memory limit of the monitored cgroup.
	limitBytes int64
}

func (m *cgroupMemoryMonitor) Snapshot(_ context.Context) (*MemorySnapshot, error) {
	usedBytes, err := getCgroupUnreclaimableMemoryBytes(m.dir)
	if err != nil {
		return nil, fmt.Errorf("read executor cgroup memory: %w", err)
	}
	return &MemorySnapshot{
		UsedBytes:      usedBytes,
		LimitBytes:     m.limitBytes,
		AvailableBytes: max(int64(0), m.limitBytes-usedBytes),
	}, nil
}

// getCgroupUnreclaimableMemoryBytes returns the memory usage of the cgroup at
// the given cgroupfs directory, excluding file page cache, which the kernel
// reclaims under memory pressure instead of OOM killing.
//
// Everything else is counted: anon memory, shmem/tmpfs pages (which are tracked
// by the anon LRU lists, not the file lists), unevictable pages, and kernel
// memory. Without swap enabled (which for now we assume is the case), the
// kernel cannot reclaim any of these, so they determine how close the cgroup is
// to a kernel OOM kill.
//
// The exception is reclaimable slab (dentry and inode caches), which the kernel
// can shrink under memory pressure but which is deliberately still counted:
// slab reclaim only frees pages when whole slabs empty out, making it less
// dependable than dropping clean file pages, and counting it errs toward
// killing tasks early instead of letting the kernel OOM kill the executor.
func getCgroupUnreclaimableMemoryBytes(dir string) (int64, error) {
	currentBytes, err := cgroup.ReadMemoryCurrent(dir)
	if err != nil {
		return 0, err
	}
	if currentBytes < 0 {
		return 0, fmt.Errorf("cgroup memory.current is negative (%d)", currentBytes)
	}
	memoryStat, err := cgroup.ReadMemoryStat(dir)
	if err != nil {
		return 0, fmt.Errorf("read memory stats: %w", err)
	}
	var fileBytes int64
	for _, field := range []string{"inactive_file", "active_file"} {
		value, ok := memoryStat[field]
		if !ok {
			return 0, fmt.Errorf("memory.stat is missing field %q", field)
		}
		fileBytes += value
	}
	// memory.current and memory.stat are read separately, so the subtraction
	// can skew slightly negative under concurrent cache growth.
	return max(int64(0), currentBytes-fileBytes), nil
}
