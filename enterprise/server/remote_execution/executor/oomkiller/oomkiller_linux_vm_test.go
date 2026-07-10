//go:build linux && !android

package oomkiller

// The tests in this file exercise the OOM killer against real cgroups. They
// run as root in their own VM via the firecracker workload isolation
// configured on the oomkiller_linux_vm_test target.

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/oom"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestVMCgroupMemoryMonitorCountsTmpfsAsUsed(t *testing.T) {
	cgroupDir := setupTestCgroup(t)

	// Mount a tmpfs. Files written to it are backed by memory that the kernel
	// cannot reclaim under memory pressure (there is no swap).
	mnt := filepath.Join(t.TempDir(), "tmpfs")
	require.NoError(t, os.Mkdir(mnt, 0755))
	require.NoError(t, unix.Mount("tmpfs", mnt, "tmpfs", 0, "size=256m"))
	t.Cleanup(func() { require.NoError(t, unix.Unmount(mnt, 0)) })

	// Write a 100MB file to the tmpfs from inside the cgroup. The tmpfs pages
	// are charged to the cgroup that allocated them and stay charged after the
	// writing process exits.
	cmd, _ := startBashInCgroup(t, cgroupDir, fmt.Sprintf("dd if=/dev/zero of=%s/f bs=1M count=100", mnt))
	require.NoError(t, cmd.Wait())

	// tmpfs memory is not reclaimable, so the monitor should count the whole
	// file as used.
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1 << 30}
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.GreaterOrEqual(t, snapshot.UsedBytes, int64(100<<20))
}

func TestVMCgroupMemoryMonitorExcludesPageCache(t *testing.T) {
	cgroupDir := setupTestCgroup(t)

	// This test needs a disk-backed scratch directory so that file writes
	// produce page cache rather than tmpfs memory. Firecracker VMs place the
	// test scratch dir on a disk-backed workspace filesystem.
	scratchDir := t.TempDir()
	var st unix.Statfs_t
	require.NoError(t, unix.Statfs(scratchDir, &st))
	require.NotEqual(t, int64(unix.TMPFS_MAGIC), st.Type, "scratch dir %s is on tmpfs; expected a disk-backed filesystem", scratchDir)

	// Write a 100MB file to disk from inside the cgroup, syncing at the end so
	// that the cached pages are clean. The page cache is charged to the cgroup
	// and stays charged after the writing process exits.
	cmd, _ := startBashInCgroup(t, cgroupDir, fmt.Sprintf("dd if=/dev/zero of=%s/f bs=1M count=100 conv=fsync", scratchDir))
	require.NoError(t, cmd.Wait())

	// Confirm the page cache was actually charged to the cgroup, so that the
	// working set assertion below can't pass vacuously.
	currentBytes, err := cgroup.ReadMemoryCurrent(cgroupDir)
	require.NoError(t, err)
	require.GreaterOrEqual(t, currentBytes, int64(100<<20))

	// Clean page cache is reclaimable under memory pressure, so the monitor
	// should exclude it from the reported usage.
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 1 << 30}
	snapshot, err := monitor.Snapshot(t.Context())
	require.NoError(t, err)
	require.Less(t, snapshot.UsedBytes, int64(50<<20))
}

func TestVMKillerKillsTaskWhenCgroupMemoryIsExhausted(t *testing.T) {
	cgroupDir := setupTestCgroup(t)
	ctx := t.Context()

	flags.Set(t, "executor.oom_killer.enabled", true)
	flags.Set(t, "executor.oom_killer.poll_interval", 100*time.Millisecond)
	flags.Set(t, "executor.oom_killer.memory_usage_threshold", 0.9)
	flags.Set(t, "executor.enable_oci", true)

	// Give the monitor an artificial 100MB limit instead of setting memory.max
	// on the cgroup. The killer must act before the limit is reached, so this
	// exercises the same logic without racing the kernel OOM killer.
	monitor := &cgroupMemoryMonitor{dir: cgroupDir, limitBytes: 100 << 20}
	oomKiller, err := New(ctx, monitor)
	require.NoError(t, err)

	task := &vmTestTask{killed: make(chan error, 1)}
	unregister := oomKiller.Register(ctx, task)
	defer unregister()

	// Allocate ~95MB of anonymous memory inside the cgroup and hold it by
	// blocking on stdin, pushing usage over the 90% kill threshold.
	_, stdin := startBashInCgroup(t, cgroupDir, `data=$(head -c 95000000 /dev/zero | tr '\0' x) && read -r _`)
	defer stdin.Close()

	// The killer should terminate the registered task with an OOM error.
	select {
	case err := <-task.killed:
		require.True(t, oom.IsError(err), "expected an OOM error, got: %s", err)
	case <-time.After(60 * time.Second):
		t.Fatal("task was not killed by the OOM killer")
	}
}

func TestVMNewMemoryMonitorReadsCgroupLimit(t *testing.T) {
	cgroupDir := setupTestCgroup(t)

	// Set a memory limit on the cgroup. NewMemoryMonitor should detect the
	// enabled memory controller and pick up the limit.
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "memory.max"), []byte("536870912"), 0))
	monitor, err := NewMemoryMonitor(filepath.Base(cgroupDir))
	require.NoError(t, err)
	cgroupMonitor, ok := monitor.(*cgroupMemoryMonitor)
	require.True(t, ok, "expected a cgroup memory monitor, got %T", monitor)
	require.Equal(t, int64(536870912), cgroupMonitor.limitBytes)
}

func TestVMNewMemoryMonitorUsesEffectiveLimitFromAncestors(t *testing.T) {
	parentDir := setupTestCgroup(t)
	childDir := filepath.Join(parentDir, "child")
	require.NoError(t, os.Mkdir(childDir, 0755))
	t.Cleanup(func() { require.NoError(t, os.Remove(childDir)) })
	require.NoError(t, cgroup.EnableController(childDir, "memory"))

	// Give the parent cgroup a smaller memory limit than the child. The
	// kernel enforces the parent limit on the child's memory usage, so the
	// monitor should use the parent limit as the effective limit.
	require.NoError(t, os.WriteFile(filepath.Join(parentDir, "memory.max"), []byte("268435456"), 0))
	require.NoError(t, os.WriteFile(filepath.Join(childDir, "memory.max"), []byte("536870912"), 0))

	monitor, err := NewMemoryMonitor(filepath.Join(filepath.Base(parentDir), "child"))
	require.NoError(t, err)
	cgroupMonitor, ok := monitor.(*cgroupMemoryMonitor)
	require.True(t, ok, "expected a cgroup memory monitor, got %T", monitor)
	require.Equal(t, int64(268435456), cgroupMonitor.limitBytes)
}

// setupTestCgroup creates a cgroup with the memory controller enabled and
// returns its absolute cgroupfs directory. The cgroup and any processes still
// running in it are cleaned up when the test finishes.
func setupTestCgroup(t *testing.T) string {
	if os.Geteuid() != 0 {
		t.Skip("test requires root to manage cgroups")
	}
	dir := filepath.Join(cgroup.RootPath, "oomkiller-test-"+uuid.New())
	require.NoError(t, os.Mkdir(dir, 0755))
	t.Cleanup(func() {
		// Kill anything still running in the cgroup. Removal fails until the
		// kernel has finished tearing down the cgroup, so retry briefly.
		_ = os.WriteFile(filepath.Join(dir, "cgroup.kill"), []byte("1"), 0)
		var err error
		for range 100 {
			if err = os.Remove(dir); err == nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Errorf("remove test cgroup: %s", err)
	})
	require.NoError(t, cgroup.EnableController(dir, "memory"))
	return dir
}

// startBashInCgroup starts a bash script in the given cgroup. The script only
// begins executing after the process has been moved into the cgroup. Closing
// the returned stdin pipe unblocks any trailing "read" in the script.
func startBashInCgroup(t *testing.T, cgroupDir, script string) (*exec.Cmd, io.WriteCloser) {
	// Block on a stdin read first so that we can move the process into the
	// cgroup before it does any work.
	cmd := exec.Command("bash", "-c", "read -r _ && "+script)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = stdin.Close()
	})
	require.NoError(t, os.WriteFile(filepath.Join(cgroupDir, "cgroup.procs"), []byte(strconv.Itoa(cmd.Process.Pid)), 0))
	_, err = io.WriteString(stdin, "\n")
	require.NoError(t, err)
	return cmd, stdin
}

type vmTestTask struct {
	killed chan error
}

func (f *vmTestTask) State(ctx context.Context) (*TaskState, error) {
	return &TaskState{
		EstimatedMemoryBytes: 1,
		StartedAt:            time.Now(),
		Active:               true,
		UsageStats:           &repb.UsageStats{MemoryBytes: 50 << 20},
	}, nil
}

func (f *vmTestTask) Kill(ctx context.Context, err error) {
	select {
	case f.killed <- err:
	default:
	}
}
