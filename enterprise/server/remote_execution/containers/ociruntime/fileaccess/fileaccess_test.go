package fileaccess_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime/fileaccess"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestRecording(t *testing.T) {
	var caps unix.CapUserData
	require.NoError(t, unix.Capget(&unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}, &caps))
	if caps.Effective&(1<<unix.CAP_SYS_ADMIN) == 0 {
		t.Skip("requires CAP_SYS_ADMIN")
	}
	if _, err := os.Stat("/sys/kernel/btf/vmlinux"); err != nil {
		t.Skip("requires kernel BTF")
	}
	tracker, err := fileaccess.NewTracker()
	require.NoError(t, err)
	t.Cleanup(func() { tracker.Close() })

	dir := t.TempDir()
	read := filepath.Join(dir, "read")
	link := filepath.Join(dir, "link")
	unread := filepath.Join(dir, "unread")
	written := filepath.Join(dir, "written")
	for _, p := range []string{read, unread, written} {
		require.NoError(t, os.WriteFile(p, []byte("x"), 0644))
	}
	require.NoError(t, os.Link(read, link))

	cgroupPath, err := os.MkdirTemp("/sys/fs/cgroup", "fileaccess-test-")
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(cgroupPath) })

	run := func(script string) {
		cg, err := os.Open(cgroupPath)
		require.NoError(t, err)
		defer cg.Close()
		cmd := exec.Command("sh", "-c", script)
		cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(cg.Fd())}
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
	}

	for range 2 {
		rec, err := tracker.Start(cgroupPath)
		require.NoError(t, err)
		run("cat " + read + " " + link + " " + read + " >/dev/null && echo y >> " + written)
		paths, err := rec.Stop()
		require.NoError(t, err)
		require.Contains(t, paths, read)
		require.Contains(t, paths, link)
		require.NotContains(t, paths, unread)
		require.NotContains(t, paths, written)
	}

	// Files opened outside of a recording are not reported.
	run("cat " + unread + " >/dev/null")
	rec, err := tracker.Start(cgroupPath)
	require.NoError(t, err)
	paths, err := rec.Stop()
	require.NoError(t, err)
	require.NotContains(t, paths, unread)
}
