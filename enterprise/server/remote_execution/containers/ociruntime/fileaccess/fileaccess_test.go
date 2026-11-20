package fileaccess_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime/fileaccess"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestRecording(t *testing.T) {
	var caps [2]unix.CapUserData
	require.NoError(t, unix.Capget(&unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}, &caps[0]))
	if caps[0].Effective&(1<<unix.CAP_SYS_ADMIN) == 0 {
		t.Skip("requires CAP_SYS_ADMIN")
	}
	if _, err := os.Stat("/sys/kernel/btf/vmlinux"); err != nil {
		t.Skip("requires kernel BTF")
	}
	tracker, err := fileaccess.NewTracker()
	if errors.Is(err, fileaccess.ErrBPFLSMNotActive) {
		t.Skip(err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { tracker.Close() })

	dir := t.TempDir()
	read := filepath.Join(dir, "read")
	readWrite := filepath.Join(dir, "read-write")
	link := filepath.Join(dir, "link")
	unread := filepath.Join(dir, "unread")
	written := filepath.Join(dir, "written")
	for _, p := range []string{read, readWrite, unread, written} {
		require.NoError(t, os.WriteFile(p, []byte("x"), 0644))
	}
	require.NoError(t, os.Link(read, link))

	cgroupPath, err := os.MkdirTemp("/sys/fs/cgroup", "fileaccess-test-")
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(cgroupPath) })

	run := func(script string, extraFiles ...*os.File) {
		cg, err := os.Open(cgroupPath)
		require.NoError(t, err)
		defer cg.Close()
		cmd := exec.Command("sh", "-c", script)
		cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(cg.Fd())}
		cmd.ExtraFiles = extraFiles
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
	}

	for range 2 {
		rec, err := tracker.Start(cgroupPath)
		require.NoError(t, err)
		run("cat " + read + " " + link + " " + read + " >/dev/null && echo y >> " + written)
		run("cat 3<>" + readWrite + " <&3 >/dev/null")
		paths, dropped, err := rec.Stop()
		require.NoError(t, err)
		require.Zero(t, dropped)
		require.Contains(t, paths, read)
		require.Contains(t, paths, readWrite)
		require.Contains(t, paths, link)
		require.NotContains(t, paths, unread)
		require.NotContains(t, paths, written)
	}

	// Files opened outside of a recording are not reported.
	run("cat " + unread + " >/dev/null")
	rec, err := tracker.Start(cgroupPath)
	require.NoError(t, err)
	paths, dropped, err := rec.Stop()
	require.NoError(t, err)
	require.Zero(t, dropped)
	require.NotContains(t, paths, unread)

	// Opens that can't be reported are counted, such as opens of files whose
	// path is longer than PATH_MAX.
	deep, err := os.Open(dir)
	require.NoError(t, err)
	component := strings.Repeat("d", 200)
	for range 25 {
		require.NoError(t, unix.Mkdirat(int(deep.Fd()), component, 0755))
		fd, err := unix.Openat(int(deep.Fd()), component, unix.O_RDONLY|unix.O_DIRECTORY, 0)
		require.NoError(t, err)
		deep.Close()
		deep = os.NewFile(uintptr(fd), component)
	}
	defer deep.Close()
	fd, err := unix.Openat(int(deep.Fd()), "f", unix.O_WRONLY|unix.O_CREAT, 0644)
	require.NoError(t, err)
	unix.Close(fd)
	rec, err = tracker.Start(cgroupPath)
	require.NoError(t, err)
	run("cat /proc/self/fd/3/f", deep)
	_, dropped, err = rec.Stop()
	require.NoError(t, err)
	require.Equal(t, uint64(1), dropped)
}
