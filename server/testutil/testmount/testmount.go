package testmount

import (
	"log"
	"os"
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// RunWithLimitedMountPermissions executes the test suite in a child process,
// running as uid 0 in a new user namespace + new mount namespace so that it
// gets limited mount permissions.
//
// In particular, this allows performing certain "safe" mounts such as FUSE
// mounts, overlayfs mounts, and bind mounts, but the mounts are only visible to
// the test process and are automatically removed by the kernel when the process
// exits.
//
// Note: this only works on Linux.
func RunWithLimitedMountPermissions(m *testing.M) {
	const envVarName = "__TEST_IS_PSEUDOROOT"
	if os.Getenv(envVarName) == "1" {
		os.Unsetenv(envVarName)
		os.Exit(m.Run())
	}
	cmd := exec.Command("/proc/self/exe", os.Args[1:]...)
	cmd.Env = append(os.Environ(), envVarName+"=1")
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWUSER | syscall.CLONE_NEWNS,
		UidMappings: []syscall.SysProcIDMap{
			{HostID: os.Getuid(), ContainerID: 0, Size: 1},
		},
		GidMappings: []syscall.SysProcIDMap{
			{HostID: os.Getgid(), ContainerID: 0, Size: 1},
		},
	}
	if err := cmd.Run(); err != nil && (cmd.ProcessState == nil || !cmd.ProcessState.Exited()) {
		log.Fatal(err)
	}
	os.Exit(cmd.ProcessState.ExitCode())
}

// Mount mounts a filesystem to the given target directory and unmounts when
// the test completes.
func Mount(t *testing.T, source, target, fstype string, flags uintptr, data string) {
	err := syscall.Mount(source, target, fstype, flags, data)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := syscall.Unmount(target, 0)
		require.NoError(t, err, "clean up mount %q", target)
	})
}
