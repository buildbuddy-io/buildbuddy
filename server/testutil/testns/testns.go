package testns

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
)

// Unshare works like the unshare(1) command on Linux (see `man 1 unshare`). It
// executes the test inside one or more new namespaces, "unshared" from the
// current namespaces (typically the root namespaces, unless the test is being
// run inside a container).
//
// An example use case is to allow the test to perform bind mounts, which
// requires that the user owns the current mount namespace. To do this, the user
// namespace and mount namespace can be unshared, with the host uid mapped to
// uid 0 within the new namespace. While bind mounts can be tested this way,
// note that the mounts are only visible to the test, since they are performed
// within a new mount namespace.
//
// Note, this is a Linux-only API.
func Unshare(m *testing.M, opts ...UnshareOption) {
	// Environment variable that signals whether we're running as the unshared
	// child process.
	const isChildEnvVarName = "__TEST_IS_UNSHARED_CHILD_PROC"
	if os.Getenv(isChildEnvVarName) == "1" {
		// We're the child process that is running in the new user namespace
		// as root - clean up the env var that we set and run the test.
		os.Unsetenv(isChildEnvVarName)
		os.Exit(m.Run())
	}
	// Re-execute the current executable in a new user namespace as uid 0 (root).
	cmd := exec.Command("/proc/self/exe", os.Args[1:]...)
	cmd.Env = append(os.Environ(), isChildEnvVarName+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	for _, opt := range opts {
		opt(cmd.SysProcAttr)
	}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err == nil {
		os.Exit(0)
	} else if ws, ok := err.(*exec.ExitError); ok {
		if ws.Exited() {
			os.Exit(ws.ExitCode())
		}
	}
	os.Exit(1)
}

// UnshareOption is an option to be passed to Unshare.
type UnshareOption func(*syscall.SysProcAttr)

// UnshareMount is an UnshareOption that unshares the mount namespace.
var UnshareMount = UnshareOption(func(p *syscall.SysProcAttr) {
	p.Cloneflags |= syscall.CLONE_NEWNS
})

// UnshareUser is an UnshareOption that unshares the user namespace.
var UnshareUser = UnshareOption(func(p *syscall.SysProcAttr) {
	p.Cloneflags |= syscall.CLONE_NEWUSER
})

// MapID returns an UnshareOption that maps the current uid and gid
// to the given values in a new user namespace. Implies UnshareUser.
func MapID(uid, gid int) UnshareOption {
	return func(p *syscall.SysProcAttr) {
		UnshareUser(p)
		p.UidMappings = append(
			p.UidMappings,
			syscall.SysProcIDMap{ContainerID: uid, HostID: os.Getuid(), Size: 1},
		)
		p.GidMappings = append(
			p.GidMappings,
			syscall.SysProcIDMap{ContainerID: gid, HostID: os.Getgid(), Size: 1},
		)
	}
}
