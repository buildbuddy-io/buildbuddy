//go:build linux

// package nsutil contains utilities for working with Linux namespaces.
//
// Example:
//
//	child, err := nsutil.Unshare(opts...)
//	if err != nil {
//		log.Fatalf("Unshare failed: %s", err)
//	}
//	// If a command is returned, we're the parent process.
//	if child != nil {
//		nsutil.TerminateAfter(child)
//	}
//	// Otherwise, we're the child, now executing with unshared namespaces!
package nsutil

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// Env vars - do not use these directly!
const (
	// Environment variable that signals whether we're running as the unshared
	// child process.
	isChildEnvVarName = "__TESTNS_IS_UNSHARED_CHILD_PROC"
	// Environment variable that lets us access the original UID from the
	// parent process that called Unshare().
	parentUidEnvVarName = "__TESTNS_PARENT_UID"
)

// Unshare works like the unshare(1) command on Linux. It re-executes the
// current process command inside one or more new namespaces, "unshared" from
// the current process namespaces. Roughly speaking, it "returns twice", similar
// to fork(), except that the entire program is re-executed from the beginning
// of main() (since Go does not really support forking).
//
// An example use case is to allow the program to perform certain "safe"
// mount(2) operations, such as bind or overlay mounts. These only require that
// the process uid can access the current mount namespace. To get this type of
// ownership without needing to run sudo, the user namespace and mount namespace
// can be unshared, with the host uid mapped to uid 0 within the new namespace.
//
// While bind mounts can be created this way, note that the mounts are only
// visible to the child process.
//
// See the package-level documentation for example code.
//
// Note, this is a Linux-only API.
func Unshare(opts ...UnshareOption) (child *exec.Cmd, err error) {
	if os.Getenv(isChildEnvVarName) == "1" {
		// We're the child process that is running in the new user namespace
		// as root - clean up the env var that we set and run the test.
		_ = os.Unsetenv(isChildEnvVarName)
		return nil, nil
	}
	// Re-execute the current executable in a new user namespace as uid 0 (root).
	cmd := exec.Command("/proc/self/exe", os.Args[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	for _, opt := range opts {
		opt(cmd.SysProcAttr)
	}
	// Note: setting env after processing opts, since some opts may set env
	// vars.
	cmd.Env = append(os.Environ(), isChildEnvVarName+"=1")
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

// TerminateAfter waits for cmd to exit, then exits with the same exit code
// as the child. If the child terminates abnormally then it exits with code 2.
func TerminateAfter(cmd *exec.Cmd) {
	err := cmd.Wait()
	if err == nil {
		os.Exit(0)
	}
	exitError, ok := err.(*exec.ExitError)
	if !ok {
		os.Exit(2)
	}
	ws, ok := exitError.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !ws.Exited() {
		os.Exit(2)
	}
	os.Exit(ws.ExitStatus())
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
		os.Setenv(parentUidEnvVarName, fmt.Sprint(os.Getuid()))
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

// ParentUid returns the uid of the parent process that called Unshare() if
// applicable.
func ParentUid() (uid int, ok bool) {
	s := os.Getenv(parentUidEnvVarName)
	uid, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return uid, true
}
