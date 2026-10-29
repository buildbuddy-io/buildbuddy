package main

import (
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func main() {
	// Invalid arguments make allowed calls return EINVAL without creating a
	// process or namespace, even on hosts that disable user namespaces.
	var errno unix.Errno
	switch os.Args[1] {
	case "clone":
		flags, stack := uintptr(unix.CLONE_NEWUSER|unix.CLONE_FS), uintptr(0)
		if runtime.GOARCH == "s390x" {
			flags, stack = stack, flags
		}
		_, _, errno = unix.RawSyscall6(unix.SYS_CLONE, flags, stack, 0, 0, 0, 0)
	case "clone3":
		_, _, errno = unix.Syscall(unix.SYS_CLONE3, 0, 0, 0)
	case "unshare":
		_, _, errno = unix.Syscall(unix.SYS_UNSHARE, unix.CLONE_NEWUSER|1, 0, 0)
	default:
		panic("unknown syscall")
	}
	os.Exit(int(errno))
}
