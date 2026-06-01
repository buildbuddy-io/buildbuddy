//go:build linux

package fsync

import "golang.org/x/sys/unix"

func mknodAt(dirfd int, path string, mode uint32, dev int) error {
	return unix.Mknodat(dirfd, path, mode, dev)
}
