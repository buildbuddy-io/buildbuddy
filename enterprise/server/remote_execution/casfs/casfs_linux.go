// +build linux
// +build !android

package casfs

import (
	"context"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"golang.org/x/sys/unix"
)

func (n *Node) CopyFileRange(ctx context.Context, fhIn fs.FileHandle, offIn uint64, out *fs.Inode, fhOut fs.FileHandle, offOut uint64, len uint64, flags uint64) (uint32, syscall.Errno) {
	lfIn, ok := fhIn.(*instrumentedLoopbackFile)
	if !ok {
		log.Warningf("[%s] In is not a *instrumentedLoopbackFile", n.cfs.taskID())
		return 0, syscall.ENOSYS
	}
	lfOut, ok := fhOut.(*instrumentedLoopbackFile)
	if !ok {
		log.Warningf("[%s] Out is not a *instrumentedLoopbackFile", n.cfs.taskID())
		return 0, syscall.ENOSYS
	}

	rOffset := int64(offIn)
	wOffset := int64(offOut)
	bytesCopied, err := unix.CopyFileRange(lfIn.fd, &rOffset, lfOut.fd, &wOffset, int(len), int(flags))
	return uint32(bytesCopied), fs.ToErrno(err)
}
