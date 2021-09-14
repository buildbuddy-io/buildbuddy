//+build darwin

package vfs_server

import (
	"syscall"

	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
)

func (h *fileHandle) allocate(req *vfspb.AllocateRequest) (*vfspb.AllocateResponse, error) {
	return nil, syscallErrStatus(syscall.ENOSYS)
}
