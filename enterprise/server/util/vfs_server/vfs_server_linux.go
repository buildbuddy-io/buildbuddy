//+build linux

package vfs_server

import (
	"syscall"

	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
)

func (h *fileHandle) allocate(req *vfspb.AllocateRequest) (*vfspb.AllocateResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := syscall.Fallocate(int(h.f.Fd()), req.GetMode(), req.GetOffset(), req.GetNumBytes()); err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.AllocateResponse{}, nil
}
