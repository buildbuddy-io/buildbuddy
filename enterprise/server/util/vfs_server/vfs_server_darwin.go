//go:build darwin
// +build darwin

package vfs_server

import (
	"context"
	"syscall"

	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
)

func (h *fileHandle) allocate(req *vfspb.AllocateRequest) (*vfspb.AllocateResponse, error) {
	return nil, syscallErrStatus(syscall.ENOSYS)
}

func (p *Server) CopyFileRange(ctx context.Context, request *vfspb.CopyFileRangeRequest) (*vfspb.CopyFileRangeResponse, error) {
	return nil, syscallErrStatus(syscall.ENOSYS)
}
