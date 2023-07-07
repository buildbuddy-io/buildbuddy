//go:build linux && !android && (amd64 || arm64)

package vfs_server

import (
	"context"
	"syscall"

	"golang.org/x/sys/unix"

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

func (p *Server) CopyFileRange(ctx context.Context, request *vfspb.CopyFileRangeRequest) (*vfspb.CopyFileRangeResponse, error) {
	rh, err := p.getFileHandle(request.GetReadHandleId())
	if err != nil {
		return nil, err
	}
	wh, err := p.getFileHandle(request.GetWriteHandleId())
	if err != nil {
		return nil, err
	}

	var lockFirst, lockSecond *fileHandle
	if request.GetReadHandleId() <= request.GetWriteHandleId() {
		lockFirst = rh
		lockSecond = wh
	} else {
		lockFirst = wh
		lockSecond = rh
	}
	lockFirst.mu.Lock()
	defer lockFirst.mu.Unlock()
	if lockSecond != lockFirst {
		lockSecond.mu.Lock()
		defer lockSecond.mu.Unlock()
	}

	n, err := unix.CopyFileRange(int(rh.f.Fd()), &request.ReadHandleOffset, int(wh.f.Fd()), &request.WriteHandleOffset, int(request.GetNumBytes()), int(request.GetFlags()))
	if err != nil {
		return nil, syscallErrStatus(err)
	}
	return &vfspb.CopyFileRangeResponse{NumBytesCopied: uint32(n)}, nil
}
