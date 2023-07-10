//go:build darwin && !ios && (amd64 || arm64)

package vfs

import (
	"syscall"

	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
)

func attrsToStat(attr *vfspb.Attrs) *syscall.Stat_t {
	return &syscall.Stat_t{
		Size:  attr.GetSize(),
		Mode:  uint16(attr.GetPerm()),
		Nlink: uint16(attr.GetNlink()),
	}
}
