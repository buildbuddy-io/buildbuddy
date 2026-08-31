//go:build freebsd || openbsd

package vfs

import (
	"context"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfscommon"
	"github.com/buildbuddy-io/buildbuddy/proto/vfs"
)

type VFS struct{}

type Options struct {
	EnablePassthrough   bool
	Verbose             bool
	LogFUSEOps          bool
	LogFUSELatencyStats bool
	LogFUSEPerFileStats bool
}

func New(vfsClient vfs.FileSystemClient, mountDir string, options *Options) *VFS {
	panic("VFS is not implemented on " + runtime.GOOS + ". Please set `executor.enable_vfs=false`")
}

func (vfs *VFS) GetMountDir() string {
	return ""
}

func (vfs *VFS) Mount() error {
	return nil
}

func (vfs *VFS) PrepareForTask(ctx context.Context, taskID string, invalidatedInodes *vfscommon.InodeInvalidations) error {
	return nil
}

func (vfs *VFS) FinishTask() error {
	return nil
}

func (vfs *VFS) Unmount() error {
	return nil
}
