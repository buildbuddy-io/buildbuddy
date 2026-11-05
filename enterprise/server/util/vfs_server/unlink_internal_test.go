//go:build linux && !android

package vfs_server

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	gstatus "google.golang.org/grpc/status"
)

func TestUnlinkMissingNameReleasesParentLockAndKeepsNlink(t *testing.T) {
	child := &fsNode{attrs: &vfspb.Attrs{Nlink: 1}}
	parent := &fsNode{children: map[string]*fsNode{}}

	// The name is no longer in the parent, as if a racing unlink won.
	err := unlink(parent, child, "gone")
	require.Error(t, err)
	for _, d := range gstatus.Convert(err).Details() {
		if se, ok := d.(*vfspb.SyscallError); ok {
			require.EqualValues(t, syscall.ENOENT, se.Errno)
		}
	}

	require.True(t, parent.mu.TryLock(), "parent lock must not be held after failed unlink")
	parent.mu.Unlock()
	require.EqualValues(t, 1, child.attrs.Nlink, "failed unlink must not decrement Nlink")
}
