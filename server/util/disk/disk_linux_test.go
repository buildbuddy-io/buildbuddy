//go:build linux && !android

package disk_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmount"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	testmount.RunWithLimitedMountPermissions(m)
}

func TestClean(t *testing.T) {
	ctx := context.Background()

	root := testfs.MakeTempDir(t)
	// Files in outsideRoot should not be deleted by CleanDirectory.
	outsideRoot := testfs.MakeTempDir(t)

	// Write some trash to the root directory that should get cleaned up.
	testfs.WriteFile(t, root, "trash.txt", "garbage")

	// Create a parent directory with restrictive permissions.
	testfs.WriteFile(t, root, "parent/child.txt", "child-content")
	err := os.Chmod(filepath.Join(root, "parent"), 0444)
	require.NoError(t, err)

	// Create an overlayfs mount within rootDir.
	overlay1Upper := testfs.MakeDirAll(t, outsideRoot, "overlay1/upper")
	overlay1Work := testfs.MakeDirAll(t, outsideRoot, "overlay1/work")
	overlay1Lower := testfs.MakeDirAll(t, outsideRoot, "overlay1/lower")
	overlay1Merged := testfs.MakeDirAll(t, root, "overlay1/merged")
	args := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s,userxattr", overlay1Lower, overlay1Upper, overlay1Work)
	err = syscall.Mount("", overlay1Merged, "overlay", syscall.MS_RELATIME, args)
	require.NoError(t, err)
	// Write some files to the overlay dir and bind-mount it outside the root
	// so that we can make sure CleanDirectory didn't recurse into it.
	overlayBind := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, overlay1Merged, map[string]string{"foo1": "bar1"})
	testmount.Mount(t, overlay1Merged, overlayBind, "bind", syscall.MS_BIND, "")

	// Create a recursive overlayfs mount with a lowerdir referencing the
	// other overlayfs mount. ociruntime does recursive mounts like this in
	// order to work around path length limits in overlayfs, so it's probably
	// worth testing.
	overlayUpper2 := testfs.MakeDirAll(t, outsideRoot, "overlay2/upper")
	overlayWork2 := testfs.MakeDirAll(t, outsideRoot, "overlay2/work")
	overlayMerged2 := testfs.MakeDirAll(t, root, "overlay2/merged")
	args = fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s,userxattr", overlay1Merged, overlayUpper2, overlayWork2)
	err = syscall.Mount("", overlayMerged2, "overlay", syscall.MS_RELATIME, args)
	require.NoError(t, err)
	// Write some files to the second overlay dir and bind-mount it outside the
	// root so that we can make sure CleanDirectory didn't recurse into it.
	overlayBind2 := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, overlayMerged2, map[string]string{"foo2": "bar2"})
	testmount.Mount(t, overlayMerged2, overlayBind2, "bind", syscall.MS_BIND, "")

	err = disk.CleanDirectory(ctx, root)
	require.NoError(t, err)

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries, "expected root directory to be empty after clean")

	testfs.AssertExactFileContents(t, overlayBind, map[string]string{"foo1": "bar1"})
	testfs.AssertExactFileContents(t, overlayBind2, map[string]string{
		"foo1": "bar1",
		"foo2": "bar2",
	})
}
