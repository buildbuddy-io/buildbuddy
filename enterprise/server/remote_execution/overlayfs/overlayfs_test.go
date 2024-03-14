package overlayfs_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/overlayfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmount"
	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	testmount.RunWithLimitedMountPermissions(m)
}

func TestOverlayWorkspace(t *testing.T) {
	tmp := testfs.MakeTempDir(t)
	ws := testfs.MakeDirAll(t, tmp, "workspace")
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"a.txt":             "A",
		"dir1/b.txt":        "B",
		"dir1/child1/c.txt": "C",
		"dir2/d.txt":        "D",
		"dir3/e.txt":        "E",
	})
	// Also create a symlink.
	err := os.Symlink("a.txt", filepath.Join(ws, "a-symlink"))
	require.NoError(t, err)

	// Get a file handle on the original 'a.txt'
	f, err := os.Open(filepath.Join(ws, "a.txt"))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	// Get the original file permissions of dir2/d.txt
	statDOriginal, err := os.Stat(filepath.Join(ws, "dir2/d.txt"))
	require.NoError(t, err)
	originalPermsD := statDOriginal.Mode().Perm()

	// Convert the workspace to overlayfs
	ctx := context.Background()
	o, err := overlayfs.Convert(ctx, ws, overlayfs.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := o.Remove(ctx)
		require.NoError(t, err)
	})

	// Now simulate an action doing some workspace modifications:
	// Overwrite 'a.txt' in the overlay workspace
	fileAOverlay, err := os.OpenFile(filepath.Join(ws, "a.txt"), os.O_RDWR|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = fileAOverlay.Write([]byte("-MODIFIED"))
	require.NoError(t, err)
	// Note, we have to close the overlayfs file handle before the overlayfs is
	// removed in t.Cleanup() above, otherwise we get 'device or resource busy'
	// when attempting to clean up the workspace.
	t.Cleanup(func() { fileAOverlay.Close() })
	// Remove dir1/b.txt and dir1/child1/.
	err = os.RemoveAll(filepath.Join(ws, "dir1/b.txt"))
	require.NoError(t, err)
	err = os.RemoveAll(filepath.Join(ws, "dir1/child1/"))
	require.NoError(t, err)
	// Change a file so that it's now a directory, and create a child.
	err = os.RemoveAll(filepath.Join(ws, "dir3/e.txt"))
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(ws, "dir3/e.txt"), 0755)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(ws, "dir3/e.txt/child.txt"), []byte("child-contents"), 0755)
	require.NoError(t, err)
	// Update the symlink target.
	err = os.Remove(filepath.Join(ws, "a-symlink"))
	require.NoError(t, err)
	err = os.Symlink("new-link-target", filepath.Join(ws, "a-symlink"))
	require.NoError(t, err)
	// Change the file permissions of dir2/d.txt.
	const newPermsD = 0444
	require.NotEqual(t, originalPermsD, newPermsD, "sanity check: should actually change the perms")
	err = os.Chmod(filepath.Join(ws, "dir2/d.txt"), newPermsD)
	require.NoError(t, err)

	// Original 'a.txt' file handle should be unaffected.
	b, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "A", string(b))
	// Original 'dir1/d.txt' file permissions (in LowerDir) should be
	// unaffected.
	statD, err := os.Stat(filepath.Join(o.LowerDir, "dir2/d.txt"))
	require.NoError(t, err)
	require.Equal(t, originalPermsD, statD.Mode().Perm())

	// Apply the changes to the lower dir.
	err = o.Apply(ctx, overlayfs.ApplyOpts{})
	require.NoError(t, err)
	lowerdir := ws + ".lower"
	testfs.AssertExactFileContents(t, lowerdir, map[string]string{
		"a.txt":                "A-MODIFIED",
		"dir2/d.txt":           "D",
		"dir3/e.txt/child.txt": "child-contents",
	})
	// Also check that dir1/ still exists but is empty.
	dir1Entries, err := os.ReadDir(filepath.Join(lowerdir, "dir1"))
	require.NoError(t, err)
	require.Empty(t, dir1Entries)
	// Also make sure the symlink update worked.
	newLinkTarget, err := os.Readlink(filepath.Join(ws, "a-symlink"))
	require.NoError(t, err)
	require.Equal(t, "new-link-target", newLinkTarget)

	// Upper dir should now be empty after applying.
	upperdirEntries, err := os.ReadDir(ws + ".upper")
	require.NoError(t, err)
	require.Empty(t, upperdirEntries)

	// Original 'a.txt' file handle should still be unaffected, even after
	// applying, since we should *replace* the file from the upper dir, not
	// overwrite it.
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	b, err = io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "A", string(b))

	// Edge case (but important): make sure that if a paused workspace had a
	// file handle open while paused, then tries to write to that same file
	// handle upon resume, it still does not affect the underlying node, even
	// though we moved that file from the upperdir to the lowerdir (and possibly
	// linked it to filecache).
	_, err = fileAOverlay.Write([]byte("-MODIFIED-AFTER-APPLY"))
	require.NoError(t, err)
	b, err = os.ReadFile(filepath.Join(lowerdir, "a.txt"))
	require.NoError(t, err)
	require.Equal(t, "A-MODIFIED", string(b))
}

func BenchmarkOverlayfsIO(b *testing.B) {
	type testCase struct {
		OverlayEnabled bool
		FileSize       int64
		Read           bool
	}
	var testCases []testCase
	for _, read := range []bool{true, false} {
		for _, fileSize := range []int64{1, 1024, 4 * 1024, 8 * 1024, 50 * 1024, 100 * 1024} {
			for _, enabled := range []bool{true, false} {
				testCases = append(testCases, testCase{
					OverlayEnabled: enabled,
					FileSize:       fileSize,
					Read:           read,
				})
			}
		}
	}
	for _, test := range testCases {
		enabledLbl := "OverlayEnabled"
		if !test.OverlayEnabled {
			enabledLbl = "OverlayDisabled"
		}
		opLbl := "Read"
		if !test.Read {
			opLbl = "Write"
		}
		name := fmt.Sprintf("%s_%sFile_%s", enabledLbl, opLbl, units.BytesSize(float64(test.FileSize)))
		tmp := testfs.MakeTempDir(b)
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			// Create a file for reading
			ws := filepath.Join(tmp, name)
			b.Cleanup(func() { os.RemoveAll(ws) })
			err := os.Mkdir(ws, 0755)
			require.NoError(b, err)
			fname := filepath.Join(ws, "file")
			f, err := os.Create(fname)
			require.NoError(b, err)
			b.Cleanup(func() { f.Close() })
			buf := make([]byte, test.FileSize)
			_, err = f.Write(buf)
			require.NoError(b, err)
			_ = f.Close()
			// If enabled, convert to overlay
			if test.OverlayEnabled {
				o, err := overlayfs.Convert(ctx, ws, overlayfs.Opts{})
				require.NoError(b, err)
				b.Cleanup(func() {
					err := o.Remove(ctx)
					require.NoError(b, err)
				})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if test.Read {
					_, err := os.ReadFile(fname)
					require.NoError(b, err)
				} else {
					err := os.WriteFile(filepath.Join(ws, strconv.Itoa(i)), buf, 0755)
					require.NoError(b, err)
				}
			}
		})
	}
}
