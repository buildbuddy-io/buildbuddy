//go:build linux && !android

package workspace_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestPool_ReusedWorkspaceMatchesFreshSetup(t *testing.T) {
	flags.Set(t, "executor.workspace_pool.slots", 4)
	te := newPoolTestEnv(t)
	ctx := t.Context()

	dirNode := func(name string, dir *repb.Directory) *repb.DirectoryNode {
		d, err := cachetools.UploadProto(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, dir)
		require.NoError(t, err)
		return &repb.DirectoryNode{Name: name, Digest: d}
	}
	pkg := &repb.Directory{Files: []*repb.FileNode{uploadFile(t, te, "index.js", "index")}}
	nodeModules := &repb.Directory{
		Directories: []*repb.DirectoryNode{dirNode("pkg", pkg)},
		Symlinks:    []*repb.SymlinkNode{{Name: "link", Target: "pkg"}},
	}
	out := &repb.Directory{}
	bin := &repb.Directory{
		Files:       []*repb.FileNode{uploadFile(t, te, "gen.js", "gen")},
		Directories: []*repb.DirectoryNode{dirNode("out", out)},
	}
	oldOnly := &repb.Directory{Files: []*repb.FileNode{uploadFile(t, te, "stale.txt", "stale")}}
	oldTree := &repb.Tree{
		Root: &repb.Directory{
			Files:       []*repb.FileNode{uploadFile(t, te, "keep.txt", "keep")},
			Directories: []*repb.DirectoryNode{dirNode("bin", bin), dirNode("node_modules", nodeModules), dirNode("old_only", oldOnly)},
		},
		Children: []*repb.Directory{bin, out, nodeModules, pkg, oldOnly},
	}
	newTree := &repb.Tree{
		Root: &repb.Directory{
			Files:       []*repb.FileNode{uploadFile(t, te, "keep.txt", "keep"), uploadFile(t, te, "new.txt", "new")},
			Directories: []*repb.DirectoryNode{dirNode("bin", bin), dirNode("node_modules", nodeModules)},
		},
		Children: []*repb.Directory{bin, out, nodeModules, pkg},
	}
	cmd := &repb.Command{
		OutputDirectories: []string{"bin/out"},
		OutputFiles:       []string{"bin/result.txt", "made/by/task/result.txt"},
	}
	setUp := func(ws *workspace.Workspace, tree *repb.Tree) {
		ws.SetTask(ctx, &repb.ExecutionTask{Command: cmd})
		require.NoError(t, ws.DownloadInputs(ctx, &container.FileSystemLayout{DigestFunction: repb.DigestFunction_SHA256, Inputs: tree}))
		require.NoError(t, ws.CreateOutputDirs())
		_, err := ws.TaskFinished()
		require.NoError(t, err)
	}
	snapshot := func(root string) map[string]string {
		entries := map[string]string{}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			require.NoError(t, err)
			rel, err := filepath.Rel(root, path)
			require.NoError(t, err)
			info, err := os.Lstat(path)
			require.NoError(t, err)
			switch {
			case info.Mode()&fs.ModeSymlink != 0:
				target, err := os.Readlink(path)
				require.NoError(t, err)
				entries[rel] = "symlink to " + target
			case info.IsDir():
				entries[rel] = info.Mode().String() + " xattrs=" + xattrs(t, path)
			default:
				content, err := os.ReadFile(path)
				require.NoError(t, err)
				entries[rel] = info.Mode().String() + " " + string(content)
			}
			return nil
		})
		require.NoError(t, err)
		return entries
	}
	write := func(t *testing.T, path, content string) {
		require.NoError(t, os.WriteFile(path, []byte(content), 0644))
	}

	for _, test := range []struct {
		name string
		// task changes the first workspace as its task would.
		task func(t *testing.T, root string)
	}{
		{
			name: "unchanged",
			task: func(t *testing.T, root string) {},
		},
		{
			name: "outputs",
			task: func(t *testing.T, root string) {
				write(t, filepath.Join(root, "bin/result.txt"), "result")
				write(t, filepath.Join(root, "bin/out/a.txt"), "a")
				write(t, filepath.Join(root, "made/by/task/result.txt"), "result")
			},
		},
		{
			name: "input replaced",
			task: func(t *testing.T, root string) {
				require.NoError(t, os.Remove(filepath.Join(root, "keep.txt")))
				write(t, filepath.Join(root, "keep.txt"), "changed")
			},
		},
		{
			name: "input deleted",
			task: func(t *testing.T, root string) {
				require.NoError(t, os.Remove(filepath.Join(root, "bin/gen.js")))
			},
		},
		{
			name: "file added to nested input directory",
			task: func(t *testing.T, root string) {
				write(t, filepath.Join(root, "node_modules/pkg/stray.js"), "stray")
			},
		},
		{
			name: "input directory replaced by symlink",
			task: func(t *testing.T, root string) {
				elsewhere := filepath.Join(testfs.MakeTempDir(t), "pkg")
				require.NoError(t, os.Rename(filepath.Join(root, "node_modules/pkg"), elsewhere))
				write(t, filepath.Join(elsewhere, "stray.js"), "stray")
				require.NoError(t, os.Symlink(elsewhere, filepath.Join(root, "node_modules/pkg")))
			},
		},
		{
			name: "input directory replaced by new directory",
			task: func(t *testing.T, root string) {
				require.NoError(t, os.Rename(filepath.Join(root, "bin"), filepath.Join(root, "bin2")))
				require.NoError(t, os.Mkdir(filepath.Join(root, "bin"), 0755))
			},
		},
		{
			name: "symlink retargeted",
			task: func(t *testing.T, root string) {
				require.NoError(t, os.Remove(filepath.Join(root, "node_modules/link")))
				require.NoError(t, os.Symlink("elsewhere", filepath.Join(root, "node_modules/link")))
			},
		},
		{
			name: "input directory made read-only",
			task: func(t *testing.T, root string) {
				require.NoError(t, os.Chmod(filepath.Join(root, "node_modules/pkg"), 0555))
			},
		},
		{
			name: "xattr set on input directory",
			task: func(t *testing.T, root string) {
				err := unix.Lsetxattr(filepath.Join(root, "node_modules/pkg"), "user.test", []byte("value"), 0)
				if errors.Is(err, errors.ErrUnsupported) {
					t.Skip("The filesystem doesn't support user xattrs")
				}
				require.NoError(t, err)
			},
		},
		{
			name: "ACL changed on input directory",
			task: func(t *testing.T, root string) {
				err := unix.Lsetxattr(filepath.Join(root, "node_modules/pkg"), "system.posix_acl_default", posixACL(5), 0)
				if errors.Is(err, errors.ErrUnsupported) {
					t.Skip("The filesystem doesn't support ACLs")
				}
				require.NoError(t, err)
			},
		},
	} {
		// A 4 byte limit removes the 5 byte inputs before pooling, which the
		// next task then links again.
		for _, maxFileSize := range []int64{0, 4} {
			t.Run(fmt.Sprintf("%s/max_file_size=%d", test.name, maxFileSize), func(t *testing.T) {
				flags.Set(t, "executor.workspace_pool.max_file_size_bytes", maxFileSize)
				buildRoot := testfs.MakeTempDir(t)
				freshBuildRoot := testfs.MakeTempDir(t)
				setDefaultACL(t, buildRoot)
				setDefaultACL(t, freshBuildRoot)
				pool, err := workspace.NewPool(buildRoot)
				require.NoError(t, err)
				opts := &workspace.Opts{Pool: pool}
				first, err := workspace.New(te, buildRoot, opts)
				require.NoError(t, err)
				setUp(first, oldTree)
				test.task(t, first.Path())
				firstRoot, err := os.Lstat(first.Path())
				require.NoError(t, err)
				first.ReturnToPool(ctx)
				require.NoError(t, first.Remove(ctx))

				second, err := workspace.New(te, buildRoot, opts)
				require.NoError(t, err)
				setUp(second, newTree)
				fresh, err := workspace.New(te, freshBuildRoot, &workspace.Opts{})
				require.NoError(t, err)
				setUp(fresh, newTree)

				secondRoot, err := os.Lstat(second.Path())
				require.NoError(t, err)
				assert.True(t, os.SameFile(firstRoot, secondRoot), "second workspace should have reused the pooled one")
				want := snapshot(fresh.Path())
				got := snapshot(second.Path())
				assert.Equal(t, want, got)
			})
		}
	}
}

// xattrs returns the extended attributes of the file at path as sorted
// "name=value" pairs, leaving out security.* attributes like the workspace
// pool does.
func xattrs(t *testing.T, path string) string {
	size, err := unix.Llistxattr(path, nil)
	if errors.Is(err, errors.ErrUnsupported) {
		return ""
	}
	require.NoError(t, err)
	list := make([]byte, size)
	n, err := unix.Llistxattr(path, list)
	require.NoError(t, err)
	var pairs []string
	for name := range strings.SplitSeq(strings.TrimSuffix(string(list[:n]), "\x00"), "\x00") {
		if name == "" || strings.HasPrefix(name, "security.") {
			continue
		}
		size, err := unix.Lgetxattr(path, name, nil)
		require.NoError(t, err)
		value := make([]byte, size)
		n, err := unix.Lgetxattr(path, name, value)
		require.NoError(t, err)
		pairs = append(pairs, fmt.Sprintf("%s=%q", name, value[:n]))
	}
	slices.Sort(pairs)
	return strings.Join(pairs, " ")
}

// posixACL returns a minimal POSIX ACL in the format of the
// system.posix_acl_* xattrs, which gives the owner the given permissions and
// everyone else read and execute permissions.
func posixACL(ownerPerm uint16) []byte {
	acl := binary.LittleEndian.AppendUint32(nil, 2) // POSIX_ACL_XATTR_VERSION
	for _, e := range []struct{ tag, perm uint16 }{
		{0x01, ownerPerm}, // ACL_USER_OBJ
		{0x04, 5},         // ACL_GROUP_OBJ
		{0x20, 5},         // ACL_OTHER
	} {
		acl = binary.LittleEndian.AppendUint16(acl, e.tag)
		acl = binary.LittleEndian.AppendUint16(acl, e.perm)
		acl = binary.LittleEndian.AppendUint32(acl, 0xffffffff) // ACL_UNDEFINED_ID
	}
	return acl
}

// setDefaultACL gives dir a default ACL, which directories created under it
// inherit as an xattr, so that input directories start out with extended
// attributes. It does nothing if the filesystem doesn't support ACLs.
func setDefaultACL(t *testing.T, dir string) {
	err := unix.Lsetxattr(dir, "system.posix_acl_default", posixACL(7), 0)
	if errors.Is(err, errors.ErrUnsupported) {
		return
	}
	require.NoError(t, err)
}

// newPoolTestEnv returns a test environment with a CAS and a file cache, for
// setting up workspaces.
func newPoolTestEnv(t *testing.T) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, lis)
	go runServer()
	fc, err := filecache.NewFileCache(testfs.MakeTempDir(t), 1e9, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	te.SetFileCache(fc)
	return te
}

// uploadFile uploads content to the CAS and returns a file node for it.
func uploadFile(t *testing.T, te *testenv.TestEnv, name, content string) *repb.FileNode {
	d, err := cachetools.UploadBlob(t.Context(), te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, strings.NewReader(content))
	require.NoError(t, err)
	return &repb.FileNode{Name: name, Digest: d}
}

// runPooledTask sets up a workspace with the given input tree, starting from
// a pooled workspace if the pool has a suitable one, then returns it to the
// pool. It returns the info of the workspace's root directory, which keeps
// its inode when a later task reuses the directory.
func runPooledTask(t *testing.T, ctx context.Context, te *testenv.TestEnv, buildRoot string, pool *workspace.Pool, tree *repb.Tree) os.FileInfo {
	ws, err := workspace.New(te, buildRoot, &workspace.Opts{Pool: pool})
	require.NoError(t, err)
	ws.SetTask(ctx, &repb.ExecutionTask{Command: &repb.Command{}})
	err = ws.DownloadInputs(ctx, &container.FileSystemLayout{DigestFunction: repb.DigestFunction_SHA256, Inputs: tree})
	require.NoError(t, err)
	root, err := os.Lstat(ws.Path())
	require.NoError(t, err)
	ws.ReturnToPool(ctx)
	err = ws.Remove(ctx)
	require.NoError(t, err)
	return root
}

func TestPool_TakesOnlySimilarWorkspacesFromTheSameGroup(t *testing.T) {
	flags.Set(t, "executor.workspace_pool.slots", 4)
	te := newPoolTestEnv(t)
	ctx := t.Context()
	groupA := claims.AuthContextWithJWT(ctx, &claims.Claims{GroupID: "GR1"}, nil)
	groupB := claims.AuthContextWithJWT(ctx, &claims.Claims{GroupID: "GR2"}, nil)
	a := uploadFile(t, te, "a.txt", "a")
	b := uploadFile(t, te, "b.txt", "b")
	c := uploadFile(t, te, "c.txt", "c")
	d := uploadFile(t, te, "d.txt", "d")
	pooledTree := &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{a, b, c}}}

	for _, test := range []struct {
		name       string
		ctx        context.Context
		tree       *repb.Tree
		wantReused bool
	}{
		{
			name:       "same group and inputs",
			ctx:        groupA,
			tree:       pooledTree,
			wantReused: true,
		},
		{
			name:       "other group",
			ctx:        groupB,
			tree:       pooledTree,
			wantReused: false,
		},
		{
			name: "low overlap",
			ctx:  groupA,
			// The pooled workspace has only one of its three files in common
			// with this tree, so reusing it would remove more files than it
			// keeps.
			tree:       &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{a, d}}},
			wantReused: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			buildRoot := testfs.MakeTempDir(t)
			pool, err := workspace.NewPool(buildRoot)
			require.NoError(t, err)
			pooled := runPooledTask(t, groupA, te, buildRoot, pool, pooledTree)
			next := runPooledTask(t, test.ctx, te, buildRoot, pool, test.tree)
			reused := os.SameFile(pooled, next)
			assert.Equal(t, test.wantReused, reused)
		})
	}
}

func TestPool_EvictsWorkspacesOverSlotLimit(t *testing.T) {
	flags.Set(t, "executor.workspace_pool.slots", 1)
	te := newPoolTestEnv(t)
	ctx := t.Context()
	tree1 := &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{uploadFile(t, te, "a.txt", "a")}}}
	tree2 := &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{uploadFile(t, te, "b.txt", "b")}}}
	buildRoot := testfs.MakeTempDir(t)
	pool, err := workspace.NewPool(buildRoot)
	require.NoError(t, err)

	runPooledTask(t, ctx, te, buildRoot, pool, tree1)
	second := runPooledTask(t, ctx, te, buildRoot, pool, tree2)

	// Both workspaces hold one file, so pooling the second one evicts the
	// first, which is older, and the pool deletes it in the background.
	require.Eventually(t, func() bool {
		entries, err := os.ReadDir(filepath.Join(buildRoot, "_WORKSPACE_POOL"))
		return err == nil && len(entries) == 1
	}, 10*time.Second, 10*time.Millisecond)
	third := runPooledTask(t, ctx, te, buildRoot, pool, tree2)
	reused := os.SameFile(second, third)
	assert.True(t, reused)
}
