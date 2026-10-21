//go:build linux && !android

package workspace_test

import (
	"context"
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
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	// newLib differs from oldLib in every entry except "kept_link" and "same",
	// so when the second task reuses the first workspace, syncDir has to remove
	// each of those entries before the tree fetcher creates the new one:
	//   - content.txt has different content.
	//   - exec.sh is executable only in newLib.
	//   - Each entry named <a>_to_<b> is an a in oldLib and a b in newLib.
	//   - retargeted_link has a different target.
	// We make the old content.txt longer than the new one. If syncDir left the
	// old copy in place, the tree fetcher would write the new content over it
	// and leave old bytes at the end, which the comparison with a fresh setup
	// would catch.
	//
	// take only reuses a pooled workspace that shares more files with the new
	// tree than it doesn't, so we put same in both trees, and its files
	// outweigh the ones that differ.
	empty := &repb.Directory{}
	same := &repb.Directory{Files: []*repb.FileNode{
		uploadFile(t, te, "a.txt", "a"),
		uploadFile(t, te, "b.txt", "b"),
		uploadFile(t, te, "c.txt", "c"),
		uploadFile(t, te, "d.txt", "d"),
	}}
	oldLib := &repb.Directory{
		Files: []*repb.FileNode{
			uploadFile(t, te, "content.txt", "old!"),
			uploadFile(t, te, "exec.sh", "sh"),
			uploadFile(t, te, "file_to_dir", "f"),
			uploadFile(t, te, "file_to_link", "f"),
		},
		Directories: []*repb.DirectoryNode{dirNode("dir_to_file", empty), dirNode("dir_to_link", empty), dirNode("same", same)},
		Symlinks: []*repb.SymlinkNode{
			{Name: "kept_link", Target: "exec.sh"},
			{Name: "link_to_dir", Target: "exec.sh"},
			{Name: "link_to_file", Target: "exec.sh"},
			{Name: "retargeted_link", Target: "content.txt"},
		},
	}
	executable := uploadFile(t, te, "exec.sh", "sh")
	executable.IsExecutable = true
	newLib := &repb.Directory{
		Files: []*repb.FileNode{
			uploadFile(t, te, "content.txt", "new"),
			uploadFile(t, te, "dir_to_file", "f"),
			executable,
			uploadFile(t, te, "link_to_file", "f"),
		},
		Directories: []*repb.DirectoryNode{dirNode("file_to_dir", empty), dirNode("link_to_dir", empty), dirNode("same", same)},
		Symlinks: []*repb.SymlinkNode{
			{Name: "dir_to_link", Target: "exec.sh"},
			{Name: "file_to_link", Target: "exec.sh"},
			{Name: "kept_link", Target: "exec.sh"},
			{Name: "retargeted_link", Target: "exec.sh"},
		},
	}
	oldTree := &repb.Tree{
		Root: &repb.Directory{
			Files:       []*repb.FileNode{uploadFile(t, te, "keep.txt", "keep")},
			Directories: []*repb.DirectoryNode{dirNode("bin", bin), dirNode("lib", oldLib), dirNode("node_modules", nodeModules), dirNode("old_only", oldOnly)},
		},
		Children: []*repb.Directory{bin, out, oldLib, empty, same, nodeModules, pkg, oldOnly},
	}
	newTree := &repb.Tree{
		Root: &repb.Directory{
			Files:       []*repb.FileNode{uploadFile(t, te, "keep.txt", "keep"), uploadFile(t, te, "new.txt", "new")},
			Directories: []*repb.DirectoryNode{dirNode("bin", bin), dirNode("lib", newLib), dirNode("node_modules", nodeModules)},
		},
		Children: []*repb.Directory{bin, out, newLib, empty, same, nodeModules, pkg},
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
				entries[rel] = info.Mode().String()
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
		// changed lists the inputs that the task changed, which the pool
		// removes along with everything under them.
		changed []string
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
			changed: []string{"keep.txt"},
		},
		{
			name: "input deleted",
			task: func(t *testing.T, root string) {
				require.NoError(t, os.Remove(filepath.Join(root, "bin/gen.js")))
			},
			changed: []string{"bin/gen.js"},
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
			changed: []string{"node_modules/pkg"},
		},
		{
			name: "input directory replaced by new directory",
			task: func(t *testing.T, root string) {
				require.NoError(t, os.Rename(filepath.Join(root, "bin"), filepath.Join(root, "bin2")))
				require.NoError(t, os.Mkdir(filepath.Join(root, "bin"), 0755))
			},
			changed: []string{"bin"},
		},
		{
			name: "symlink retargeted",
			task: func(t *testing.T, root string) {
				require.NoError(t, os.Remove(filepath.Join(root, "node_modules/link")))
				require.NoError(t, os.Symlink("elsewhere", filepath.Join(root, "node_modules/link")))
			},
			changed: []string{"node_modules/link"},
		},
		{
			name: "input directory made read-only",
			task: func(t *testing.T, root string) {
				require.NoError(t, os.Chmod(filepath.Join(root, "node_modules/pkg"), 0555))
			},
			changed: []string{"node_modules/pkg"},
		},
	} {
		// A 4 byte limit removes the 5 byte inputs before pooling, which the
		// next task then links again.
		for _, maxFileSize := range []int64{0, 4} {
			t.Run(fmt.Sprintf("%s/max_file_size=%d", test.name, maxFileSize), func(t *testing.T) {
				flags.Set(t, "executor.workspace_pool.max_file_size_bytes", maxFileSize)
				buildRoot := testfs.MakeTempDir(t)
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

				// The pool holds the first workspace's directory.
				poolDir := filepath.Join(buildRoot, "_WORKSPACE_POOL")
				entries, err := os.ReadDir(poolDir)
				require.NoError(t, err)
				require.Len(t, entries, 1)
				pooledPath := filepath.Join(poolDir, entries[0].Name())
				pooledRoot, err := os.Lstat(pooledPath)
				require.NoError(t, err)
				assert.True(t, os.SameFile(firstRoot, pooledRoot), "pool should hold the first workspace's directory")

				// The pooled directory holds the same inputs as a fresh setup of
				// the same tree, without the inputs that the task changed or
				// that are over the size limit.
				fresh, err := workspace.New(te, testfs.MakeTempDir(t), &workspace.Opts{})
				require.NoError(t, err)
				fresh.SetTask(ctx, &repb.ExecutionTask{Command: &repb.Command{}})
				err = fresh.DownloadInputs(ctx, &container.FileSystemLayout{DigestFunction: repb.DigestFunction_SHA256, Inputs: oldTree})
				require.NoError(t, err)
				removed := test.changed
				if maxFileSize > 0 {
					removed = append(slices.Clone(removed), "node_modules/pkg/index.js", "old_only/stale.txt")
				}
				want := snapshot(fresh.Path())
				for rel := range want {
					for _, r := range removed {
						if rel == r || strings.HasPrefix(rel, r+"/") {
							delete(want, rel)
						}
					}
				}
				got := snapshot(pooledPath)
				assert.Equal(t, want, got)

				// The next task, whose tree shares most of its inputs with the
				// first one's, reuses the pooled directory, and gets the same
				// workspace as a fresh setup of its tree.
				second, err := workspace.New(te, buildRoot, opts)
				require.NoError(t, err)
				setUp(second, newTree)
				secondRoot, err := os.Lstat(second.Path())
				require.NoError(t, err)
				assert.True(t, os.SameFile(firstRoot, secondRoot), "second workspace should have reused the pooled one")
				freshSecond, err := workspace.New(te, testfs.MakeTempDir(t), &workspace.Opts{})
				require.NoError(t, err)
				setUp(freshSecond, newTree)
				want = snapshot(freshSecond.Path())
				got = snapshot(second.Path())
				assert.Equal(t, want, got)
			})
		}
	}
}

func TestPool_RecordsReuseMetrics(t *testing.T) {
	flags.Set(t, "executor.workspace_pool.slots", 4)
	te := newPoolTestEnv(t)
	ctx := claims.AuthContextWithJWT(t.Context(), &claims.Claims{GroupID: "GR1"}, nil)
	groupLabel := prometheus.Labels{metrics.GroupID: "GR1"}
	metrics.WorkspacePoolReturns.Reset()
	metrics.WorkspacePoolReuseAttempts.Reset()
	metrics.WorkspacePoolCount.Reset()
	metrics.WorkspacePoolInputFiles.Reset()
	a := uploadFile(t, te, "a.txt", "a")
	b := uploadFile(t, te, "b.txt", "b")
	c := uploadFile(t, te, "c.txt", "c")
	x := uploadFile(t, te, "x.txt", "x")
	sub := &repb.Directory{Files: []*repb.FileNode{uploadFile(t, te, "d.txt", "d"), uploadFile(t, te, "e.txt", "e")}}
	subDigest, err := cachetools.UploadProto(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, sub)
	require.NoError(t, err)
	firstTree := &repb.Tree{
		Root: &repb.Directory{
			Files:       []*repb.FileNode{a, b, c, x, uploadFile(t, te, "y.txt", "y")},
			Directories: []*repb.DirectoryNode{{Name: "sub", Digest: subDigest}},
		},
		Children: []*repb.Directory{sub},
	}
	// The second tree shares a, b, c and x with the first, which outweighs
	// the 3 files that only the first has, so the second task reuses the
	// first workspace.
	secondTree := &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{a, b, c, x, uploadFile(t, te, "f.txt", "f")}}}
	buildRoot := testfs.MakeTempDir(t)
	pool, err := workspace.NewPool(buildRoot)
	require.NoError(t, err)

	// The first task deletes its input b.txt, so the pool removes it from the
	// workspace, but still counts it among the workspace's input files.
	first, err := workspace.New(te, buildRoot, &workspace.Opts{Pool: pool})
	require.NoError(t, err)
	first.SetTask(ctx, &repb.ExecutionTask{Command: &repb.Command{}})
	err = first.DownloadInputs(ctx, &container.FileSystemLayout{DigestFunction: repb.DigestFunction_SHA256, Inputs: firstTree})
	require.NoError(t, err)
	err = os.Remove(filepath.Join(first.Path(), "b.txt"))
	require.NoError(t, err)
	first.ReturnToPool(ctx)
	err = first.Remove(ctx)
	require.NoError(t, err)
	pooledReturns := testmetrics.CounterValueForLabels(t, metrics.WorkspacePoolReturns, prometheus.Labels{metrics.WorkspacePoolReturnStatusLabel: "pooled"})
	assert.Equal(t, 1.0, pooledReturns)
	pooledCount := testmetrics.GaugeValueForLabels(t, metrics.WorkspacePoolCount, groupLabel)
	assert.Equal(t, 1.0, pooledCount)
	pooledFiles := testmetrics.GaugeValueForLabels(t, metrics.WorkspacePoolInputFiles, groupLabel)
	assert.Equal(t, 7.0, pooledFiles)

	// The second task keeps a, c and x, links b again, since the first task
	// deleted it, and links the new f. It removes y and the 2 files in sub.
	histograms := []struct {
		name        string
		metric      prometheus.Histogram
		want        float64
		countBefore uint64
		sumBefore   float64
	}{
		{name: "kept", metric: metrics.WorkspacePoolKeptInputs, want: 3},
		{name: "missing", metric: metrics.WorkspacePoolMissingInputs, want: 1},
		{name: "removed", metric: metrics.WorkspacePoolRemovedInputs, want: 3},
		{name: "new", metric: metrics.WorkspacePoolNewInputs, want: 1},
	}
	for i, h := range histograms {
		histograms[i].countBefore, histograms[i].sumBefore = testmetrics.HistogramCountAndSum(t, h.metric)
	}
	second, err := workspace.New(te, buildRoot, &workspace.Opts{Pool: pool})
	require.NoError(t, err)
	second.SetTask(ctx, &repb.ExecutionTask{Command: &repb.Command{}})
	err = second.DownloadInputs(ctx, &container.FileSystemLayout{DigestFunction: repb.DigestFunction_SHA256, Inputs: secondTree})
	require.NoError(t, err)
	reused := testmetrics.CounterValueForLabels(t, metrics.WorkspacePoolReuseAttempts, prometheus.Labels{metrics.WorkspacePoolReuseStatusLabel: "reused"})
	assert.Equal(t, 1.0, reused)
	for _, h := range histograms {
		count, sum := testmetrics.HistogramCountAndSum(t, h.metric)
		assert.Equal(t, h.countBefore+1, count, h.name)
		assert.Equal(t, h.sumBefore+h.want, sum, h.name)
	}

	// The pool no longer holds any of the group's workspaces, so it deleted
	// the group's series.
	countDeleted := metrics.WorkspacePoolCount.Delete(groupLabel)
	assert.False(t, countDeleted)
	filesDeleted := metrics.WorkspacePoolInputFiles.Delete(groupLabel)
	assert.False(t, filesDeleted)

	// Returning the second workspace brings the group's series back.
	second.ReturnToPool(ctx)
	err = second.Remove(ctx)
	require.NoError(t, err)
	pooledCount = testmetrics.GaugeValueForLabels(t, metrics.WorkspacePoolCount, groupLabel)
	assert.Equal(t, 1.0, pooledCount)
	pooledFiles = testmetrics.GaugeValueForLabels(t, metrics.WorkspacePoolInputFiles, groupLabel)
	assert.Equal(t, 5.0, pooledFiles)
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
	a := uploadFile(t, te, "a.txt", "a")
	b := uploadFile(t, te, "b.txt", "b")
	c := uploadFile(t, te, "c.txt", "c")

	for _, test := range []struct {
		name string
		// The trees share no files, so the second task doesn't reuse the first
		// workspace, and pooling the second workspace evicts one of the two.
		first, second *repb.Tree
		// wantSecondKept is whether the pool keeps the second workspace
		// rather than the first.
		wantSecondKept bool
		// wantReturns is the number of returned workspaces by status.
		wantReturns map[string]float64
		// wantPooledFiles is the number of input files in the workspace that
		// the pool keeps.
		wantPooledFiles float64
	}{
		{
			name:   "same file count",
			first:  &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{a}}},
			second: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{b}}},
			// The pool evicts the older of two workspaces with the same value.
			wantSecondKept:  true,
			wantReturns:     map[string]float64{"pooled": 2, "evicted": 0},
			wantPooledFiles: 1,
		},
		{
			name:   "new workspace holds fewer files",
			first:  &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{a, b}}},
			second: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{c}}},
			// The pool evicts the workspace with the lowest value, even if it's
			// the one being added.
			wantSecondKept:  false,
			wantReturns:     map[string]float64{"pooled": 1, "evicted": 1},
			wantPooledFiles: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			metrics.UnexpectedEvent.Reset()
			metrics.WorkspacePoolReturns.Reset()
			metrics.WorkspacePoolCount.Reset()
			metrics.WorkspacePoolInputFiles.Reset()
			buildRoot := testfs.MakeTempDir(t)
			pool, err := workspace.NewPool(buildRoot)
			require.NoError(t, err)

			first := runPooledTask(t, ctx, te, buildRoot, pool, test.first)
			second := runPooledTask(t, ctx, te, buildRoot, pool, test.second)

			// The pool deletes the evicted workspace in the background.
			poolDir := filepath.Join(buildRoot, "_WORKSPACE_POOL")
			require.Eventually(t, func() bool {
				entries, err := os.ReadDir(poolDir)
				return err == nil && len(entries) == 1
			}, 10*time.Second, 10*time.Millisecond)
			entries, err := os.ReadDir(poolDir)
			require.NoError(t, err)
			remaining, err := os.Lstat(filepath.Join(poolDir, entries[0].Name()))
			require.NoError(t, err)
			secondKept := os.SameFile(second, remaining)
			assert.Equal(t, test.wantSecondKept, secondKept)
			firstKept := os.SameFile(first, remaining)
			assert.Equal(t, !test.wantSecondKept, firstKept)
			notInterned := testmetrics.CounterValueForLabels(t, metrics.UnexpectedEvent, prometheus.Labels{metrics.EventName: "workspace_pool_dir_not_interned"})
			assert.Zero(t, notInterned)

			// The metrics count the evicted workspace, and show the one that
			// the pool kept.
			for status, want := range test.wantReturns {
				returns := testmetrics.CounterValueForLabels(t, metrics.WorkspacePoolReturns, prometheus.Labels{metrics.WorkspacePoolReturnStatusLabel: status})
				assert.Equal(t, want, returns, status)
			}
			groupLabel := prometheus.Labels{metrics.GroupID: filecache.GroupIDStringFromContext(ctx)}
			pooledCount := testmetrics.GaugeValueForLabels(t, metrics.WorkspacePoolCount, groupLabel)
			assert.Equal(t, 1.0, pooledCount)
			pooledFiles := testmetrics.GaugeValueForLabels(t, metrics.WorkspacePoolInputFiles, groupLabel)
			assert.Equal(t, test.wantPooledFiles, pooledFiles)
		})
	}
}
