package workspace_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/fspath"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func newWorkspace(t *testing.T, opts *workspace.Opts) *workspace.Workspace {
	te := testenv.GetTestEnv(t)
	root := testfs.MakeTempDir(t)
	caseInsensitive, err := fspath.IsCaseInsensitiveFS(root)
	require.NoError(t, err)
	opts.CaseInsensitive = caseInsensitive
	ws, err := workspace.New(te, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func writeEmptyFiles(t *testing.T, ws *workspace.Workspace, paths []string) {
	for _, path := range paths {
		fullPath := filepath.Join(ws.Path(), path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0777); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fullPath, []byte{}, 0777); err != nil {
			t.Fatal(err)
		}
	}
}

func keepmePaths(paths []string) map[string]struct{} {
	expected := map[string]struct{}{}
	for _, path := range paths {
		if strings.Contains(path, "KEEPME") {
			expected[filepath.FromSlash(path)] = struct{}{}
		}
	}
	return expected
}

func actualFilePaths(t *testing.T, ws *workspace.Workspace) map[string]struct{} {
	paths := map[string]struct{}{}
	err := filepath.WalkDir(ws.Path(), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if !d.IsDir() {
			relPath := strings.TrimPrefix(path, ws.Path()+string(os.PathSeparator))
			paths[relPath] = struct{}{}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func TestWorkspaceRemove_ReadOnlyTree_DeletesEntireTree(t *testing.T) {
	ctx := context.Background()
	ws := newWorkspace(t, &workspace.Opts{})
	writeEmptyFiles(t, ws, []string{
		"READONLY",
		"dir/READONLY",
	})
	err := os.Chmod(filepath.Join(ws.Path(), "READONLY"), 0400)
	require.NoError(t, err)
	err = os.Chmod(filepath.Join(ws.Path(), "dir/READONLY"), 0400)
	require.NoError(t, err)
	err = os.Chmod(filepath.Join(ws.Path(), "dir"), 0400)
	require.NoError(t, err)

	err = ws.Remove(ctx)
	require.NoError(t, err)
}

func TestWorkspaceCleanup_NoPreserveWorkspace_DeletesAllFiles(t *testing.T) {
	ctx := context.Background()
	filePaths := []string{
		"some_output_directory/DELETEME",
		"some/nested/output/directory/DELETEME",
		"some_output_file_DELETEME",
		"some/nested/output/file/DELETEME",
		"DELETEME",
		"foo/DELETEME",
		"foo/bar/DELETEME",
	}

	ws := newWorkspace(t, &workspace.Opts{Preserve: false})
	ws.SetTask(ctx, &repb.ExecutionTask{
		Command: &repb.Command{
			OutputDirectories: []string{
				"some_output_directory",
				"some/nested/output/directory",
			},
			OutputFiles: []string{
				"some_output_file_DELETEME",
				"some/nested/output/file/DELETEME",
			},
		},
	})
	writeEmptyFiles(t, ws, filePaths)

	err := ws.Clean()

	require.NoError(t, err)
	assert.Empty(t, actualFilePaths(t, ws))
}

func TestWorkspaceCleanup_PreserveWorkspace_CaseInsensitiveFilesystem_Files(t *testing.T) {
	root := testfs.MakeTempDir(t)
	caseInsensitive, err := fspath.IsCaseInsensitiveFS(testfs.MakeTempDir(t))
	require.NoError(t, err)
	if !caseInsensitive {
		t.Skip("test must be run on a case-insensitive filesystem")
	}

	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, lis)
	go runServer()
	ws, err := workspace.New(te, root, &workspace.Opts{
		Preserve:        true,
		CleanInputs:     "**",
		CaseInsensitive: caseInsensitive,
	})
	require.NoError(t, err)
	ws.SetTask(ctx, &repb.ExecutionTask{})

	digestA, err := cachetools.UploadBlob(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, strings.NewReader("A"))
	require.NoError(t, err)
	dirA := &repb.Directory{
		Files: []*repb.FileNode{
			{
				Name:   "A.txt",
				Digest: digestA,
			},
		},
	}
	dirDigestA, err := cachetools.UploadProto(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, dirA)
	require.NoError(t, err)
	err = ws.DownloadInputs(ctx, &container.FileSystemLayout{
		RemoteInstanceName: "",
		DigestFunction:     repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{
			Root: &repb.Directory{
				Files: []*repb.FileNode{
					{
						Name:   "A.txt",
						Digest: digestA,
					},
				},
				Directories: []*repb.DirectoryNode{
					{
						Name:   "CHILD",
						Digest: dirDigestA,
					},
				},
			},
			Children: []*repb.Directory{
				dirA,
			},
		},
	})
	require.NoError(t, err)

	// Clean
	err = ws.Clean()
	require.NoError(t, err)

	// Check inputs still present.
	testfs.AssertExactFileContents(t, ws.Path(), map[string]string{
		"A.txt":       "A",
		"CHILD/A.txt": "A",
	})

	// Now run another task in this workspace, with the same input paths, except
	// the case of the filename is different, and the file has different
	// contents.
	digestB, err := cachetools.UploadBlob(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, strings.NewReader("B"))
	require.NoError(t, err)
	dirB := &repb.Directory{
		Files: []*repb.FileNode{
			{
				Name:   "a.txt",
				Digest: digestB,
			},
		},
	}
	dirDigestB, err := cachetools.UploadProto(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, dirB)
	require.NoError(t, err)
	err = ws.DownloadInputs(ctx, &container.FileSystemLayout{
		RemoteInstanceName: "",
		DigestFunction:     repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{
			Root: &repb.Directory{
				Files: []*repb.FileNode{
					{
						Name:   "a.txt",
						Digest: digestB,
					},
				},
				Directories: []*repb.DirectoryNode{
					{
						Name:   "child",
						Digest: dirDigestB,
					},
				},
			},
			Children: []*repb.Directory{
				dirB,
			},
		},
	})
	require.NoError(t, err)

	// Check inputs still present.
	testfs.AssertExactFileContents(t, ws.Path(), map[string]string{
		// A.txt should be deleted and overwritten with the new file name and
		// contents, since the filesystem is case-insensitive and the file
		// contents are different.
		"a.txt": "B",
		// CHILD/A.txt should be deleted and overwritten as well, since the
		// new file is "child/a.txt".
		// TODO: for directories, we are case-insensitive but not
		// case-preserving. Ideally, "CHILD" should be renamed to "child".
		"CHILD/a.txt": "B",
	})
}

func TestWorkspaceCleanup_PreserveWorkspace_PreservesAllFilesExceptOutputs(t *testing.T) {
	ctx := context.Background()
	filePaths := []string{
		"some_output_directory/DELETEME",
		"some/nested/output/directory/DELETEME",
		"some_output_file_DELETEME",
		"some/nested/output/file/DELETEME",
		"KEEPME",
		"foo/KEEPME",
		"foo/bar/KEEPME",
	}
	ws := newWorkspace(t, &workspace.Opts{Preserve: true})
	ws.SetTask(ctx, &repb.ExecutionTask{
		Command: &repb.Command{
			OutputDirectories: []string{
				"some_output_directory",
				"some/nested/output/directory",
			},
			OutputFiles: []string{
				"some_output_file_DELETEME",
				"some/nested/output/file/DELETEME",
			},
		},
	})
	writeEmptyFiles(t, ws, filePaths)

	err := ws.Clean()

	require.NoError(t, err)
	assert.Equal(
		t, keepmePaths(filePaths), actualFilePaths(t, ws),
		"expected all KEEPME filePaths (and no others) in the workspace after cleanup",
	)
}

func TestCleanInputsIfNecessary_CleanNone(t *testing.T) {
	filePaths := []string{
		"some_input_directory/foo.framework/KEEPME",
		"some/nested/input/directory/KEEPME.h",
		"some_input_file_KEEPME",
		"some/nested/input/file/KEEPME",
		"KEEPME",
		"foo/KEEPME",
		"foo/bar/KEEPME",
	}
	ws := newWorkspace(t, &workspace.Opts{
		Preserve: true,
	})
	writeEmptyFiles(t, ws, filePaths)

	for _, file := range filePaths {
		ws.Inputs[fspath.NewKey(file, ws.Opts.CaseInsensitive)] = &repb.FileNode{}
	}

	keep := map[fspath.Key]*repb.FileNode{
		fspath.NewKey("KEEPME", ws.Opts.CaseInsensitive):         &repb.FileNode{},
		fspath.NewKey("foo/KEEPME", ws.Opts.CaseInsensitive):     &repb.FileNode{},
		fspath.NewKey("foo/bar/KEEPME", ws.Opts.CaseInsensitive): &repb.FileNode{}}

	err := ws.CleanInputsIfNecessary(keep)
	require.NoError(t, err)

	assert.Equal(
		t, keepmePaths(filePaths), actualFilePaths(t, ws),
		"expected all KEEPME filePaths (and no others) in the workspace after cleanup",
	)
}

func TestCleanInputsIfNecessary_CleanAll(t *testing.T) {
	filePaths := []string{
		"some_input_directory/foo.framework/DELETEME",
		"some/nested/input/directory/DELETEME.h",
		"some_input_file_DELETEME",
		"some/nested/input/file/DELETEME",
		"KEEPME",
		"foo/KEEPME",
		"foo/bar/KEEPME",
	}
	ws := newWorkspace(t, &workspace.Opts{Preserve: true, CleanInputs: "*"})
	writeEmptyFiles(t, ws, filePaths)

	for _, file := range filePaths {
		ws.Inputs[fspath.NewKey(file, ws.Opts.CaseInsensitive)] = &repb.FileNode{}
	}

	keep := map[fspath.Key]*repb.FileNode{
		fspath.NewKey("KEEPME", ws.Opts.CaseInsensitive):         &repb.FileNode{},
		fspath.NewKey("foo/KEEPME", ws.Opts.CaseInsensitive):     &repb.FileNode{},
		fspath.NewKey("foo/bar/KEEPME", ws.Opts.CaseInsensitive): &repb.FileNode{}}

	ws.CleanInputsIfNecessary(keep)

	assert.Equal(
		t, keepmePaths(filePaths), actualFilePaths(t, ws),
		"expected all KEEPME filePaths (and no others) in the workspace after cleanup",
	)
}

func TestCleanInputsIfNecessary_CleanMatching(t *testing.T) {
	filePaths := []string{
		"some_input_directory/foo.framework/DELETEME",
		"some/nested/input/directory/DELETEME.h",
		"some_input_file_KEEPME",
		"some/nested/input/file/KEEPME",
		"KEEPME",
		"foo/KEEPME",
		"foo/bar/KEEPME",
	}
	ws := newWorkspace(t, &workspace.Opts{Preserve: true, CleanInputs: "**.h,**.framework/**"})
	writeEmptyFiles(t, ws, filePaths)

	for _, file := range filePaths {
		ws.Inputs[fspath.NewKey(file, ws.Opts.CaseInsensitive)] = &repb.FileNode{}
	}

	keep := map[fspath.Key]*repb.FileNode{
		fspath.NewKey("KEEPME", ws.Opts.CaseInsensitive):         &repb.FileNode{},
		fspath.NewKey("foo/KEEPME", ws.Opts.CaseInsensitive):     &repb.FileNode{},
		fspath.NewKey("foo/bar/KEEPME", ws.Opts.CaseInsensitive): &repb.FileNode{}}

	err := ws.CleanInputsIfNecessary(keep)
	require.NoError(t, err)

	assert.Equal(
		t, keepmePaths(filePaths), actualFilePaths(t, ws),
		"expected all KEEPME filePaths (and no others) in the workspace after cleanup",
	)
}

func TestManyNewWorkspaces(t *testing.T) {
	te := testenv.GetTestEnv(t)
	root := testfs.MakeTempDir(t)
	// https://github.com/bazelbuild/bazel/blob/819aa9688229e244dc90dda1278d7444d910b48a/src/main/java/com/google/devtools/build/lib/rules/cpp/ShowIncludesFilter.java#L101
	expectedPath := regexp.MustCompile(`.*execroot\\(?P<headerPath>.*)`)
	allPaths := make(map[string]struct{})
	for i := 0; i < 1000; i++ {
		ws, err := workspace.New(te, root, &workspace.Opts{})
		require.NoError(t, err)
		if runtime.GOOS == "windows" {
			matches := expectedPath.FindStringSubmatch(ws.Path())
			assert.NotNil(t, matches)
			idx := expectedPath.SubexpIndex("headerPath")
			assert.Equal(t, "_main", matches[idx])
		}
		allPaths[ws.Path()] = struct{}{}
	}
	// Check that all paths are unique.
	assert.Len(t, allPaths, 1000)
}

func TestPreserveWorkspace_DoesNotPreserveOutputPaths(t *testing.T) {
	ctx := context.Background()
	ws := newWorkspace(t, &workspace.Opts{Preserve: true})
	ws.SetTask(ctx, &repb.ExecutionTask{
		Command: &repb.Command{
			OutputPaths: []string{"foo.out"},
		},
	})
	testfs.WriteAllFileContents(t, ws.Path(), map[string]string{
		"foo.out": "foo",
	})

	err := ws.Clean()
	require.NoError(t, err)
	assert.Empty(t, actualFilePaths(t, ws))
}

func TestPreserveWorkspace_WorkingDirectory_OutputPathsRelativeToWorkDir(t *testing.T) {
	ctx := context.Background()
	ws := newWorkspace(t, &workspace.Opts{Preserve: true})
	ws.SetTask(ctx, &repb.ExecutionTask{
		Command: &repb.Command{
			WorkingDirectory: "subdir",
			OutputPaths:      []string{"foo.out"},
		},
	})
	testfs.WriteAllFileContents(t, ws.Path(), map[string]string{
		"subdir/foo.out":  "output",
		"subdir/input.in": "input",
		"other.txt":       "other",
	})

	err := ws.Clean()
	require.NoError(t, err)

	remaining := actualFilePaths(t, ws)
	assert.Contains(t, remaining, filepath.Join("subdir", "input.in"))
	assert.Contains(t, remaining, "other.txt")
	assert.NotContains(t, remaining, filepath.Join("subdir", "foo.out"))
}

func TestPreserveWorkspace_WorkingDirectory_CleansInputIndex(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, lis)
	go runServer()
	root := testfs.MakeTempDir(t)
	ws, err := workspace.New(te, root, &workspace.Opts{Preserve: true})
	require.NoError(t, err)

	// Upload file contents to CAS.
	inputDigest, err := cachetools.UploadBlob(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, strings.NewReader("input"))
	require.NoError(t, err)
	outputDigest, err := cachetools.UploadBlob(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, strings.NewReader("output"))
	require.NoError(t, err)

	// Build an input tree: subdir/ contains both input.txt and out.txt.
	subdir := &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "input.txt", Digest: inputDigest},
			{Name: "out.txt", Digest: outputDigest},
		},
	}
	subdirDigest, err := cachetools.UploadProto(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, subdir)
	require.NoError(t, err)

	ws.SetTask(ctx, &repb.ExecutionTask{
		Command: &repb.Command{
			WorkingDirectory: "subdir",
			OutputPaths:      []string{"out.txt"},
		},
	})
	err = ws.DownloadInputs(ctx, &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{
			Root: &repb.Directory{
				Directories: []*repb.DirectoryNode{
					{Name: "subdir", Digest: subdirDigest},
				},
			},
			Children: []*repb.Directory{subdir},
		},
	})
	require.NoError(t, err)

	// Both files should be tracked in the inputs index.
	assert.Contains(t, ws.Inputs, fspath.NewKey("subdir/input.txt", false))
	assert.Contains(t, ws.Inputs, fspath.NewKey("subdir/out.txt", false))

	err = ws.Clean()
	require.NoError(t, err)

	// After cleanup, the output file should be removed from both the
	// filesystem and the inputs index, while the input file remains.
	remaining := actualFilePaths(t, ws)
	assert.Contains(t, remaining, filepath.Join("subdir", "input.txt"))
	assert.NotContains(t, remaining, filepath.Join("subdir", "out.txt"))

	assert.Contains(t, ws.Inputs, fspath.NewKey("subdir/input.txt", false),
		"non-output input should remain in inputs index")
	assert.NotContains(t, ws.Inputs, fspath.NewKey("subdir/out.txt", false),
		"output file should be removed from inputs index")
}

func TestDownloadInputs_WorkingDirectoryMissing(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, lis)
	go runServer()
	root := testfs.MakeTempDir(t)
	ws, err := workspace.New(te, root, &workspace.Opts{})
	require.NoError(t, err)
	ws.SetTask(ctx, &repb.ExecutionTask{
		Command: &repb.Command{
			WorkingDirectory: "nonexistent",
		},
	})

	err = ws.DownloadInputs(ctx, &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{
			Root: &repb.Directory{},
		},
	})

	require.Error(t, err)
	assert.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition, got: %v", err)
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestDownloadInputs_WorkingDirectoryExists(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, lis)
	go runServer()
	root := testfs.MakeTempDir(t)
	ws, err := workspace.New(te, root, &workspace.Opts{})
	require.NoError(t, err)

	// Build an input tree with "subdir" as a directory.
	subdir := &repb.Directory{}
	subdirDigest, err := cachetools.UploadProto(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, subdir)
	require.NoError(t, err)
	ws.SetTask(ctx, &repb.ExecutionTask{
		Command: &repb.Command{
			WorkingDirectory: "subdir",
		},
	})

	err = ws.DownloadInputs(ctx, &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{
			Root: &repb.Directory{
				Directories: []*repb.DirectoryNode{
					{Name: "subdir", Digest: subdirDigest},
				},
			},
			Children: []*repb.Directory{subdir},
		},
	})

	require.NoError(t, err)
}

func TestDownloadInputs_WorkingDirectoryNestedMissing(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, lis)
	go runServer()
	root := testfs.MakeTempDir(t)
	ws, err := workspace.New(te, root, &workspace.Opts{})
	require.NoError(t, err)

	// Build an input tree with "a" but not "a/b".
	dirA := &repb.Directory{}
	dirADigest, err := cachetools.UploadProto(ctx, te.GetByteStreamClient(), "", repb.DigestFunction_SHA256, dirA)
	require.NoError(t, err)
	ws.SetTask(ctx, &repb.ExecutionTask{
		Command: &repb.Command{
			WorkingDirectory: "a/b",
		},
	})

	err = ws.DownloadInputs(ctx, &container.FileSystemLayout{
		DigestFunction: repb.DigestFunction_SHA256,
		Inputs: &repb.Tree{
			Root: &repb.Directory{
				Directories: []*repb.DirectoryNode{
					{Name: "a", Digest: dirADigest},
				},
			},
			Children: []*repb.Directory{dirA},
		},
	})

	require.Error(t, err)
	assert.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition, got: %v", err)
	assert.Contains(t, err.Error(), "a/b")
}
