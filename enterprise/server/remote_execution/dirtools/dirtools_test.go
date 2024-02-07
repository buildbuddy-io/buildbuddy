package dirtools_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func TestUploadTree(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cmd            *repb.Command
		directoryPaths []string
		fileContents   map[string]string
		symlinkPaths   map[string]string

		expectedResult *repb.ActionResult
		expectedInfo   *dirtools.TransferInfo
	}{
		{
			name:           "NoFiles",
			cmd:            &repb.Command{},
			directoryPaths: []string{},
			fileContents:   map[string]string{},
			symlinkPaths:   map[string]string{},
			expectedResult: &repb.ActionResult{},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        0,
				BytesTransferred: 0,
			},
		},
		{
			name: "SomeFile",
			cmd: &repb.Command{
				OutputFiles: []string{"fileA.txt"},
			},
			directoryPaths: []string{},
			fileContents: map[string]string{
				"fileA.txt": "a",
			},
			symlinkPaths: map[string]string{},
			expectedResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					{
						Path: "fileA.txt",
						Digest: &repb.Digest{
							SizeBytes: 1,
							Hash:      hash.String("a"),
						},
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        1,
				BytesTransferred: 1,
			},
		},
		{
			name: "OutputDirectory",
			cmd: &repb.Command{
				OutputDirectories: []string{"a"},
			},
			directoryPaths: []string{
				"a",
			},
			fileContents: map[string]string{
				"a/fileA.txt": "a",
			},
			symlinkPaths: map[string]string{},
			expectedResult: &repb.ActionResult{
				OutputDirectories: []*repb.OutputDirectory{
					{
						Path: "a",
						TreeDigest: &repb.Digest{
							SizeBytes: 85,
							Hash:      "895545df6841b7efb2e9cc903a4eac7a60c645199be059f6056817ae6feb071d",
						},
					},
				},
				OutputFiles: []*repb.OutputFile{
					{
						Path: "a/fileA.txt",
						Digest: &repb.Digest{
							SizeBytes: 1,
							Hash:      hash.String("a"),
						},
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        2,
				BytesTransferred: 84,
			},
		},
		{
			name: "SymlinkToFile",
			cmd: &repb.Command{
				OutputFiles: []string{
					"fileA.txt",
					"linkA.txt",
				},
			},
			directoryPaths: []string{},
			fileContents: map[string]string{
				"fileA.txt": "a",
			},
			symlinkPaths: map[string]string{
				"linkA.txt": "fileA.txt",
			},
			expectedResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					{
						Path: "fileA.txt",
						Digest: &repb.Digest{
							SizeBytes: 1,
							Hash:      "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
						},
					},
				},
				OutputFileSymlinks: []*repb.OutputSymlink{
					{
						Path:   "linkA.txt",
						Target: "fileA.txt",
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        1,
				BytesTransferred: 1,
			},
		},
		{
			name: "SymlinkToFileInOutputPaths",
			cmd: &repb.Command{
				OutputPaths: []string{
					"fileA.txt",
					"linkA.txt",
				},
			},
			directoryPaths: []string{},
			fileContents: map[string]string{
				"fileA.txt": "a",
			},
			symlinkPaths: map[string]string{
				"linkA.txt": "fileA.txt",
			},
			expectedResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					{
						Path: "fileA.txt",
						Digest: &repb.Digest{
							SizeBytes: 1,
							Hash:      "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
						},
					},
				},
				OutputSymlinks: []*repb.OutputSymlink{
					{
						Path:   "linkA.txt",
						Target: "fileA.txt",
					},
				},
				OutputFileSymlinks: []*repb.OutputSymlink{
					{
						Path:   "linkA.txt",
						Target: "fileA.txt",
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        1,
				BytesTransferred: 1,
			},
		},
		{
			name: "SymlinkToFileInBothOutputPathsAndOutputFiles",
			cmd: &repb.Command{
				OutputFiles: []string{
					"fileA.txt",
					"linkA.txt",
				},
				OutputPaths: []string{
					"fileA.txt",
					"linkA.txt",
				},
			},
			directoryPaths: []string{},
			fileContents: map[string]string{
				"fileA.txt": "a",
			},
			symlinkPaths: map[string]string{
				"linkA.txt": "fileA.txt",
			},
			expectedResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					{
						Path: "fileA.txt",
						Digest: &repb.Digest{
							SizeBytes: 1,
							Hash:      "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
						},
					},
				},
				OutputFileSymlinks: []*repb.OutputSymlink{
					{
						Path:   "linkA.txt",
						Target: "fileA.txt",
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        1,
				BytesTransferred: 1,
			},
		},
		{
			name: "SymlinkToDirectory",
			cmd: &repb.Command{
				OutputDirectories: []string{
					"a",
					"linkA",
				},
			},
			directoryPaths: []string{
				"a",
			},
			fileContents: map[string]string{
				"a/fileA.txt": "a",
			},
			symlinkPaths: map[string]string{
				"linkA": "a",
			},
			expectedResult: &repb.ActionResult{
				OutputDirectories: []*repb.OutputDirectory{
					{
						Path: "a",
						TreeDigest: &repb.Digest{
							SizeBytes: 85,
							Hash:      "895545df6841b7efb2e9cc903a4eac7a60c645199be059f6056817ae6feb071d",
						},
					},
				},
				OutputFiles: []*repb.OutputFile{
					{
						Path: "a/fileA.txt",
						Digest: &repb.Digest{
							SizeBytes: 1,
							Hash:      hash.String("a"),
						},
					},
				},
				OutputDirectorySymlinks: []*repb.OutputSymlink{
					{
						Path:   "linkA",
						Target: "a",
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        2,
				BytesTransferred: 84,
			},
		},
		{
			name: "SymlinkToDirectoryInOutputPaths",
			cmd: &repb.Command{
				OutputPaths: []string{
					"a",
					"linkA",
				},
			},
			directoryPaths: []string{
				"a",
			},
			fileContents: map[string]string{
				"a/fileA.txt": "a",
			},
			symlinkPaths: map[string]string{
				"linkA": "a",
			},
			expectedResult: &repb.ActionResult{
				OutputDirectories: []*repb.OutputDirectory{
					{
						Path: "a",
						TreeDigest: &repb.Digest{
							SizeBytes: 85,
							Hash:      "895545df6841b7efb2e9cc903a4eac7a60c645199be059f6056817ae6feb071d",
						},
					},
				},
				OutputFiles: []*repb.OutputFile{
					{
						Path: "a/fileA.txt",
						Digest: &repb.Digest{
							SizeBytes: 1,
							Hash:      hash.String("a"),
						},
					},
				},
				OutputSymlinks: []*repb.OutputSymlink{
					{
						Path:   "linkA",
						Target: "a",
					},
				},
				OutputDirectorySymlinks: []*repb.OutputSymlink{
					{
						Path:   "linkA",
						Target: "a",
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        2,
				BytesTransferred: 84,
			},
		},
		{
			name: "SymlinkToDirectoryInBothOutputPathsAndOutputDirectories",
			cmd: &repb.Command{
				OutputDirectories: []string{
					"a",
					"linkA",
				},
				OutputPaths: []string{
					"a",
					"linkA",
				},
			},
			directoryPaths: []string{
				"a",
			},
			fileContents: map[string]string{
				"a/fileA.txt": "a",
			},
			symlinkPaths: map[string]string{
				"linkA": "a",
			},
			expectedResult: &repb.ActionResult{
				OutputDirectories: []*repb.OutputDirectory{
					{
						Path: "a",
						TreeDigest: &repb.Digest{
							SizeBytes: 85,
							Hash:      "895545df6841b7efb2e9cc903a4eac7a60c645199be059f6056817ae6feb071d",
						},
					},
				},
				OutputFiles: []*repb.OutputFile{
					{
						Path: "a/fileA.txt",
						Digest: &repb.Digest{
							SizeBytes: 1,
							Hash:      hash.String("a"),
						},
					},
				},
				OutputDirectorySymlinks: []*repb.OutputSymlink{
					{
						Path:   "linkA",
						Target: "a",
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        2,
				BytesTransferred: 84,
			},
		},
		{
			name: "DanglingFileSymlink",
			cmd: &repb.Command{
				OutputFiles: []string{"a"},
			},
			symlinkPaths: map[string]string{
				"a": "b",
			},
			expectedResult: &repb.ActionResult{
				OutputFileSymlinks: []*repb.OutputSymlink{
					{
						Path:   "a",
						Target: "b",
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        0,
				BytesTransferred: 0,
			},
		},
		{
			name: "DanglingDirectorySymlink",
			cmd: &repb.Command{
				OutputDirectories: []string{"a"},
			},
			symlinkPaths: map[string]string{
				"a": "b",
			},
			expectedResult: &repb.ActionResult{
				OutputDirectorySymlinks: []*repb.OutputSymlink{
					{
						Path:   "a",
						Target: "b",
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        0,
				BytesTransferred: 0,
			},
		},
		{
			name: "SomeNestedFile",
			cmd: &repb.Command{
				OutputFiles: []string{"foo/bar/baz/fileA.txt"},
			},
			directoryPaths: []string{},
			fileContents: map[string]string{
				"foo/bar/baz/fileA.txt": "a",
			},
			symlinkPaths: map[string]string{},
			expectedResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					{
						Path: "foo/bar/baz/fileA.txt",
						Digest: &repb.Digest{
							SizeBytes: 1,
							Hash:      hash.String("a"),
						},
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        1,
				BytesTransferred: 1,
			},
		},
		{
			name: "LotsOfNesting",
			cmd: &repb.Command{
				OutputDirectories: []string{"a/b"},
			},
			directoryPaths: []string{
				"a/f",
				"a/b/c",
				"a/b/c/d",
				"a/b/e/g",
			},
			fileContents: map[string]string{
				"a/b/c/fileA.txt": "a",
			},
			symlinkPaths: map[string]string{},
			expectedResult: &repb.ActionResult{
				OutputDirectories: []*repb.OutputDirectory{
					{
						Path: "a/b/c",
						TreeDigest: &repb.Digest{
							SizeBytes: 159,
							Hash:      "d69a774656d42ed99962d1267db7be99d3d70875475d42122077ba60ef3ea8e2",
						},
					},
					{
						Path: "a/b/c/d",
						TreeDigest: &repb.Digest{
							SizeBytes: 2,
							Hash:      "102b51b9765a56a3e899f7cf0ee38e5251f9c503b357b330a49183eb7b155604",
						},
					},
					{
						Path: "a/b/e/g",
						TreeDigest: &repb.Digest{
							SizeBytes: 2,
							Hash:      "102b51b9765a56a3e899f7cf0ee38e5251f9c503b357b330a49183eb7b155604",
						},
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        5,
				BytesTransferred: 157,
			},
		},
		{
			name: "NestedOutputDirectory",
			cmd: &repb.Command{
				OutputDirectories: []string{"a/b/c"},
			},
			directoryPaths: []string{
				"a/b/c",
			},
			fileContents: map[string]string{
				"a/b/c/fileA.txt": "a",
			},
			symlinkPaths: map[string]string{},
			expectedResult: &repb.ActionResult{
				OutputDirectories: []*repb.OutputDirectory{
					{
						Path: "a/b/c",
						TreeDigest: &repb.Digest{
							SizeBytes: 85,
							Hash:      "895545df6841b7efb2e9cc903a4eac7a60c645199be059f6056817ae6feb071d",
						},
					},
				},
				OutputFiles: []*repb.OutputFile{
					{
						Path: "a/b/c/fileA.txt",
						Digest: &repb.Digest{
							SizeBytes: 1,
							Hash:      hash.String("a"),
						},
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        2,
				BytesTransferred: 84,
			},
		},
		{
			name: "DanglingSymlinkInOutputPaths",
			cmd: &repb.Command{
				OutputPaths: []string{"a"},
			},
			symlinkPaths: map[string]string{
				"a": "b",
			},
			expectedResult: &repb.ActionResult{
				OutputSymlinks: []*repb.OutputSymlink{
					{
						Path:   "a",
						Target: "b",
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        0,
				BytesTransferred: 0,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, ctx := testEnv(t)
			rootDir := testfs.MakeTempDir(t)

			// Prepare inputs
			testfs.WriteAllFileContents(t, rootDir, tc.fileContents)
			for name, target := range tc.symlinkPaths {
				err := os.Symlink(target, filepath.Join(rootDir, name))
				require.NoError(t, err)
			}
			for _, path := range tc.directoryPaths {
				err := os.MkdirAll(filepath.Join(rootDir, path), fs.FileMode(0o755))
				require.NoError(t, err)
			}

			var outputPaths []string
			outputPaths = append(outputPaths, tc.directoryPaths...)
			for path := range tc.fileContents {
				outputPaths = append(outputPaths, path)
			}
			for path := range tc.symlinkPaths {
				outputPaths = append(outputPaths, path)
			}
			dirHelper := dirtools.NewDirHelper(rootDir, []string{} /*outputDirs*/, outputPaths, fs.FileMode(0o755))

			actionResult := &repb.ActionResult{}
			txInfo, err := dirtools.UploadTree(ctx, env, dirHelper, "", repb.DigestFunction_SHA256, rootDir, tc.cmd, actionResult)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedInfo.FileCount, txInfo.FileCount)
			assert.Equal(t, tc.expectedInfo.BytesTransferred, txInfo.BytesTransferred)

			for _, file := range tc.expectedResult.OutputFiles {
				has, err := env.GetCache().Contains(ctx, &rspb.ResourceName{
					InstanceName: "",
					CacheType:    rspb.CacheType_CAS,
					Digest:       file.Digest,
				})
				assert.NoError(t, err)
				assert.True(t, has)
			}
			for _, expectedDir := range tc.expectedResult.OutputDirectories {
				found := false
				for _, dir := range actionResult.OutputDirectories {
					if dir.Path == expectedDir.Path {
						assert.Equal(t, expectedDir.TreeDigest.SizeBytes, dir.TreeDigest.SizeBytes)
						assert.Equal(t, expectedDir.TreeDigest.Hash, dir.TreeDigest.Hash)
						found = true
						break
					}
				}
				assert.True(t, found)
			}
			for _, expectedSymlink := range tc.expectedResult.OutputSymlinks {
				found := false
				for _, symlink := range actionResult.OutputSymlinks {
					if symlink.Path == expectedSymlink.Path {
						assert.Equal(t, expectedSymlink.Target, symlink.Target)
						found = true
						break
					}
				}
				assert.True(t, found)
			}
			for _, expectedSymlink := range tc.expectedResult.OutputFileSymlinks {
				found := false
				for _, symlink := range actionResult.OutputFileSymlinks {
					if symlink.Path == expectedSymlink.Path {
						assert.Equal(t, expectedSymlink.Target, symlink.Target)
						found = true
						break
					}
				}
				assert.True(t, found)
			}
			for _, expectedSymlink := range tc.expectedResult.OutputDirectorySymlinks {
				found := false
				for _, symlink := range actionResult.OutputDirectorySymlinks {
					if symlink.Path == expectedSymlink.Path {
						assert.Equal(t, expectedSymlink.Target, symlink.Target)
						found = true
						break
					}
				}
				assert.True(t, found)
			}
		})
	}
}

func TestDownloadTree(t *testing.T) {
	env, ctx := testEnv(t)
	tmpDir := testfs.MakeTempDir(t)
	instanceName := "foo"
	fileADigest := setFile(t, env, ctx, instanceName, "mytestdataA")
	fileBDigest := setFile(t, env, ctx, instanceName, "mytestdataB")

	childDir := &repb.Directory{
		Files: []*repb.FileNode{
			&repb.FileNode{
				Name:   "fileA.txt",
				Digest: fileADigest,
			},
		},
		Symlinks: []*repb.SymlinkNode{
			&repb.SymlinkNode{
				Name:   "fileA.symlink",
				Target: "./fileA.txt",
			},
		},
	}

	childDigest, err := digest.ComputeForMessage(childDir, repb.DigestFunction_SHA256)
	if err != nil {
		t.Fatal(err)
	}

	directory := &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				&repb.FileNode{
					Name:   "fileB.txt",
					Digest: fileBDigest,
				},
			},
			Directories: []*repb.DirectoryNode{
				&repb.DirectoryNode{
					Name:   "my-directory",
					Digest: childDigest,
				},
			},
		},
		Children: []*repb.Directory{
			childDir,
		},
	}
	info, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, tmpDir, &dirtools.DownloadTreeOpts{})
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, info, "transfers are not nil")
	assert.Equal(t, int64(2), info.FileCount, "two files were transferred")
	assert.DirExists(t, filepath.Join(tmpDir, "my-directory"), "my-directory should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "my-directory/fileA.txt"), "fileA.txt should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "fileB.txt"), "fileB.txt should exist")
	target, err := os.Readlink(filepath.Join(tmpDir, "my-directory/fileA.symlink"))
	assert.NoError(t, err, "should be able to read symlink target")
	assert.Equal(t, "./fileA.txt", target)
	targetContents, err := os.ReadFile(filepath.Join(tmpDir, "my-directory/fileA.symlink"))
	assert.NoError(t, err)
	assert.Equal(t, "mytestdataA", string(targetContents), "symlinked file contents should match target file")
}

func TestDownloadTreeWithFileCache(t *testing.T) {
	env, ctx := testEnv(t)
	tmpDir := testfs.MakeTempDir(t)
	instanceName := "foo"
	fileAContents := "mytestdataA"
	fileBContents := "mytestdataB-withDifferentLength"
	fileADigest := setFile(t, env, ctx, instanceName, fileAContents)
	fileBDigest := setFile(t, env, ctx, instanceName, fileBContents)
	tmp := testfs.MakeTempDir(t)
	addToFileCache(t, ctx, env, tmp, fileAContents)

	childDir := &repb.Directory{
		Files: []*repb.FileNode{
			&repb.FileNode{
				Name:   "fileA.txt",
				Digest: fileADigest,
			},
		},
	}

	childDigest, err := digest.ComputeForMessage(childDir, repb.DigestFunction_SHA256)
	if err != nil {
		t.Fatal(err)
	}

	directory := &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				&repb.FileNode{
					Name:   "fileB.txt",
					Digest: fileBDigest,
				},
			},
			Directories: []*repb.DirectoryNode{
				&repb.DirectoryNode{
					Name:   "my-directory",
					Digest: childDigest,
				},
			},
		},
		Children: []*repb.Directory{
			childDir,
		},
	}
	info, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, tmpDir, &dirtools.DownloadTreeOpts{})
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, info, "transfers are not nil")
	assert.Equal(t, int64(1), info.FileCount, "one file should be transferred, one linked from filecache")
	assert.Equal(t, int64(len(fileBContents)), info.BytesTransferred, "only file B should be downloaded; file A should be linked from filecache")
	assert.DirExists(t, filepath.Join(tmpDir, "my-directory"), "my-directory should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "my-directory/fileA.txt"), "fileA.txt should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "fileB.txt"), "fileB.txt should exist")
}

func TestDownloadTreeEmptyDigest(t *testing.T) {
	env, ctx := testEnv(t)
	tmpDir := testfs.MakeTempDir(t)
	instanceName := "foo"

	fileContents := "mytestdata"
	fileDigest := setFile(t, env, ctx, instanceName, fileContents)
	emptyDigest := &repb.Digest{
		Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		SizeBytes: 0,
	}

	childDir := &repb.Directory{
		Files: []*repb.FileNode{
			&repb.FileNode{
				Name:   "fileA.txt",
				Digest: fileDigest,
			},
		},
	}

	childDigest, err := digest.ComputeForMessage(childDir, repb.DigestFunction_SHA256)
	if err != nil {
		t.Fatal(err)
	}

	directory := &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				&repb.FileNode{
					Name:   "file_empty.txt",
					Digest: emptyDigest,
				},
				&repb.FileNode{
					Name:   "file_notempty.txt",
					Digest: fileDigest,
				},
			},
			Directories: []*repb.DirectoryNode{
				&repb.DirectoryNode{
					Name:   "my-empty-directory",
					Digest: emptyDigest,
				},
				&repb.DirectoryNode{
					Name:   "my-notempty-directory",
					Digest: childDigest,
				},
			},
		},
		Children: []*repb.Directory{
			childDir,
		},
	}
	info, err := dirtools.DownloadTree(ctx, env, "foo", repb.DigestFunction_SHA256, directory, tmpDir, &dirtools.DownloadTreeOpts{})
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, info, "transfers are not nil")
	assert.Equal(t, int64(1), info.FileCount, "only one unique file should be transferred")
	assert.Equal(t, int64(len(fileContents)), info.BytesTransferred)
	assert.DirExists(t, filepath.Join(tmpDir, "my-empty-directory"), "my-empty-directory should exist")
	assert.DirExists(t, filepath.Join(tmpDir, "my-notempty-directory"), "my-notempty-directory should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "my-notempty-directory/fileA.txt"), "fileA.txt should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "file_empty.txt"), "file_empty.txt should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "file_notempty.txt"), "file_notempty.txt should exist")
}

func TestDownloadTreeExistingCorrectSymlink(t *testing.T) {
	env, ctx := testEnv(t)
	tmpDir := testfs.MakeTempDir(t)
	instanceName := "foo"
	fileADigest := setFile(t, env, ctx, instanceName, "mytestdataA")
	fileBDigest := setFile(t, env, ctx, instanceName, "mytestdataB")

	directory := &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				&repb.FileNode{
					Name:   "fileA.txt",
					Digest: fileADigest,
				},
			},
			Symlinks: []*repb.SymlinkNode{
				&repb.SymlinkNode{
					Name:   "fileA.symlink",
					Target: "./fileA.txt",
				},
			},
		},
	}

	_, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, tmpDir, &dirtools.DownloadTreeOpts{})
	if err != nil {
		t.Fatal(err)
	}

	target, err := os.Readlink(filepath.Join(tmpDir, "fileA.symlink"))
	assert.NoError(t, err, "should be able to read symlink target")
	assert.Equal(t, "./fileA.txt", target)
	targetContents, err := os.ReadFile(filepath.Join(tmpDir, "fileA.symlink"))
	assert.NoError(t, err)
	assert.Equal(t, "mytestdataA", string(targetContents), "symlinked file contents should match target file")

	directory = &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				&repb.FileNode{
					Name:   "fileB.txt",
					Digest: fileBDigest,
				},
			},
			Symlinks: []*repb.SymlinkNode{
				&repb.SymlinkNode{
					Name:   "fileA.symlink",
					Target: "./fileA.txt",
				},
			},
		},
	}

	_, err = dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, tmpDir, &dirtools.DownloadTreeOpts{})
	if err != nil {
		t.Fatal(err)
	}
	target, err = os.Readlink(filepath.Join(tmpDir, "fileA.symlink"))
	assert.NoError(t, err, "should be able to read symlink target")
	assert.Equal(t, "./fileA.txt", target)
	targetContents, err = os.ReadFile(filepath.Join(tmpDir, "fileA.symlink"))
	assert.NoError(t, err)
	assert.Equal(t, "mytestdataA", string(targetContents), "symlinked file contents should match target file")
}

func TestDownloadTreeExistingIncorrectSymlink(t *testing.T) {
	env, ctx := testEnv(t)
	tmpDir := testfs.MakeTempDir(t)
	instanceName := "foo"
	fileADigest := setFile(t, env, ctx, instanceName, "mytestdataA")
	fileBDigest := setFile(t, env, ctx, instanceName, "mytestdataB")

	directory := &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				&repb.FileNode{
					Name:   "fileA.txt",
					Digest: fileADigest,
				},
			},
			Symlinks: []*repb.SymlinkNode{
				&repb.SymlinkNode{
					Name:   "fileA.symlink",
					Target: "./fileA.txt",
				},
			},
		},
	}

	_, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, tmpDir, &dirtools.DownloadTreeOpts{})
	if err != nil {
		t.Fatal(err)
	}

	target, err := os.Readlink(filepath.Join(tmpDir, "fileA.symlink"))
	assert.NoError(t, err, "should be able to read symlink target")
	assert.Equal(t, "./fileA.txt", target)
	targetContents, err := os.ReadFile(filepath.Join(tmpDir, "fileA.symlink"))
	assert.NoError(t, err)
	assert.Equal(t, "mytestdataA", string(targetContents), "symlinked file contents should match target file")

	directory = &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				&repb.FileNode{
					Name:   "fileB.txt",
					Digest: fileBDigest,
				},
			},
			Symlinks: []*repb.SymlinkNode{
				&repb.SymlinkNode{
					Name:   "fileA.symlink",
					Target: "./fileB.txt",
				},
			},
		},
	}

	_, err = dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, tmpDir, &dirtools.DownloadTreeOpts{})
	if err != nil {
		t.Fatal(err)
	}
	target, err = os.Readlink(filepath.Join(tmpDir, "fileA.symlink"))
	assert.NoError(t, err, "should be able to read symlink target")
	assert.Equal(t, "./fileB.txt", target)
	targetContents, err = os.ReadFile(filepath.Join(tmpDir, "fileA.symlink"))
	assert.NoError(t, err)
	assert.Equal(t, "mytestdataB", string(targetContents), "symlinked file contents should match target file")
}

func testEnv(t *testing.T) (*testenv.TestEnv, context.Context) {
	env := testenv.GetTestEnv(t)
	ctx := context.Background()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, env)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		t.Error(err)
	}
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}
	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(env)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	conn, err := testenv.LocalGRPCConn(ctx, env)
	if err != nil {
		t.Error(err)
	}
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	filecacheRootDir := testfs.MakeTempDir(t)
	fileCacheMaxSizeBytes := int64(10e9)
	fc, err := filecache.NewFileCache(filecacheRootDir, fileCacheMaxSizeBytes, false)
	if err != nil {
		t.Fatal(err)
	}
	fc.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc)
	return env, ctx
}

func setFile(t *testing.T, env *testenv.TestEnv, ctx context.Context, instanceName, data string) *repb.Digest {
	dataBytes := []byte(data)
	hashString := hash.String(data)
	d := &repb.Digest{
		Hash:      hashString,
		SizeBytes: int64(len(dataBytes)),
	}
	r := &rspb.ResourceName{
		Digest:       d,
		CacheType:    rspb.CacheType_CAS,
		InstanceName: instanceName,
	}
	env.GetCache().Set(ctx, r, dataBytes)
	t.Logf("Added digest %s/%d to cache (content: %q)", d.GetHash(), d.GetSizeBytes(), data)
	return d
}

func addToFileCache(t *testing.T, ctx context.Context, env *testenv.TestEnv, tempDir, data string) {
	path := testfs.MakeTempFile(t, tempDir, "filecache-tmp-*")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	d, err := digest.Compute(strings.NewReader(data), repb.DigestFunction_SHA256)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Added digest %s/%d to filecache (content: %q)", d.GetHash(), d.GetSizeBytes(), data)
	err = env.GetFileCache().AddFile(ctx, &repb.FileNode{Name: filepath.Base(path), Digest: d}, path)
	require.NoError(t, err)
}
