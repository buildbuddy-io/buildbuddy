package dirtools_test

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

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
				FileCount:        1,
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
				FileCount:        2,
				BytesTransferred: 84,
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
				FileCount:        4,
				BytesTransferred: 244,
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
				FileCount:        2,
				BytesTransferred: 108,
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
				FileCount:        2,
				BytesTransferred: 108,
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
				FileCount:        2,
				BytesTransferred: 108,
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
				FileCount:        4,
				BytesTransferred: 256,
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
				FileCount:        4,
				BytesTransferred: 256,
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
				FileCount:        4,
				BytesTransferred: 256,
			},
		},
		{
			name: "SymlinkInOutputDir",
			cmd: &repb.Command{
				OutputDirectories: []string{
					"a",
				},
			},
			directoryPaths: []string{
				"a",
			},
			fileContents: map[string]string{
				"a/fileA.txt": "a",
			},
			symlinkPaths: map[string]string{
				"a/linkA": "fileA.txt",
			},
			expectedResult: &repb.ActionResult{
				OutputDirectories: []*repb.OutputDirectory{
					{
						Path: "a",
						TreeDigest: getDigestForMsg(t, &repb.Tree{
							Root: &repb.Directory{
								Files: []*repb.FileNode{
									{Name: "fileA.txt", Digest: &repb.Digest{Hash: hash.String("a"), SizeBytes: 1}},
								},
								Symlinks: []*repb.SymlinkNode{
									{Name: "linkA", Target: "fileA.txt"},
								},
							},
						}),
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        4,
				BytesTransferred: 284,
			},
		},
		{
			name: "SymlinkInOutputDirWithOutputPaths",
			cmd: &repb.Command{
				OutputPaths: []string{
					"a",
				},
			},
			directoryPaths: []string{
				"a",
			},
			fileContents: map[string]string{
				"a/fileA.txt": "a",
			},
			symlinkPaths: map[string]string{
				"a/linkA": "fileA.txt",
			},
			expectedResult: &repb.ActionResult{
				OutputDirectories: []*repb.OutputDirectory{
					{
						Path: "a",
						TreeDigest: getDigestForMsg(t, &repb.Tree{
							Root: &repb.Directory{
								Files: []*repb.FileNode{
									{Name: "fileA.txt", Digest: &repb.Digest{Hash: hash.String("a"), SizeBytes: 1}},
								},
								Symlinks: []*repb.SymlinkNode{
									{Name: "linkA", Target: "fileA.txt"},
								},
							},
						}),
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				FileCount:        4,
				BytesTransferred: 284,
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
				FileCount:        1,
				BytesTransferred: 8,
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
				FileCount:        1,
				BytesTransferred: 8,
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
				FileCount:        3,
				BytesTransferred: 84,
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
						Path: "a/b",
						TreeDigest: &repb.Digest{
							SizeBytes: 392,
							Hash:      "59620196c9761b313ff20ed0dfb06bf81b824afe2bf7046ce49949ab51605b6b",
						},
					},
				},
			},
			expectedInfo: &dirtools.TransferInfo{
				// This should includes:
				//
				//   Dir:  a/b
				//   Dir:  a/b/c
				//   Dir:  a/b/c/d
				//   Dir:  a/b/e
				//   Dir:  a/b/e/g
				//   File: a/b/c/fileA.txt
				//
				FileCount:        7,
				BytesTransferred: 849,
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
				FileCount:        5,
				BytesTransferred: 244,
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
				FileCount:        1,
				BytesTransferred: 8,
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

			dirHelper := dirtools.NewDirHelper(rootDir, tc.cmd, fs.FileMode(0o755))

			actionResult := &repb.ActionResult{}
			txInfo, err := dirtools.UploadTree(ctx, env, dirHelper, "", repb.DigestFunction_SHA256, rootDir, tc.cmd, actionResult, true /*=addToFileCache*/)
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
				assert.True(
					t,
					slices.ContainsFunc(actionResult.OutputDirectories, func(dir *repb.OutputDirectory) bool {
						return expectedDir.Path == dir.Path && expectedDir.TreeDigest.SizeBytes == dir.TreeDigest.SizeBytes && expectedDir.TreeDigest.Hash == dir.TreeDigest.Hash
					}),
					fmt.Sprintf("expected dir %s to be in actionResult output directories %v", expectedDir, actionResult.OutputDirectories),
				)
			}
			assert.Equal(t, len(tc.expectedResult.OutputSymlinks), len(actionResult.OutputSymlinks))
			for _, expectedSymlink := range tc.expectedResult.OutputSymlinks {
				assert.True(
					t,
					slices.ContainsFunc(actionResult.OutputSymlinks, func(symlink *repb.OutputSymlink) bool {
						return symlink.Path == expectedSymlink.Path && symlink.Target == expectedSymlink.Target
					}),
					fmt.Sprintf("expected symlink %s to be in actionResult.OutputSymlinks %v", expectedSymlink, actionResult.OutputSymlinks),
				)
			}
			assert.Equal(t, len(tc.expectedResult.OutputFileSymlinks), len(actionResult.OutputFileSymlinks))
			for _, expectedSymlink := range tc.expectedResult.OutputFileSymlinks {
				assert.True(
					t,
					slices.ContainsFunc(actionResult.OutputFileSymlinks, func(symlink *repb.OutputSymlink) bool {
						return symlink.Path == expectedSymlink.Path && symlink.Target == expectedSymlink.Target
					}),
					fmt.Sprintf("expected symlink %s to be in actionResult.OutputFileSymlinks %v", expectedSymlink, actionResult.OutputFileSymlinks),
				)
			}
			assert.Equal(t, len(tc.expectedResult.OutputDirectorySymlinks), len(actionResult.OutputDirectorySymlinks))
			for _, expectedSymlink := range tc.expectedResult.OutputDirectorySymlinks {
				assert.True(
					t,
					slices.ContainsFunc(actionResult.OutputDirectorySymlinks, func(symlink *repb.OutputSymlink) bool {
						return symlink.Path == expectedSymlink.Path && symlink.Target == expectedSymlink.Target
					}),
					fmt.Sprintf("expected symlink %s to be in actionResult.OutputDirectorySymlinks %v", expectedSymlink, actionResult.OutputDirectorySymlinks),
				)
			}
		})
	}
}

func getDigestForMsg(t *testing.T, in proto.Message) *repb.Digest {
	d, err := digest.ComputeForMessage(in, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	return d
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
	info, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{RootDir: tmpDir})
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

func TestDownloadTreeDedupeInflight(t *testing.T) {
	env, ctx := testEnv(t)
	tmpDir := testfs.MakeTempDir(t)

	rnA, bufA := testdigest.RandomCASResourceBuf(t, dirtools.BatchReadLimitBytes+1)
	env.GetCache().Set(ctx, rnA, bufA)
	fileADigest := rnA.GetDigest()

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

	mu := sync.Mutex{} // PROTECTS(totalTransferCount)
	totalTransferCount := int64(0)

	eg := errgroup.Group{}
	for i := 0; i < 10; i++ {
		eg.Go(func() error {
			info, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{RootDir: tmpDir})
			if err != nil {
				return err
			}
			mu.Lock()
			totalTransferCount += info.FileCount
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int64(1), totalTransferCount, "two files were transferred")
	assert.DirExists(t, filepath.Join(tmpDir, "my-directory"), "my-directory should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "my-directory/fileA.txt"), "fileA.txt should exist")
	target, err := os.Readlink(filepath.Join(tmpDir, "my-directory/fileA.symlink"))
	assert.NoError(t, err, "should be able to read symlink target")
	assert.Equal(t, "./fileA.txt", target)
	targetContents, err := os.ReadFile(filepath.Join(tmpDir, "my-directory/fileA.symlink"))
	assert.NoError(t, err)
	assert.Equal(t, bufA, targetContents, "symlinked file contents should match target file")
}

func TestDownloadTreeBatchDownloadNotDeduped(t *testing.T) {
	env, ctx := testEnv(t)
	tmpDirA := testfs.MakeTempDir(t)
	tmpDirB := testfs.MakeTempDir(t)

	instanceName := "foo"
	fileDigest := setFile(t, env, ctx, instanceName, "small-file")

	directory := &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				{
					Name:   "file.txt",
					Digest: fileDigest,
				},
			},
		},
	}

	cc := env.GetCache().(*controlledCache)
	cc.mu.Lock()
	cc.getMultiCalls = make(chan struct{}, 4)
	cc.mu.Unlock()
	unblockGetMulti := cc.InjectGetMultiPause(digest.NewKey(fileDigest))

	var mu sync.Mutex
	totalTransferCount := int64(0)

	eg := errgroup.Group{}
	for _, rootDir := range []string{tmpDirA, tmpDirB} {
		rootDir := rootDir
		eg.Go(func() error {
			info, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{RootDir: rootDir})
			if err != nil {
				return err
			}
			mu.Lock()
			totalTransferCount += info.FileCount
			mu.Unlock()
			return nil
		})
	}

	for i := 0; i < 2; i++ {
		select {
		case <-cc.getMultiCalls:
		case <-time.After(2 * time.Second):
			unblockGetMulti()
			t.Fatal("timed out waiting for batch read requests")
		}
	}
	unblockGetMulti()

	require.NoError(t, eg.Wait())
	// TODO(sluongng): ideally we should flip this in the future
	require.Equal(t, int64(2), totalTransferCount, "batch downloads should not be deduplicated")
	assert.FileExists(t, filepath.Join(tmpDirA, "file.txt"))
	assert.FileExists(t, filepath.Join(tmpDirB, "file.txt"))
}

func TestDownloadTreeBytestreamDownloadDeduped(t *testing.T) {
	env, ctx := testEnv(t)
	tmpDirA := testfs.MakeTempDir(t)
	tmpDirB := testfs.MakeTempDir(t)

	largeRN, largeFileContent := testdigest.RandomCASResourceBuf(t, 5*1024*1024)
	err := env.GetCache().Set(ctx, largeRN, largeFileContent)
	require.NoError(t, err)

	directory := &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				{
					Name:   "large-file.txt",
					Digest: largeRN.GetDigest(),
				},
			},
		},
	}

	cc := env.GetCache().(*controlledCache)
	cc.mu.Lock()
	cc.readerCalls = make(chan struct{}, 4)
	cc.mu.Unlock()
	unblockReader := cc.InjectReaderPause(digest.NewKey(largeRN.GetDigest()))

	var mu sync.Mutex
	totalTransferCount := int64(0)

	start := make(chan struct{})
	eg := errgroup.Group{}
	for _, rootDir := range []string{tmpDirA, tmpDirB} {
		rootDir := rootDir
		eg.Go(func() error {
			<-start
			info, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{RootDir: rootDir})
			if err != nil {
				return err
			}
			mu.Lock()
			totalTransferCount += info.FileCount
			mu.Unlock()
			return nil
		})
	}
	close(start)

	select {
	case <-cc.readerCalls:
	case <-time.After(2 * time.Second):
		unblockReader()
		t.Fatal("timed out waiting for bytestream read")
	}

	select {
	case <-cc.readerCalls:
		unblockReader()
		t.Fatal("expected bytestream downloads to be deduplicated")
	case <-time.After(250 * time.Millisecond):
	}

	unblockReader()
	require.NoError(t, eg.Wait())

	require.Equal(t, int64(1), totalTransferCount, "bytestream downloads should be deduplicated")
	select {
	case <-cc.readerCalls:
		t.Fatal("expected only one bytestream read")
	default:
	}
	assert.FileExists(t, filepath.Join(tmpDirA, "large-file.txt"))
	assert.FileExists(t, filepath.Join(tmpDirB, "large-file.txt"))
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
	info, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{RootDir: tmpDir})
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
	info, err := dirtools.DownloadTree(ctx, env, "foo", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{RootDir: tmpDir})
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

	_, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{RootDir: tmpDir})
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

	_, err = dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{RootDir: tmpDir})
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

	_, err := dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{RootDir: tmpDir})
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

	_, err = dirtools.DownloadTree(ctx, env, "", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{RootDir: tmpDir})
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

type controlledCache struct {
	interfaces.Cache

	mu            sync.Mutex
	readerDelay   map[digest.Key]chan struct{}
	readerCalls   chan struct{}
	getMultiDelay map[digest.Key]chan struct{}
	getMultiCalls chan struct{}
}

func (cc *controlledCache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	dk := digest.NewKey(r.GetDigest())
	cc.mu.Lock()
	delay, ok := cc.readerDelay[dk]
	calls := cc.readerCalls
	cc.mu.Unlock()

	if calls != nil {
		calls <- struct{}{}
	}
	if ok {
		log.CtxInfof(ctx, "Injecting artificial reader delay for %s", digest.String(r.GetDigest()))
		<-delay
	}

	return cc.Cache.Reader(ctx, r, uncompressedOffset, limit)
}

func (cc *controlledCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	var calls chan struct{}
	delays := make([]chan struct{}, 0)
	cc.mu.Lock()
	calls = cc.getMultiCalls
	for _, r := range resources {
		if delay, ok := cc.getMultiDelay[digest.NewKey(r.GetDigest())]; ok {
			delays = append(delays, delay)
		}
	}
	cc.mu.Unlock()

	if calls != nil {
		calls <- struct{}{}
	}
	for _, delay := range delays {
		<-delay
	}

	return cc.Cache.GetMulti(ctx, resources)
}

func (cc *controlledCache) InjectReaderPause(dk digest.Key) func() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	done := make(chan struct{})
	cc.readerDelay[dk] = done
	return func() {
		close(done)
		cc.mu.Lock()
		delete(cc.readerDelay, dk)
		cc.mu.Unlock()
	}
}

func (cc *controlledCache) InjectGetMultiPause(dk digest.Key) func() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.getMultiDelay == nil {
		cc.getMultiDelay = make(map[digest.Key]chan struct{})
	}
	done := make(chan struct{})
	cc.getMultiDelay[dk] = done
	return func() {
		close(done)
		cc.mu.Lock()
		delete(cc.getMultiDelay, dk)
		cc.mu.Unlock()
	}
}

func TestDownloadTreeDirectlyToFileCache(t *testing.T) {
	env, ctx := testEnv(t)

	instanceName := "foo"
	file1Contents := "mytestdataA"
	file2Contents := "mytestdataB-withDifferentLength"
	file1Digest := setFile(t, env, ctx, instanceName, file1Contents)
	file2Digest := setFile(t, env, ctx, instanceName, file2Contents)
	tmp := testfs.MakeTempDir(t)
	addToFileCache(t, ctx, env, tmp, file1Contents)

	largeRN, largeFileContent := testdigest.RandomCASResourceBuf(t, rpcutil.GRPCMaxSizeBytes+1)
	err := env.GetCache().Set(ctx, largeRN, largeFileContent)
	require.NoError(t, err)

	fileNode1 := &repb.FileNode{
		Name:   "fileA.txt",
		Digest: file1Digest,
	}
	childDir := &repb.Directory{
		Files: []*repb.FileNode{
			fileNode1,
		},
	}

	childDigest, err := digest.ComputeForMessage(childDir, repb.DigestFunction_SHA256)
	require.NoError(t, err)

	fileNode2 := &repb.FileNode{
		Name:   "fileB.txt",
		Digest: file2Digest,
	}
	largeFileNode := &repb.FileNode{
		Name:   "largeFile.txt",
		Digest: largeRN.GetDigest(),
	}
	directory := &repb.Tree{
		Root: &repb.Directory{
			Files: []*repb.FileNode{
				fileNode2,
				largeFileNode,
			},
			Directories: []*repb.DirectoryNode{
				{
					Name:   "my-directory",
					Digest: childDigest,
				},
			},
		},
		Children: []*repb.Directory{
			childDir,
		},
	}

	cc := env.GetCache().(*controlledCache)
	unblockLargeRead := cc.InjectReaderPause(digest.NewKey(largeRN.GetDigest()))

	// Not specifying a download directory so the inputs should be downloaded directly into the file cache.
	tf, err := dirtools.NewTreeFetcher(ctx, env, "", repb.DigestFunction_SHA256, directory, &dirtools.DownloadTreeOpts{})
	require.NoError(t, err)
	_, err = tf.Start()
	require.NoError(t, err)

	err = tf.Fetch(ctx, fileNode1)
	require.NoError(t, err)
	err = tf.Fetch(ctx, fileNode2)
	require.NoError(t, err)

	{
		// Try fetching a digest for which there is already a fetch in progress.
		// As the cache reader is intentionally blocked, the fetch should timeout.
		timeoutCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		err = tf.Fetch(timeoutCtx, largeFileNode)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)

		// Now unblock the reader after a short delay. The fetch should be
		// unblocked when the cache read succeeds.
		go func() {
			time.Sleep(100 * time.Millisecond)
			unblockLargeRead()
		}()
		err = tf.Fetch(ctx, largeFileNode)
		require.NoError(t, err)
	}

	info, err := tf.Wait()
	require.NoError(t, err)

	require.Equal(t, int64(2), info.FileCount, "two files should be transferred, one linked from filecache")
	require.Equal(t, int64(len(file2Contents)+len(largeFileContent)), info.BytesTransferred, "two files should be transferred. file 1 should be linked from filecache")

	fc := env.GetFileCache()
	require.True(t, fc.ContainsFile(ctx, fileNode1), "file 1 should be in the file cache")
	require.True(t, fc.ContainsFile(ctx, fileNode2), "file 2 should be in the file cache")
	require.True(t, fc.ContainsFile(ctx, largeFileNode), "large file should be in the file cache")
}

func testEnv(t *testing.T) (*testenv.TestEnv, context.Context) {
	env := testenv.GetTestEnv(t)

	// wrap the cache so we can add artificial delays to operations
	cc := &controlledCache{
		Cache:       env.GetCache(),
		readerDelay: make(map[digest.Key]chan struct{}),
	}
	env.SetCache(cc)

	ctx := context.Background()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
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
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	conn, err := testenv.LocalGRPCConn(ctx, lis)
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
	err := env.GetCache().Set(ctx, r, dataBytes)
	require.NoError(t, err)
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
