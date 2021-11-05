package dirtools_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/assert"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

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
	}

	childDigest, err := digest.ComputeForMessage(childDir)
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
	info, err := dirtools.DownloadTree(ctx, env, "", directory, tmpDir, &dirtools.DownloadTreeOpts{})
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, info, "transfers are not nil")
	assert.Equal(t, int64(2), info.FileCount, "two files were transferred")
	assert.DirExists(t, filepath.Join(tmpDir, "my-directory"), "my-directory should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "my-directory/fileA.txt"), "fileA.txt should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "fileB.txt"), "fileB.txt should exist")
}

func TestDownloadTreeEmptyDigest(t *testing.T) {
	env, ctx := testEnv(t)
	tmpDir := testfs.MakeTempDir(t)
	instanceName := "foo"

	fileDigest := setFile(t, env, ctx, instanceName, "mytestdata")
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

	childDigest, err := digest.ComputeForMessage(childDir)
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
	info, err := dirtools.DownloadTree(ctx, env, "foo", directory, tmpDir, &dirtools.DownloadTreeOpts{})
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, info, "transfers are not nil")
	assert.Equal(t, int64(3), info.FileCount, "three files were transferred")
	assert.DirExists(t, filepath.Join(tmpDir, "my-empty-directory"), "my-empty-directory should exist")
	assert.DirExists(t, filepath.Join(tmpDir, "my-notempty-directory"), "my-notempty-directory should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "my-notempty-directory/fileA.txt"), "fileA.txt should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "file_empty.txt"), "file_empty.txt should exist")
	assert.FileExists(t, filepath.Join(tmpDir, "file_notempty.txt"), "file_notempty.txt should exist")
}

func testEnv(t *testing.T) (*testenv.TestEnv, context.Context) {
	env := testenv.GetTestEnv(t)
	ctx := context.Background()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, env)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}
	grpcServer, runFunc := env.LocalGRPCServer()
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()
	conn, err := env.LocalGRPCConn(ctx)
	if err != nil {
		t.Error(err)
	}
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	return env, ctx
}

func setFile(t *testing.T, env *testenv.TestEnv, ctx context.Context, instanceName, data string) *repb.Digest {
	dataBytes := []byte(data)
	hashString := hash.String(data)
	d := &repb.Digest{
		Hash:      hashString,
		SizeBytes: int64(len(dataBytes)),
	}
	c, err := env.GetCache().WithIsolation(ctx, interfaces.CASCacheType, instanceName)
	if err != nil {
		t.Fatal(err)
	}
	c.Set(ctx, d, dataBytes)
	return d
}
