package content_addressable_storage_server_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gcodes "google.golang.org/grpc/codes"
)

func runCASServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		t.Error(err)
	}
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc := env.LocalGRPCServer()
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	clientConn, err := env.LocalGRPCConn(ctx)
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

type evilCache struct {
	interfaces.Cache
}

func (e *evilCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	rsp, err := e.Cache.GetMulti(ctx, digests)
	for d, _ := range rsp {
		rsp[d] = []byte{}
	}
	return rsp, err
}

func TestMalevolentCache(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	c, err := memory_cache.NewMemoryCache(1000000)
	if err != nil {
		t.Fatal(err)
	}
	te.SetCache(&evilCache{c})
	clientConn := runCASServer(ctx, te, t)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	set, err := casClient.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			&repb.BatchUpdateBlobsRequest_Request{
				Digest: d,
				Data:   buf,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(set.GetResponses()))
	assert.Equal(t, d.GetHash(), set.GetResponses()[0].GetDigest().GetHash())
	assert.Equal(t, int32(gcodes.OK), set.GetResponses()[0].GetStatus().GetCode())

	get, err := casClient.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
		Digests: []*repb.Digest{d},
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(get.GetResponses()))
	assert.Equal(t, d.GetHash(), get.GetResponses()[0].GetDigest().GetHash())
	assert.Equal(t, int32(gcodes.NotFound), get.GetResponses()[0].GetStatus().GetCode())
}

func TestTreeTruncation(t *testing.T) {
	instanceName := ""
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, te, t)
	bsClient := bspb.NewByteStreamClient(clientConn)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	// Upload a child file.
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	_, err = cachetools.UploadBlob(ctx, bsClient, instanceName, bytes.NewReader(buf))
	assert.Nil(t, err)
	child1Dir := &repb.Directory{
		Files: []*repb.FileNode{
			&repb.FileNode{
				Name:   "child1",
				Digest: d,
			},
		},
	}
	// Upload a directory containing this file.
	dir1Digest, err := cachetools.UploadProto(ctx, bsClient, instanceName, child1Dir)
	assert.Nil(t, err)

	// Upload another child file.
	d, buf = testdigest.NewRandomDigestBuf(t, 100)
	_, err = cachetools.UploadBlob(ctx, bsClient, instanceName, bytes.NewReader(buf))
	assert.Nil(t, err)

	// Upload another directory containing this file.
	child2Dir := &repb.Directory{
		Files: []*repb.FileNode{
			&repb.FileNode{
				Name:   "child2",
				Digest: d,
			},
		},
	}
	dir2Digest, err := cachetools.UploadProto(ctx, bsClient, instanceName, child2Dir)
	assert.Nil(t, err)

	// Upload a root directory containing both child directories.
	rootDir := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			&repb.DirectoryNode{
				Name:   "dir1",
				Digest: dir1Digest,
			},
			&repb.DirectoryNode{
				Name:   "dir2",
				Digest: dir2Digest,
			},
		},
	}
	rootDigest, err := cachetools.UploadProto(ctx, bsClient, instanceName, rootDir)
	assert.Nil(t, err)

	stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{
		InstanceName: instanceName,
		RootDigest:   rootDigest,
		PreExistingDigests: []*repb.Digest{
			dir2Digest,
		},
	})
	assert.Nil(t, err)
	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, dir := range rsp.GetDirectories() {
			for _, file := range dir.GetFiles() {
				if file.GetName() == "child2" {
					t.Fatalf("Contents of pre-existing-dir %q were not skipped.", dir2Digest.GetHash())
				}
			}
		}
	}
}
