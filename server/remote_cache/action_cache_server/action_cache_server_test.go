package action_cache_server_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func runActionCacheServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	actionCacheServer, err := action_cache_server.NewActionCacheServer(env)
	if err != nil {
		t.Error(err)
	}

	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc := env.LocalGRPCServer()
	repb.RegisterActionCacheServer(grpcServer, actionCacheServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)

	go runFunc()

	clientConn, err := env.LocalGRPCConn(ctx)
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

func TestGetActionResult(t *testing.T) {
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := testenv.GetTestEnv(t)
	instanceName := ""
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)

	clientConn := runActionCacheServer(ctx, te, t)
	acClient := repb.NewActionCacheClient(clientConn)

	digests := make([]*repb.Digest, 0)
	dirs := make([]*repb.Directory, 0)
	dirDigests := make([]*repb.Digest, 0)
	actionResult := repb.ActionResult{
		ExitCode: 0,
	}

	for i := 0; i < 10; i++ {
		_, buf := testdigest.NewRandomDigestBuf(t, 100)
		d, err := cachetools.UploadBlobToCAS(ctx, te.GetCache(), instanceName, buf)
		assert.Nil(t, err)
		digests = append(digests, d)
		log.Printf("Uploaded file %d: %q", i, d.GetHash())

		actionResult.OutputFiles = append(actionResult.OutputFiles, &repb.OutputFile{
			Path:   fmt.Sprintf("/%s", d.GetHash()),
			Digest: d,
		})

		dir := &repb.Directory{}
		dir.Files = append(dir.Files, &repb.FileNode{
			Name:   d.GetHash(),
			Digest: d,
		})
		if len(dirDigests) > 0 {
			lastDirectoryDigest := dirDigests[len(dirDigests)-1]
			dir.Directories = append(dir.Directories, &repb.DirectoryNode{
				Name:   lastDirectoryDigest.GetHash(),
				Digest: lastDirectoryDigest,
			})
		}
		dirs = append(dirs, dir)

		d, err = cachetools.UploadProtoToCAS(ctx, te.GetCache(), instanceName, dir)
		assert.Nil(t, err)
		dirDigests = append(dirDigests, d)
		log.Printf("Uploaded dir %d: %q", i, d.GetHash())
	}

	rootTree := &repb.Tree{Root: dirs[0], Children: dirs[1:]}
	d, err := cachetools.UploadProtoToCAS(ctx, te.GetCache(), instanceName, rootTree)
	assert.Nil(t, err)
	log.Printf("Tree is %+v, digest: %q, err: %s", rootTree, d.GetHash(), err)

	if len(dirDigests) > 0 {
		actionResult.OutputDirectories = append(actionResult.OutputDirectories, &repb.OutputDirectory{
			Path:       "root",
			TreeDigest: d,
		})
	}

	// Call UpdateActionResult to set an action result in the cache.
	ad, _ := testdigest.NewRandomDigestBuf(t, 142)
	rsp, err := acClient.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
		ActionDigest: ad,
		ActionResult: &actionResult,
	})
	assert.Nil(t, err)

	// GetActionResult to get the action back. It should still exist.
	rsp, err = acClient.GetActionResult(ctx, &repb.GetActionResultRequest{
		ActionDigest: ad,
	})

	// Now delete a file that was one of the action outputs.
	digestToDelete := dirDigests[3]
	log.Printf("Deleting digest: %q", digestToDelete.GetHash())
	err = te.GetCache().Delete(ctx, digestToDelete)
	assert.Nil(t, err)

	// log.Printf("Deleting digest: %q", dirDigests[0].GetHash())
	// err = te.GetCache().Delete(ctx, dirDigests[0])
	//	assert.Nil(t, err)

	// And ensure that when we fetch the action again, it's
	// no longer returned (since one of the outputs in it is now gone).
	rsp, err = acClient.GetActionResult(ctx, &repb.GetActionResultRequest{
		ActionDigest: ad,
	})
	assert.Nil(t, rsp)
	assert.Equal(t, status.Code(err), codes.NotFound)
}
