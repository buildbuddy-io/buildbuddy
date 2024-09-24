package action_cache_server_test

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func runACServer(ctx context.Context, t *testing.T, env *testenv.TestEnv) *grpc.ClientConn {
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		t.Error(err)
	}
	acServer, err := action_cache_server.NewActionCacheServer(env)
	if err != nil {
		t.Error(err)
	}
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	repb.RegisterActionCacheServer(grpcServer, acServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	clientConn, err := testenv.LocalGRPCConn(ctx, lis)
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

func TestInlineFile(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runACServer(ctx, t, te)
	acClient := repb.NewActionCacheClient(clientConn)
	bsClient := bspb.NewByteStreamClient(clientConn)

	digestA, err := cachetools.UploadBlobToCAS(ctx, bsClient, "", repb.DigestFunction_SHA256, []byte("hello world"))
	require.NoError(t, err)

	update(t, ctx, acClient, []*repb.OutputFile{
		{
			Path:   "my/pkg/file",
			Digest: digestA,
		},
	})

	actionResult := getWithInlining(t, ctx, acClient, []string{"my/pkg/file"})
	require.Len(t, actionResult.OutputFiles, 1)
	assert.Equal(t, "my/pkg/file", actionResult.OutputFiles[0].Path)
	assert.Equal(t, digestA, actionResult.OutputFiles[0].Digest)
	assert.Equal(t, "hello world", string(actionResult.OutputFiles[0].Contents))
}

func update(t *testing.T, ctx context.Context, client repb.ActionCacheClient, outputFiles []*repb.OutputFile) {
	req := repb.UpdateActionResultRequest{
		ActionDigest: &repb.Digest{
			Hash:      strings.Repeat("a", 64),
			SizeBytes: 1024,
		},
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult: &repb.ActionResult{
			OutputFiles: outputFiles,
		},
	}
	_, err := client.UpdateActionResult(ctx, &req)
	require.NoError(t, err)
}

func getWithInlining(t *testing.T, ctx context.Context, client repb.ActionCacheClient, inline []string) *repb.ActionResult {
	req := &repb.GetActionResultRequest{
		ActionDigest: &repb.Digest{
			Hash:      strings.Repeat("a", 64),
			SizeBytes: 1024,
		},
		DigestFunction:    repb.DigestFunction_SHA256,
		InlineOutputFiles: inline,
	}
	resp, err := client.GetActionResult(ctx, req)
	require.NoError(t, err)
	return resp
}
