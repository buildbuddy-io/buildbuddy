package action_cache_server_proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gcodes "google.golang.org/grpc/codes"
)

func runACServer(ctx context.Context, t *testing.T) repb.ActionCacheClient {
	env := testenv.GetTestEnv(t)
	acServer, err := action_cache_server.NewActionCacheServer(env)
	require.NoError(t, err)
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterActionCacheServer(grpcServer, acServer)
	go runFunc()
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return repb.NewActionCacheClient(conn)
}

func runACProxy(ctx context.Context, t *testing.T, client repb.ActionCacheClient) (repb.ActionCacheClient, *fakeCASServer) {
	env := testenv.GetTestEnv(t)
	cas := &fakeCASServer{}
	env.SetCASServer(cas)

	env.SetActionCacheClient(client)
	proxyServer, err := NewActionCacheServerProxy(env)
	require.NoError(t, err)
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterContentAddressableStorageServer(grpcServer, cas)
	repb.RegisterActionCacheServer(grpcServer, proxyServer)

	go runFunc()
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	t.Cleanup(func() { conn.Close() })
	return repb.NewActionCacheClient(conn), cas
}

func update(ctx context.Context, client repb.ActionCacheClient, digest *repb.Digest, code int32, t *testing.T) {
	req := repb.UpdateActionResultRequest{
		ActionDigest:   digest,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult:   &repb.ActionResult{ExitCode: code},
	}
	_, err := client.UpdateActionResult(ctx, &req)
	require.NoError(t, err)
}

func updateWithOutputs(ctx context.Context, client repb.ActionCacheClient, digest *repb.Digest, code int32, outfiles []*repb.OutputFile, t *testing.T) {
	req := repb.UpdateActionResultRequest{
		ActionDigest:   digest,
		DigestFunction: repb.DigestFunction_SHA256,
		ActionResult: &repb.ActionResult{
			ExitCode:    code,
			OutputFiles: outfiles,
		},
	}
	_, err := client.UpdateActionResult(ctx, &req)
	require.NoError(t, err)
}

func get(ctx context.Context, client repb.ActionCacheClient, digest *repb.Digest, t *testing.T) *repb.ActionResult {
	req := &repb.GetActionResultRequest{
		ActionDigest:   digest,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	resp, err := client.GetActionResult(ctx, req)
	require.NoError(t, err)
	return resp
}

func TestActionCacheProxy(t *testing.T) {
	ctx := context.Background()
	ac := runACServer(ctx, t)
	proxy, _ := runACProxy(ctx, t, ac)

	digestA := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}
	digestB := &repb.Digest{
		Hash:      strings.Repeat("b", 64),
		SizeBytes: 1024,
	}

	// DigestA shouldn't be present initially.
	readReqA := &repb.GetActionResultRequest{
		ActionDigest:   digestA,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	_, err := proxy.GetActionResult(ctx, readReqA)
	require.True(t, status.IsNotFoundError(err))

	// Write it through the proxy and confirm it's readable from the proxy and
	// backing cache.
	update(ctx, proxy, digestA, 1, t)
	require.Equal(t, int32(1), get(ctx, ac, digestA, t).GetExitCode())
	require.Equal(t, int32(1), get(ctx, proxy, digestA, t).GetExitCode())

	// Change the action result, write it, and confirm the new result is
	// readable from the proxy and backing cache.
	update(ctx, proxy, digestA, 2, t)
	require.Equal(t, int32(2), get(ctx, ac, digestA, t).GetExitCode())
	require.Equal(t, int32(2), get(ctx, proxy, digestA, t).GetExitCode())

	// DigestB shouldn't be present initially.
	readReqB := &repb.GetActionResultRequest{
		ActionDigest:   digestB,
		DigestFunction: repb.DigestFunction_SHA256,
	}
	_, err = proxy.GetActionResult(ctx, readReqB)
	require.True(t, status.IsNotFoundError(err))

	// Write it to the backing cache and confirm it's readable from the proxy
	// and backing cache.
	update(ctx, ac, digestB, 999, t)
	require.Equal(t, int32(999), get(ctx, ac, digestB, t).GetExitCode())
	require.Equal(t, int32(999), get(ctx, proxy, digestB, t).GetExitCode())

	// Change the action result, write it, and confirm the new result is
	// readable from the proxy and backing cache.
	update(ctx, ac, digestB, 998, t)
	require.Equal(t, int32(998), get(ctx, ac, digestB, t).GetExitCode())
	require.Equal(t, int32(998), get(ctx, proxy, digestB, t).GetExitCode())
}

func TestActionCacheProxy_InliningSentToRemote(t *testing.T) {
	flags.Set(t, "cache_proxy.inline_action_outputs_in_proxy", false)
	ctx := context.Background()
	ac := newFakeACClient()
	proxy, fakeCas := runACProxy(ctx, t, ac)

	digestA := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}

	// DigestA shouldn't be present initially.
	readReqA := &repb.GetActionResultRequest{
		ActionDigest:      digestA,
		DigestFunction:    repb.DigestFunction_SHA256,
		InlineOutputFiles: []string{"a", "b", "c"},
	}
	_, err := proxy.GetActionResult(ctx, readReqA)
	require.True(t, status.IsNotFoundError(err))

	// Value should read through properly, and request to remote should
	// contain the InlineOutputFiles values above.
	update(ctx, proxy, digestA, 1, t)
	out, err := proxy.GetActionResult(ctx, readReqA)
	require.NoError(t, err)
	require.Equal(t, int32(1), out.GetExitCode())
	require.Equal(t, 2, len(ac.getReqs))
	require.Equal(t, 3, len(ac.getReqs[0].InlineOutputFiles))
	require.Equal(t, readReqA, ac.getReqs[0])
	require.Equal(t, readReqA, ac.getReqs[1])

	// AC Proxy shouldn't use the CAS at all in this case.
	require.Equal(t, 0, fakeCas.reqCount)
}

func TestActionCacheProxy_InliningSentToLocalCASServer(t *testing.T) {
	flags.Set(t, "cache_proxy.inline_action_outputs_in_proxy", true)
	ctx := context.Background()
	ac := newFakeACClient()
	proxy, fakeCas := runACProxy(ctx, t, ac)

	outfiles := []*repb.OutputFile{
		&repb.OutputFile{
			Digest: &repb.Digest{
				Hash:      "abc123",
				SizeBytes: 2,
			},
			Path:     "b",
			Contents: []byte{20, 20, 20},
		},
		&repb.OutputFile{
			Digest: &repb.Digest{
				Hash:      "zyx987",
				SizeBytes: 2,
			},
			Path:     "cc",
			Contents: []byte{65, 66, 67},
		},
	}

	fakeCas.data = map[string]*repb.BatchReadBlobsResponse_Response{
		"abc123": {
			Digest: outfiles[0].Digest,
			Data:   outfiles[0].Contents,
			Status: &statuspb.Status{Code: int32(gcodes.OK)},
		},
		"zyx987": {
			Digest: outfiles[1].Digest,
			Data:   outfiles[1].Contents,
			Status: &statuspb.Status{Code: int32(gcodes.OK)},
		},
	}

	digestA := &repb.Digest{
		Hash:      strings.Repeat("a", 64),
		SizeBytes: 1024,
	}

	// DigestA shouldn't be present initially.
	readReqA := &repb.GetActionResultRequest{
		ActionDigest:      digestA,
		DigestFunction:    repb.DigestFunction_SHA256,
		InlineOutputFiles: []string{"b", "cc"},
	}
	_, err := proxy.GetActionResult(ctx, readReqA)
	require.True(t, status.IsNotFoundError(err))

	// Value should read through properly, and request to remote should
	// contain the InlineOutputFiles values above.
	updateWithOutputs(ctx, proxy, digestA, 1, outfiles, t)
	out, err := proxy.GetActionResult(ctx, readReqA)
	require.NoError(t, err)
	require.Equal(t, int32(1), out.GetExitCode())
	require.Equal(t, 2, len(ac.getReqs))
	require.Empty(t, ac.getReqs[0].GetInlineOutputFiles())
	require.Empty(t, ac.getReqs[1].GetInlineOutputFiles())

	// Second request should hit the fake cas.
	require.Equal(t, 1, fakeCas.reqCount)
	require.Equal(t, 2, len(out.OutputFiles))
	require.Equal(t, outfiles[0].String(), out.OutputFiles[0].String())
	require.Equal(t, outfiles[1].String(), out.OutputFiles[1].String())
}

type fakeACClient struct {
	getReqs []*repb.GetActionResultRequest
	data    map[string]*repb.ActionResult
}

func newFakeACClient() *fakeACClient {
	return &fakeACClient{
		data: make(map[string]*repb.ActionResult),
	}
}

// GetActionResult implements remote_execution.ActionCacheClient.
func (f *fakeACClient) GetActionResult(ctx context.Context, in *repb.GetActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	f.getReqs = append(f.getReqs, in.CloneVT())
	resp, ok := f.data[in.GetActionDigest().GetHash()]
	if !ok {
		return nil, status.NotFoundErrorf("FakeAC: not found: %s", in.GetActionDigest().GetHash())
	}
	return resp, nil
}

// UpdateActionResult implements remote_execution.ActionCacheClient.
func (f *fakeACClient) UpdateActionResult(ctx context.Context, in *repb.UpdateActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	f.data[in.GetActionDigest().GetHash()] = in.GetActionResult()
	return in.GetActionResult(), nil
}

type fakeCASServer struct {
	reqCount int
	data     map[string]*repb.BatchReadBlobsResponse_Response
}

// BatchReadBlobs implements remote_execution.ContentAddressableStorageServer.
func (f *fakeCASServer) BatchReadBlobs(c context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	f.reqCount++
	resp := &repb.BatchReadBlobsResponse{}
	for _, r := range req.Digests {
		data, ok := f.data[r.GetHash()]
		if !ok {
			return nil, status.InternalErrorf("Test is set up poorly, missing data: %s", r.GetHash())
		}
		resp.Responses = append(resp.Responses, data)
	}
	return resp, nil
}

// BatchUpdateBlobs implements remote_execution.ContentAddressableStorageServer.
func (f *fakeCASServer) BatchUpdateBlobs(context.Context, *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	panic("unimplemented")
}

// FindMissingBlobs implements remote_execution.ContentAddressableStorageServer.
func (f *fakeCASServer) FindMissingBlobs(context.Context, *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	panic("unimplemented")
}

// GetTree implements remote_execution.ContentAddressableStorageServer.
func (f *fakeCASServer) GetTree(*repb.GetTreeRequest, repb.ContentAddressableStorage_GetTreeServer) error {
	panic("unimplemented")
}
