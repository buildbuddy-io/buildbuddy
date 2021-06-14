package content_addressable_storage_server_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcodes "google.golang.org/grpc/codes"
)

func runCASServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc := env.LocalGRPCServer()
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)

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
	for d := range rsp {
		rsp[d] = []byte{}
	}
	return rsp, err
}

func TestBatchUpdateBlobs(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, te, t)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	req := &repb.BatchUpdateBlobsRequest{}
	for i := 0; i < 100; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
			Digest: d,
			Data:   buf,
		})
	}
	rsp, err := casClient.BatchUpdateBlobs(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 100, len(rsp.GetResponses()))
	for _, singleRsp := range rsp.GetResponses() {
		assert.Equal(t, int32(gcodes.OK), singleRsp.GetStatus().GetCode())
	}
}

func TestBatchUpdateRejectCorruptBlobs(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	clientConn := runCASServer(ctx, te, t)
	casClient := repb.NewContentAddressableStorageClient(clientConn)

	req := &repb.BatchUpdateBlobsRequest{}
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	buf[0] = ^buf[0] // corrupt the data in buf
	req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest: d,
		Data:   buf,
	})

	d2, buf := testdigest.NewRandomDigestBuf(t, 100)
	d2.SizeBytes += 1 // corrupt the payload size of d2
	req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest: d2,
		Data:   buf,
	})

	d3, buf := testdigest.NewRandomDigestBuf(t, 100)
	req.Requests = append(req.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest: d3,
		Data:   buf,
	})

	rsp, err := casClient.BatchUpdateBlobs(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, len(rsp.GetResponses()))
	assert.Equal(t, int32(gcodes.DataLoss), rsp.GetResponses()[0].GetStatus().GetCode())
	assert.Equal(t, int32(gcodes.DataLoss), rsp.GetResponses()[1].GetStatus().GetCode())
	assert.Equal(t, int32(gcodes.OK), rsp.GetResponses()[2].GetStatus().GetCode())
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
			{
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
}
