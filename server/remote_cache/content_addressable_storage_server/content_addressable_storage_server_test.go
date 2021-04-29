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
