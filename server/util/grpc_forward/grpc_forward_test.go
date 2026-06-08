package grpc_forward

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func cleanup() {
	for _, pool := range backendConnectionPools {
		pool.Close()
	}
	backendConnectionPools = nil
}

func TestGetConnectionPool_DedupesConcurrentDialsForSameTarget(t *testing.T) {
	t.Cleanup(cleanup)

	dial := func(_ string, _ ...grpc.DialOption) (*grpc_client.ClientConnPool, error) {
		// Sleep for a bit to increase the chance of race conditions.
		time.Sleep(10 * time.Millisecond)
		return &grpc_client.ClientConnPool{}, nil
	}

	pools := make([]*grpc_client.ClientConnPool, 10)
	var wg sync.WaitGroup
	for i := range pools {
		wg.Go(func() {
			p, err := getConnectionPool(dial, "remote.example.com")
			require.NotNil(t, p)
			require.NoError(t, err)
			pools[i] = p
		})
	}
	wg.Wait()

	require.NotNil(t, pools[0])
	for _, pool := range pools[1:] {
		require.Same(t, pools[0], pool)
	}
}

// fakeIdentityService records IdentityHeader calls and returns a canned header.
type fakeIdentityService struct {
	mu         sync.Mutex
	calls      int
	lastClient string
	header     string
}

func (f *fakeIdentityService) IdentityHeader(si *interfaces.ClientIdentity, _ time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastClient = si.Client
	return f.header, nil
}

func (f *fakeIdentityService) AddIdentityToContext(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (f *fakeIdentityService) ValidateIncomingIdentity(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (f *fakeIdentityService) IdentityFromContext(context.Context) (*interfaces.ClientIdentity, error) {
	return nil, nil
}

func ctxWithResolvedClientIP(ip string) context.Context {
	return context.WithValue(context.Background(), clientip.ContextKey, ip)
}

func TestCtxWithClientIP(t *testing.T) {
	const clientIP = "1.2.3.4"
	const identity = "signed-grpc-proxy-header"

	t.Run("attaches client IP and identity", func(t *testing.T) {
		idHeader := newIdentityHeader(&fakeIdentityService{header: identity})
		ctx, err := ctxWithClientIP(ctxWithResolvedClientIP(clientIP), idHeader)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{clientIP}, md.Get(clientip.HeaderName))
		require.Equal(t, []string{identity}, md.Get(authutil.ClientIdentityHeaderName))
	})

	t.Run("no client IP attaches nothing", func(t *testing.T) {
		idHeader := newIdentityHeader(&fakeIdentityService{header: identity})
		ctx, err := ctxWithClientIP(context.Background(), idHeader)
		require.NoError(t, err)

		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			require.Empty(t, md.Get(clientip.HeaderName))
			require.Empty(t, md.Get(authutil.ClientIdentityHeaderName))
		}
	})

	t.Run("existing client IP header is left untouched and not attested", func(t *testing.T) {
		idHeader := newIdentityHeader(&fakeIdentityService{header: identity})
		ctx := metadata.AppendToOutgoingContext(ctxWithResolvedClientIP(clientIP), clientip.HeaderName, "9.9.9.9")
		ctx, err := ctxWithClientIP(ctx, idHeader)
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"9.9.9.9"}, md.Get(clientip.HeaderName))
		require.Empty(t, md.Get(authutil.ClientIdentityHeaderName))
	})

	t.Run("nil identity service sets IP without identity", func(t *testing.T) {
		ctx, err := ctxWithClientIP(ctxWithResolvedClientIP(clientIP), newIdentityHeader(nil))
		require.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{clientIP}, md.Get(clientip.HeaderName))
		require.Empty(t, md.Get(authutil.ClientIdentityHeaderName))
	})
}

func TestIdentityHeaderMintsGRPCProxyIdentityAndCaches(t *testing.T) {
	fake := &fakeIdentityService{header: "signed"}
	idHeader := newIdentityHeader(fake)

	h1, err := idHeader.get()
	require.NoError(t, err)
	require.Equal(t, "signed", h1)

	h2, err := idHeader.get()
	require.NoError(t, err)
	require.Equal(t, "signed", h2)

	require.Equal(t, 1, fake.calls, "header should be minted once and reused within the TTL")
	require.Equal(t, interfaces.ClientIdentityGRPCProxy, fake.lastClient)
}
