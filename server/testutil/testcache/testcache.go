package testcache

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// Setup configures ActionCache, Bytestream, and ContentAddressableStorage
// services in the given test env and registers them to a local gRPC server
// listening on the provided bufconn. The gRPC server must be registered to the
// env before calling this func.
//
// Example:
//
//	srv, runServer, lis := testenv.RegisterLocalGRPCServer(env)
//	testcache.Setup(t, env, lis)
//	// Register more services to srv if needed...
//	go runServer()
func Setup(t *testing.T, env *testenv.TestEnv, lis *bufconn.Listener) {
	require.NotNil(t, env.GetGRPCServer(), "GRPC server is missing from env. Call testenv.RegisterLocalGRPCServer first")

	RegisterServers(t, env)

	conn, err := testenv.LocalGRPCConn(env.GetServerContext(), lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	RegisterClients(env, conn)
}

func RegisterServers(t *testing.T, env *testenv.TestEnv) {
	ac, err := action_cache_server.NewActionCacheServer(env)
	require.NoError(t, err)
	bs, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	cas, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	require.NoError(t, err)
	caps := capabilities_server.NewCapabilitiesServer(env, true /*cas*/, true /*rbe*/, true /*zstd*/)

	repb.RegisterActionCacheServer(env.GetGRPCServer(), ac)
	bspb.RegisterByteStreamServer(env.GetGRPCServer(), bs)
	repb.RegisterContentAddressableStorageServer(env.GetGRPCServer(), cas)
	repb.RegisterCapabilitiesServer(env.GetGRPCServer(), caps)
}

func RegisterClients(env *testenv.TestEnv, conn grpc.ClientConnInterface) {
	env.SetActionCacheClient(repb.NewActionCacheClient(conn))
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))
}
