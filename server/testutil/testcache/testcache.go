package testcache

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// Setup configures ActionCache, Bytestream, and ContentAddressableStorage
// services in the given test env and registers them to the local gRPC server.
// The gRPC server must be registered to the env before calling this func.
//
// Example:
//
//	srv, runServer := testenv.RegisterLocalGRPCServer(env)
//	testcache.Setup(t, env)
//	// Register more services to srv if needed...
//	go runServer()
func Setup(t *testing.T, env *testenv.TestEnv) {
	require.NotNil(t, env.GetGRPCServer(), "GRPC server is missing from env. Call testenv.RegisterLocalGRPCServer first")
	require.NotNil(t, env.GetLocalBufconnListener(), "Missing bufconn listener in env. Make sure you aren't calling SetGRPCServer in the test env, and call testenv.RegisterLocalGRPCServer instead")

	ac, err := action_cache_server.NewActionCacheServer(env)
	require.NoError(t, err)
	bs, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	cas, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	require.NoError(t, err)

	repb.RegisterActionCacheServer(env.GetGRPCServer(), ac)
	bspb.RegisterByteStreamServer(env.GetGRPCServer(), bs)
	repb.RegisterContentAddressableStorageServer(env.GetGRPCServer(), cas)

	conn, err := testenv.LocalGRPCConn(env.GetServerContext(), env)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	env.SetActionCacheClient(repb.NewActionCacheClient(conn))
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
}
