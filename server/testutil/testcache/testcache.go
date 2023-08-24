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
// services in the given test env.
func Setup(t *testing.T, env *testenv.TestEnv) {
	ac, err := action_cache_server.NewActionCacheServer(env)
	require.NoError(t, err)
	bs, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	cas, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	require.NoError(t, err)

	srv, runFunc := env.LocalGRPCServer()
	repb.RegisterActionCacheServer(srv, ac)
	bspb.RegisterByteStreamServer(srv, bs)
	repb.RegisterContentAddressableStorageServer(srv, cas)

	go runFunc()

	conn, err := env.LocalGRPCConn(env.GetServerContext())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	env.SetActionCacheClient(repb.NewActionCacheClient(conn))
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
}
