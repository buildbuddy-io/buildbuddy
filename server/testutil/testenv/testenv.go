package testenv

import (
	"context"
	"flag"
	"fmt"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_client"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmysql"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testpostgres"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/vtprotocodec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

var (
	databaseType  = flag.String("testenv.database_type", "sqlite", "What database to use for tests.")
	reuseServer   = flag.Bool("testenv.reuse_server", false, "If true, reuse database server between tests.")
	useClickHouse = flag.Bool("testenv.use_clickhouse", false, "Whether to use Clickhouse in tests")
)

func init() {
	*log.LogLevel = "debug"
	*log.IncludeShortFileName = true
	log.Configure()
}

type TestEnv = real_environment.RealEnv

type ConfigTemplateParams struct {
	TestRootDir string
}

const testConfigData string = `
app:
  build_buddy_url: "http://localhost:8080"
database:
  data_source: "sqlite3://:memory:"
storage:
  enable_chunked_event_logs: true
cache:
  max_size_bytes: 1000000000  # 1 GB
executor:
  app_target: "grpc://localhost:1985"
  local_cache_size_bytes: 1000000000  # 1GB
  # Guarantee that we can fit at least one workflow task.
  # If we don't actually have the memory, we'll OOM, which is OK
  # for testing purposes.
  memory_bytes: 10_000_000_000
auth:
  oauth_providers:
    - issuer_url: 'https://auth.test.buildbuddy.io'
      client_id: 'test.buildbuddy.io'
      client_secret: 'buildbuddy'
  enable_anonymous_usage: true
remote_execution:
   enable_remote_exec: true
`

var lis *bufconn.Listener

func init() {
	vtprotocodec.Register()
}

// LocalGRPCServer starts a gRPC server with standard BuildBudy filters that uses an in-memory
// buffer for communication.
// Call LocalGRPCConn to get a connection to the returned server.
func LocalGRPCServer(te *real_environment.RealEnv) (*grpc.Server, func()) {
	if te.GetLocalBufconnListener() == nil {
		te.SetLocalBufconnListener(bufconn.Listen(1024 * 1024))
	}
	return GRPCServer(te, te.GetLocalBufconnListener())
}

func LocalGRPCConn(ctx context.Context, te *real_environment.RealEnv, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	bufDialer := func(context.Context, string) (net.Conn, error) {
		return te.GetLocalBufconnListener().Dial()
	}

	dialOptions := grpc_client.CommonGRPCClientOptions()
	dialOptions = append(dialOptions, grpc.WithContextDialer(bufDialer))
	dialOptions = append(dialOptions, grpc.WithInsecure())
	dialOptions = append(dialOptions, opts...)
	return grpc.DialContext(ctx, "bufnet", dialOptions...)
}

// gRPCServer starts a gRPC server with standard BuildBuddy filters that uses the given listener.
func GRPCServer(env environment.Env, lis net.Listener) (*grpc.Server, func()) {
	srv := grpc.NewServer(grpc_server.CommonGRPCServerOptions(env)...)
	runFunc := func() {
		if err := srv.Serve(lis); err != nil {
			log.Fatal(err.Error())
		}
	}
	return srv, runFunc
}

func GetTestEnv(t testing.TB) *real_environment.RealEnv {
	flags.PopulateFlagsFromData(t, testConfigData)
	testRootDir := testfs.MakeTempDir(t)
	if flag.Lookup("storage.disk.root_directory") != nil {
		flags.Set(t, "storage.disk.root_directory", fmt.Sprintf("%s/storage", testRootDir))
	}
	if flag.Lookup("cache.disk.root_directory") != nil {
		flags.Set(t, "cache.disk.root_directory", fmt.Sprintf("%s/cache", testRootDir))
	}
	if flag.Lookup("executor.root_directory") != nil {
		flags.Set(t, "executor.root_directory", fmt.Sprintf("%s/remote_execution/builds", testRootDir))
	}
	if flag.Lookup("executor.local_cache_directory") != nil {
		flags.Set(t, "executor.local_cache_directory", fmt.Sprintf("%s/remote_execution/cache", testRootDir))
	}

	healthChecker := healthcheck.NewHealthChecker("test")
	te := real_environment.NewRealEnv(healthChecker)
	c, err := memory_cache.NewMemoryCache(1000 * 1000 * 1000 /* 1GB */)
	if err != nil {
		t.Fatal(err)
	}
	te.SetCache(c)
	byte_stream_client.RegisterPooledBytestreamClient(te)

	switch *databaseType {
	case "sqlite":
		// do nothing, this is the default
	case "mysql":
		flags.Set(t, "database.data_source", testmysql.GetOrStart(t, *reuseServer))
	case "postgres":
		flags.Set(t, "database.data_source", testpostgres.GetOrStart(t, *reuseServer))
	default:
		t.Fatalf("Unsupported db type: %s", *databaseType)
	}
	dbHandle, err := db.GetConfiguredDatabase(context.Background(), te)
	if err != nil {
		t.Fatal(err)
	}
	te.SetDBHandle(dbHandle)
	te.SetInvocationDB(invocationdb.NewInvocationDB(te, dbHandle))

	if *useClickHouse {
		flags.Set(t, "olap_database.data_source", testclickhouse.GetOrStart(t, *reuseServer))
		err := clickhouse.Register(te)
		if err != nil {
			log.Fatalf("Error configuring ClickHouse: %s", err)
		}
	}
	bs, err := blobstore.GetConfiguredBlobstore(te)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	te.SetBlobstore(bs)
	te.SetAuthenticator(&nullauth.NullAuthenticator{})
	require.NoError(t, buildbuddy_server.Register(te))

	return te
}
