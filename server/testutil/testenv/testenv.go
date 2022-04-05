package testenv

import (
	"bytes"
	"context"
	"flag"
	"io/ioutil"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"text/template"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmysql"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	rpcfilters "github.com/buildbuddy-io/buildbuddy/server/rpc/filters"
)

var (
	useMySQL = flag.Bool("testenv.use_mysql", false, "Whether to use MySQL instead of sqlite for tests.")

	configureLoggerOnce sync.Once
)

type ConfigTemplateParams struct {
	TestRootDir string
}

const testConfigFileTemplate string = `
app:
  build_buddy_url: "http://localhost:8080"
database:
  data_source: "sqlite3://:memory:"
storage:
  enable_chunked_event_logs: true
  disk:
    root_directory: "{{.TestRootDir}}/storage"
cache:
  max_size_bytes: 1000000000  # 1 GB
  disk:
    root_directory: "{{.TestRootDir}}/cache"
executor:
  root_directory: "{{.TestRootDir}}/remote_execution/builds"
  app_target: "grpc://localhost:1985"
  local_cache_directory: "{{.TestRootDir}}/remote_execution/cache"
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

type TestEnv struct {
	*real_environment.RealEnv
	lis *bufconn.Listener
}

func (te *TestEnv) bufDialer(context.Context, string) (net.Conn, error) {
	return te.lis.Dial()
}

// LocalGRPCServer starts a gRPC server with standard BuildBudy filters that uses an in-memory
// buffer for communication.
// Call LocalGRPCConn to get a connection to the returned server.
func (te *TestEnv) LocalGRPCServer() (*grpc.Server, func()) {
	te.lis = bufconn.Listen(1024 * 1024)
	return te.GRPCServer(te.lis)
}

func (te *TestEnv) LocalGRPCConn(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	dialOptions := grpc_client.CommonGRPCClientOptions()
	dialOptions = append(dialOptions, grpc.WithContextDialer(te.bufDialer))
	dialOptions = append(dialOptions, grpc.WithInsecure())
	dialOptions = append(dialOptions, opts...)
	return grpc.DialContext(ctx, "bufnet", dialOptions...)
}

// GRPCServer starts a gRPC server with standard BuildBuddy filters that uses the given listener.
func (te *TestEnv) GRPCServer(lis net.Listener) (*grpc.Server, func()) {
	grpcOptions := []grpc.ServerOption{
		rpcfilters.GetUnaryInterceptor(te),
		rpcfilters.GetStreamInterceptor(te),
	}
	srv := grpc.NewServer(grpcOptions...)
	runFunc := func() {
		if err := srv.Serve(lis); err != nil {
			log.Fatal(err.Error())
		}
	}
	return srv, runFunc
}

func writeTmpConfigFile(testRootDir string) (string, error) {
	tmpl, err := template.New("config").Parse(testConfigFileTemplate)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, &ConfigTemplateParams{TestRootDir: testRootDir}); err != nil {
		return "", err
	}
	configPath := filepath.Join(testRootDir, "config.yaml")
	if err := ioutil.WriteFile(configPath, buf.Bytes(), 0644); err != nil {
		return "", err
	}
	return configPath, nil
}

// All instances of the configurator use the same config struct instance in order to support flag overrides.
// We instantiate a single configurator per test to avoid triggering the race detector.
var currentConfigurator *config.Configurator

func GetTestEnv(t testing.TB) *TestEnv {

	configureLoggerOnce.Do(func() {
		*log.LogLevel = "debug"
		*log.IncludeShortFileName = true
		log.Configure()
	})

	testRootDir := testfs.MakeTempDir(t)
	tmpConfigFile, err := writeTmpConfigFile(testRootDir)
	if err != nil {
		t.Fatal(err)
	}
	if currentConfigurator == nil {
		configurator, err := config.ParseAndReconcileFlagsAndConfig(tmpConfigFile)
		if err != nil {
			t.Fatal(err)
		}
		currentConfigurator = configurator
		t.Cleanup(func() {
			currentConfigurator = nil
		})
	}
	healthChecker := healthcheck.NewHealthChecker("test")
	te := &TestEnv{
		RealEnv: real_environment.NewRealEnv(currentConfigurator, healthChecker),
	}
	c, err := memory_cache.NewMemoryCache(1000 * 1000 * 1000 /* 1GB */)
	if err != nil {
		t.Fatal(err)
	}
	te.SetCache(c)

	if *useMySQL {
		flags.Set(t, "database.data_source", testmysql.GetOrStart(t))
	}
	currentConfigurator.ReconcileFlagsAndConfig()
	dbHandle, err := db.GetConfiguredDatabase(healthChecker)
	if err != nil {
		t.Fatal(err)
	}
	te.SetDBHandle(dbHandle)
	te.RealEnv.SetInvocationDB(invocationdb.NewInvocationDB(te, dbHandle))
	bs, err := blobstore.GetConfiguredBlobstore(te)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	te.RealEnv.SetBlobstore(bs)
	te.RealEnv.SetAuthenticator(&nullauth.NullAuthenticator{})

	return te
}
