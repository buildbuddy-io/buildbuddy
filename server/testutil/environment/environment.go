package environment

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"text/template"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pubsub"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	rpcfilters "github.com/buildbuddy-io/buildbuddy/server/rpc/filters"
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
auth:
  oauth_providers:
    - issuer_url: 'https://auth.test.buildbuddy.io'
      client_id: 'test.buildbuddy.io'
      client_secret: 'buildbuddy'
  enable_anonymous_usage: true
remote_execution:
   enable_remote_exec: true
`

type Options struct {
	RedisTarget string
}

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

func (te *TestEnv) LocalGRPCConn(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(te.bufDialer), grpc.WithInsecure())
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
			log.Fatal(err)
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

func GetTestEnv(t *testing.T) *TestEnv {
	return GetCustomTestEnv(t, &Options{})
}

func GetCustomTestEnv(t *testing.T, opts *Options) *TestEnv {
	testRootDir, err := ioutil.TempDir("/tmp", "buildbuddy_test_*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := os.RemoveAll(testRootDir)
		if err != nil {
			t.Fatal(err)
		}
	})
	tmpConfigFile, err := writeTmpConfigFile(testRootDir)
	if err != nil {
		t.Fatal(err)
	}
	configurator, err := config.NewConfigurator(tmpConfigFile)
	if err != nil {
		t.Fatal(err)
	}
	healthChecker := healthcheck.NewTestingHealthChecker()
	te := &TestEnv{
		RealEnv: real_environment.NewRealEnv(configurator, healthChecker),
	}

	var cache interfaces.Cache
	if opts.RedisTarget != "" {
		log.Print("Using redis cachhe")
		cache = redis.NewCache(opts.RedisTarget, healthChecker)
		te.SetPubSub(pubsub.NewPubSub(opts.RedisTarget))
	} else {
		log.Print("Using in-memory cache")
		memoryCache, err := memory_cache.NewMemoryCache(1000 * 1000 * 1000 /* 1GB */)
		if err != nil {
			assert.FailNowf(t, "could not initialize in-memory cache", err.Error())
		}
		cache = memoryCache
	}
	te.SetCache(cache)
	dbHandle, err := db.GetConfiguredDatabase(configurator, healthChecker)
	if err != nil {
		t.Fatal(err)
	}
	te.SetDBHandle(dbHandle)
	te.RealEnv.SetInvocationDB(invocationdb.NewInvocationDB(te, dbHandle))
	bs, err := blobstore.GetConfiguredBlobstore(configurator)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err)
	}
	te.RealEnv.SetBlobstore(bs)

	return te
}
