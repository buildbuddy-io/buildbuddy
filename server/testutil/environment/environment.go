package environment

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	rpcfilters "github.com/buildbuddy-io/buildbuddy/server/rpc/filters"
)

const testConfigFileContents string = `
app:
  build_buddy_url: "http://localhost:8080"
database:
  data_source: "sqlite3://:memory:"
storage:
  disk:
    root_directory: /tmp/buildbuddy
cache:
  max_size_bytes: 1000000000  # 1 GB
  #in_memory: true
  disk:
    root_directory: /tmp/buildbuddy_cache
executor:
  root_directory: "/tmp/remote_build"
  app_target: "grpc://localhost:1985"
  local_cache_directory: "/tmp/filecache"
  local_cache_size_bytes: 1000000000  # 1GB
`

type TestEnv struct {
	*real_environment.RealEnv
	lis *bufconn.Listener
}

func (te *TestEnv) bufDialer(context.Context, string) (net.Conn, error) {
	return te.lis.Dial()
}

func (te *TestEnv) LocalGRPCServer() (*grpc.Server, func()) {
	te.lis = bufconn.Listen(1024 * 1024)
	grpcOptions := []grpc.ServerOption{
		rpcfilters.GetUnaryInterceptor(te),
		rpcfilters.GetStreamInterceptor(te),
	}
	srv := grpc.NewServer(grpcOptions...)
	runFunc := func() {
		if err := srv.Serve(te.lis); err != nil {
			log.Fatal(err)
		}
	}
	return srv, runFunc
}

func (te *TestEnv) LocalGRPCConn(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(te.bufDialer), grpc.WithInsecure())
}

func writeTmpConfigFile() (string, error) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "config-*.yaml")
	if err != nil {
		return "", err
	}
	if _, err := tmpFile.Write([]byte(testConfigFileContents)); err != nil {
		return "", err
	}
	return tmpFile.Name(), nil
}

func GetTestEnv() (*TestEnv, error) {
	tmpConfigFile, err := writeTmpConfigFile()
	if err != nil {
		return nil, err
	}
	configurator, err := config.NewConfigurator(tmpConfigFile)
	if err != nil {
		return nil, err
	}
	healthChecker := healthcheck.NewHealthChecker("test-server-type")
	te := &TestEnv{
		RealEnv: real_environment.NewRealEnv(configurator, healthChecker),
	}
	c, err := memory_cache.NewMemoryCache(1000 * 1000 * 1000 /* 1GB */)
	if err != nil {
		return nil, err
	}
	te.SetCache(c)
	dbHandle, err := db.GetConfiguredDatabase(configurator)
	te.SetDBHandle(dbHandle)

	return te, nil
}
