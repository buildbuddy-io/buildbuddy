package test_environment

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
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
    root_directory: /tmp/buildbuddy_cache`

type TestEnv struct {
	*real_environment.RealEnv
	lis *bufconn.Listener
}

func (te *TestEnv) bufDialer(context.Context, string) (net.Conn, error) {
	return te.lis.Dial()
}

func (te *TestEnv) LocalGRPCServer() (*grpc.Server, func()) {
	te.lis = bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
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
	c, err := memory_cache.NewMemoryCache(1024 * 1024 * 10 /* 10MB */)
	if err != nil {
		return nil, err
	}
	te.SetCache(c)
	te.SetDigestCache(digest.NewDigestCache(c))
	return te, nil
}
