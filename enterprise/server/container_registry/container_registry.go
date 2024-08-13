package container_registry

import (
	"context"
	"flag"
	"net"
	"net/http"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/distribution/distribution/v3/configuration"
	registry_handlers "github.com/distribution/distribution/v3/registry/handlers"
)

const (
	v2Path = "/v2/"
)

var port = flag.Int("container_registry.port", 0, "The port to run the container registry on. If it is unspecified, the container registry will not run. The recommended port to run on is 443, as that is what `docker buildx` pushes to, regardless of port is specified to it.")

var _ http.Handler = ((*registryHandler)(nil))

type registry struct {
	server *http.Server
	handler *registryHandler
}

type registryHandler struct {
	registryApp *registry_handlers.App
}

func Register(env real_environment.RealEnv) error {
	if *port == 0 {
		// only set up the container registry if a port is specified.
		return nil
	}
	rs, err := NewRegistryServer(env.GetServerContext(), env.GetListenAddr())
	if err != nil {
		return status.InternalErrorf("Error initializing container registry: %s", err)
	}
	env.SetContainerRegistry(rs)
	return nil
}

func NewRegistryServer(ctx context.Context, listenHost string) (*registry, error) {
	rh := &registryHandler{
		registryApp: registry_handlers.NewApp(
			ctx,
			&configuration.Configuration{
				Version: configuration.Version("0.1"),
			},
		),
	}
	rs := &registry{
		server: &http.Server{
			Addr: net.JoinHostPort(listenHost, strconv.Itoa(*port)),
			// Handler: ,
		},
		handler: rh,
	}
	return rs, nil
}

func (r *registry) GetServer() *http.Server {
	return r.server
}

func (h *registryHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {

}

