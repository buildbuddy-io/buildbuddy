package container_registry

import (
	"bytes"
	"context"
	"flag"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/container_registry/endpoints"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/distribution/distribution/v3/configuration"
	registry_handlers "github.com/distribution/distribution/v3/registry/handlers"
	_ "github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	"github.com/k0kubun/pp/v3"
	"gopkg.in/yaml.v3"
)

var (
	port          = flag.Int("container_registry.port", 0, "The port to run the container registry on. If it is unspecified, the container registry will not run. The recommended port to run on is 443, as that is what `docker buildx` pushes to, regardless of port is specified to it.")
	rootDirectory = flag.String("container_registry.disk.root_directory", "", "The directory to use for container registry storage.")
)

const (
	bb = "bb"
	cr = "cr"
)

type registry struct {
	server  *http.Server
	handler *registryHandler
}

func (r *registry) GetServer() *http.Server {
	return r.server
}

func (r *registry) GetRegistryHandler() interfaces.ContainerRegistryHandler {
	return r.handler
}

type registryHandler struct {
	registryApp *registry_handlers.App

	end2Handler   func(http.ResponseWriter, interfaces.CREnd2)
	end3Handler   func(http.ResponseWriter, interfaces.CREnd3)
	end4aHandler  func(http.ResponseWriter, interfaces.CREnd4a)
	end4bHandler  func(http.ResponseWriter, interfaces.CREnd4b)
	end5Handler   func(http.ResponseWriter, interfaces.CREnd5)
	end6Handler   func(http.ResponseWriter, interfaces.CREnd6)
	end7Handler   func(http.ResponseWriter, interfaces.CREnd7)
	end8aHandler  func(http.ResponseWriter, interfaces.CREnd8a)
	end8bHandler  func(http.ResponseWriter, interfaces.CREnd8b)
	end9Handler   func(http.ResponseWriter, interfaces.CREnd9)
	end10Handler  func(http.ResponseWriter, interfaces.CREnd10)
	end11Handler  func(http.ResponseWriter, interfaces.CREnd11)
	end12aHandler func(http.ResponseWriter, interfaces.CREnd12a)
	end12bHandler func(http.ResponseWriter, interfaces.CREnd12b)
	end13Handler  func(http.ResponseWriter, interfaces.CREnd13)
}

func (r *registryHandler) SetEnd2Handler(f func(http.ResponseWriter, interfaces.CREnd2)) {
	r.end2Handler = f
}

func (r *registryHandler) SetEnd3Handler(f func(http.ResponseWriter, interfaces.CREnd3)) {
	r.end3Handler = f
}

func (r *registryHandler) SetEnd4aHandler(f func(http.ResponseWriter, interfaces.CREnd4a)) {
	r.end4aHandler = f
}

func (r *registryHandler) SetEnd4bHandler(f func(http.ResponseWriter, interfaces.CREnd4b)) {
	r.end4bHandler = f
}

func (r *registryHandler) SetEnd5Handler(f func(http.ResponseWriter, interfaces.CREnd5)) {
	r.end5Handler = f
}

func (r *registryHandler) SetEnd6Handler(f func(http.ResponseWriter, interfaces.CREnd6)) {
	r.end6Handler = f
}

func (r *registryHandler) SetEnd7Handler(f func(http.ResponseWriter, interfaces.CREnd7)) {
	r.end7Handler = f
}

func (r *registryHandler) SetEnd8aHandler(f func(http.ResponseWriter, interfaces.CREnd8a)) {
	r.end8aHandler = f
}

func (r *registryHandler) SetEnd8bHandler(f func(http.ResponseWriter, interfaces.CREnd8b)) {
	r.end8bHandler = f
}

func (r *registryHandler) SetEnd9Handler(f func(http.ResponseWriter, interfaces.CREnd9)) {
	r.end9Handler = f
}

func (r *registryHandler) SetEnd10Handler(f func(http.ResponseWriter, interfaces.CREnd10)) {
	r.end10Handler = f
}

func (r *registryHandler) SetEnd11Handler(f func(http.ResponseWriter, interfaces.CREnd11)) {
	r.end11Handler = f
}

func (r *registryHandler) SetEnd12aHandler(f func(http.ResponseWriter, interfaces.CREnd12a)) {
	r.end12aHandler = f
}

func (r *registryHandler) SetEnd12bHandler(f func(http.ResponseWriter, interfaces.CREnd12b)) {
	r.end12bHandler = f
}

func (r *registryHandler) SetEnd13Handler(f func(http.ResponseWriter, interfaces.CREnd13)) {
	r.end13Handler = f
}

func Register(env *real_environment.RealEnv) error {
	if *port == 0 {
		// only set up the container registry if a port is specified.
		return nil
	}
	rs, err := NewRegistryServer(env.GetServerContext(), env.GetListenAddr())
	if err != nil {
		return status.InternalErrorf("Error initializing container registry: %s", err)
	}
	go rs.GetServer().ListenAndServe()
	env.SetContainerRegistry(rs)
	return nil
}

func NewRegistryServer(ctx context.Context, listenHost string) (*registry, error) {
	addr := net.JoinHostPort(listenHost, strconv.Itoa(*port))
	u := build_buddy_url.WithPath("")
	u.Host = net.JoinHostPort(u.Hostname(), strconv.Itoa(*port))
	containerRegistryHost := u.String()
	type section map[string]any
	cfgMap := section{
		"version": "0.1",
		"storage": section{
			"filesystem": section{
				"rootdirectory": *rootDirectory,
			},
		},
		"http": section{
			"addr": addr,
			"host": containerRegistryHost,
		},
	}
	b, err := yaml.Marshal(cfgMap)
	if err != nil {
		return nil, err
	}
	cfg, err := configuration.Parse(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	rh := &registryHandler{
		registryApp: registry_handlers.NewApp(
			ctx,
			cfg,
		),
	}
	rs := &registry{
		server: &http.Server{
			Addr:    addr,
			Handler: rh,
		},
		handler: rh,
	}
	return rs, nil
}

func (h *registryHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	log.Debugf("Container registry request:\n%s", pp.Sprint(req.WithContext(context.Background())))
	u, err := url.ParseRequestURI(req.RequestURI)
	if err != nil {
		log.Debugf("Failed to parse container registry RequestURI. RequestURI: %s, Error: %s", req.RequestURI, err)
		resp.WriteHeader(http.StatusBadRequest)
	}
	e := endpoints.Endpoint(req.Method, u)
	if e == nil {
		log.Debugf("Bad request made to container registry. Method: %s, RequestURI: %s", req.Method, req.RequestURI)
		resp.WriteHeader(http.StatusBadRequest)
	}
	if e.Name() == "" || strings.HasPrefix(e.Name(), cr+"/") {
		h.registryApp.ServeHTTP(resp, req)
	} else if strings.HasPrefix(e.Name(), bb+"/") {
		switch e := e.(type) {
		case *endpoints.End1:
			// we depend on the normal registryApp for this endpoint, this code should never be reached.
			log.Errorf("Tried to access end-1 on the bb-specific container registry; this call should have been intercepted by the standard registry.")
		case *endpoints.End2:
			// Must be implemented for pull
			if h.end2Handler != nil {
				h.end2Handler(resp, e)
				return
			}
			log.Errorf("end-2 is not implemented on the bb-specific container registry; this endpoint is required for pull, which is the minimum a container registry may implement.")
		case *endpoints.End3:
			// Must be implemented for pull
			if h.end3Handler != nil {
				h.end3Handler(resp, e)
				return
			}
			log.Errorf("end-3 is not implemented on the bb-specific container registry; this endpoint is required for pull, which is the minimum a container registry may implement.")
		case *endpoints.End4a:
			if h.end4aHandler != nil {
				h.end4aHandler(resp, e)
				return
			}
		case *endpoints.End4b:
			if h.end4bHandler != nil {
				h.end4bHandler(resp, e)
				return
			}
		case *endpoints.End5:
			if h.end5Handler != nil {
				h.end5Handler(resp, e)
				return
			}
		case *endpoints.End6:
			if h.end6Handler != nil {
				h.end6Handler(resp, e)
				return
			}
		case *endpoints.End7:
			if h.end7Handler != nil {
				h.end7Handler(resp, e)
				return
			}
		case *endpoints.End8a:
			if h.end8aHandler != nil {
				h.end8aHandler(resp, e)
				return
			}
		case *endpoints.End8b:
			if h.end8bHandler != nil {
				h.end8bHandler(resp, e)
				return
			}
		case *endpoints.End9:
			if h.end9Handler != nil {
				h.end9Handler(resp, e)
				return
			}
		case *endpoints.End10:
			if h.end10Handler != nil {
				h.end10Handler(resp, e)
				return
			}
		case *endpoints.End11:
			if h.end11Handler != nil {
				h.end11Handler(resp, e)
				return
			}
		case *endpoints.End12a:
			if h.end12aHandler != nil {
				h.end12aHandler(resp, e)
				return
			}
		case *endpoints.End12b:
			if h.end12bHandler != nil {
				h.end12bHandler(resp, e)
				return
			}
		case *endpoints.End13:
			if h.end13Handler != nil {
				h.end13Handler(resp, e)
				return
			}
		}
		resp.WriteHeader(http.StatusNotImplemented)
	} else {
		// only support names beginning with cr/ or bb/
		resp.WriteHeader(http.StatusBadRequest)
	}
}
