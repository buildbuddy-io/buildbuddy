package registry

import (
	"flag"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	registryEnabled = flag.Bool("registry.enabled", false, "Whether the registry service should be enabled.")
	registryBackend = flag.String("registry.backend", "https://bcr.bazel.build/", "The registry backend to forward requests to")
)

type RegistryServer struct{}

func NewRegistryServer(realEnv *real_environment.RealEnv) *RegistryServer {
	return &RegistryServer{}
}

func Register(realEnv *real_environment.RealEnv) error {
	if !*registryEnabled {
		return nil
	}
	rs := NewRegistryServer(realEnv)
	realEnv.SetRegistryService(rs)
	return nil
}

func (t *RegistryServer) RegisterHandlers(mux interfaces.HttpServeMux) {
	log.Debug("Registry enabled")

	url, err := url.Parse(*registryBackend)
	if err != nil {
		log.Fatal(err.Error())
	}

	proxy := httputil.NewSingleHostReverseProxy(url)

	mux.Handle("/bazel_registry.json", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte("{}"))
	}))

	mux.Handle("/modules/", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		path := req.URL.Path
		pathPaths := strings.Split(path, "/")

		if len(pathPaths) > 3 && pathPaths[1] == "modules" && strings.HasPrefix(pathPaths[3], "github.") {
			buf, status, err := handleGitHub(path)
			w.WriteHeader(status)
			if err != nil {
				log.Errorf("error serving github module %s: %s", path, err)
			}
			w.Write(buf)
			return
		}

		if len(pathPaths) > 3 && pathPaths[1] == "modules" && strings.HasPrefix(pathPaths[3], "npm") {
			buf, status, err := handleNPM(path)
			w.WriteHeader(status)
			if err != nil {
				log.Errorf("error serving npm module %s: %s", path, err)
			}
			w.Write(buf)
			return
		}

		req.Host = req.URL.Host
		proxy.ServeHTTP(w, req)
	}))
}

func request(url string) ([]byte, int, error) {
	log.Debugf("fetching: %s", url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, 500, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 500, err
	}
	return body, resp.StatusCode, nil
}
