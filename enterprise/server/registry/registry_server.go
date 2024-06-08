package registry

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	registryPort    = flag.Int("registry_port", 0, "The port on which to listen for registry requests")
	registryBackend = flag.String("registry_backend", "https://bcr.bazel.build/", "The registry backend to forward requests to")
)

type RegistryServer struct{}

func NewRegistryServer(env environment.Env, h interfaces.DBHandle) *RegistryServer {
	return &RegistryServer{}
}

func (t *RegistryServer) Start() {
	if *registryPort <= 0 {
		log.Debug("Registry server disabled")
		return
	}
	log.Debug("Registry server enabled")

	url, err := url.Parse(*registryBackend)
	if err != nil {
		log.Fatal(err.Error())
	}

	proxy := httputil.NewSingleHostReverseProxy(url)

	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
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
	})

	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", *registryPort), nil)
		if err != nil {
			log.Fatal(err.Error())
		}
	}()
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
