package registry

import (
	"flag"
	"fmt"
	"io/ioutil"
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
		urlParts := strings.Split(req.URL.Path, "/")

		if len(urlParts) > 3 && urlParts[1] == "modules" && strings.HasPrefix(urlParts[3], "github.") {
			handleGitHub(w, req)
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

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 500, err
	}
	return body, resp.StatusCode, nil
}
