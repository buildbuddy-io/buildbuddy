package devserverproxy

import (
	"flag"
	"net/http"
	"net/http/httputil"
	"net/url"
)

var (
	devServerUrl = flag.String("dev_server_url", "http://localhost:8082", "host and port that the dev server is expected to be running on")
)

func NewDevServerProxy() (http.Handler, error) {
	url, err := url.Parse(*devServerUrl)
	if err != nil {
		return nil, err
	}
	return httputil.NewSingleHostReverseProxy(url), nil
}
