package remote_exec_api_url

import (
	"flag"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

var remoteExecAPIURL string

func init() {
	flag.Var(flagutil.NewURLFlag(&remoteExecAPIURL), "app.remote_execution_api_url", "The external URL where your BuildBuddy instance can be found.")
}

func RemoteExecAPIURL(path string) *url.URL {
	u, err := url.Parse(remoteExecAPIURL)
	if err != nil {
		// Shouldn't happen, URLFlag should validate it.
		alert.UnexpectedEvent("flag app.remote_execution_api_url was not a parseable URL: %v", err)
	}
	if path == "" {
		return u
	}
	return u.ResolveReference(&url.URL{Path: path})
}

func RemoteExecAPIURLString() string {
	return remoteExecAPIURL
}
