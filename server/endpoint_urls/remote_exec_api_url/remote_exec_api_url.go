package remote_exec_api_url

import (
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

var remoteExecAPIURL = flagutil.URLString("app.remote_execution_api_url", "", "Overrides the default remote execution protocol gRPC address shown by BuildBuddy on the configuration screen.")

func WithPath(path string) *url.URL {
	u, err := url.Parse(*remoteExecAPIURL)
	if err != nil {
		// Shouldn't happen, URLFlag should validate it.
		alert.UnexpectedEvent("flag app.remote_execution_api_url was not a parseable URL: %v", err)
	}
	if path == "" {
		return u
	}
	return u.ResolveReference(&url.URL{Path: path})
}

func String() string {
	return *remoteExecAPIURL
}
