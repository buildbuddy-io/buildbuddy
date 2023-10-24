package remote_exec_api_url

import (
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var remoteExecAPIURL = flag.URL("app.remote_execution_api_url", "", "Overrides the default remote execution protocol gRPC address shown by BuildBuddy on the configuration screen.")

func WithPath(path string) *url.URL {
	return remoteExecAPIURL.ResolveReference(&url.URL{Path: path})
}

func String() string {
	return remoteExecAPIURL.String()
}
