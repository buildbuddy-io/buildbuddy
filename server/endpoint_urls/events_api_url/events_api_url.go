package events_api_url

import (
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var eventsAPIURL = flag.URL("app.events_api_url", "", "Overrides the default build event protocol gRPC address shown by BuildBuddy on the configuration screen.")

func WithPath(path string) *url.URL {
	return eventsAPIURL.ResolveReference(&url.URL{Path: path})
}

func String() string {
	return eventsAPIURL.String()
}
