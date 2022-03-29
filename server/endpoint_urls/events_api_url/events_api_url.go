package events_api_url

import (
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

var eventsAPIURL = flagutil.URLString("app.events_api_url", "", "Overrides the default build event protocol gRPC address shown by BuildBuddy on the configuration screen.")

func WithPath(path string) *url.URL {
	u, err := url.Parse(*eventsAPIURL)
	if err != nil {
		// Shouldn't happen, URLFlag should validate it.
		alert.UnexpectedEvent("flag app.events_api_url was not a parseable URL: %v", err)
	}
	if path == "" {
		return u
	}
	return u.ResolveReference(&url.URL{Path: path})
}

func String() string {
	return *eventsAPIURL
}
