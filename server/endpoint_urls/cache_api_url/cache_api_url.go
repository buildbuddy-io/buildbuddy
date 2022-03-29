package cache_api_url

import (
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

var cacheAPIURL = flagutil.URLString("app.cache_api_url", "", "Overrides the default remote cache protocol gRPC address shown by BuildBuddy on the configuration screen.")

func WithPath(path string) *url.URL {
	u, err := url.Parse(*cacheAPIURL)
	if err != nil {
		// Shouldn't happen, URLFlag should validate it.
		alert.UnexpectedEvent("flag app.cache_api_url was not a parseable URL: %v", err)
	}
	if path == "" {
		return u
	}
	return u.ResolveReference(&url.URL{Path: path})
}

func String() string {
	return *cacheAPIURL
}
