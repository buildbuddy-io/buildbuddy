package cache_api_url

import (
	"flag"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

var cacheAPIURL string

func init() {
	flag.Var(flagutil.NewURLFlag(&cacheAPIURL), "app.cache_api_url", "Overrides the default remote cache protocol gRPC address shown by BuildBuddy on the configuration screen.")
}

func CacheAPIURL(path string) *url.URL {
	u, err := url.Parse(cacheAPIURL)
	if err != nil {
		// Shouldn't happen, URLFlag should validate it.
		alert.UnexpectedEvent("flag app.cache_api_url was not a parseable URL: %v", err)
	}
	if path == "" {
		return u
	}
	return u.ResolveReference(&url.URL{Path: path})
}

func CacheAPIURLString() string {
	return cacheAPIURL
}
