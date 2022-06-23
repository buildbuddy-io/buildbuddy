package cache_api_url

import (
	"net/url"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
)

var cacheAPIURL = flagtypes.URLFromString("app.cache_api_url", "", "Overrides the default remote cache protocol gRPC address shown by BuildBuddy on the configuration screen.")

func WithPath(path string) *url.URL {
	return cacheAPIURL.ResolveReference(&url.URL{Path: path})
}

func String() string {
	return cacheAPIURL.String()
}
