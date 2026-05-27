package cache_proxy_auth

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var (
	apiKey = flag.String("cache_proxy.api_key", "", "API Key used to authorize the cache proxy with the BuildBuddy app server.", flag.Secret)
)

// APIKey returns the configured API key used to authenticate the cache proxy
// with the BuildBuddy app server.
func APIKey() string {
	return *apiKey
}
