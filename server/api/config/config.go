package config

import (
	"flag"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

var (
	enableAPI   = flag.Bool("api.enable_api", true, "Whether or not to enable the BuildBuddy API.")
	enableCache = flag.Bool("api.enable_cache", false, "Whether or not to enable the API cache.")
	apiKey      = flagutil.New("api.api_key", "", "The default API key to use for on-prem enterprise deploys with a single organization/group.", flagutil.SecretTag)
)

func Key() string {
	return *apiKey
}

func APIEnabled() bool {
	return *enableAPI
}

func CacheEnabled() bool {
	return *enableCache
}
