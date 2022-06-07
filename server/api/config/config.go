package config

import "flag"

var (
	enableAPI   = flag.Bool("api.enable_api", true, "Whether or not to enable the BuildBuddy API.")
	enableCache = flag.Bool("api.enable_cache", true, "Whether or not to enable the API cache.")
	apiKey      = flag.String("api.api_key", "", "The default API key to use for on-prem enterprise deploys with a single organization/group.")
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
