package config

import (
	"flag"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

var (
	enableAPI   = flag.Bool("api.enable_api", true, "Whether or not to enable the BuildBuddy API.")
	enableCache = flag.Bool("api.enable_cache", false, "Whether or not to enable the API cache.")
	apiKey      = flagutil.New(
		"api.api_key",
		"",
		"The default API key to use for on-prem enterprise deploys with a single organization/group.",
		flagutil.SecretTag,
		flagutil.DeprecatedTag(
			"Manual API key specification is no longer supported; to retrieve specific API keys programmatically, please use the API key table. This field will still specify an API key to redact in case a manual API key was specified when buildbuddy was first set up.",
		),
	)
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
