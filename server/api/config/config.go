package config

import "flag"

var (
	apiKey    = flag.String("api.api_key", "", "The default API key to use for on-prem enterprise deploys with a single organization/group.")
	enableAPI = flag.Bool("api.enable_api", false, "Whether or not to enable the BuildBuddy API.")
)

func Enabled() bool {
	return *enableAPI
}

func Key() string {
	return *apiKey
}
