package config

import "flag"

var apiKey = flag.String("api.api_key", "", "The default API key to use for on-prem enterprise deploys with a single organization/group.")

func Key() string {
	return *apiKey
}
