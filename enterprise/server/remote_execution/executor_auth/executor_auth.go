package executor_auth

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var (
	apiKey = flag.String("executor.api_key", "", "API Key used to authorize the executor with the BuildBuddy app server.", flag.Secret)
)

// APIKey returns the configured API key used to authenticate the executor with
// the BuildBuddy app server.
func APIKey() string {
	return *apiKey
}
