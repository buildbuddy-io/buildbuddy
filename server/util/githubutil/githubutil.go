package githubutil

import "github.com/buildbuddy-io/buildbuddy/server/util/flag"

var (
	enterpriseHost = flag.String("github.enterprise_host", "", "The Github enterprise hostname to use if using GitHub enterprise server, not including https:// and no trailing slash.", flag.Secret)
)

func IsEnterpriseConfigured() bool {
	return *enterpriseHost != ""
}

func Host() string {
	if IsEnterpriseConfigured() {
		return *enterpriseHost
	}
	return "github.com"
}

func APIEndpoint() string {
	if IsEnterpriseConfigured() {
		return *enterpriseHost + "/api/v3"
	}
	return "api.github.com"
}
