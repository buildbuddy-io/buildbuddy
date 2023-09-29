package region

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IsRegionalServer(t *testing.T) {
	for _, testCase := range []struct {
		url     string
		match   bool
		regions []Region
	}{
		{"https://subdomain.buildbuddy.io", true, []Region{{Subdomains: "https://*.buildbuddy.io"}}},
		{"https://subdomain-wish-dashes.buildbuddy.io", true, []Region{{Subdomains: "https://*.buildbuddy.io"}}},
		{"https://subdomain.europe.buildbuddy.io", true, []Region{{Subdomains: "https://*.europe.buildbuddy.io"}}},
		{"https://evilbuildbuddy.io", false, []Region{{Subdomains: "https://*.buildbuddy.io"}}},
		{"https://subdomain.evilbuildbuddy.io", false, []Region{{Subdomains: "https://*.buildbuddy.io"}}},
		{"https:///subdomain.buildbuddy.io", false, []Region{{Subdomains: "https://*.buildbuddy.io"}}},
		{"subdomain.buildbuddy.io", false, []Region{{Subdomains: "https://*.buildbuddy.io"}}},
		{"http://subdomain.buildbuddy", false, []Region{{Subdomains: "https://*.buildbuddy.io"}}},
		{"http://subdomain.buildbuddy.io.foo", false, []Region{{Subdomains: "https://*.buildbuddy.io"}}},
	} {
		assert.Equal(t, testCase.match, isRegionalServer(testCase.regions, testCase.url))
	}
}
