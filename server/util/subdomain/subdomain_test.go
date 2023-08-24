package subdomain_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestSetHost(t *testing.T) {
	flags.Set(t, "app.enable_subdomain_matching", true)
	flags.Set(t, "app.default_subdomains", []string{"app", "cache"})
	u, err := url.Parse("https://app.buildbuddy.io")
	require.NoError(t, err)
	flags.Set(t, "app.build_buddy_url", *u)

	getSubdomain := func(in string) string {
		return subdomain.Get(subdomain.SetHost(context.Background(), in))
	}

	require.Equal(t, "", getSubdomain("buildbuddy.io"))
	require.Equal(t, "sub", getSubdomain("sub.buildbuddy.io"))

	// Subdomains in default_subdomains list should not be returned.
	require.Equal(t, "", getSubdomain("app.buildbuddy.io"))
	require.Equal(t, "", getSubdomain("cache.buildbuddy.io"))

	// Subdomains on a different domain shouldn't match.
	require.Equal(t, "", getSubdomain("sub.notbuildbuddy.io"))
}
