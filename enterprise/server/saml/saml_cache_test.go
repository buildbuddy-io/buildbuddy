package saml

import (
	"testing"

	"github.com/crewjam/saml/samlsp"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestSAMLProviderCacheExpiresEntries(t *testing.T) {
	clock := clockwork.NewFakeClock()
	cache, err := newSAMLProviderCache(clock)
	require.NoError(t, err)

	provider := &samlsp.Middleware{}
	cache.Add("slug", provider)

	got, ok := cache.Get("slug")
	require.True(t, ok)
	require.Same(t, provider, got)

	clock.Advance(samlProviderCacheTTL)
	_, ok = cache.Get("slug")
	require.False(t, ok)
}
