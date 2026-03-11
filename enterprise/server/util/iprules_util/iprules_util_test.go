package iprules_util_test

import (
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/iprules_util"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestCache_AddAndGet(t *testing.T) {
	flags.Set(t, "auth.ip_rules.cache_ttl", time.Minute)
	flags.Set(t, "auth.ip_rules.cache_size", 10)

	cache, err := iprules_util.NewCache()
	require.NoError(t, err)

	allowed := []*net.IPNet{mustParseCIDR(t, "1.2.3.0/24")}
	cache.Add("GR1", allowed)

	got, ok := cache.Get("GR1")
	require.True(t, ok)
	require.Equal(t, allowed, got)
}

func TestCache_ExpiredEntryIsRemoved(t *testing.T) {
	flags.Set(t, "auth.ip_rules.cache_ttl", time.Millisecond)
	flags.Set(t, "auth.ip_rules.cache_size", 10)

	cache, err := iprules_util.NewCache()
	require.NoError(t, err)

	cache.Add("GR1", []*net.IPNet{mustParseCIDR(t, "1.2.3.0/24")})
	time.Sleep(10 * time.Millisecond)

	got, ok := cache.Get("GR1")
	require.False(t, ok)
	require.Nil(t, got)
}

func TestCache_DisabledTTLReturnsNoopCache(t *testing.T) {
	flags.Set(t, "auth.ip_rules.cache_ttl", 0)
	flags.Set(t, "auth.ip_rules.cache_size", 10)

	cache, err := iprules_util.NewCache()
	require.NoError(t, err)

	cache.Add("GR1", []*net.IPNet{mustParseCIDR(t, "1.2.3.0/24")})

	got, ok := cache.Get("GR1")
	require.False(t, ok)
	require.Nil(t, got)
}

func TestCache_Eviction(t *testing.T) {
	flags.Set(t, "auth.ip_rules.cache_ttl", time.Minute)
	flags.Set(t, "auth.ip_rules.cache_size", 1)

	cache, err := iprules_util.NewCache()
	require.NoError(t, err)

	group1 := []*net.IPNet{mustParseCIDR(t, "1.2.3.0/24")}
	group2 := []*net.IPNet{mustParseCIDR(t, "4.5.6.0/24")}
	cache.Add("GR1", group1)
	cache.Add("GR2", group2)

	got1, ok1 := cache.Get("GR1")
	require.False(t, ok1)
	require.Nil(t, got1)

	got2, ok2 := cache.Get("GR2")
	require.True(t, ok2)
	require.Equal(t, group2, got2)
}

func mustParseCIDR(t *testing.T, cidr string) *net.IPNet {
	t.Helper()
	_, ipNet, err := net.ParseCIDR(cidr)
	require.NoError(t, err)
	return ipNet
}
