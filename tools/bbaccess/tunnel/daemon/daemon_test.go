package daemon

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config"
	"github.com/stretchr/testify/require"
)

func TestResolverDomains(t *testing.T) {
	cfg := config.Default()
	cfg.Zones = []config.Zone{
		{Suffix: "foo.bb.internal", Gateway: "a"},
		{Suffix: "bar.bb.internal", Gateway: "b"},
	}
	// Only the parent and the reverse zone: install must not depend on the
	// zone list, so that the zones can change afterwards.
	require.Equal(t, []string{"bb.internal", "18.198.in-addr.arpa"}, ResolverDomains(cfg))
	require.Equal(t, ResolverDomains(config.Default()), ResolverDomains(cfg))
}
