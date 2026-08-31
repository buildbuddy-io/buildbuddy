package relay

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestRelayTargetAllowed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		suffixes []string
		host     string
		want     bool
	}{
		{"no allowlist permits anything", nil, "anything.example.com", true},
		{"exact match", []string{"prod.buildbuddy.io"}, "prod.buildbuddy.io", true},
		{"subdomain match", []string{"prod.buildbuddy.io"}, "sjc-prod-abc.prod.buildbuddy.io", true},
		{"trailing dot in host", []string{"prod.buildbuddy.io"}, "sjc-prod-abc.prod.buildbuddy.io.", true},
		{"case insensitive", []string{"prod.buildbuddy.io"}, "SJC-Prod-ABC.Prod.BuildBuddy.io", true},
		{"leading dot in suffix", []string{".prod.buildbuddy.io"}, "abc.prod.buildbuddy.io", true},
		{"not a suffix", []string{"prod.buildbuddy.io"}, "evil.example.com", false},
		{"partial label is not a match", []string{"cluster.local"}, "svc.evil-cluster.local", false},
		{"suffix of a longer name", []string{"cluster.local"}, "notcluster.local", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "gateway.relay.allowed_target_suffixes", tc.suffixes)
			require.Equal(t, tc.want, relayTargetAllowed(tc.host))
		})
	}
}
