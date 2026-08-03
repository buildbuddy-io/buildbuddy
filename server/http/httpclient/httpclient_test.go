package httpclient_test

import (
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/stretchr/testify/require"
)

func TestResolveHostIPs(t *testing.T) {
	_, loopbackNet, err := net.ParseCIDR("127.0.0.0/8")
	require.NoError(t, err)

	resolvedIPs, err := httpclient.ResolveHostIPs(
		t.Context(), "localhost", []*net.IPNet{loopbackNet},
	)
	require.NoError(t, err)
	require.NotEmpty(t, resolvedIPs)
	for _, ip := range resolvedIPs {
		require.True(t, loopbackNet.Contains(ip))
	}
}

func TestResolveHostIPsRejectsPrivateAddress(t *testing.T) {
	resolvedIPs, err := httpclient.ResolveHostIPs(t.Context(), "localhost", nil)
	require.Nil(t, resolvedIPs)
	require.EqualError(t, err, `host "localhost" has no allowed IP addresses`)
}
