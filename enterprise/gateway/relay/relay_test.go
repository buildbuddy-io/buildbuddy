package relay

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestRefusalForDialError(t *testing.T) {
	const host = "db.svc.cluster.local"
	const target = host + ":5432"
	for _, tc := range []struct {
		name    string
		err     error
		wantIs  func(error) bool
		wantMsg string
	}{
		{
			name:    "no such host is NotFound",
			err:     &net.DNSError{Err: "no such host", Name: host, IsNotFound: true},
			wantIs:  status.IsNotFoundError,
			wantMsg: `"` + host + `" does not resolve`,
		},
		{
			// The dial deadline expiring during resolution is what the Go
			// resolver reports when DNS is down or slow; it must not read as
			// a bad name.
			name:    "resolver timeout is DeadlineExceeded",
			err:     &net.DNSError{Err: "i/o timeout", Name: host, IsTimeout: true, UnwrapErr: context.DeadlineExceeded},
			wantIs:  status.IsDeadlineExceededError,
			wantMsg: "resolving",
		},
		{
			name:    "resolver failure is Unavailable",
			err:     &net.DNSError{Err: "server misbehaving", Name: host, IsTemporary: true},
			wantIs:  status.IsUnavailableError,
			wantMsg: "server misbehaving",
		},
		{
			name:    "connect timeout is DeadlineExceeded",
			err:     &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded},
			wantIs:  status.IsDeadlineExceededError,
			wantMsg: "dialing " + target,
		},
		{
			name:    "connection refused is Unavailable",
			err:     &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")},
			wantIs:  status.IsUnavailableError,
			wantMsg: "connection refused",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := refusalForDialError(tc.err, target)
			require.True(t, tc.wantIs(got), "got %v", got)
			require.Contains(t, status.Message(got), tc.wantMsg)
		})
	}
}

// tcpPair returns both ends of a loopback TCP connection.
func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	client, err = net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	server, err = ln.Accept()
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

func TestSplice_ErrorOnOneSideClosesTheOther(t *testing.T) {
	for _, tc := range []struct {
		name        string
		resetClient bool
	}{
		{"client reset closes upstream", true},
		{"upstream reset closes client", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, relayClientSide := tcpPair(t)
			relayUpstreamSide, upstream := tcpPair(t)

			done := make(chan struct{})
			go func() {
				splice(relayClientSide, relayUpstreamSide)
				close(done)
			}()

			// Reset one side rather than closing it cleanly: with SO_LINGER=0,
			// Close sends a RST, which the relay's read reports as an error
			// rather than EOF.
			broken, survivor := client, upstream
			if !tc.resetClient {
				broken, survivor = upstream, client
			}
			require.NoError(t, broken.(*net.TCPConn).SetLinger(0))
			require.NoError(t, broken.Close())

			// The survivor never sends anything and never closes on its own.
			// Were the relay to merely half-close it, the copy out of it would
			// block forever and splice would never return.
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("splice did not return after one side was reset")
			}
			survivor.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, err := survivor.Read(make([]byte, 1))
			require.ErrorIs(t, err, io.EOF, "the surviving side should see the relay close its connection")
		})
	}
}

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
