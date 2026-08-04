package ssh

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTarget(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		userFlag string
		portFlag int
		wantHost string
		wantUser string
		wantPort int
	}{
		{name: "bare host", target: "box1", portFlag: 22, wantHost: "box1", wantPort: 22},
		{name: "user at host", target: "alice@box1", portFlag: 22, wantHost: "box1", wantUser: "alice", wantPort: 22},
		{name: "user flag", target: "box1", userFlag: "bob", portFlag: 22, wantHost: "box1", wantUser: "bob", wantPort: 22},
		{name: "user at host beats user flag", target: "alice@box1", userFlag: "bob", portFlag: 22, wantHost: "box1", wantUser: "alice", wantPort: 22},
		{name: "host with port", target: "box1:2222", portFlag: 22, wantHost: "box1", wantPort: 2222},
		{name: "non-numeric port keeps host intact", target: "box1:2222x", portFlag: 22, wantHost: "box1:2222x", wantPort: 22},
		{name: "ipv6 with port", target: "[fd00:bb::2]:2222", portFlag: 22, wantHost: "fd00:bb::2", wantPort: 2222},
		{name: "bare ipv6 keeps port flag", target: "fd00:bb::2", portFlag: 22, wantHost: "fd00:bb::2", wantPort: 22},
		{name: "bb-ssh url", target: "bb-ssh://[fd00:bb::2]:2022", portFlag: 22, wantHost: "fd00:bb::2", wantPort: 2022},
		{name: "bb-ssh url with user", target: "bb-ssh://alice@[fd00:bb::2]:2022", portFlag: 22, wantHost: "fd00:bb::2", wantUser: "alice", wantPort: 2022},
		{name: "bb-ssh url without port", target: "bb-ssh://box1", portFlag: 22, wantHost: "box1", wantPort: 22},
		// The exact form the server prints in its "Listening on" log line.
		{name: "bb-ssh url with name query", target: "bb-ssh://[fd00:bb::2]:2022?name=box1", portFlag: 22, wantHost: "fd00:bb::2", wantPort: 2022},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host, user, port := parseTarget(tc.target, tc.userFlag, tc.portFlag)
			require.Equal(t, tc.wantHost, host)
			require.Equal(t, tc.wantUser, user)
			require.Equal(t, tc.wantPort, port)
		})
	}
}

// TestFlagParsingStopsAtFirstPositional pins the property that makes
// `bb ssh box claude --continue` pass --continue to the remote command
// rather than parsing it as a bb flag.
func TestFlagParsingStopsAtFirstPositional(t *testing.T) {
	t.Cleanup(func() { flags.Set("p", "22"); flags.Set("t", "false") })
	require.NoError(t, flags.Parse([]string{"-p", "2222", "box1", "claude", "--continue", "-t"}))
	require.Equal(t, []string{"box1", "claude", "--continue", "-t"}, flags.Args())
	require.Equal(t, 2222, *port)
	require.False(t, *forceTTY, "-t after the positional must not set the local flag")
}

// TestFlagParsingDoubleDash pins `--` as the flag terminator, allowing
// targets that begin with a dash.
func TestFlagParsingDoubleDash(t *testing.T) {
	t.Cleanup(func() { flags.Set("p", "22"); flags.Set("t", "false") })
	require.NoError(t, flags.Parse([]string{"--", "-weird-host", "echo", "hi"}))
	require.Equal(t, []string{"-weird-host", "echo", "hi"}, flags.Args())
}
