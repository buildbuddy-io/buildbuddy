package ssh

import (
	"io"
	"net"
	"testing"
	"time"

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

func TestParseForward(t *testing.T) {
	tests := []struct {
		name       string
		spec       string
		wantListen string
		wantDial   string
		wantErr    bool
	}{
		{name: "port host hostport", spec: "8080:localhost:80", wantListen: "127.0.0.1:8080", wantDial: "localhost:80"},
		{name: "explicit bind", spec: "0.0.0.0:8080:localhost:80", wantListen: "0.0.0.0:8080", wantDial: "localhost:80"},
		// Empty bind means "all interfaces" to OpenSSH; we keep loopback so a
		// forward is never exposed to the network without asking.
		{name: "empty bind stays loopback", spec: ":8080:localhost:80", wantListen: "127.0.0.1:8080", wantDial: "localhost:80"},
		{name: "ipv6 dial host", spec: "8080:[::1]:80", wantListen: "127.0.0.1:8080", wantDial: "[::1]:80"},
		{name: "ipv6 bind", spec: "[::1]:8080:localhost:80", wantListen: "[::1]:8080", wantDial: "localhost:80"},
		{name: "too few parts", spec: "8080:localhost", wantErr: true},
		{name: "too many parts", spec: "a:b:8080:localhost:80", wantErr: true},
		{name: "non-numeric listen port", spec: "http:localhost:80", wantErr: true},
		{name: "non-numeric dial port", spec: "8080:localhost:http", wantErr: true},
		{name: "port zero", spec: "0:localhost:80", wantErr: true},
		{name: "port out of range", spec: "8080:localhost:65536", wantErr: true},
		{name: "missing host", spec: "8080::80", wantErr: true},
		{name: "unbalanced brackets", spec: "8080:]::1[:80", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			listen, dial, err := parseForward(tc.spec)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantListen, listen)
			require.Equal(t, tc.wantDial, dial)
		})
	}
}

// TestForwardPropagatesEOF pins the half-close behavior that request/response
// protocols depend on: after the client half-closes, the far end must see EOF
// (so it can reply), and the reply must arrive in full.
func TestForwardPropagatesEOF(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	// The "far end" reads until EOF, then writes a reply and closes. It has to
	// be a real TCP conn: net.Pipe satisfies net.Conn but has no CloseWrite,
	// so a half-close could never reach it.
	far, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer far.Close()
	served := make(chan struct{})
	go func() {
		defer close(served)
		c, err := far.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		req, err := io.ReadAll(c)
		if err == nil {
			c.Write([]byte("reply-to:" + string(req)))
		}
	}()

	dial := func() (net.Conn, error) { return net.Dial("tcp", far.Addr().String()) }
	go forward(ln, dial, "test")

	c, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer c.Close()

	_, err = c.Write([]byte("ping"))
	require.NoError(t, err)
	// Half-close: without EOF propagation the far end's ReadAll never returns
	// and this test times out.
	require.NoError(t, c.(*net.TCPConn).CloseWrite())

	// Without the deadline a missing half-close hangs this read until the
	// package timeout, reporting a goroutine dump instead of a real failure.
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
	got, err := io.ReadAll(c)
	require.NoError(t, err, "reply should arrive before the deadline")
	require.Equal(t, "reply-to:ping", string(got), "reply should arrive complete")

	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("far end never observed EOF")
	}
}

// TestJoinRemoteCommand covers the "--" separator, which flag parsing does not
// consume because it stops at the target.
func TestJoinRemoteCommand(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{args: nil, want: ""},
		{args: []string{"make", "tests"}, want: "make tests"},
		{args: []string{"--", "make", "tests"}, want: "make tests"},
		{args: []string{"--"}, want: ""},
		// Only the leading separator is dropped; the rest is the command.
		{args: []string{"--", "ls", "--", "x"}, want: "ls -- x"},
		{args: []string{"echo", "--"}, want: "echo --"},
	}
	for _, tc := range tests {
		require.Equal(t, tc.want, JoinRemoteCommand(tc.args))
	}
}
