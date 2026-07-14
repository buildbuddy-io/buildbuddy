package git_trace2_test

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/git_trace2"
	"github.com/stretchr/testify/require"
)

func TestServerProcessesEventsFromMultipleConnections(t *testing.T) {
	// Git child processes open independent, short-lived connections. Verify
	// that valid events from both connections are drained during shutdown and
	// malformed lines do not prevent subsequent events from being processed.
	var mu sync.Mutex
	var sessionIDs []string
	server, err := git_trace2.NewServer(func(event git_trace2.Event) {
		mu.Lock()
		defer mu.Unlock()
		sessionIDs = append(sessionIDs, event.SessionID)
	})
	require.NoError(t, err)
	t.Cleanup(server.Stop)

	for _, trace := range []string{
		"not json\n" + `{"event":"start","sid":"parent","thread":"main"}` + "\n",
		`{"event":"data","sid":"parent/child","thread":"main","category":"progress","key":"total_bytes","value":"123"}` + "\n",
	} {
		const prefix = "af_unix:stream:"
		require.True(t, strings.HasPrefix(server.Target(), prefix))
		conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", strings.TrimPrefix(server.Target(), prefix))
		require.NoError(t, err)
		_, err = io.WriteString(conn, trace)
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}

	socketDir := filepath.Dir(strings.TrimPrefix(server.Target(), "af_unix:stream:"))
	server.Stop()
	server.Stop()

	mu.Lock()
	sort.Strings(sessionIDs)
	require.Equal(t, []string{"parent", "parent/child"}, sessionIDs)
	mu.Unlock()
	_, err = os.Stat(socketDir)
	require.ErrorIs(t, err, os.ErrNotExist)
}
