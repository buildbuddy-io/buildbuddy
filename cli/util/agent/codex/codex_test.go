package codex

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseResponse(t *testing.T) {
	output := strings.Join([]string{
		`{"type":"thread.started","thread_id":"1234"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.completed","item":{"type":"agent_message","text":"Working..."}}`,
		`{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"Analysis complete."}}`,
		`{"type":"turn.completed"}`,
	}, "\n")

	got, err := parseResponse([]byte(output))
	require.NoError(t, err)

	// If there are multiple agent messages, return only the last one.
	require.Equal(t, "Analysis complete.", got.Output)
	require.Equal(t, "1234", got.SessionID)
	require.Equal(t, "codex exec resume 1234", got.ResumeCommand)
}
