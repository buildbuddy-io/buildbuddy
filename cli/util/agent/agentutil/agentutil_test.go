package agentutil

import (
	"errors"
	"strings"
	"testing"

	"github.com/armon/circbuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatCommandErrorIncludesStderr(t *testing.T) {
	stderr, err := circbuf.NewBuffer(StderrTailBytes)
	require.NoError(t, err)
	_, err = stderr.Write([]byte("useful diagnostic\n"))
	require.NoError(t, err)

	commandErr := FormatCommandError("agent", errors.New("exit status 1"), stderr)
	assert.EqualError(t, commandErr, "agent failed: exit status 1\nuseful diagnostic")
}

func TestFormatCommandErrorReportsTruncatedTail(t *testing.T) {
	stderr, err := circbuf.NewBuffer(StderrTailBytes)
	require.NoError(t, err)
	_, err = stderr.Write([]byte("discarded" + strings.Repeat("x", int(StderrTailBytes)) + "tail"))
	require.NoError(t, err)

	commandErr := FormatCommandError("agent", errors.New("exit status 1"), stderr)
	assert.NotContains(t, commandErr.Error(), "discarded")
	assert.Contains(t, commandErr.Error(), "stderr truncated to last")
	assert.True(t, strings.HasSuffix(commandErr.Error(), "tail"))
}
