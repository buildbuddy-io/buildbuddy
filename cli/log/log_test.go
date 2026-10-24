package log

import (
	"bytes"
	stdlog "log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetQuiet(t *testing.T) {
	var out bytes.Buffer
	stdlog.SetOutput(&out)
	t.Cleanup(func() {
		stdlog.SetOutput(os.Stderr)
		SetQuiet(false)
	})

	SetQuiet(true)
	Printf("progress")
	// A quiet run still has to say when something went wrong.
	Warnf("something went wrong")

	require.NotContains(t, out.String(), "progress")
	require.Contains(t, out.String(), "something went wrong")

	SetQuiet(false)
	Printf("progress")

	require.Contains(t, out.String(), "progress")
}
