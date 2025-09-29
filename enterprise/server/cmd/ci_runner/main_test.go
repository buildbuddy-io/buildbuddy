package main

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestInvocationLog() (*invocationLog, *bytes.Buffer) {
	invLog := &invocationLog{}
	invLog.writeListener = func(string) {}
	buf := &bytes.Buffer{}
	invLog.writer = io.MultiWriter(&invLog.LockingBuffer, buf)
	return invLog, buf
}

func TestInvocationLogRedactsRemoteExecHeaderAcrossWrites(t *testing.T) {
	invLog, buf := newTestInvocationLog()

	firstChunk := "common --remote_exec_header=secret-token"
	n, err := invLog.Write([]byte(firstChunk))
	require.NoError(t, err)
	require.Equal(t, len(firstChunk), n)
	require.Equal(t, "", buf.String(), "should not flush before newline")

	secondChunk := "-continued\n"
	n, err = invLog.Write([]byte(secondChunk))
	require.NoError(t, err)
	require.Equal(t, len(secondChunk), n)
	require.Equal(t, "common --remote_exec_header=<REDACTED>\n", buf.String())
}

func TestInvocationLogRedactsMultipleLines(t *testing.T) {
	invLog, buf := newTestInvocationLog()

	chunk := "line1 --remote_exec_header=first-secret\nline2 --remote_exec_header=second-secret\n"
	n, err := invLog.Write([]byte(chunk))
	require.NoError(t, err)
	require.Equal(t, len(chunk), n)
	require.Equal(t,
		"line1 --remote_exec_header=<REDACTED>\nline2 --remote_exec_header=<REDACTED>\n",
		buf.String())
}
