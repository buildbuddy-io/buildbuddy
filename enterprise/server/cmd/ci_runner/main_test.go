package main

import (
	"bytes"
	"errors"
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

func TestInvocationLogFlushesPartialLine(t *testing.T) {
	invLog, buf := newTestInvocationLog()

	chunk := "common --remote_exec_header=secret-token"
	n, err := invLog.Write([]byte(chunk))
	require.NoError(t, err)
	require.Equal(t, len(chunk), n)
	require.Equal(t, "", buf.String(), "should not flush before newline")

	require.NoError(t, invLog.Flush())
	require.Equal(t, "common --remote_exec_header=<REDACTED>", buf.String())
}

func TestInvocationLogHandlesCRLF(t *testing.T) {
	invLog, buf := newTestInvocationLog()

	chunk := "line1 --remote_exec_header=secret\r\nline2\r\n"
	n, err := invLog.Write([]byte(chunk))
	require.NoError(t, err)
	require.Equal(t, len(chunk), n)
	require.Equal(t, "line1 --remote_exec_header=<REDACTED>\r\nline2\r\n", buf.String())
}

// failingWriter simulates a writer that fails after a certain number of writes
type failingWriter struct {
	failAfter int
	callCount int
}

func (fw *failingWriter) Write(p []byte) (int, error) {
	fw.callCount++
	if fw.callCount > fw.failAfter {
		return 0, errors.New("write error")
	}
	return len(p), nil
}

func TestInvocationLogFailsFastAfterWriteError(t *testing.T) {
	invLog := &invocationLog{}
	invLog.writeListener = func(string) {}
	failWriter := &failingWriter{failAfter: 1}
	invLog.writer = io.MultiWriter(&invLog.LockingBuffer, failWriter)

	// First write should succeed
	n, err := invLog.Write([]byte("line1\n"))
	require.NoError(t, err)
	require.Equal(t, 6, n)

	// Second write should fail
	n, err = invLog.Write([]byte("line2\n"))
	require.Error(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, "write error", err.Error())

	// Third write should fail fast without attempting to write
	n, err = invLog.Write([]byte("line3\n"))
	require.Error(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, "write error", err.Error())
	require.Equal(t, 2, failWriter.callCount, "should not attempt write after error")
}

func TestInvocationLogFlushFailsFastAfterWriteError(t *testing.T) {
	invLog := &invocationLog{}
	invLog.writeListener = func(string) {}
	failWriter := &failingWriter{failAfter: 1}
	invLog.writer = io.MultiWriter(&invLog.LockingBuffer, failWriter)

	// First write should succeed
	_, err := invLog.Write([]byte("line1\n"))
	require.NoError(t, err)

	// Second write should fail
	_, err = invLog.Write([]byte("line2\n"))
	require.Error(t, err)

	// Flush should fail fast without attempting to write
	err = invLog.Flush()
	require.Error(t, err)
	require.Equal(t, "write error", err.Error())
	require.Equal(t, 2, failWriter.callCount, "should not attempt write after error")
}
