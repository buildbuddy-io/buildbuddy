package ioutil_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func TestCustomCommitWriteCloser_SecondCommitFails(t *testing.T) {
	w := &bytes.Buffer{}
	cwc := ioutil.NewCustomCommitWriteCloser(w)
	_, buf := testdigest.RandomCASResourceBuf(t, 1024)
	written, err := cwc.Write(buf)
	require.NoError(t, err)
	require.Equal(t, 1024, written)

	err = cwc.Commit()
	require.NoError(t, err)

	err = cwc.Commit()
	require.Error(t, err)

	err = cwc.Close()
	require.NoError(t, err)
}

func TestTeeReadCloser(t *testing.T) {
	_, buf := testdigest.RandomCASResourceBuf(t, 1024)
	rc := &testReadCloser{ReadCloser: io.NopCloser(bytes.NewReader(buf))}
	w := &bytes.Buffer{}
	wc := ioutil.NewCustomCommitWriteCloser(w)
	closed := false
	wc.CloseFn = func() error {
		closed = true
		return nil
	}
	tee := ioutil.TeeReadCloser(rc, wc)
	out, err := io.ReadAll(tee)
	require.NoError(t, err)
	require.Len(t, out, 1024)
	require.Empty(t, cmp.Diff(buf, out))
	require.Equal(t, 1024, w.Len())
	require.Empty(t, cmp.Diff(buf, w.Bytes()))

	require.False(t, rc.closed)
	require.False(t, closed)

	err = tee.Close()
	require.NoError(t, err)
	require.True(t, rc.closed)
	require.True(t, closed)
}

func TestTeeReadCloser_CloseErrors(t *testing.T) {
	_, buf := testdigest.RandomCASResourceBuf(t, 1024)

	t.Run("reader close error still closes writer", func(t *testing.T) {
		rc := &failOnCloser{Reader: bytes.NewReader(buf)}
		w := &bytes.Buffer{}
		wc := ioutil.NewCustomCommitWriteCloser(w)
		writerClosed := false
		wc.CloseFn = func() error {
			writerClosed = true
			return nil
		}
		tee := ioutil.TeeReadCloser(rc, wc)
		out, err := io.ReadAll(tee)
		require.NoError(t, err)
		require.Len(t, out, 1024)
		require.Empty(t, cmp.Diff(buf, out))
		require.Equal(t, 1024, w.Len())
		require.Empty(t, cmp.Diff(buf, w.Bytes()))

		err = tee.Close()
		require.Error(t, err)
		require.True(t, writerClosed)
		require.Equal(t, "reader fail on close", err.Error())
	})

	t.Run("reader and writer close errors returns reader error", func(t *testing.T) {
		rc := &failOnCloser{Reader: bytes.NewReader(buf)}
		w := &bytes.Buffer{}
		wc := ioutil.NewCustomCommitWriteCloser(w)
		wc.CloseFn = func() error {
			return errors.New("writer fail on close")
		}
		tee := ioutil.TeeReadCloser(rc, wc)
		out, err := io.ReadAll(tee)
		require.NoError(t, err)
		require.Len(t, out, 1024)
		require.Empty(t, cmp.Diff(buf, out))
		require.Equal(t, 1024, w.Len())
		require.Empty(t, cmp.Diff(buf, w.Bytes()))

		err = tee.Close()
		require.Error(t, err)
		require.Equal(t, "reader fail on close", err.Error())
	})

	t.Run("reader close success writer close error returns writer error", func(t *testing.T) {
		rc := io.NopCloser(bytes.NewReader(buf))
		w := &bytes.Buffer{}
		wc := ioutil.NewCustomCommitWriteCloser(w)
		wc.CloseFn = func() error {
			return errors.New("writer fail on close")
		}
		tee := ioutil.TeeReadCloser(rc, wc)
		out, err := io.ReadAll(tee)
		require.NoError(t, err)
		require.Len(t, out, 1024)
		require.Empty(t, cmp.Diff(buf, out))
		require.Equal(t, 1024, w.Len())
		require.Empty(t, cmp.Diff(buf, w.Bytes()))

		err = tee.Close()
		require.Error(t, err)
		require.Equal(t, "writer fail on close", err.Error())
	})
}

type testReadCloser struct {
	io.ReadCloser

	closed bool
}

func (t *testReadCloser) Close() error {
	t.closed = true
	return t.ReadCloser.Close()
}

type failOnCloser struct {
	io.Reader
}

func (f *failOnCloser) Close() error {
	return errors.New("reader fail on close")
}
