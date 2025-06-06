package ioutil_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
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

func TestTeeReadCacher(t *testing.T) {
	w := &bytes.Buffer{}
	committed := false
	cache := ioutil.NewCustomCommitWriteCloser(w)
	cache.CommitFn = func(_ int64) error {
		committed = true
		return nil
	}
	_, buf := testdigest.RandomCASResourceBuf(t, 1024)

	rc := io.NopCloser(bytes.NewReader(buf))

	{
		tee, err := ioutil.TeeReadCacher(nil, cache)
		require.Error(t, err)
		require.Nil(t, tee)
	}

	{
		tee, err := ioutil.TeeReadCacher(rc, nil)
		require.Error(t, err)
		require.Nil(t, tee)
	}

	tee, err := ioutil.TeeReadCacher(rc, cache)
	require.NoError(t, err)
	out, err := io.ReadAll(tee)
	require.NoError(t, err)
	require.Len(t, out, 1024)
	require.Empty(t, cmp.Diff(buf, out))
	require.True(t, committed)
	require.Equal(t, 1024, w.Len())
	require.Empty(t, cmp.Diff(buf, w.Bytes()))

	err = tee.Close()
	require.NoError(t, err)
}

func TestTeeReadCacher_IgnoreCacheErrors(t *testing.T) {
	t.Run("fail_on_write", func(t *testing.T) {
		w := &bytes.Buffer{}
		committed := false
		cwc := ioutil.NewCustomCommitWriteCloser(w)
		cwc.CommitFn = func(_ int64) error {
			committed = true
			return nil
		}
		cache := &testCache{
			w:           cwc,
			failOnWrite: true,
		}
		_, buf := testdigest.RandomCASResourceBuf(t, 1024)
		rc := io.NopCloser(bytes.NewReader(buf))
		tee, err := ioutil.TeeReadCacher(rc, cache)
		require.NoError(t, err)
		out, err := io.ReadAll(tee)
		require.NoError(t, err)
		require.Len(t, out, 1024)
		require.Empty(t, cmp.Diff(buf, out))
		require.False(t, committed)
		require.Zero(t, w.Len())

		err = tee.Close()
		require.NoError(t, err)
	})

	t.Run("fail_on_commit", func(t *testing.T) {
		w := &bytes.Buffer{}
		committed := false
		cwc := ioutil.NewCustomCommitWriteCloser(w)
		cwc.CommitFn = func(_ int64) error {
			committed = true
			return nil
		}
		cache := &testCache{
			w:            cwc,
			failOnCommit: true,
		}
		_, buf := testdigest.RandomCASResourceBuf(t, 1024)
		rc := io.NopCloser(bytes.NewReader(buf))
		tee, err := ioutil.TeeReadCacher(rc, cache)
		require.NoError(t, err)
		out, err := io.ReadAll(tee)
		require.NoError(t, err)
		require.Len(t, out, 1024)
		require.Empty(t, cmp.Diff(buf, out))
		require.False(t, committed)
		// writes to buffer succeeded, just not marked as committed
		require.Equal(t, 1024, w.Len())

		err = tee.Close()
		require.NoError(t, err)
	})

	t.Run("fail_on_close", func(t *testing.T) {
		w := &bytes.Buffer{}
		committed := false
		cwc := ioutil.NewCustomCommitWriteCloser(w)
		cwc.CommitFn = func(_ int64) error {
			committed = true
			return nil
		}
		cache := &testCache{
			w:           cwc,
			failOnClose: true,
		}
		_, buf := testdigest.RandomCASResourceBuf(t, 1024)
		rc := io.NopCloser(bytes.NewReader(buf))
		tee, err := ioutil.TeeReadCacher(rc, cache)
		require.NoError(t, err)
		out, err := io.ReadAll(tee)
		require.NoError(t, err)
		require.Len(t, out, 1024)
		require.Empty(t, cmp.Diff(buf, out))
		require.True(t, committed)
		require.Equal(t, 1024, w.Len())

		err = tee.Close()
		require.NoError(t, err)
	})
}

type testCache struct {
	w interfaces.CommittedWriteCloser

	failOnWrite  bool
	failOnCommit bool
	failOnClose  bool
}

func (t *testCache) Write(p []byte) (int, error) {
	if t.failOnWrite {
		return 0, errors.New("fail on write")
	}
	return t.w.Write(p)
}

func (t *testCache) Commit() error {
	if t.failOnCommit {
		return errors.New("fail on commit")
	}
	return t.w.Commit()
}

func (t *testCache) Close() error {
	if t.failOnClose {
		return errors.New("fail on close")
	}
	return t.w.Close()
}
