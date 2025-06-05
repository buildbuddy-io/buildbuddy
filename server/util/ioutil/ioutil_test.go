package ioutil_test

import (
	"bytes"
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
}
