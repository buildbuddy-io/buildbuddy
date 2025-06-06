package ioutil_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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

type failAfterNumWritesWriter struct {
	w io.Writer

	failAfterNumWrites int
	numWrites          int
}

func (f *failAfterNumWritesWriter) Write(p []byte) (int, error) {
	if f.numWrites >= f.failAfterNumWrites {
		return 0, errors.New("fail on write")
	}
	f.numWrites += 1
	return f.w.Write(p)
}

// TestBestEffortWriter tests that BestEffortWriter writes to the underlying writer successfully,
// returns any errors from the underlying writer, and will not allow further writes to succeed
// after encountering an error from the underlying writer.
func TestBestEffortWriter(t *testing.T) {
	bytesToWrite := []byte("hello beautiful best-effort world")
	w := &bytes.Buffer{}
	f := &failAfterNumWritesWriter{
		w:                  w,
		failAfterNumWrites: 2,
	}
	b := ioutil.NewBestEffortWriter(f)

	written, err := b.Write(bytesToWrite[:11])
	require.NoError(t, err)
	require.Nil(t, b.Err())
	require.Equal(t, 11, written)
	require.Empty(t, cmp.Diff(bytesToWrite[:11], w.Bytes()))

	written, err = b.Write(bytesToWrite[11:22])
	require.NoError(t, err)
	require.Nil(t, b.Err())
	require.Equal(t, 11, written)
	require.Empty(t, cmp.Diff(bytesToWrite[:22], w.Bytes()))

	written, err = b.Write(bytesToWrite[22:])
	require.Error(t, err)
	require.Error(t, b.Err())
	// Write will return the error from the underlying writer,
	// and store it for access with Err().
	// Later calls will fail with a FailedPreconditionError.
	require.Equal(t, err.Error(), b.Err().Error())
	require.Zero(t, written)
	require.Empty(t, cmp.Diff(bytesToWrite[:22], w.Bytes()))

	written, err = b.Write(bytesToWrite[22:])
	require.Error(t, err)
	// Now that the BestEffortWriter has encountered an error,
	// all subsequent writes will fail with FailedPreconditionError.
	require.True(t, status.IsFailedPreconditionError(err))
	require.Error(t, b.Err())
	require.Zero(t, written)
	require.Empty(t, cmp.Diff(bytesToWrite[:22], w.Bytes()))

	written, err = b.Write(bytesToWrite[:0])
	require.Error(t, err)
	require.True(t, status.IsFailedPreconditionError(err))
	require.Error(t, b.Err())
	require.Zero(t, written)
	require.Empty(t, cmp.Diff(bytesToWrite[:22], w.Bytes()))
}
