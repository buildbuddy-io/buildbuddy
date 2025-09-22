package ioutil_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
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
	require.NoError(t, err)
	require.Equal(t, len(bytesToWrite)-22, written)
	require.Error(t, b.Err())
	require.Empty(t, cmp.Diff(bytesToWrite[:22], w.Bytes()))

	written, err = b.Write(bytesToWrite[22:])
	require.NoError(t, err)
	require.Equal(t, len(bytesToWrite)-22, written)
	require.Error(t, b.Err())
	require.Empty(t, cmp.Diff(bytesToWrite[:22], w.Bytes()))

	written, err = b.Write(bytesToWrite[:0])
	require.NoError(t, err)
	require.Zero(t, written)
	require.Error(t, b.Err())
	require.Empty(t, cmp.Diff(bytesToWrite[:22], w.Bytes()))
}

func mustWrite(t *testing.T, w io.Writer, p []byte) {
	n, err := w.Write(p)
	require.NoError(t, err)
	require.Equal(t, len(p), n)
}

func mustRead(t *testing.T, r io.Reader, p []byte) int {
	buf := make([]byte, len(p))
	n, err := ioutil.ReadTryFillBuffer(r, buf)
	require.NoError(t, err)
	require.Equal(t, p, buf[:n])
	fmt.Println("\t\t\t\t\t\tREAD", n, "bytes")
	return n
}

func TestDoubleBufferWriter(t *testing.T) {
	pr, pw := io.Pipe()
	cwc := ioutil.NewCustomCommitWriteCloser(pw)
	var committed int64
	cwc.CommitFn = func(n int64) error {
		committed = n
		return pw.Close()
	}
	dbw := ioutil.NewDoubleBufferWriter(context.Background(), cwc, bytebufferpool.VariableSize(8), 4, 8)

	// Write 1 byte and immediately let it get written by pulling from the pipe.
	mustWrite(t, dbw, []byte{1})
	read := mustRead(t, pr, []byte{1})

	// Should be able to write 4 bytes without blocking
	mustWrite(t, dbw, []byte{1})
	mustWrite(t, dbw, []byte{2, 3, 4})

	// Should be able to write another 5 bytes without blocking
	mustWrite(t, dbw, []byte{5, 6, 7, 8, 9})
	read += mustRead(t, pr, []byte{1, 2, 3, 4})
	read += mustRead(t, pr, []byte{5, 6, 7, 8, 9})

	// The buffer increased to 8, so writing 12 more bytes shouldn't block.
	// The first 4 bytes should be written immediately, and the next 8 bytes
	// should be written once we read from the pipe.
	for range 3 {
		mustWrite(t, dbw, []byte{1, 2, 3, 4})
	}
	_ = committed
	// The next write will block until we read from the pipe.
	go mustWrite(t, dbw, []byte{5, 6, 7, 8, 9, 10, 11, 12})
	read += mustRead(t, pr, []byte{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4})
	read += mustRead(t, pr, []byte{5, 6, 7, 8, 9, 10, 11, 12})

	require.NoError(t, dbw.Commit())
	require.Equal(t, int64(read), committed)

	require.NoError(t, dbw.Close())
	// Pipe should be closed now.
	_, err := pr.Read(nil)
	require.Equal(t, io.EOF, err)
}

func TestDoubleBufferWriter_RandomWrites(t *testing.T) {
	pr, pw := io.Pipe()
	cwc := ioutil.NewCustomCommitWriteCloser(pw)
	var committed int64
	cwc.CommitFn = func(n int64) error {
		committed = n
		return pw.Close()
	}
	dbw := ioutil.NewDoubleBufferWriter(context.Background(), cwc, bytebufferpool.VariableSize(8), 4, 8)

	writesDone, readsDone := make(chan struct{}), make(chan struct{})
	_, buf := testdigest.RandomCASResourceBuf(t, 10_000)
	go func() {
		defer close(writesDone)
		for left := buf; len(left) > 0; {
			toWrite := rand.Intn((len(left)/2)+1) + 1
			n, err := dbw.Write(left[:toWrite])
			require.NoError(t, err)
			require.Equal(t, toWrite, n)
			left = left[toWrite:]
		}
	}()
	go func() {
		defer close(readsDone)
		actual := make([]byte, len(buf))
		n, err := io.ReadFull(pr, actual)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.Equal(t, buf, actual)
	}()
	<-writesDone
	<-readsDone

	require.NoError(t, dbw.Commit())
	require.Equal(t, int64(len(buf)), committed)

	require.NoError(t, dbw.Close())
	// Pipe should be closed now.
	_, err := pr.Read(nil)
	require.Equal(t, io.EOF, err)
}

func TestDoubleBufferWriter_Errors(t *testing.T) {
	pr, pw := io.Pipe()
	cwc := ioutil.NewCustomCommitWriteCloser(pw)
	cwc.CommitFn = func(n int64) error {
		return pw.Close()
	}
	dbw := ioutil.NewDoubleBufferWriter(context.Background(), cwc, bytebufferpool.VariableSize(8), 4, 8)

	require.NoError(t, pw.Close())

	// The first write will buffer but then fail to write. The second one
	// might buffer before the first fails. The third will definitely return an
	// error.
	_, err := dbw.Write([]byte{1, 2, 3, 4})
	require.NoError(t, err)
	_, err = dbw.Write([]byte{1, 2, 3, 4})
	if err != nil {
		require.Equal(t, io.ErrClosedPipe, err)
	}
	_, err = dbw.Write([]byte{1, 2, 3, 4})
	require.Equal(t, io.ErrClosedPipe, err)

	require.NoError(t, pr.Close())
}

func TestDoubleBufferWriter_WriteAfterClose(t *testing.T) {
	cwc := ioutil.NewCustomCommitWriteCloser(io.Discard)
	dbw := ioutil.NewDoubleBufferWriter(context.Background(), cwc, bytebufferpool.VariableSize(8), 4, 8)
	require.NoError(t, dbw.Close())
	_, err := dbw.Write(nil)
	require.Error(t, err)

	cwc = ioutil.NewCustomCommitWriteCloser(io.Discard)
	dbw = ioutil.NewDoubleBufferWriter(context.Background(), cwc, bytebufferpool.VariableSize(8), 4, 8)
	require.NoError(t, dbw.Commit())
	_, err = dbw.Write(nil)
	require.Error(t, err)
}

func TestDoubleBufferWriter_WriteAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, pw := io.Pipe()
	cwc := ioutil.NewCustomCommitWriteCloser(pw)
	dbw := ioutil.NewDoubleBufferWriter(ctx, cwc, bytebufferpool.VariableSize(8), 4, 8)

	_, err := dbw.Write([]byte{1, 2, 3, 4})
	require.NoError(t, err)
	cancel()
	_, err = dbw.Write([]byte{1, 2, 3, 4})
	require.Equal(t, ctx.Err(), err)
}
