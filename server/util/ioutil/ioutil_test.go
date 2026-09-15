package ioutil_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
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

func TestReadAllLimited(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		body      string
		limit     int64
		wantBody  string
		wantError error
	}{
		{
			name:     "under limit",
			body:     "hello",
			limit:    10,
			wantBody: "hello",
		},
		{
			name:     "at limit",
			body:     "hello",
			limit:    5,
			wantBody: "hello",
		},
		{
			name:      "over limit",
			body:      "hello",
			limit:     4,
			wantError: ioutil.ErrLimitExceeded,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Read a small body so the test can spell out exactly where the
			// configured limit is expected to accept or reject it.
			got, err := ioutil.ReadAllLimited(bytes.NewBufferString(testCase.body), testCase.limit)
			if testCase.wantError != nil {
				require.ErrorIs(t, err, testCase.wantError)
				return
			}

			// The helper should return the complete body when the body is at or
			// under the configured limit.
			require.NoError(t, err)
			require.Equal(t, []byte(testCase.wantBody), got)
		})
	}
}

func TestNewByteRepeater(t *testing.T) {
	// Repeat a byte enough times to prove the reader can fill a larger buffer.
	got := make([]byte, 7)
	n, err := io.ReadFull(ioutil.NewByteRepeater('x'), got)
	require.NoError(t, err)
	require.Equal(t, len(got), n)
	require.Equal(t, []byte("xxxxxxx"), got)
}

func TestCustomCommitWriteCloser_SeekerPropagation(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "seekable-*")
	require.NoError(t, err)
	defer f.Close()

	cwc := ioutil.NewCustomCommitWriteCloser(f)
	seeker, ok := cwc.(io.Seeker)
	require.True(t, ok)
	_, err = seeker.Seek(0, io.SeekStart)
	require.NoError(t, err)

	cwc = ioutil.NewCustomCommitWriteCloser(&bytes.Buffer{})
	_, ok = cwc.(io.Seeker)
	require.False(t, ok)
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

func TestSpillBuffer_UnderLimit(t *testing.T) {
	// Write less data than the memory limit. All of it should stay in memory,
	// and no file should be created at the spill path.
	path := filepath.Join(testfs.MakeTempDir(t), "spill")
	b := ioutil.NewSpillBuffer(path, 10)
	n, err := b.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, 5, n)

	r, err := b.Reader()
	require.NoError(t, err)
	content, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(content))
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err), "spill file should not exist, got: %v", err)

	// Once a reader has been obtained, writes should fail rather than
	// invalidating the reader.
	_, err = b.Write([]byte("more"))
	assert.ErrorContains(t, err, "not writable")

	// Close releases the in-memory buffer, so reads and writes afterwards
	// should fail rather than silently observing reused memory.
	require.NoError(t, b.Close())
	_, err = b.Reader()
	assert.ErrorContains(t, err, "before Close")
	_, err = b.Write([]byte("more"))
	assert.ErrorContains(t, err, "not writable")
}

func TestSpillBuffer_AtLimit(t *testing.T) {
	// Write exactly up to the memory limit. The data should stay in memory
	// with no spill file created.
	path := filepath.Join(testfs.MakeTempDir(t), "spill")
	b := ioutil.NewSpillBuffer(path, 5)
	n, err := b.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, 5, n)

	r, err := b.Reader()
	require.NoError(t, err)
	content, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(content))
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err), "spill file should not exist, got: %v", err)
	require.NoError(t, b.Close())
}

func TestSpillBuffer_OverLimit(t *testing.T) {
	// Write past the memory limit across several writes. Once the limit is
	// exceeded, all data (including the previously buffered prefix) should be
	// moved to the spill file.
	path := filepath.Join(testfs.MakeTempDir(t), "spill")
	b := ioutil.NewSpillBuffer(path, 8)
	_, err := b.Write([]byte("hello "))
	require.NoError(t, err)
	// This write exceeds the 8-byte limit and should trigger the spill.
	_, err = b.Write([]byte("world "))
	require.NoError(t, err)
	// Writes after the spill should be appended to the file.
	_, err = b.Write([]byte("again"))
	require.NoError(t, err)

	// The reader should return the full contents, and should be seekable so
	// that the contents can be read multiple times.
	r, err := b.Reader()
	require.NoError(t, err)
	content, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "hello world again", string(content))
	_, err = r.Seek(0, io.SeekStart)
	require.NoError(t, err)
	content, err = io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "hello world again", string(content))

	// The spill file should contain the full contents. (Reader flushed any
	// writes that were still buffered for the file.)
	spilled, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "hello world again", string(spilled))

	// Once a reader has been obtained, writes should fail rather than being
	// written at the shared handle's read offset.
	_, err = b.Write([]byte("more"))
	assert.ErrorContains(t, err, "not writable")

	// Close should remove the spill file and be safe to call more than once.
	require.NoError(t, b.Close())
	require.NoError(t, b.Close())
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err), "spill file should be removed, got: %v", err)
}

func TestSpillBuffer_ZeroLimit(t *testing.T) {
	// With a zero memory limit, the first non-empty write should go straight
	// to the spill file.
	path := filepath.Join(testfs.MakeTempDir(t), "spill")
	b := ioutil.NewSpillBuffer(path, 0)
	_, err := b.Write([]byte("hello"))
	require.NoError(t, err)

	// Obtaining a reader flushes any writes still buffered for the file.
	_, err = b.Reader()
	require.NoError(t, err)
	spilled, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(spilled))
	require.NoError(t, b.Close())
}
