package testtar

import (
	"archive/tar"
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// Entry represents a single tar entry.
type Entry struct {
	*tar.Header
	Data []byte
}

// EntriesBytes returns a tar archive from multiple entries.
func EntriesBytes(t *testing.T, entries []Entry) []byte {
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)
	for _, entry := range entries {
		header := *entry.Header
		header.Size = int64(len(entry.Data))
		err := w.WriteHeader(&header)
		require.NoError(t, err)
		_, err = w.Write(entry.Data)
		require.NoError(t, err)
	}
	err := w.Close()
	require.NoError(t, err)
	return buf.Bytes()
}

// Entry returns a tar archive from a single entry as bytes.
func EntryBytes(t *testing.T, header *tar.Header, b []byte) []byte {
	return EntriesBytes(t, []Entry{{Header: header, Data: b}})
}
