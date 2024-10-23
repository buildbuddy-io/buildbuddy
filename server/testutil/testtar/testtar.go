package testtar

import (
	"archive/tar"
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// Entry returns a tar archive entry as bytes.
//
// Because tar archives are just a sequence of entries, the returned bytes
// represent a valid tar file. To create a tar archive with multiple entries,
// simply concatenate the bytes (e.g. using slices.Concat).
func EntryBytes(t *testing.T, header *tar.Header, b []byte) []byte {
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)
	err := w.WriteHeader(header)
	require.NoError(t, err)
	_, err = w.Write(b)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)
	return buf.Bytes()
}
