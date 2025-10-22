package testdata

import (
	"math/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/stretchr/testify/require"
)

// Writes the provided data buffer to the provided CommittedWritecloser in
// randomly-sized chunks.
func WriteInRandomChunks(t *testing.T, w interfaces.CommittedWriteCloser, data []byte) {
	for len(data) > 0 {
		n := rand.Intn(2048)
		if n > len(data) {
			n = len(data)
		}
		_, err := w.Write(data[:n])
		require.NoError(t, err)
		data = data[n:]
	}
	err := w.Commit()
	require.NoError(t, err)
}
