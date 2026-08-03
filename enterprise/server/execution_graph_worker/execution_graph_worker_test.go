package execution_graph_worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDashedUUID(t *testing.T) {
	iid, err := dashedUUID("8c3c4a4e89cf4f9e9a29a3b5997acaae")
	require.NoError(t, err)
	assert.Equal(t, "8c3c4a4e-89cf-4f9e-9a29-a3b5997acaae", iid)
}

func TestDashedUUID_Malformed(t *testing.T) {
	for _, input := range []string{"", "abc", "8c3c4a4e-89cf-4f9e-9a29-a3b5997acaae"} {
		_, err := dashedUUID(input)
		assert.Error(t, err, "input %q", input)
	}
}
