package accumulator_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

func TestAddEvent_IndexesBLAKE3TestOutput(t *testing.T) {
	const hash = "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d"
	file := &bespb.File{
		Name: "test.log",
		File: &bespb.File_Uri{Uri: "bytestream://remote.buildbuddy.io/blobs/blake3/" + hash + "/1234"},
	}
	event := &bespb.BuildEvent{
		Payload: &bespb.BuildEvent_TestResult{
			TestResult: &bespb.TestResult{
				Status:           bespb.TestStatus_PASSED,
				TestActionOutput: []*bespb.File{file},
			},
		},
	}

	// Add a BLAKE3 test output so it is eligible for artifact persistence and
	// can also be discovered when replaying the invocation.
	values := accumulator.NewBEValues(&inpb.Invocation{})
	require.NoError(t, values.AddEvent(event))
	require.Len(t, values.PassedTestOutputURIs(), 1)

	// The output file should be indexed by its digest regardless of the digest
	// function segment in the ByteStream URI.
	indexedFile, ok := values.OutputFiles()[hash]
	require.True(t, ok)
	assert.Equal(t, file, indexedFile)
}
