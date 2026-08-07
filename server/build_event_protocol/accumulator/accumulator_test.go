package accumulator_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

func TestAddEvent_IndexesBLAKE3OutputFile(t *testing.T) {
	const hash = "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d"
	file := &bespb.File{
		Name: "profile.gz",
		File: &bespb.File_Uri{Uri: "bytestream://test.buildbuddy.io/test-instance/blobs/blake3/" + hash + "/1234"},
	}
	event := &bespb.BuildEvent{
		Payload: &bespb.BuildEvent_BuildToolLogs{
			BuildToolLogs: &bespb.BuildToolLogs{Log: []*bespb.File{file}},
		},
	}
	values := accumulator.NewBEValues(&inpb.Invocation{})
	require.NoError(t, values.AddEvent(event))

	indexedFile, ok := values.OutputFiles()[hash]
	require.True(t, ok)
	assert.Empty(t, cmp.Diff(file, indexedFile, protocmp.Transform()))
}

func TestAddEvent_TrimsLeadingSlashFromResourceInstanceName(t *testing.T) {
	const hash = "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d"
	event := &bespb.BuildEvent{
		Payload: &bespb.BuildEvent_NamedSetOfFiles{
			NamedSetOfFiles: &bespb.NamedSetOfFiles{Files: []*bespb.File{
				{
					Name: accumulator.KytheOutputName,
					File: &bespb.File_Uri{Uri: "bytestream://test.buildbuddy.io/cache4/blobs/blake3/" + hash + "/1234"},
				},
			}},
		},
	}

	// Add a Kythe artifact whose URL path begins with the slash separating the
	// host from the ByteStream resource name.
	values := accumulator.NewBEValues(&inpb.Invocation{})
	require.NoError(t, values.AddEvent(event))

	// The URL separator should not become part of the remote instance name.
	require.NotNil(t, values.KytheSSTableResourceName())
	assert.Equal(t, "cache4", values.KytheSSTableResourceName().GetInstanceName())
}
