package pbwireutil_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/pbwireutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

func TestWireUtils(t *testing.T) {
	msg := &repb.ExecutedActionMetadata{
		Worker:          "foo",
		QueuedTimestamp: &tspb.Timestamp{Seconds: 7},
	}
	b, err := proto.Marshal(msg)
	require.NoError(t, err)

	const (
		mdWorkerFieldNumber          = 1
		mdQueuedTimestampFieldNumber = 2

		tsSecondsFieldNumber = 1
	)
	// Get string
	{
		worker, _ := pbwireutil.ConsumeFirstString(b, mdWorkerFieldNumber)
		require.Equal(t, worker, "foo")
	}
	// Get submessage (as bytes), then consume int64 field from that submessage
	{
		b, _ := pbwireutil.ConsumeFirstBytes(b, mdQueuedTimestampFieldNumber)
		s, _ := pbwireutil.ConsumeFirstVarint(b, tsSecondsFieldNumber)
		require.Equal(t, uint64(7), s)
	}
}
