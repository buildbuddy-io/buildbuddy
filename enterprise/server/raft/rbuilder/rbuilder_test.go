package rbuilder_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

// splittableKey returns a key that lies in a normal, splittable data
// range (first byte >= UnsplittableMaxByte).
func splittableKey() []byte {
	return []byte{constants.UnsplittableMaxByte, 'a', 'b', 'c'}
}

func TestBatchBuilder_CASOnSplittableKey_ReturnsError(t *testing.T) {
	_, err := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   splittableKey(),
			Value: []byte("new"),
		},
		ExpectedValue: []byte("old"),
	}).ToProto()
	require.Error(t, err)
	require.True(t, status.IsFailedPreconditionError(err))
}

func TestBatchBuilder_IncrementOnSplittableKey_ReturnsError(t *testing.T) {
	_, err := rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   splittableKey(),
		Delta: 1,
	}).ToProto()
	require.Error(t, err)
	require.True(t, status.IsFailedPreconditionError(err))
}

func TestBatchBuilder_CASOnLocalKey_OK(t *testing.T) {
	_, err := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: []byte("new"),
		},
		ExpectedValue: []byte("old"),
	}).ToProto()
	require.NoError(t, err)
}

func TestBatchBuilder_IncrementOnSystemKey_OK(t *testing.T) {
	_, err := rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: 1,
	}).ToProto()
	require.NoError(t, err)
}

func TestBatchBuilder_CASOnMetaRangeKey_OK(t *testing.T) {
	_, err := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey([]byte("zzz")),
			Value: []byte("v"),
		},
		ExpectedValue: []byte("p"),
	}).ToProto()
	require.NoError(t, err)
}

func TestBatchBuilder_DirectWriteOnSplittableKey_OK(t *testing.T) {
	// DirectWrite is state- and response-idempotent, so it's allowed
	// on splittable keys.
	_, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   splittableKey(),
			Value: []byte("v"),
		},
	}).ToProto()
	require.NoError(t, err)
}
