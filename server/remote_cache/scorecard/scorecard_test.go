package scorecard_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/scorecard"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gcodes "google.golang.org/grpc/codes"
)

const (
	invocationID = "ed3c71b0-25f8-4e49-bda2-d2c5fa4d5a33"
)

var (
	// Test data.

	besUpload = &capb.ScoreCard_Result{
		ActionId:    "bes-upload",
		Digest:      &repb.Digest{Hash: "aaa", SizeBytes: 1_000},
		CacheType:   capb.CacheType_CAS,
		RequestType: capb.RequestType_WRITE,
		Status:      &statuspb.Status{Code: int32(gcodes.OK)},
		StartTime:   timestamppb.New(time.Unix(100, 0)),
		Duration:    durationpb.New(300 * time.Millisecond),
	}
	acMiss = &capb.ScoreCard_Result{
		ActionId:       "abc",
		ActionMnemonic: "GoCompile",
		TargetId:       "//foo",
		Digest:         &repb.Digest{Hash: "abc", SizeBytes: 111},
		CacheType:      capb.CacheType_AC,
		RequestType:    capb.RequestType_READ,
		Status:         &statuspb.Status{Code: int32(gcodes.NotFound)},
		StartTime:      timestamppb.New(time.Unix(300, 0)),
		Duration:       durationpb.New(100 * time.Millisecond),
	}
	casUpload = &capb.ScoreCard_Result{
		ActionId:       "abc",
		ActionMnemonic: "GoCompile",
		TargetId:       "//foo",
		Digest:         &repb.Digest{Hash: "ccc", SizeBytes: 10_000},
		CacheType:      capb.CacheType_CAS,
		RequestType:    capb.RequestType_WRITE,
		Status:         &statuspb.Status{Code: int32(gcodes.OK)},
		StartTime:      timestamppb.New(time.Unix(200, 0)),
		Duration:       durationpb.New(200 * time.Millisecond),
	}
	casDownload = &capb.ScoreCard_Result{
		ActionId:       "edf",
		ActionMnemonic: "GoLink",
		TargetId:       "//bar",
		Digest:         &repb.Digest{Hash: "fff", SizeBytes: 100_000},
		CacheType:      capb.CacheType_CAS,
		RequestType:    capb.RequestType_READ,
		Status:         &statuspb.Status{Code: int32(gcodes.OK)},
		StartTime:      timestamppb.New(time.Unix(400, 0)),
		Duration:       durationpb.New(150 * time.Millisecond),
	}

	testScorecard = &capb.ScoreCard{
		Results: []*capb.ScoreCard_Result{
			// NOTE: keep the order here matching the above.
			besUpload,
			acMiss,
			casUpload,
			casDownload,
		},
	}
)

func TestGetCacheScoreCard_Filter_Search(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t, testScorecard)
	req := &capb.GetCacheScoreCardRequest{
		InvocationId: invocationID,
		Filter: &capb.GetCacheScoreCardRequest_Filter{
			Mask:   &fieldmaskpb.FieldMask{Paths: []string{"search"}},
			Search: "bes-upload",
		},
	}

	res, err := scorecard.GetCacheScoreCard(ctx, env, req)
	require.NoError(t, err)

	assertResults(t, res, besUpload)
}

func TestGetCacheScoreCard_Filter_CacheType(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t, testScorecard)
	req := &capb.GetCacheScoreCardRequest{
		InvocationId: invocationID,
		Filter: &capb.GetCacheScoreCardRequest_Filter{
			Mask:      &fieldmaskpb.FieldMask{Paths: []string{"cache_type"}},
			CacheType: capb.CacheType_AC,
		},
	}

	res, err := scorecard.GetCacheScoreCard(ctx, env, req)
	require.NoError(t, err)

	assertResults(t, res, acMiss)
}

func TestGetCacheScoreCard_Filter_RequestType(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t, testScorecard)
	req := &capb.GetCacheScoreCardRequest{
		InvocationId: invocationID,
		Filter: &capb.GetCacheScoreCardRequest_Filter{
			Mask:        &fieldmaskpb.FieldMask{Paths: []string{"request_type"}},
			RequestType: capb.RequestType_READ,
		},
	}

	res, err := scorecard.GetCacheScoreCard(ctx, env, req)
	require.NoError(t, err)

	assertResults(t, res, acMiss, casDownload)
}

func TestGetCacheScoreCard_Filter_ResponseType(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t, testScorecard)
	req := &capb.GetCacheScoreCardRequest{
		InvocationId: invocationID,
		Filter: &capb.GetCacheScoreCardRequest_Filter{
			Mask:         &fieldmaskpb.FieldMask{Paths: []string{"response_type"}},
			ResponseType: capb.ResponseType_NOT_FOUND,
		},
	}

	res, err := scorecard.GetCacheScoreCard(ctx, env, req)
	require.NoError(t, err)

	assertResults(t, res, acMiss)
}

func TestGetCacheScoreCard_Sort_StartTime(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t, testScorecard)
	req := &capb.GetCacheScoreCardRequest{
		InvocationId: invocationID,
		OrderBy:      capb.GetCacheScoreCardRequest_ORDER_BY_START_TIME,
	}

	res, err := scorecard.GetCacheScoreCard(ctx, env, req)
	require.NoError(t, err)

	assertResults(t, res, besUpload, casUpload, acMiss, casDownload)
}

func TestGetCacheScoreCard_Sort_Duration(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t, testScorecard)
	req := &capb.GetCacheScoreCardRequest{
		InvocationId: invocationID,
		OrderBy:      capb.GetCacheScoreCardRequest_ORDER_BY_DURATION,
	}

	res, err := scorecard.GetCacheScoreCard(ctx, env, req)
	require.NoError(t, err)

	assertResults(t, res, acMiss, casDownload, casUpload, besUpload)
}

func TestGetCacheScoreCard_Sort_Size(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t, testScorecard)
	req := &capb.GetCacheScoreCardRequest{
		InvocationId: invocationID,
		OrderBy:      capb.GetCacheScoreCardRequest_ORDER_BY_SIZE,
	}

	res, err := scorecard.GetCacheScoreCard(ctx, env, req)
	require.NoError(t, err)

	assertResults(t, res, acMiss, besUpload, casUpload, casDownload)
}

func TestGetCacheScoreCard_GroupByActionOrderByDurationDesc(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t, testScorecard)
	req := &capb.GetCacheScoreCardRequest{
		InvocationId: invocationID,
		OrderBy:      capb.GetCacheScoreCardRequest_ORDER_BY_DURATION,
		GroupBy:      capb.GetCacheScoreCardRequest_GROUP_BY_ACTION,
		Descending:   true,
	}

	res, err := scorecard.GetCacheScoreCard(ctx, env, req)
	require.NoError(t, err)

	assertResults(t, res, besUpload, casUpload, acMiss, casDownload)
}

func TestGetCacheScoreCard_GroupByTargetOrderByDuration(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	expectedResults := []*capb.ScoreCard_Result{}
	// Set up results so that expected action IDs alternate within each target
	// group, and so that durations across groups have some overlap.
	for target := 0; target < 2; target++ {
		dur := time.Duration(target) * time.Millisecond
		for i := 0; i < 4; i++ {
			expectedResults = append(expectedResults, &capb.ScoreCard_Result{
				TargetId: fmt.Sprintf("%d", target),
				ActionId: fmt.Sprintf("%d%d", target, i%2),
				Duration: durationpb.New(dur),
			})
			dur += 1 * time.Millisecond
		}
	}
	// Shuffle results but keep the original slice untouched since it already has
	// the order we expect.
	sc := &capb.ScoreCard{
		Results: make([]*capb.ScoreCard_Result, len(expectedResults)),
	}
	copy(sc.Results, expectedResults)
	rand.Shuffle(len(sc.Results), func(i, j int) {
		sc.Results[i], sc.Results[j] = sc.Results[j], sc.Results[i]
	})

	ctx := context.Background()
	env := setupEnv(t, sc)
	req := &capb.GetCacheScoreCardRequest{
		InvocationId: invocationID,
		OrderBy:      capb.GetCacheScoreCardRequest_ORDER_BY_DURATION,
		GroupBy:      capb.GetCacheScoreCardRequest_GROUP_BY_TARGET,
	}

	res, err := scorecard.GetCacheScoreCard(ctx, env, req)
	require.NoError(t, err)

	assertResults(t, res, expectedResults...)
}

func assertResults(t *testing.T, res *capb.GetCacheScoreCardResponse, msg ...*capb.ScoreCard_Result) {
	// Note: not asserting directly on the protos because the diff is too hard to read.
	t.Log("EXPECTED:")
	expected := &capb.GetCacheScoreCardResponse{Results: msg}
	t.Log(prototext.Format(expected))
	t.Log("ACTUAL:")
	t.Log(prototext.Format(res))

	assert.True(t, proto.Equal(expected, res), "unexpected response")
}

func setupEnv(t *testing.T, scorecard *capb.ScoreCard) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetBlobstore(&fakeBlobStore{ScoreCard: scorecard})
	te.GetInvocationDB().CreateInvocation(context.Background(), &tables.Invocation{
		InvocationID: invocationID,
	})
	return te
}

type fakeBlobStore struct {
	interfaces.Blobstore
	ScoreCard *capb.ScoreCard
}

func (bs *fakeBlobStore) ReadBlob(ctx context.Context, name string) ([]byte, error) {
	return proto.Marshal(bs.ScoreCard)
}
