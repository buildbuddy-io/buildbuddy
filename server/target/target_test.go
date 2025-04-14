package target_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	"github.com/buildbuddy-io/buildbuddy/server/target"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
)

func TestGetTargetHistory(t *testing.T) {
	flags.Set(t, "testenv.reuse_server", true)
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "app.enable_read_target_statuses_from_olap_db", true)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	testAuth := testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1"))
	env.SetAuthenticator(testAuth)

	iid1Hex, iid1 := makeInvocationID('1')
	iid2Hex, iid2 := makeInvocationID('2')
	iid3Hex, _ := makeInvocationID('3')

	// Insert some test data
	for _, row := range []*olaptables.TestTargetStatus{
		// GR1: invocation1, repo1@commit1 (main): 2 tests, 1 passed, 1 flaky
		{
			GroupID:                 "GR1",
			RepoURL:                 "https://github.com/gr1/repo1",
			BranchName:              "main",
			CommitSHA:               "commit1",
			Label:                   "//:passing_test",
			Status:                  int32(bespb.TestStatus_PASSED),
			InvocationUUID:          iid1Hex,
			InvocationStartTimeUsec: 1e6,
			StartTimeUsec:           1e6,
		},
		{
			GroupID:                 "GR1",
			RepoURL:                 "https://github.com/gr1/repo1",
			BranchName:              "main",
			CommitSHA:               "commit1",
			Label:                   "//:flaky_test",
			Status:                  int32(bespb.TestStatus_FLAKY),
			InvocationUUID:          iid1Hex,
			InvocationStartTimeUsec: 1e6,
			StartTimeUsec:           1e6 + 1,
		},
		// GR1: invocation2, repo1@commit2 (feature-1): 2 tests, both passed
		{
			GroupID:                 "GR1",
			RepoURL:                 "https://github.com/gr1/repo1",
			BranchName:              "feature-1",
			CommitSHA:               "commit2",
			Label:                   "//:passing_test",
			Status:                  int32(bespb.TestStatus_PASSED),
			InvocationUUID:          iid2Hex,
			InvocationStartTimeUsec: 2e6,
			StartTimeUsec:           2e6,
		},
		{
			GroupID:                 "GR1",
			RepoURL:                 "https://github.com/gr1/repo1",
			BranchName:              "feature-1",
			CommitSHA:               "commit2",
			Label:                   "//:flaky_test",
			Status:                  int32(bespb.TestStatus_PASSED),
			InvocationUUID:          iid2Hex,
			InvocationStartTimeUsec: 2e6,
			StartTimeUsec:           2e6 + 1,
		},
		// GR2: invocation3, repo1@commit1 (main): 2 tests, 1 passed, 1 failed
		{
			GroupID:                 "GR2",
			RepoURL:                 "https://github.com/gr2/repo1",
			BranchName:              "main",
			CommitSHA:               "commit1",
			Label:                   "//:passing_test",
			Status:                  int32(bespb.TestStatus_PASSED),
			InvocationUUID:          iid3Hex,
			InvocationStartTimeUsec: 3e6,
			StartTimeUsec:           3e6,
		},
	} {
		err := env.GetOLAPDBHandle().GORM(ctx, "insert_test_data").Create(row).Error
		require.NoError(t, err)
	}

	for _, test := range []struct {
		name             string
		request          *trpb.GetTargetHistoryRequest
		expectedResponse *trpb.GetTargetHistoryResponse
	}{
		{
			name: "GR1 repo1, no branch filter",
			request: &trpb.GetTargetHistoryRequest{
				RequestContext:       &ctxpb.RequestContext{GroupId: "GR1"},
				StartTimeUsec:        0,
				EndTimeUsec:          10e6,
				ServerSidePagination: true,
				Query: &trpb.TargetQuery{
					RepoUrl: "https://github.com/gr1/repo1",
				},
			},
			expectedResponse: &trpb.GetTargetHistoryResponse{
				InvocationTargets: []*trpb.TargetHistory{
					{
						Target: &trpb.TargetMetadata{
							Label: "//:flaky_test",
						},
						RepoUrl: "https://github.com/gr1/repo1",
						TargetStatus: []*trpb.TargetStatus{
							{
								InvocationId:            iid2,
								InvocationCreatedAtUsec: 2e6,
								Timing:                  timingUsec(2e6+1, 0),
								CommitSha:               "commit2",
								Status:                  common.Status_PASSED,
							},
							{
								InvocationId:            iid1,
								InvocationCreatedAtUsec: 1e6,
								Timing:                  timingUsec(1e6+1, 0),
								CommitSha:               "commit1",
								Status:                  common.Status_FLAKY,
							},
						},
					},
					{
						Target: &trpb.TargetMetadata{
							Label: "//:passing_test",
						},
						RepoUrl: "https://github.com/gr1/repo1",
						TargetStatus: []*trpb.TargetStatus{
							{
								InvocationId:            iid2,
								InvocationCreatedAtUsec: 2e6,
								Timing:                  timingUsec(2e6, 0),
								CommitSha:               "commit2",
								Status:                  common.Status_PASSED,
							},
							{
								InvocationId:            iid1,
								InvocationCreatedAtUsec: 1e6,
								Timing:                  timingUsec(1e6, 0),
								CommitSha:               "commit1",
								Status:                  common.Status_PASSED,
							},
						},
					},
				},
			},
		},
		{
			name: "GR1 repo1, feature-1 branch",
			request: &trpb.GetTargetHistoryRequest{
				RequestContext:       &ctxpb.RequestContext{GroupId: "GR1"},
				StartTimeUsec:        0,
				EndTimeUsec:          10e6,
				ServerSidePagination: true,
				Query: &trpb.TargetQuery{
					RepoUrl:    "https://github.com/gr1/repo1",
					BranchName: "feature-1",
				},
			},
			expectedResponse: &trpb.GetTargetHistoryResponse{
				InvocationTargets: []*trpb.TargetHistory{
					{
						Target: &trpb.TargetMetadata{
							Label: "//:flaky_test",
						},
						RepoUrl: "https://github.com/gr1/repo1",
						TargetStatus: []*trpb.TargetStatus{
							{
								InvocationId:            iid2,
								InvocationCreatedAtUsec: 2e6,
								Timing:                  timingUsec(2e6+1, 0),
								CommitSha:               "commit2",
								Status:                  common.Status_PASSED,
							},
						},
					},
					{
						Target: &trpb.TargetMetadata{
							Label: "//:passing_test",
						},
						RepoUrl: "https://github.com/gr1/repo1",
						TargetStatus: []*trpb.TargetStatus{
							{
								InvocationId:            iid2,
								InvocationCreatedAtUsec: 2e6,
								Timing:                  timingUsec(2e6, 0),
								CommitSha:               "commit2",
								Status:                  common.Status_PASSED,
							},
						},
					},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			gr1Ctx, err := testAuth.WithAuthenticatedUser(ctx, "US1")
			require.NoError(t, err)

			response, err := target.GetTargetHistory(gr1Ctx, env, test.request)

			require.NoError(t, err)
			require.Empty(t, cmp.Diff(
				test.expectedResponse,
				response,
				protocmp.Transform(),
				// We're not specifically testing pagination in this test, so
				// ignore for now.
				protocmp.IgnoreFields(&trpb.GetTargetHistoryResponse{}, "next_page_token"),
			))
		})
	}
}

func timingUsec(startUsec, durationUsec int64) *common.Timing {
	return &common.Timing{
		StartTime: timestamppb.New(time.UnixMicro(startUsec)),
		Duration:  durationpb.New(time.Duration(durationUsec) * time.Microsecond),
	}
}

func makeInvocationID(char byte) (hex, ascii string) {
	hex = strings.Repeat(string(char), 32)
	// Insert dashes for human-readable ASCII representation
	ascii = hex[:8] + "-" + hex[8:12] + "-" + hex[12:16] + "-" + hex[16:20] + "-" + hex[20:]
	return
}
