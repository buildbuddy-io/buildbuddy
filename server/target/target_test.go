package target_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_index"
	"github.com/buildbuddy-io/buildbuddy/server/target"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	ispb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
)

func TestGetTargetHistory(t *testing.T) {
	flags.Set(t, "app.enable_read_target_statuses_from_olap_db", true)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	testclickhouse.Configure(t, env)
	testAuth := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
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
		name                   string
		request                *trpb.GetTargetHistoryRequest
		expectedResponse       *trpb.GetTargetHistoryResponse
		expectPermissionDenied bool
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
		{
			name: "GR2 repo1 denied for GR1 user",
			request: &trpb.GetTargetHistoryRequest{
				RequestContext:       &ctxpb.RequestContext{GroupId: "GR2"},
				ServerSidePagination: true,
				Query: &trpb.TargetQuery{
					RepoUrl: "https://github.com/gr2/repo1",
				},
			},
			expectPermissionDenied: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			gr1Ctx, err := testAuth.WithAuthenticatedUser(ctx, "US1")
			require.NoError(t, err)

			response, err := target.GetTargetHistory(gr1Ctx, env, test.request)
			if test.expectPermissionDenied {
				require.True(t, status.IsPermissionDeniedError(err), "expected permission denied, got response %v and error %v", response, err)
				return
			}

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

func TestGetTargetStats_NearlyTimedOutRuns(t *testing.T) {
	flags.Set(t, "app.enable_read_target_statuses_from_olap_db", true)

	env := testenv.GetTestEnv(t)
	testclickhouse.Configure(t, env)
	testAuth := testauth.NewTestAuthenticator(t, testauth.TestUsers("US3", "GR3"))
	env.SetAuthenticator(testAuth)
	ctx, err := testAuth.WithAuthenticatedUser(t.Context(), "US3")
	require.NoError(t, err)

	const repoURL = "https://github.com/gr3/repo1"
	iid, _ := makeInvocationID('a')
	start := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	for _, run := range []struct {
		label              string
		maxAttemptDuration time.Duration
	}{
		{"//:slow_test", 96 * time.Second},
		{"//:fast_test", 50 * time.Second},
	} {
		err := env.GetOLAPDBHandle().GORM(ctx, "insert_test_target_status").Create(&olaptables.TestTargetStatus{
			GroupID:                 "GR3",
			RepoURL:                 repoURL,
			CommitSHA:               "commit1",
			Label:                   run.label,
			InvocationUUID:          iid,
			InvocationStartTimeUsec: start.UnixMicro(),
			Status:                  int32(bespb.TestStatus_PASSED),
			TestTimeoutUsec:         (100 * time.Second).Microseconds(),
			MaxAttemptDurationUsec:  run.maxAttemptDuration.Microseconds(),
		}).Error
		require.NoError(t, err)
	}

	targetStats, err := target.GetTargetStats(ctx, env, &trpb.GetTargetStatsRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR3"},
		Repo:           repoURL,
		StartedAfter:   timestamppb.New(start.Add(-time.Hour)),
	})
	require.NoError(t, err)
	diff := cmp.Diff(&trpb.GetTargetStatsResponse{Stats: []*trpb.AggregateTargetStats{
		{Label: "//:slow_test", Data: &trpb.TargetStatsData{TotalRuns: 1, NearlyTimedOutRuns: 1}},
	}}, targetStats, protocmp.Transform())
	require.Empty(t, diff)

	dailyTargetStats, err := target.GetDailyTargetStats(ctx, env, &trpb.GetDailyTargetStatsRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR3"},
		Repo:           repoURL,
		StartedAfter:   timestamppb.New(start.Add(-time.Hour)),
	})
	require.NoError(t, err)
	diff = cmp.Diff(&trpb.GetDailyTargetStatsResponse{Stats: []*trpb.DailyTargetStats{
		{Date: "2026-01-15", Data: &trpb.TargetStatsData{TotalRuns: 2, NearlyTimedOutRuns: 1}},
	}}, dailyTargetStats, protocmp.Transform())
	require.Empty(t, diff)
}

func TestGetTarget_FileLimitPagination(t *testing.T) {
	idx := event_index.New()
	for _, target := range []struct {
		label     string
		fileCount int
	}{
		// After 100 files have been returned in a single page, the server stops returning additional targets.
		// Additional targets must be fetched via a subsequent page token.
		{"//pkg:a", 101},
		{"//pkg:b", 1},
	} {
		files := &bespb.NamedSetOfFiles{}
		for i := range target.fileCount {
			files.Files = append(files.Files, &bespb.File{
				Name: fmt.Sprintf("output-%d", i),
				File: &bespb.File_Uri{Uri: fmt.Sprintf("bytestream://cache/blobs/hash-%d/1", i)},
			})
		}
		idx.AllTargetLabels = append(idx.AllTargetLabels, target.label)
		idx.NamedSetOfFilesByID[target.label] = files
		idx.TargetCompleteEventByLabel[target.label] = &bespb.BuildEvent{
			Payload: &bespb.BuildEvent_Completed{Completed: &bespb.TargetComplete{
				Success: true,
				OutputGroup: []*bespb.OutputGroup{{
					FileSets: []*bespb.BuildEventId_NamedSetOfFilesId{{Id: target.label}},
				}},
			}},
		}
	}
	idx.Finalize()

	for _, test := range []struct {
		name   string
		status ispb.InvocationStatus
	}{
		// When the invocation is in progress, a token to fetch package b is not returned because
		// returning it might cause duplicate entries in the UI.
		{"running", ispb.InvocationStatus_PARTIAL_INVOCATION_STATUS},
		// Once the invocation is no longer running, a token to fetch package b
		// should be returned so that target is accessible.
		{"disconnected", ispb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS},
		{"completed", ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS},
	} {
		t.Run(test.name, func(t *testing.T) {
			inv := &inpb.Invocation{InvocationId: "iid", InvocationStatus: test.status}
			listing := func(pageToken string) *trpb.TargetGroup {
				rsp, err := target.GetTarget(t.Context(), nil, inv, idx, &trpb.GetTargetRequest{
					InvocationId: inv.GetInvocationId(),
					Status:       common.Status_STATUS_UNSPECIFIED.Enum(),
					PageToken:    pageToken,
				})
				require.NoError(t, err)
				require.Len(t, rsp.GetTargetGroups(), 1)
				return rsp.GetTargetGroups()[0]
			}

			// The first page should only include the first target, which exceeds the 100 file limit.
			first := listing("")
			require.Equal(t, int64(2), first.GetTotalCount())
			require.Len(t, first.GetTargets(), 1)
			require.Equal(t, "//pkg:a", first.GetTargets()[0].GetMetadata().GetLabel())
			require.Len(t, first.GetTargets()[0].GetFiles(), 101, "the soft limit must not truncate any single target's files")

			// When the invocation is in progress, no additional targets should be available.
			if test.status == ispb.InvocationStatus_PARTIAL_INVOCATION_STATUS {
				require.Empty(t, first.GetNextPageToken())
				return
			}

			// Once the invocation is no longer running, additional targets should be available.
			require.NotEmpty(t, first.GetNextPageToken(), "remaining targets must be reachable when the invocation is no longer running")
			second := listing(first.GetNextPageToken())
			require.Equal(t, int64(2), second.GetTotalCount())
			require.Len(t, second.GetTargets(), 1)
			require.Equal(t, "//pkg:b", second.GetTargets()[0].GetMetadata().GetLabel())
			require.Len(t, second.GetTargets()[0].GetFiles(), 1)
			require.Empty(t, second.GetNextPageToken(), "pagination must stop after the final target")
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
