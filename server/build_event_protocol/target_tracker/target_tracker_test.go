package target_tracker_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/target_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

type Row struct {
	GroupID    string
	CommitSHA  string
	TestSize   int32
	Status     int32
	Cached     bool
	TargetType int32
	RuleType   string
	Label      string
	RepoURL    string
	Role       string
	Command    string
}

type reportStream struct {
	grpc.ClientStream
	requests []*tbpb.ReportTestResultsRequest
}

func (s *reportStream) Send(req *tbpb.ReportTestResultsRequest) error {
	s.requests = append(s.requests, req)
	return nil
}

func (s *reportStream) CloseAndRecv() (*tbpb.ReportTestResultsResponse, error) {
	count := 0
	for _, req := range s.requests {
		count += len(req.GetTargetObservations()) + len(req.GetCaseObservations())
	}
	return &tbpb.ReportTestResultsResponse{AcceptedCount: int32(count)}, nil
}

type testBuddyClient struct {
	tbpb.TestBuddyServiceClient
	stream *reportStream
}

func (c *testBuddyClient) ReportTestResults(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[tbpb.ReportTestResultsRequest, tbpb.ReportTestResultsResponse], error) {
	c.stream = &reportStream{}
	return c.stream, nil
}

func targetConfiguredId(label string) *build_event_stream.BuildEventId {
	return targetConfiguredIdWithAspect(label, "")
}

func targetConfiguredIdWithAspect(label string, aspect string) *build_event_stream.BuildEventId {
	return &build_event_stream.BuildEventId{
		Id: &build_event_stream.BuildEventId_TargetConfigured{
			TargetConfigured: &build_event_stream.BuildEventId_TargetConfiguredId{
				Label:  label,
				Aspect: aspect,
			},
		},
	}
}

func targetCompletedId(label string) *build_event_stream.BuildEventId {
	return targetCompletedIdWithAspect(label, "")
}

func targetCompletedIdWithAspect(label string, aspect string) *build_event_stream.BuildEventId {
	return &build_event_stream.BuildEventId{
		Id: &build_event_stream.BuildEventId_TargetCompleted{
			TargetCompleted: &build_event_stream.BuildEventId_TargetCompletedId{
				Label:  label,
				Aspect: aspect,
			},
		},
	}
}

func testResultId(label string) *build_event_stream.BuildEventId {
	return &build_event_stream.BuildEventId{
		Id: &build_event_stream.BuildEventId_TestResult{
			TestResult: &build_event_stream.BuildEventId_TestResultId{
				Label: label,
			},
		},
	}
}

func testSummaryId(label string) *build_event_stream.BuildEventId {
	return &build_event_stream.BuildEventId{
		Id: &build_event_stream.BuildEventId_TestSummary{
			TestSummary: &build_event_stream.BuildEventId_TestSummaryId{
				Label: label,
			},
		},
	}
}

type fakeAccumulator struct {
	role         string
	command      string
	repoURL      string
	invocationID string
	commitSHA    string
	branchName   string
}

func (a *fakeAccumulator) Invocation() *inpb.Invocation {
	return &inpb.Invocation{
		Role:         a.role,
		Command:      a.command,
		RepoUrl:      a.repoURL,
		InvocationId: a.invocationID,
		CommitSha:    a.commitSHA,
		BranchName:   a.branchName,
	}
}

func (a *fakeAccumulator) StartTime() time.Time {
	return time.Now()
}

func (a *fakeAccumulator) DisableCommitStatusReporting() bool {
	return false
}

func (a *fakeAccumulator) DisableTargetTracking() bool {
	return false
}

func (a *fakeAccumulator) Pattern() string {
	return ""
}

func (a *fakeAccumulator) WorkflowID() string {
	return ""
}

func (a *fakeAccumulator) ActionName() string {
	return ""
}

func (a *fakeAccumulator) CommitStatusLabel() string {
	return ""
}

func (a *fakeAccumulator) MetadataIsLoaded() bool {
	return true
}

func (a *fakeAccumulator) BuildFinished() bool {
	return true
}

func newFakeAccumulator(t *testing.T, testInvocationID string) *fakeAccumulator {
	return &fakeAccumulator{
		invocationID: testInvocationID,
		command:      "test",
		role:         "CI",
		repoURL:      "bb/foo",
		commitSHA:    "abcdef",
		branchName:   "main",
	}
}

func TestReportsCITargetObservationsToTestBuddy(t *testing.T) {
	for _, test := range []struct {
		name        string
		role        string
		branch      string
		status      build_event_stream.TestStatus
		testXML     bool
		wantSource  tbpb.TestObservationSource
		wantOutcome tbpb.TestOutcome
		wantCount   int
	}{
		{name: "main", role: "CI", branch: "main", status: build_event_stream.TestStatus_FAILED, testXML: true, wantSource: tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_POSTSUBMIT, wantOutcome: tbpb.TestOutcome_TEST_OUTCOME_PASS, wantCount: 1},
		{name: "feature", role: "CI", branch: "feature", status: build_event_stream.TestStatus_TIMEOUT, wantSource: tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_PRESUBMIT, wantOutcome: tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT, wantCount: 1},
		{name: "harness failure", role: "CI", branch: "main", status: build_event_stream.TestStatus_FAILED, wantSource: tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_POSTSUBMIT, wantOutcome: tbpb.TestOutcome_TEST_OUTCOME_FAIL, wantCount: 1},
		{name: "non CI", role: "", branch: "main", wantCount: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			authenticator := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
			te.SetAuthenticator(authenticator)
			client := &testBuddyClient{}
			te.SetTestBuddyServiceClient(client)
			flags.Set(t, "app.enable_target_tracking", false)
			flags.Set(t, "app.enable_write_test_target_statuses_to_olap_db", false)
			ctx, err := authenticator.WithAuthenticatedUser(context.Background(), "USER1")
			require.NoError(t, err)
			invocationID := uuid.NewString()
			accumulator := newFakeAccumulator(t, invocationID)
			accumulator.role = test.role
			accumulator.branchName = test.branch
			tracker := target_tracker.NewTargetTracker(te, accumulator)
			for _, event := range []*build_event_stream.BuildEvent{
				{
					Children: []*build_event_stream.BuildEventId{targetConfiguredId("//pkg:test")},
					Payload:  &build_event_stream.BuildEvent_Expanded{},
				},
				{
					Id: targetConfiguredId("//pkg:test"),
					Payload: &build_event_stream.BuildEvent_Configured{Configured: &build_event_stream.TargetConfigured{
						TargetKind: "go_test rule", TestSize: build_event_stream.TestSize_SMALL,
					}},
				},
				{Payload: &build_event_stream.BuildEvent_WorkspaceStatus{WorkspaceStatus: &build_event_stream.WorkspaceStatus{
					Item: []*build_event_stream.WorkspaceStatus_Item{{Key: "GIT_TREE_STATUS", Value: "Modified"}},
				}}},
				{
					Id: testResultId("//pkg:test"),
					Payload: &build_event_stream.BuildEvent_TestResult{TestResult: &build_event_stream.TestResult{
						Status: test.status,
					}},
				},
				{
					Id: testSummaryId("//pkg:test"),
					Payload: &build_event_stream.BuildEvent_TestSummary{TestSummary: &build_event_stream.TestSummary{
						OverallStatus:        test.status,
						FirstStartTimeMillis: 1_000, TotalRunDurationMillis: 2_000,
					}},
				},
				{LastMessage: true},
			} {
				if result := event.GetTestResult(); result != nil && test.testXML {
					result.TestActionOutput = []*build_event_stream.File{{Name: "test.xml"}}
				}
				tracker.TrackTargetsForEvent(ctx, event)
			}
			if test.wantCount == 0 {
				require.Nil(t, client.stream)
				return
			}
			require.NotNil(t, client.stream)
			require.Len(t, client.stream.requests, 1)
			require.Equal(t, "bb/foo", client.stream.requests[0].GetRepoUrl())
			require.Len(t, client.stream.requests[0].GetTargetObservations(), test.wantCount)
			observation := client.stream.requests[0].GetTargetObservations()[0]
			require.Equal(t, "//pkg:test", observation.GetIdentity().GetTargetLabel())
			require.Equal(t, test.wantOutcome, observation.GetObservation().GetOutcome())
			require.Equal(t, test.wantSource, observation.GetObservation().GetSource())
			require.Equal(t, "abcdef", observation.GetObservation().GetCommitSha())
			require.True(t, observation.GetObservation().GetWorkspaceDirty())
			require.Contains(t, observation.GetObservation().GetSourceUrl(), "/invocation/"+invocationID)
			require.NotEmpty(t, observation.GetObservation().GetObservationId())
		})
	}
}

func TestTrackTargetForEvents_OLAP(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "app.enable_write_test_target_statuses_to_olap_db", true)
	runTrackTargetsForEventsTest(t)
}

func TestTrackTargetForEvents_NonOLAP(t *testing.T) {
	flags.Set(t, "app.enable_write_test_target_statuses_to_olap_db", false)
	runTrackTargetsForEventsTest(t)
}

func runTrackTargetsForEventsTest(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(ta)
	flags.Set(t, "app.enable_target_tracking", true)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "USER1")
	require.NoError(t, err)

	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)

	accumulator := newFakeAccumulator(t, testUUID.String())
	tracker := target_tracker.NewTargetTracker(te, accumulator)

	events := []*build_event_stream.BuildEvent{
		&build_event_stream.BuildEvent{
			Children: []*build_event_stream.BuildEventId{
				targetConfiguredId("//server:lib"),
				targetConfiguredId("//server:baz_test"),
				targetConfiguredId("//server:foo_test"),
				targetConfiguredId("//server:foo_local_cache_test"),
				targetConfiguredId("//server:foo_remote_cache_test"),
				targetConfiguredId("//server:bar_test"),
			},
			Payload: &build_event_stream.BuildEvent_Expanded{},
		},
		// Configured Events
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:lib"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:lib"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_library rule",
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:foo_test"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:foo_test"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_test rule",
					TestSize:   build_event_stream.TestSize_MEDIUM,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:foo_local_cache_test"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:foo_local_cache_test"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_test rule",
					TestSize:   build_event_stream.TestSize_MEDIUM,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:foo_remote_cache_test"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:foo_remote_cache_test"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_test rule",
					TestSize:   build_event_stream.TestSize_MEDIUM,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:baz_test"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:baz_test"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_test rule",
					TestSize:   build_event_stream.TestSize_SMALL,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:bar_test"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:bar_test"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_test rule",
					TestSize:   build_event_stream.TestSize_SMALL,
				},
			},
		},
		// WorkspaceStatus Event
		&build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_WorkspaceStatus{},
		},
		// Completed Event
		&build_event_stream.BuildEvent{
			Id: targetCompletedId("//server:lib"),
			Payload: &build_event_stream.BuildEvent_Completed{
				Completed: &build_event_stream.TargetComplete{
					Success: true,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetCompletedId("//server:foo_test"),
			Children: []*build_event_stream.BuildEventId{
				testResultId("//server:foo_test"),
				testSummaryId("//server:foo_test"),
			},
			Payload: &build_event_stream.BuildEvent_Completed{
				Completed: &build_event_stream.TargetComplete{
					Success: true,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetCompletedId("//server:foo_local_cache_test"),
			Children: []*build_event_stream.BuildEventId{
				testResultId("//server:foo_local_cache_test"),
				testSummaryId("//server:foo_local_cache_test"),
			},
			Payload: &build_event_stream.BuildEvent_Completed{
				Completed: &build_event_stream.TargetComplete{
					Success: true,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetCompletedId("//server:foo_remote_cache_test"),
			Children: []*build_event_stream.BuildEventId{
				testResultId("//server:foo_remote_cache_test"),
				testSummaryId("//server:foo_remote_cache_test"),
			},
			Payload: &build_event_stream.BuildEvent_Completed{
				Completed: &build_event_stream.TargetComplete{
					Success: true,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetCompletedId("//server:baz_test"),
			Children: []*build_event_stream.BuildEventId{
				testResultId("//server:baz_test"),
				testSummaryId("//server:baz_test"),
			},
			Payload: &build_event_stream.BuildEvent_Completed{
				Completed: &build_event_stream.TargetComplete{
					Success: true,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetCompletedId("//server:bar_test"),
			Children: []*build_event_stream.BuildEventId{
				testResultId("//server:bar_test"),
				testSummaryId("//server:bar_test"),
			},
			Payload: &build_event_stream.BuildEvent_Completed{
				Completed: &build_event_stream.TargetComplete{},
			},
		},
		// No TestResult event for bar_test
		&build_event_stream.BuildEvent{
			Id: testResultId("//server:foo_test"),
			Payload: &build_event_stream.BuildEvent_TestResult{
				TestResult: &build_event_stream.TestResult{
					Status: build_event_stream.TestStatus_PASSED,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: testResultId("//server:foo_local_cache_test"),
			Payload: &build_event_stream.BuildEvent_TestResult{
				TestResult: &build_event_stream.TestResult{
					Status:        build_event_stream.TestStatus_PASSED,
					CachedLocally: true,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: testResultId("//server:foo_remote_cache_test"),
			Payload: &build_event_stream.BuildEvent_TestResult{
				TestResult: &build_event_stream.TestResult{
					Status: build_event_stream.TestStatus_PASSED,
					ExecutionInfo: &build_event_stream.TestResult_ExecutionInfo{
						CachedRemotely: true,
					},
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: testResultId("//server:baz_test"),
			Payload: &build_event_stream.BuildEvent_TestResult{
				TestResult: &build_event_stream.TestResult{
					Status: build_event_stream.TestStatus_FAILED,
				},
			},
		},
		// No TestSummary event for bar_test
		&build_event_stream.BuildEvent{
			Id: testSummaryId("//server:foo_test"),
			Payload: &build_event_stream.BuildEvent_TestSummary{
				TestSummary: &build_event_stream.TestSummary{
					OverallStatus: build_event_stream.TestStatus_PASSED,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: testSummaryId("//server:foo_local_cache_test"),
			Payload: &build_event_stream.BuildEvent_TestSummary{
				TestSummary: &build_event_stream.TestSummary{
					OverallStatus: build_event_stream.TestStatus_PASSED,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: testSummaryId("//server:foo_remote_cache_test"),
			Payload: &build_event_stream.BuildEvent_TestSummary{
				TestSummary: &build_event_stream.TestSummary{
					OverallStatus: build_event_stream.TestStatus_PASSED,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: testSummaryId("//server:baz_test"),
			Payload: &build_event_stream.BuildEvent_TestSummary{
				TestSummary: &build_event_stream.TestSummary{
					OverallStatus: build_event_stream.TestStatus_FAILED,
				},
			},
		},
		// Last Event
		&build_event_stream.BuildEvent{
			LastMessage: true,
		},
	}

	for _, e := range events {
		tracker.TrackTargetsForEvent(ctx, e)
	}

	expected := []Row{
		{
			Role:       "CI",
			GroupID:    "GROUP1",
			CommitSHA:  "abcdef",
			Command:    "test",
			RuleType:   "go_test rule",
			Label:      "//server:baz_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_SMALL),
			Status:     int32(build_event_stream.TestStatus_FAILED),
			TargetType: int32(cmpb.TargetType_TEST),
		},
		{
			Role:       "CI",
			GroupID:    "GROUP1",
			CommitSHA:  "abcdef",
			Command:    "test",
			RuleType:   "go_test rule",
			Label:      "//server:foo_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_MEDIUM),
			Status:     int32(build_event_stream.TestStatus_PASSED),
			TargetType: int32(cmpb.TargetType_TEST),
		},
		{
			Role:       "CI",
			GroupID:    "GROUP1",
			CommitSHA:  "abcdef",
			Command:    "test",
			RuleType:   "go_test rule",
			Label:      "//server:foo_local_cache_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_MEDIUM),
			Status:     int32(build_event_stream.TestStatus_PASSED),
			Cached:     true,
			TargetType: int32(cmpb.TargetType_TEST),
		},
		{
			Role:       "CI",
			GroupID:    "GROUP1",
			CommitSHA:  "abcdef",
			Command:    "test",
			RuleType:   "go_test rule",
			Label:      "//server:foo_remote_cache_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_MEDIUM),
			Status:     int32(build_event_stream.TestStatus_PASSED),
			Cached:     true,
			TargetType: int32(cmpb.TargetType_TEST),
		},
		{
			Role:       "CI",
			GroupID:    "GROUP1",
			CommitSHA:  "abcdef",
			Command:    "test",
			RuleType:   "go_test rule",
			Label:      "//server:bar_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_SMALL),
			Status:     int32(build_event_stream.TestStatus_FAILED_TO_BUILD),
			TargetType: int32(cmpb.TargetType_TEST),
		},
	}
	if tracker.WriteToOLAPDBEnabled() {
		assertTestTargetStatusesMatchOLAPDB(t, te, expected)
	} else {
		assertTestTargetStatusesMatchPrimaryDB(t, ctx, te, testUUID, expected)
	}
}

func TestTargetTracking_BuildGraphIsADag(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(ta)
	flags.Set(t, "app.enable_target_tracking", true)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "USER1")
	require.NoError(t, err)

	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)

	accumulator := newFakeAccumulator(t, testUUID.String())
	tracker := target_tracker.NewTargetTracker(te, accumulator)

	events := []*build_event_stream.BuildEvent{
		&build_event_stream.BuildEvent{
			Children: []*build_event_stream.BuildEventId{
				targetConfiguredId("//server:baz_test"),
				targetConfiguredId("//server:bar_test"),
			},
			Payload: &build_event_stream.BuildEvent_Expanded{},
		},
		// Configured Events
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:baz_test"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:baz_test"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_test rule",
					TestSize:   build_event_stream.TestSize_SMALL,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetConfiguredIdWithAspect("//server:baz_test", "some.bzl%some_aspect"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedIdWithAspect("//server:baz_test", "some.bzl%some_aspect"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:bar_test"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:bar_test"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_test rule",
					TestSize:   build_event_stream.TestSize_SMALL,
				},
			},
		},
		// WorkspaceStatus Event
		&build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_WorkspaceStatus{},
		},
		// Completed Events
		&build_event_stream.BuildEvent{
			Id: targetCompletedId("//server:baz_test"),
			Children: []*build_event_stream.BuildEventId{
				testResultId("//server:baz_test"),
				testSummaryId("//server:baz_test"),
			},
			Payload: &build_event_stream.BuildEvent_Completed{
				Completed: &build_event_stream.TargetComplete{
					Success: true,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetCompletedIdWithAspect("//server:baz_test", "some.bzl%some_aspect"),
			Payload: &build_event_stream.BuildEvent_Completed{
				Completed: &build_event_stream.TargetComplete{
					Success: true,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetCompletedId("//server:bar_test"),
			Children: []*build_event_stream.BuildEventId{
				testResultId("//server:bar_test"),
				testSummaryId("//server:bar_test"),
				// Having these events as children shouldn't break target tracking.
				testResultId("//server:baz_test"),
				testSummaryId("//server:baz_test"),
			},
			Payload: &build_event_stream.BuildEvent_Completed{
				Completed: &build_event_stream.TargetComplete{},
			},
		},
		&build_event_stream.BuildEvent{
			Id: testResultId("//server:bar_test"),
			Payload: &build_event_stream.BuildEvent_TestResult{
				TestResult: &build_event_stream.TestResult{
					Status: build_event_stream.TestStatus_PASSED,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: testResultId("//server:baz_test"),
			Payload: &build_event_stream.BuildEvent_TestResult{
				TestResult: &build_event_stream.TestResult{
					Status: build_event_stream.TestStatus_FAILED,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: testSummaryId("//server:bar_test"),
			Payload: &build_event_stream.BuildEvent_TestSummary{
				TestSummary: &build_event_stream.TestSummary{
					OverallStatus: build_event_stream.TestStatus_PASSED,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: testSummaryId("//server:baz_test"),
			Payload: &build_event_stream.BuildEvent_TestSummary{
				TestSummary: &build_event_stream.TestSummary{
					OverallStatus: build_event_stream.TestStatus_FAILED,
				},
			},
		},
		// Last Event
		&build_event_stream.BuildEvent{
			LastMessage: true,
		},
	}

	for _, e := range events {
		tracker.TrackTargetsForEvent(ctx, e)
	}

	expected := []Row{
		{
			Role:       "CI",
			GroupID:    "GROUP1",
			CommitSHA:  "abcdef",
			Command:    "test",
			RuleType:   "go_test rule",
			Label:      "//server:baz_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_SMALL),
			Status:     int32(build_event_stream.TestStatus_FAILED),
			TargetType: int32(cmpb.TargetType_TEST),
		},
		{
			Role:       "CI",
			GroupID:    "GROUP1",
			CommitSHA:  "abcdef",
			Command:    "test",
			RuleType:   "go_test rule",
			Label:      "//server:bar_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_SMALL),
			Status:     int32(build_event_stream.TestStatus_PASSED),
			TargetType: int32(cmpb.TargetType_TEST),
		},
	}
	if tracker.WriteToOLAPDBEnabled() {
		assertTestTargetStatusesMatchOLAPDB(t, te, expected)
	} else {
		assertTestTargetStatusesMatchPrimaryDB(t, ctx, te, testUUID, expected)
	}
}

func TestTrackTargetsForEventsAborted(t *testing.T) {
	te := testenv.GetTestEnv(t)
	user := &testauth.TestUser{
		UserID:  "USER1",
		GroupID: "GROUP1",
	}
	ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{user.UserID: user})
	te.SetAuthenticator(ta)
	ctx := testauth.WithAuthenticatedUserInfo(context.Background(), user)

	flags.Set(t, "app.enable_target_tracking", true)

	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)

	accumulator := newFakeAccumulator(t, testUUID.String())
	tracker := target_tracker.NewTargetTracker(te, accumulator)

	events := []*build_event_stream.BuildEvent{
		&build_event_stream.BuildEvent{
			Children: []*build_event_stream.BuildEventId{
				targetConfiguredId("//server:lib"),
				targetConfiguredId("//server:baz_test"),
				targetConfiguredId("//server:foo_test"),
			},
			Payload: &build_event_stream.BuildEvent_Expanded{},
		},
		// Configured Events
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:lib"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:lib"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_library rule",
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:foo_test"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:foo_test"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_test rule",
					TestSize:   build_event_stream.TestSize_MEDIUM,
				},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetConfiguredId("//server:baz_test"),
			Children: []*build_event_stream.BuildEventId{
				targetCompletedId("//server:baz_test"),
			},
			Payload: &build_event_stream.BuildEvent_Configured{
				Configured: &build_event_stream.TargetConfigured{
					TargetKind: "go_test rule",
					TestSize:   build_event_stream.TestSize_SMALL,
				},
			},
		},
		// WorkspaceStatus Event
		&build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_WorkspaceStatus{},
		},
		// Completed Event
		&build_event_stream.BuildEvent{
			Id: targetCompletedId("//server:lib"),
			Payload: &build_event_stream.BuildEvent_Aborted{
				Aborted: &build_event_stream.Aborted{},
			},
		},
		&build_event_stream.BuildEvent{
			Id: targetCompletedId("//server:foo_test"),
			Payload: &build_event_stream.BuildEvent_Aborted{
				Aborted: &build_event_stream.Aborted{},
			},
		},
		&build_event_stream.BuildEvent{
			Id:      targetCompletedId("//server:baz_test"),
			Payload: &build_event_stream.BuildEvent_Aborted{},
		},
		// Last Event
		&build_event_stream.BuildEvent{
			LastMessage: true,
		},
	}

	for _, e := range events {
		tracker.TrackTargetsForEvent(ctx, e)
	}

	expected := []Row{
		{
			RuleType:   "go_test rule",
			Label:      "//server:baz_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_SMALL),
			Status:     int32(build_event_stream.TestStatus_FAILED_TO_BUILD),
			TargetType: int32(cmpb.TargetType_TEST),
			Role:       "CI",
			GroupID:    "GROUP1",
			CommitSHA:  "abcdef",
			Command:    "test",
		},
		{
			RuleType:   "go_test rule",
			Label:      "//server:foo_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_MEDIUM),
			Status:     int32(build_event_stream.TestStatus_FAILED_TO_BUILD),
			TargetType: int32(cmpb.TargetType_TEST),
			Role:       "CI",
			GroupID:    "GROUP1",
			CommitSHA:  "abcdef",
			Command:    "test",
		},
	}
	if tracker.WriteToOLAPDBEnabled() {
		assertTestTargetStatusesMatchOLAPDB(t, te, expected)
	} else {
		assertTestTargetStatusesMatchPrimaryDB(t, ctx, te, testUUID, expected)
	}
}

func assertTestTargetStatusesMatchOLAPDB(t *testing.T, te *testenv.TestEnv, expected []Row) {
	var got []Row
	query := `SELECT group_id, commit_sha, rule_type, label, repo_url, branch_name, role, command, test_size, status, cached, target_type FROM "TestTargetStatuses"`
	err := te.GetOLAPDBHandle().NewQuery(context.Background(), "get_target_status").Raw(query).Take(&got)
	require.NoError(t, err)
	assert.ElementsMatch(t, got, expected)
}

func assertTestTargetStatusesMatchPrimaryDB(t *testing.T, ctx context.Context, te *testenv.TestEnv, testUUID uuid.UUID, expected []Row) {
	invocationUUID, err := testUUID.MarshalBinary()
	require.NoError(t, err)
	te.GetInvocationDB().CreateInvocation(ctx, &tables.Invocation{
		InvocationID:   testUUID.String(),
		InvocationUUID: invocationUUID,
		RepoURL:        "bb/foo",
		Role:           "CI",
		CommitSHA:      "abcdef",
		Command:        "test",
	})
	var got []Row
	query := `SELECT i.group_id, i.commit_sha, t.rule_type, t.label, i.repo_url, i.branch_name,
      i.role, i.command, ts.test_size, ts.status, ts.cached, ts.target_type 
	  FROM "Targets" as t 
	  JOIN "TargetStatuses" as ts ON ts.target_id = t.target_id 
	  JOIN Invocations as i ON i.invocation_uuid = ts.invocation_uuid`
	err = te.GetDBHandle().NewQuery(ctx, "get_target_statuses").Raw(query).Take(&got)
	require.NoError(t, err)
	assert.ElementsMatch(t, got, expected)
}
