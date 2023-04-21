package target_tracker_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/target_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

type Row struct {
	TestSize   int32
	Status     int32
	TargetType int32
	RuleType   string
	Label      string
	RepoURL    string
}

func targetConfiguredId(label string) *build_event_stream.BuildEventId {
	return &build_event_stream.BuildEventId{
		Id: &build_event_stream.BuildEventId_TargetConfigured{
			TargetConfigured: &build_event_stream.BuildEventId_TargetConfiguredId{
				Label: label,
			},
		},
	}
}

func targetCompletedId(label string) *build_event_stream.BuildEventId {
	return &build_event_stream.BuildEventId{
		Id: &build_event_stream.BuildEventId_TargetCompleted{
			TargetCompleted: &build_event_stream.BuildEventId_TargetCompletedId{
				Label: label,
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
}

func (a *fakeAccumulator) Invocation() *inpb.Invocation {
	return &inpb.Invocation{
		Role:         a.role,
		Command:      a.command,
		RepoUrl:      a.repoURL,
		InvocationId: a.invocationID,
	}
}

func (a *fakeAccumulator) Role() string {
	return a.role
}

func (a *fakeAccumulator) RepoURL() string {
	return a.repoURL
}

func (a *fakeAccumulator) StartTime() time.Time {
	return time.Now()
}

func (a *fakeAccumulator) CommitSHA() string {
	return ""
}

func (a *fakeAccumulator) DisableCommitStatusReporting() bool {
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

func (a *fakeAccumulator) WorkspaceIsLoaded() bool {
	return true
}

func (a *fakeAccumulator) BuildMetadataIsLoaded() bool {
	return true
}

func (a *fakeAccumulator) BuildFinished() bool {
	return true
}

func newFakeAccumulator(t *testing.T) *fakeAccumulator {
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()
	return &fakeAccumulator{
		invocationID: testInvocationID,
		command:      "test",
		role:         "CI",
		repoURL:      "bb/foo",
	}
}

func TestTrackTargetsForEvents(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(ta)
	flags.Set(t, "app.enable_target_tracking", true)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "USER1")
	require.NoError(t, err)

	accumulator := newFakeAccumulator(t)
	tracker := target_tracker.NewTargetTracker(te, accumulator)

	events := []*build_event_stream.BuildEvent{
		&build_event_stream.BuildEvent{
			Children: []*build_event_stream.BuildEventId{
				targetConfiguredId("//server:lib"),
				targetConfiguredId("//server:baz_test"),
				targetConfiguredId("//server:foo_test"),
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
		// TestResult event for foo_test and baz_test; no TestResult event for
		// bar_test
		&build_event_stream.BuildEvent{
			Id: testResultId("//server:foo_test"),
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
		// TestSummary Event for foo_test and baz_test; no TestSummary event for
		// bar_test
		&build_event_stream.BuildEvent{
			Id: testSummaryId("//server:foo_test"),
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
			RuleType:   "go_test rule",
			Label:      "//server:baz_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_SMALL),
			Status:     int32(build_event_stream.TestStatus_FAILED),
			TargetType: int32(cmpb.TargetType_TEST),
		},
		{
			RuleType:   "go_test rule",
			Label:      "//server:foo_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_MEDIUM),
			Status:     int32(build_event_stream.TestStatus_PASSED),
			TargetType: int32(cmpb.TargetType_TEST),
		},
		{
			RuleType:   "go_test rule",
			Label:      "//server:bar_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_SMALL),
			Status:     int32(build_event_stream.TestStatus_FAILED_TO_BUILD),
			TargetType: int32(cmpb.TargetType_TEST),
		},
	}
	assertTargetsAndTargetStatusesMatch(t, te, expected)
}

func TestTrackTargetsForEventsAborted(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(ta)
	flags.Set(t, "app.enable_target_tracking", true)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "USER1")
	require.NoError(t, err)

	accumulator := newFakeAccumulator(t)
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
		},
		{
			RuleType:   "go_test rule",
			Label:      "//server:foo_test",
			RepoURL:    "bb/foo",
			TestSize:   int32(cmpb.TestSize_MEDIUM),
			Status:     int32(build_event_stream.TestStatus_FAILED_TO_BUILD),
			TargetType: int32(cmpb.TargetType_TEST),
		},
	}
	assertTargetsAndTargetStatusesMatch(t, te, expected)
}

func assertTargetsAndTargetStatusesMatch(t *testing.T, te *testenv.TestEnv, expected []Row) {
	var got []Row
	query := `SELECT rule_type, label, repo_url, test_size, status, target_type FROM "Targets" t JOIN "TargetStatuses" ts ON t.target_id = ts.target_id`
	err := te.GetDBHandle().DB(context.Background()).Raw(query).Scan(&got).Error
	require.NoError(t, err)
	assert.ElementsMatch(t, got, expected)
}
