package event_index_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/api/common"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_index"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cmnpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const label = "//foo:bar_test"

func testResult(attempt int32, status bespb.TestStatus, start time.Time, duration time.Duration) *inpb.InvocationEvent {
	return &inpb.InvocationEvent{
		BuildEvent: &bespb.BuildEvent{
			Id: &bespb.BuildEventId{
				Id: &bespb.BuildEventId_TestResult{
					TestResult: &bespb.BuildEventId_TestResultId{Label: label, Run: 1, Shard: 1, Attempt: attempt},
				},
			},
			Payload: &bespb.BuildEvent_TestResult{
				TestResult: &bespb.TestResult{
					Status:              status,
					TestAttemptStart:    timestamppb.New(start),
					TestAttemptDuration: durationpb.New(duration),
				},
			},
		},
	}
}

func testSummary(status bespb.TestStatus, start time.Time, duration time.Duration) *inpb.InvocationEvent {
	return &inpb.InvocationEvent{
		BuildEvent: &bespb.BuildEvent{
			Id: &bespb.BuildEventId{
				Id: &bespb.BuildEventId_TestSummary{
					TestSummary: &bespb.BuildEventId_TestSummaryId{Label: label},
				},
			},
			Payload: &bespb.BuildEvent_TestSummary{
				TestSummary: &bespb.TestSummary{
					OverallStatus:    status,
					TotalRunCount:    1,
					FirstStartTime:   timestamppb.New(start),
					TotalRunDuration: durationpb.New(duration),
				},
			},
		},
	}
}

func TestAdd_TestResultBeforeTestSummary_ReportsProvisionalStatus(t *testing.T) {
	start := time.Unix(1000, 0)
	idx := event_index.New()

	idx.Add(testResult(1, bespb.TestStatus_FAILED, start, 2*time.Second))
	target := idx.TestTargetByLabel[label]
	require.NotNil(t, target)
	assert.Equal(t, cmnpb.Status_FAILED, target.GetStatus())

	idx.Add(testSummary(bespb.TestStatus_FLAKY, start, 5*time.Second))
	assert.Equal(t, cmnpb.Status_FLAKY, idx.TestTargetByLabel[label].GetStatus())
}

func TestAdd_TestResultAfterTestSummary_KeepsSummaryStatusAndTiming(t *testing.T) {
	start := time.Unix(1000, 0)
	idx := event_index.New()
	idx.Add(testResult(1, bespb.TestStatus_FAILED, start, 2*time.Second))
	idx.Add(testResult(2, bespb.TestStatus_PASSED, start.Add(2*time.Second), 3*time.Second))
	summary := testSummary(bespb.TestStatus_FLAKY, start, 5*time.Second)
	idx.Add(summary)

	// The test action was rewound after a later action lost one of its outputs
	// and executed again, reporting a passing attempt after the summary.
	idx.Add(testResult(1, bespb.TestStatus_PASSED, start.Add(10*time.Second), time.Second))

	target := idx.TestTargetByLabel[label]
	require.NotNil(t, target)
	assert.Equal(t, cmnpb.Status_FLAKY, target.GetStatus())
	expectedTiming := common.TestTimingFromSummary(summary.GetBuildEvent().GetTestSummary())
	assert.Empty(t, cmp.Diff(expectedTiming, target.GetTiming(), protocmp.Transform()))
	// The late result is still indexed for display.
	assert.Len(t, idx.TestResultEventsByLabel[label], 3)
}
