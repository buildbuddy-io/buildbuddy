package build_event_handler

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/error_tracking"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/junit"
	"github.com/stretchr/testify/require"

	bspb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
)

func TestErrorOutputBoundsInlineContentsBeforeConversion(t *testing.T) {
	event := &bspb.BuildEvent{Payload: &bspb.BuildEvent_Action{Action: &bspb.ActionExecuted{
		Stderr: &bspb.File{File: &bspb.File_Contents{Contents: []byte(strings.Repeat("x", error_tracking.MaxMessageBytes*100))}},
	}}}

	output, file := errorOutput(event)

	require.Len(t, output, error_tracking.MaxMessageBytes)
	require.Nil(t, file)
}

func TestLiveErrorTrackingBudgetIsSharedAndReleased(t *testing.T) {
	var liveBytes atomic.Int64
	channels := make([]*EventChannel, maxLiveErrorBytes/liveErrorStreamReservation+1)
	for i := range channels {
		channels[i] = &EventChannel{liveErrorBytes: &liveBytes}
	}
	for _, channel := range channels[:len(channels)-1] {
		require.True(t, channel.reserveErrorTracking())
	}
	require.Equal(t, int64(maxLiveErrorBytes), liveBytes.Load())
	require.False(t, channels[len(channels)-1].reserveErrorTracking())

	channels[0].releaseErrorTrackingReservation()
	replacement := &EventChannel{liveErrorBytes: &liveBytes}
	require.True(t, replacement.reserveErrorTracking())
	for _, channel := range channels {
		channel.releaseErrorTrackingReservation()
	}
	replacement.releaseErrorTrackingReservation()
	require.Zero(t, liveBytes.Load())
}

func TestCollectTestAttemptDoesNotChargeDroppedArtifact(t *testing.T) {
	e := &EventChannel{testAttempts: make([]*testAttempt, 0, maxTestAttemptsPerInvocation)}
	for i := 0; i < maxTestAttemptsPerInvocation; i++ {
		e.testAttempts = append(e.testAttempts, &testAttempt{key: testTargetKey{targetLabel: "//pkg:test_" + strconv.Itoa(i)}})
	}
	event := testResultWithArtifact("//pkg:dropped", []byte(strings.Repeat("x", 1024)))

	e.collectTestAttempt(event, 1, 1)

	require.Zero(t, e.testArtifactBytes)
	require.Len(t, e.testAttempts, maxTestAttemptsPerInvocation)
}

func TestCollectTestAttemptRefundsReplacedArtifact(t *testing.T) {
	oldArtifact := &bspb.File{Name: "test.xml", File: &bspb.File_Contents{Contents: []byte(strings.Repeat("x", 1024))}}
	oldBytes := retainedArtifactBytes(oldArtifact)
	e := &EventChannel{
		testAttempts: []*testAttempt{
			{key: testTargetKey{targetLabel: "//pkg:duplicate"}, testXML: oldArtifact, artifactBytes: oldBytes},
			{key: testTargetKey{targetLabel: "//pkg:duplicate"}},
		},
		testArtifactBytes: oldBytes,
	}
	for len(e.testAttempts) < maxTestAttemptsPerInvocation {
		e.testAttempts = append(e.testAttempts, &testAttempt{key: testTargetKey{targetLabel: "//pkg:unique_" + strconv.Itoa(len(e.testAttempts))}})
	}
	newContents := []byte(`<testsuite><testcase name="new"><failure/></testcase></testsuite>`)

	e.collectTestAttempt(testResultWithArtifact("//pkg:new", newContents), 2, 2)

	require.Equal(t, "//pkg:new", e.testAttempts[0].key.targetLabel)
	require.Equal(t, retainedArtifactBytes(e.testAttempts[0].testXML), e.testArtifactBytes)
}

func TestCollectTestAttemptBoundsLiveMetadata(t *testing.T) {
	large := strings.Repeat("x", 1<<20)
	e := &EventChannel{}
	event := &bspb.BuildEvent{
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_TestResult{TestResult: &bspb.BuildEventId_TestResultId{
			Label: large, Configuration: &bspb.BuildEventId_ConfigurationId{Id: large},
		}}},
		Payload: &bspb.BuildEvent_TestResult{TestResult: &bspb.TestResult{
			Status: bspb.TestStatus_FAILED, StatusDetails: large,
			ExecutionInfo: &bspb.TestResult_ExecutionInfo{Strategy: large},
		}},
	}

	e.collectTestAttempt(event, 1, 1)

	require.Len(t, e.testAttempts, 1)
	require.LessOrEqual(t, len(e.testAttempts[0].key.targetLabel), maxTestTargetBytes)
	require.LessOrEqual(t, len(e.testAttempts[0].key.configurationID), maxTestConfigurationBytes)
	require.LessOrEqual(t, len(e.testAttempts[0].statusDetails), maxTestStatusDetailsBytes)
	require.LessOrEqual(t, len(e.testAttempts[0].strategy), maxTestStrategyBytes)
}

func TestCollectTestSummaryRefundsSuppressedAttemptArtifacts(t *testing.T) {
	artifact := &bspb.File{Name: "test.xml", File: &bspb.File_Contents{Contents: []byte("failure")}}
	artifactBytes := retainedArtifactBytes(artifact)
	key := testTargetKey{targetLabel: "//pkg:flaky", configurationID: "cfg"}
	e := &EventChannel{
		testAttempts:      []*testAttempt{{key: key, testXML: artifact, artifactBytes: artifactBytes}},
		testSummaries:     make(map[testTargetKey]bspb.TestStatus),
		testArtifactBytes: artifactBytes,
	}
	event := &bspb.BuildEvent{
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_TestSummary{TestSummary: &bspb.BuildEventId_TestSummaryId{
			Label: key.targetLabel, Configuration: &bspb.BuildEventId_ConfigurationId{Id: key.configurationID},
		}}},
		Payload: &bspb.BuildEvent_TestSummary{TestSummary: &bspb.TestSummary{OverallStatus: bspb.TestStatus_FLAKY}},
	}

	e.collectTestSummary(event)

	require.Empty(t, e.testAttempts)
	require.Zero(t, e.testArtifactBytes)
}

func TestSnapshotErrorFinalizerRetainsStructuredTestFailures(t *testing.T) {
	e := &EventChannel{
		attempt:                          1,
		testSummaries:                    make(map[testTargetKey]bspb.TestStatus),
		errorOutputFiles:                 make(map[*schema.ErrorOccurrence]*bspb.File),
		errorOccurrenceFingerprintCounts: make(map[string]int),
	}
	xml := []byte(`<testsuite name="checkout"><testcase classname="CardTest" name="Checkout"><failure message="Failed">aggregate failure</failure></testcase>` +
		`<testcase classname="CardTest" name="Checkout/expired"><failure type="AssertionError" message="expected active">checkout_test.py:42: expected active</failure></testcase>` +
		`<testcase classname="CardTest" name="Checkout/declined"><error type="RuntimeError" message="gateway unavailable">stack.py:81</error></testcase></testsuite>`)
	e.collectTestAttempt(testResultWithArtifact("//checkout:test", xml), 2, 3)

	finalizer := e.snapshotErrorFinalizer()
	occurrences := finalizer.testErrorOccurrences(context.Background(), "invocation-id")

	require.Len(t, occurrences, 2)
	require.ElementsMatch(t, []string{"Checkout/expired", "Checkout/declined"}, []string{occurrences[0].TestName, occurrences[1].TestName})
}

func TestLeafFailedTestCasesRetainsParentWithMeaningfulBody(t *testing.T) {
	cases := []junit.TestCase{
		{SuiteName: "suite", ClassName: "class", Name: "parent", Failures: []junit.Failure{{Body: "panic: cleanup failed"}}},
		{SuiteName: "suite", ClassName: "class", Name: "parent/child", Failures: []junit.Failure{{Message: "expected true"}}},
	}

	require.Len(t, leafFailedTestCases(cases), 2)
}

func TestAddErrorOccurrenceRefundsReplacedArtifact(t *testing.T) {
	oldArtifact := &bspb.File{Name: "stderr", File: &bspb.File_Contents{Contents: []byte(strings.Repeat("x", 1024))}}
	oldOccurrence := &schema.ErrorOccurrence{Fingerprint: "duplicate"}
	e := &EventChannel{
		errorOccurrences:                 make([]*schema.ErrorOccurrence, error_tracking.MaxRawOccurrencesPerInvocation),
		errorOccurrenceFingerprintCounts: map[string]int{"duplicate": error_tracking.MaxRawOccurrencesPerInvocation},
		errorOutputFiles:                 map[*schema.ErrorOccurrence]*bspb.File{oldOccurrence: oldArtifact},
		testArtifactBytes:                retainedArtifactBytes(oldArtifact),
	}
	for i := range e.errorOccurrences {
		e.errorOccurrences[i] = &schema.ErrorOccurrence{Fingerprint: "duplicate"}
	}
	e.errorOccurrences[0] = oldOccurrence
	newArtifact := &bspb.File{Name: "stderr", File: &bspb.File_Contents{Contents: []byte("new diagnostic")}}
	newOccurrence := &schema.ErrorOccurrence{Fingerprint: "new"}

	e.addErrorOccurrence(newOccurrence, newArtifact)

	require.Same(t, newOccurrence, e.errorOccurrences[0])
	require.Equal(t, retainedArtifactBytes(e.errorOutputFiles[newOccurrence]), e.testArtifactBytes)
}

func testResultWithArtifact(target string, contents []byte) *bspb.BuildEvent {
	return &bspb.BuildEvent{
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_TestResult{TestResult: &bspb.BuildEventId_TestResultId{Label: target}}},
		Payload: &bspb.BuildEvent_TestResult{TestResult: &bspb.TestResult{
			Status:           bspb.TestStatus_FAILED,
			TestActionOutput: []*bspb.File{{Name: "test.xml", File: &bspb.File_Contents{Contents: contents}}},
		}},
	}
}
