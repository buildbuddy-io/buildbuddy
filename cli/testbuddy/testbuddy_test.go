package testbuddy_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/junit"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/normalize"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/buildbuddy-io/buildbuddy/cli/testbuddy"
)

func TestTargetLabelFromXMLPath(t *testing.T) {
	for _, test := range []struct {
		path  string
		label string
	}{
		{
			path:  "bazel-testlogs/server/foo/foo_test/test.xml",
			label: "//server/foo:foo_test",
		},
		{
			path:  "bazel-out/k8-fastbuild/testlogs/root_test/test.xml",
			label: "//:root_test",
		},
		{
			path:  "bazel-testlogs/server/foo/foo_test/run_1_of_100/test.xml",
			label: "//server/foo:foo_test",
		},
		{
			path:  "bazel-testlogs/server/foo/foo_test/shard_2_of_4/test.xml",
			label: "//server/foo:foo_test",
		},
		{
			path:  "bazel-testlogs/server/foo/foo_test/shard_2_of_4_run_37_of_100/test.xml",
			label: "//server/foo:foo_test",
		},
	} {
		t.Run(test.path, func(t *testing.T) {
			label, err := testbuddy.TargetLabelFromXMLPath("/workspace", test.path)
			require.NoError(t, err)
			require.Equal(t, test.label, label)
		})
	}
}

func TestBazelTargetOutcome(t *testing.T) {
	dir := t.TempDir()
	xmlPath := filepath.Join(dir, "test.xml")
	require.NoError(t, os.WriteFile(xmlPath, []byte("<testsuite/>"), 0o644))

	outcome, err := testbuddy.BazelTargetOutcome(xmlPath)
	require.NoError(t, err)
	require.Equal(t, tbpb.TestOutcome_TEST_OUTCOME_PASS, outcome)

	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "test.log"),
		[]byte("test output\n-- Test timed out at 2026-07-30 12:00:00 UTC --\n"),
		0o644))
	outcome, err = testbuddy.BazelTargetOutcome(xmlPath)
	require.NoError(t, err)
	require.Equal(t, tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT, outcome)

	target, cases, err := testbuddy.ResultsForReport(
		xmlPath, "//pkg:timeout_test", "https://app.buildbuddy.io/invocation/one", 1_700_000, &junit.Report{
			DurationUsec: 1_000_000,
			Cases: []normalize.CaseRecord{{
				TargetLabel: "//pkg:timeout_test", CaseName: "TestTimeout",
				Outcome: tbpb.TestOutcome_TEST_OUTCOME_FAIL,
			}},
		})
	require.NoError(t, err)
	require.Equal(t, tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT, target.GetResult().GetOutcome())
	require.Equal(t, int64(1_000_000), target.GetResult().GetDurationUsec())
	require.Equal(t, int64(1_700_000), target.GetResult().GetEventTimeUsec())
	require.Len(t, target.GetResult().GetResultId(), 64)
	require.Empty(t, cases)

	require.NoError(t, os.Remove(filepath.Join(dir, "test.log")))
	target, cases, err = testbuddy.ResultsForReport(
		xmlPath, "//pkg:harness_test", "https://app.buildbuddy.io/invocation/one",
		1_700_000, &junit.Report{UnattributedFailure: true})
	require.NoError(t, err)
	require.Equal(t, tbpb.TestOutcome_TEST_OUTCOME_FAIL, target.GetResult().GetOutcome())
	require.Empty(t, cases)
}

func TestResultsForReportRetainsTimeAndStableIdentity(t *testing.T) {
	report := &junit.Report{
		EventTimeUsec: 1_000_000,
		Cases: []normalize.CaseRecord{
			{TargetLabel: "//pkg:test", CaseName: "TestCase", Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS, OccurrenceIndex: 0},
			{TargetLabel: "//pkg:test", CaseName: "TestCase", Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS, EventTimeUsec: 2_000_000, OccurrenceIndex: 1},
		},
	}
	pathA := filepath.Join(t.TempDir(), "bazel-testlogs/pkg/test/run_1_of_2/test.xml")
	pathB := filepath.Join(t.TempDir(), "bazel-testlogs/pkg/test/run_1_of_2/test.xml")
	targetA, casesA, err := testbuddy.ResultsForReport(
		pathA, "//pkg:test", "https://app.buildbuddy.io/invocation/one", 3_000_000, report)
	require.NoError(t, err)
	targetB, casesB, err := testbuddy.ResultsForReport(
		pathB, "//pkg:test", "https://app.buildbuddy.io/invocation/one", 3_000_000, report)
	require.NoError(t, err)
	require.Equal(t, targetA.GetResult().GetResultId(), targetB.GetResult().GetResultId())
	require.Equal(t, int64(1_000_000), targetA.GetResult().GetEventTimeUsec())
	require.Equal(t, casesA[0].GetResult().GetResultId(), casesB[0].GetResult().GetResultId())
	require.NotEqual(t, casesA[0].GetResult().GetResultId(), casesA[1].GetResult().GetResultId())
	require.Equal(t, int64(1_000_000), casesA[0].GetResult().GetEventTimeUsec())
	require.Equal(t, int64(2_000_000), casesA[1].GetResult().GetEventTimeUsec())
}

func TestReportBatcherKeepsMessagesWithinBudget(t *testing.T) {
	// Enough results that no single message can hold them all.
	const caseCount = 40_000
	var sent []*tbpb.ReportTestResultsRequest
	batcher := testbuddy.NewReportBatcher("https://github.com/acme/repo",
		func(req *tbpb.ReportTestResultsRequest) error {
			sent = append(sent, req)
			return nil
		})
	require.NoError(t, batcher.AddTarget(&tbpb.TestTargetResult{
		Identity: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
		Result: &tbpb.TestResult{
			Outcome:   tbpb.TestOutcome_TEST_OUTCOME_FAIL,
			SourceUrl: "https://app.buildbuddy.io/invocation/one",
			ResultId:  "target-result",
		},
	}))
	for i := range caseCount {
		require.NoError(t, batcher.AddCase(&tbpb.TestCaseResult{
			Identity: &tbpb.TestCaseIdentity{
				Target:   &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
				CaseName: fmt.Sprintf("TestCase%05d", i),
			},
			Result: &tbpb.TestResult{
				Outcome:        tbpb.TestOutcome_TEST_OUTCOME_FAIL,
				DurationUsec:   1_000,
				SourceUrl:      "https://app.buildbuddy.io/invocation/one",
				ResultId:       fmt.Sprintf("case-result-%05d", i),
				FailureMessage: strings.Repeat("f", normalize.MaxFailureMessageBytes),
			},
		}))
	}
	require.NoError(t, batcher.Flush())

	// The report spans messages rather than being truncated to one.
	require.Greater(t, len(sent), 1)
	seen := 0
	caseNames := make(map[string]bool, caseCount)
	for _, req := range sent {
		require.LessOrEqual(t, proto.Size(req), testbuddy.ReportRequestTargetBytes)
		// Every message must carry the repository; it is not sent once.
		require.Equal(t, "https://github.com/acme/repo", req.GetRepoUrl())
		seen += len(req.GetTestCases()) + len(req.GetTestTargets())
		for _, result := range req.GetTestCases() {
			caseNames[result.GetIdentity().GetCaseName()] = true
		}
	}
	// Nothing is dropped and nothing is duplicated across the split.
	require.Equal(t, caseCount+1, seen)
	require.Len(t, caseNames, caseCount)

	// Flushing again sends nothing: an empty message costs a round trip.
	before := len(sent)
	require.NoError(t, batcher.Flush())
	require.Len(t, sent, before)
}

func TestReportBatcherSendsOneMessageForASmallReport(t *testing.T) {
	var sent []*tbpb.ReportTestResultsRequest
	batcher := testbuddy.NewReportBatcher("https://github.com/acme/repo",
		func(req *tbpb.ReportTestResultsRequest) error {
			sent = append(sent, req)
			return nil
		})
	require.NoError(t, batcher.AddTarget(&tbpb.TestTargetResult{
		Identity: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
		Result:   &tbpb.TestResult{Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS},
	}))
	require.NoError(t, batcher.AddCase(&tbpb.TestCaseResult{
		Identity: &tbpb.TestCaseIdentity{
			Target:   &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
			CaseName: "TestCase",
		},
		Result: &tbpb.TestResult{Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS},
	}))
	require.Empty(t, sent, "nothing is sent before the report is complete")
	require.NoError(t, batcher.Flush())
	require.Len(t, sent, 1)
	require.Len(t, sent[0].GetTestTargets(), 1)
	require.Len(t, sent[0].GetTestCases(), 1)
}

func TestReportBatcherReportsSendFailure(t *testing.T) {
	batcher := testbuddy.NewReportBatcher("https://github.com/acme/repo",
		func(*tbpb.ReportTestResultsRequest) error {
			return errors.New("stream closed")
		})
	require.NoError(t, batcher.AddTarget(&tbpb.TestTargetResult{
		Identity: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
		Result:   &tbpb.TestResult{Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS},
	}))
	require.ErrorContains(t, batcher.Flush(), "stream closed")
}
