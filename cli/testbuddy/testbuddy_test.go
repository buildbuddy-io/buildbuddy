package testbuddy_test

import (
	"os"
	"path/filepath"
	"testing"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/junit"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/normalize"
	"github.com/stretchr/testify/require"

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
