package testbuddy_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/junit"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/normalize"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/buildbuddy-io/buildbuddy/cli/testbuddy"
)

func observationMetadata(source tbpb.TestObservationSource) testbuddy.ObservationMetadata {
	return testbuddy.ObservationMetadata{
		SourceURL: "https://app.buildbuddy.io/invocation/one", Source: source,
		CommitSHA: "abc123",
	}
}

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

func TestXMLPathsSelectsTheMostRecentlyWrittenBazelLayout(t *testing.T) {
	for _, test := range []struct {
		name          string
		olderLayout   []string
		currentLayout []string
	}{
		{
			name: "runs increased",
			olderLayout: []string{
				"run_1_of_3", "run_2_of_3", "run_3_of_3",
			},
			currentLayout: []string{
				"run_1_of_5", "run_2_of_5", "run_3_of_5", "run_4_of_5", "run_5_of_5",
			},
		},
		{
			name: "runs decreased",
			olderLayout: []string{
				"run_1_of_5", "run_2_of_5", "run_3_of_5", "run_4_of_5", "run_5_of_5",
			},
			currentLayout: []string{
				"run_1_of_3", "run_2_of_3", "run_3_of_3",
			},
		},
		{
			name: "shards and runs changed",
			olderLayout: []string{
				"shard_1_of_2_run_1_of_2", "shard_1_of_2_run_2_of_2",
				"shard_2_of_2_run_1_of_2", "shard_2_of_2_run_2_of_2",
			},
			currentLayout: []string{
				"shard_1_of_3_run_1_of_1", "shard_2_of_3_run_1_of_1", "shard_3_of_3_run_1_of_1",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "bazel-testlogs", "pkg", "unit_test")
			older := time.Unix(1_000, 0)
			current := older.Add(time.Minute)
			write := func(layout string, modified time.Time) string {
				path := filepath.Join(root, layout, "test.xml")
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
				require.NoError(t, os.WriteFile(path, []byte("<testsuite/>"), 0o644))
				require.NoError(t, os.Chtimes(path, modified, modified))
				return path
			}
			for _, layout := range test.olderLayout {
				write(layout, older)
			}
			want := make([]string, 0, len(test.currentLayout))
			for _, layout := range test.currentLayout {
				want = append(want, write(layout, current))
			}
			sort.Strings(want)

			got, err := testbuddy.FindTestXMLFiles([]string{filepath.Dir(filepath.Dir(root))})
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestXMLPathsSelectsDirectOutputWhenItIsNewest(t *testing.T) {
	root := filepath.Join(t.TempDir(), "bazel-testlogs", "pkg", "unit_test")
	stale := filepath.Join(root, "run_1_of_2", "test.xml")
	direct := filepath.Join(root, "test.xml")
	for _, path := range []string{stale, direct} {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte("<testsuite/>"), 0o644))
	}
	older := time.Unix(1_000, 0)
	require.NoError(t, os.Chtimes(stale, older, older))
	require.NoError(t, os.Chtimes(direct, older.Add(time.Minute), older.Add(time.Minute)))

	got, err := testbuddy.FindTestXMLFiles([]string{filepath.Dir(filepath.Dir(root))})
	require.NoError(t, err)
	require.Equal(t, []string{direct}, got)

	// Explicit files remain explicit even if a directory scan would select a
	// different layout.
	got, err = testbuddy.FindTestXMLFiles([]string{stale})
	require.NoError(t, err)
	require.Equal(t, []string{stale}, got)
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

	target, cases, err := testbuddy.ObservationsForReport(
		xmlPath, "//pkg:timeout_test",
		observationMetadata(tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR), &junit.Report{
			EventTimeUsec: 1_700_000,
			DurationUsec:  1_000_000,
			Cases: []normalize.CaseRecord{{
				TargetLabel: "//pkg:timeout_test", CaseName: "TestTimeout",
				Outcome: tbpb.TestOutcome_TEST_OUTCOME_FAIL,
			}},
		})
	require.NoError(t, err)
	require.Equal(t, tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT, target.GetObservation().GetOutcome())
	require.Equal(t, int64(1_000_000), target.GetObservation().GetDurationUsec())
	require.Equal(t, int64(1_700_000), target.GetObservation().GetEventTimeUsec())
	require.Len(t, target.GetObservation().GetObservationId(), 64)
	require.Empty(t, cases)

	require.NoError(t, os.Remove(filepath.Join(dir, "test.log")))
	target, cases, err = testbuddy.ObservationsForReport(
		xmlPath, "//pkg:harness_test",
		observationMetadata(tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR),
		&junit.Report{EventTimeUsec: 1_700_000, UnattributedFailure: true})
	require.NoError(t, err)
	require.Equal(t, tbpb.TestOutcome_TEST_OUTCOME_FAIL, target.GetObservation().GetOutcome())
	require.Empty(t, cases)
}

func TestObservationsForReportRetainsTimeAndStableIdentity(t *testing.T) {
	report := &junit.Report{
		EventTimeUsec: 1_000_000,
		Cases: []normalize.CaseRecord{
			{TargetLabel: "//pkg:test", CaseName: "TestCase", Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS, OccurrenceIndex: 0},
			{TargetLabel: "//pkg:test", CaseName: "TestCase", Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS, EventTimeUsec: 2_000_000, OccurrenceIndex: 1},
		},
	}
	pathA := filepath.Join(t.TempDir(), "bazel-testlogs/pkg/test/run_1_of_2/test.xml")
	pathB := filepath.Join(t.TempDir(), "bazel-testlogs/pkg/test/run_1_of_2/test.xml")
	targetA, casesA, err := testbuddy.ObservationsForReport(
		pathA, "//pkg:test",
		observationMetadata(tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_PRESUBMIT), report)
	require.NoError(t, err)
	targetB, casesB, err := testbuddy.ObservationsForReport(
		pathB, "//pkg:test",
		observationMetadata(tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_PRESUBMIT), report)
	require.NoError(t, err)
	require.Equal(t, targetA.GetObservation().GetObservationId(), targetB.GetObservation().GetObservationId())
	require.Equal(t, int64(1_000_000), targetA.GetObservation().GetEventTimeUsec())
	require.Equal(t, tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_PRESUBMIT, targetA.GetObservation().GetSource())
	require.Equal(t, "abc123", targetA.GetObservation().GetCommitSha())
	require.False(t, targetA.GetObservation().GetWorkspaceDirty())
	require.Equal(t, casesA[0].GetObservation().GetObservationId(), casesB[0].GetObservation().GetObservationId())
	require.NotEqual(t, casesA[0].GetObservation().GetObservationId(), casesA[1].GetObservation().GetObservationId())
	require.Equal(t, int64(1_000_000), casesA[0].GetObservation().GetEventTimeUsec())
	require.Equal(t, int64(2_000_000), casesA[1].GetObservation().GetEventTimeUsec())
}

func TestObservationsForReportUsesStableFileTimeWhenJUnitHasNoTimestamp(t *testing.T) {
	dir := t.TempDir()
	xmlPath := filepath.Join(dir, "test.xml")
	require.NoError(t, os.WriteFile(xmlPath, []byte("<testsuite/>"), 0o644))
	modified := time.Unix(1_700_000_000, 123_456_000)
	require.NoError(t, os.Chtimes(xmlPath, modified, modified))
	report := &junit.Report{Cases: []normalize.CaseRecord{{
		TargetLabel: "//pkg:test", CaseName: "TestCase",
		Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS,
	}}}

	targetA, casesA, err := testbuddy.ObservationsForReport(
		xmlPath, "//pkg:test",
		observationMetadata(tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR), report)
	require.NoError(t, err)
	targetB, casesB, err := testbuddy.ObservationsForReport(
		xmlPath, "//pkg:test",
		observationMetadata(tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR), report)
	require.NoError(t, err)

	require.True(t, proto.Equal(targetA, targetB))
	require.Len(t, casesA, 1)
	require.Len(t, casesB, 1)
	require.True(t, proto.Equal(casesA[0], casesB[0]))
	require.Equal(t, modified.UnixMicro(), targetA.GetObservation().GetEventTimeUsec())
	require.Equal(t, modified.UnixMicro(), casesA[0].GetObservation().GetEventTimeUsec())
}

func TestParseObservationSource(t *testing.T) {
	require.Equal(t, "monitor", testbuddy.TestReportFlags.Lookup("source").DefValue)
	for value, want := range map[string]tbpb.TestObservationSource{
		"presubmit":  tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_PRESUBMIT,
		"postsubmit": tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_POSTSUBMIT,
		"monitor":    tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR,
	} {
		got, err := testbuddy.ParseObservationSource(value)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	_, err := testbuddy.ParseObservationSource("local")
	require.ErrorContains(t, err, "presubmit, postsubmit, or monitor")
}

func TestWorkspaceRevision(t *testing.T) {
	dir := t.TempDir()
	run := func(args ...string) string {
		command := exec.Command("git", args...)
		command.Dir = dir
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return strings.TrimSpace(string(output))
	}
	run("init")
	run("config", "user.email", "test@example.com")
	run("config", "user.name", "Test")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file"), []byte("one"), 0o644))
	run("add", "file")
	run("commit", "-m", "initial")
	wantCommit := run("rev-parse", "HEAD")

	commitSHA, dirty, err := testbuddy.WorkspaceRevision(dir)
	require.NoError(t, err)
	require.Equal(t, wantCommit, commitSHA)
	require.False(t, dirty)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "file"), []byte("two"), 0o644))
	commitSHA, dirty, err = testbuddy.WorkspaceRevision(dir)
	require.NoError(t, err)
	require.Equal(t, wantCommit, commitSHA)
	require.True(t, dirty)
}

func TestReportBatcherKeepsMessagesWithinBudget(t *testing.T) {
	// Enough observations that no single message can hold them all.
	const caseCount = 40_000
	var sent []*tbpb.ReportTestResultsRequest
	batcher := testbuddy.NewReportBatcher("https://github.com/acme/repo",
		func(req *tbpb.ReportTestResultsRequest) error {
			sent = append(sent, req)
			return nil
		})
	require.NoError(t, batcher.AddTargetObservation(&tbpb.TestTargetObservation{
		Identity: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
		Observation: &tbpb.TestObservation{
			Outcome:       tbpb.TestOutcome_TEST_OUTCOME_FAIL,
			SourceUrl:     "https://app.buildbuddy.io/invocation/one",
			ObservationId: "target-observation",
		},
	}))
	for i := range caseCount {
		require.NoError(t, batcher.AddCaseObservation(&tbpb.TestCaseObservation{
			Identity: &tbpb.TestCaseIdentity{
				Target:   &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
				CaseName: fmt.Sprintf("TestCase%05d", i),
			},
			Observation: &tbpb.TestObservation{
				Outcome:        tbpb.TestOutcome_TEST_OUTCOME_FAIL,
				DurationUsec:   1_000,
				SourceUrl:      "https://app.buildbuddy.io/invocation/one",
				ObservationId:  fmt.Sprintf("case-observation-%05d", i),
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
		seen += len(req.GetCaseObservations()) + len(req.GetTargetObservations())
		for _, observation := range req.GetCaseObservations() {
			caseNames[observation.GetIdentity().GetCaseName()] = true
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
	require.NoError(t, batcher.AddTargetObservation(&tbpb.TestTargetObservation{
		Identity:    &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
		Observation: &tbpb.TestObservation{Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS},
	}))
	require.NoError(t, batcher.AddCaseObservation(&tbpb.TestCaseObservation{
		Identity: &tbpb.TestCaseIdentity{
			Target:   &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
			CaseName: "TestCase",
		},
		Observation: &tbpb.TestObservation{Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS},
	}))
	require.Empty(t, sent, "nothing is sent before the report is complete")
	require.NoError(t, batcher.Flush())
	require.Len(t, sent, 1)
	require.Len(t, sent[0].GetTargetObservations(), 1)
	require.Len(t, sent[0].GetCaseObservations(), 1)
}

func TestReportBatcherReportsSendFailure(t *testing.T) {
	batcher := testbuddy.NewReportBatcher("https://github.com/acme/repo",
		func(*tbpb.ReportTestResultsRequest) error {
			return errors.New("stream closed")
		})
	require.NoError(t, batcher.AddTargetObservation(&tbpb.TestTargetObservation{
		Identity:    &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
		Observation: &tbpb.TestObservation{Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS},
	}))
	require.ErrorContains(t, batcher.Flush(), "stream closed")
}

func TestDiagnosticLogSeparatesDroppedCasesFromIgnoredFields(t *testing.T) {
	var lines []string
	diagnostics := testbuddy.NewDiagnosticLog(func(line string) { lines = append(lines, line) })
	diagnostics.Add("bazel-testlogs/pkg/test/test.xml", "//pkg:test", &junit.Report{
		Diagnostics: []junit.Diagnostic{
			{Code: junit.DiagnosticMissingName, CaseIndex: 4},
			{Code: junit.DiagnosticInvalidIdentity, CaseIndex: 5, CaseName: "Test\tTab"},
			{Code: junit.DiagnosticInvalidDuration, CaseIndex: 6, CaseName: "TestSlow"},
			{Code: junit.DiagnosticInvalidTimestamp, CaseIndex: -1},
			{Code: junit.DiagnosticInvalidUTF8, CaseIndex: -1},
		},
	})

	require.Equal(t, 2, diagnostics.Dropped)
	require.Equal(t, 2, diagnostics.Ignored)
	require.Equal(t, 1, diagnostics.Normalized)
	require.Equal(t, 0, diagnostics.Truncated)

	require.Len(t, lines, 5)
	// A case with no usable name is still located by file and index.
	require.Equal(t,
		`dropped case //pkg:test "<unnamed>" missing_name (bazel-testlogs/pkg/test/test.xml case 4)`, lines[0])
	// An unusable name is quoted so control characters are visible.
	require.Equal(t,
		`dropped case //pkg:test "Test\tTab" invalid_identity (bazel-testlogs/pkg/test/test.xml case 5)`, lines[1])
	require.Equal(t,
		`ignored field //pkg:test "TestSlow" invalid_duration (bazel-testlogs/pkg/test/test.xml case 6)`, lines[2])
	// A file-level diagnostic belongs to no case, so it names no index.
	require.Equal(t,
		`ignored field //pkg:test "<unnamed>" invalid_timestamp (bazel-testlogs/pkg/test/test.xml)`, lines[3])
	require.Equal(t,
		`normalized report //pkg:test invalid_utf8 (bazel-testlogs/pkg/test/test.xml)`, lines[4])

	summary := diagnostics.Summary()
	require.Contains(t, summary, "5 JUnit diagnostics: 2 cases dropped, 2 fields ignored, normalized reports: 1")
	// The bb parser rejects "--verbose=1"; the flag is a bare boolean.
	require.Contains(t, summary, "rerun with --verbose (or BB_VERBOSE=1) to list them")
	require.NotContains(t, summary, "--verbose=1")
}

func TestDiagnosticLogReportsTheParserCap(t *testing.T) {
	var lines []string
	diagnostics := testbuddy.NewDiagnosticLog(func(line string) { lines = append(lines, line) })
	diagnostics.Add("a.xml", "//pkg:test", &junit.Report{
		Diagnostics:        []junit.Diagnostic{{Code: junit.DiagnosticMissingName, CaseIndex: 0}},
		DroppedDiagnostics: 17,
	})
	require.Equal(t, 17, diagnostics.Truncated)
	require.Contains(t, lines[len(lines)-1], "17 more diagnostics in a.xml were not recorded")
	require.Contains(t, diagnostics.Summary(), "17 beyond the per-file cap")
}

func TestDiagnosticLogIsSilentWhenEverythingParsed(t *testing.T) {
	diagnostics := testbuddy.NewDiagnosticLog(nil)
	diagnostics.Add("a.xml", "//pkg:test", &junit.Report{})
	require.Empty(t, diagnostics.Summary())
}

func TestDiagnosticCodesThatDropTheCase(t *testing.T) {
	require.True(t, junit.DiagnosticMissingName.DropsCase())
	require.True(t, junit.DiagnosticInvalidIdentity.DropsCase())
	for _, code := range []junit.DiagnosticCode{
		junit.DiagnosticInvalidDuration,
		junit.DiagnosticInvalidTimestamp,
		junit.DiagnosticUnknownStatus,
		junit.DiagnosticInvalidDisabled,
		junit.DiagnosticInvalidUTF8,
	} {
		require.False(t, code.DropsCase(), code)
	}
}
