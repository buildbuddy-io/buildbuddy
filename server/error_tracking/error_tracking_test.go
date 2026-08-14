package error_tracking

import (
	"strings"
	"testing"
	"time"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	etpb "github.com/buildbuddy-io/buildbuddy/proto/error_tracking"
	fdpb "github.com/buildbuddy-io/buildbuddy/proto/failure_details"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/stretchr/testify/require"
)

func TestClassifyErrorOrigin(t *testing.T) {
	for _, test := range []struct {
		name        string
		role        string
		parentRunID string
		want        etpb.ErrorOrigin
	}{
		{name: "workflow orchestration", role: "CI_RUNNER", want: etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW},
		{name: "workflow bazel child", role: "CI", parentRunID: "run-parent", want: etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW_BAZEL_CHILD},
		{name: "standalone ci bazel", role: "CI", want: etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL},
		{name: "default bazel", want: etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL},
		{name: "hosted bazel", role: "HOSTED_BAZEL", want: etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, classifyErrorOrigin(test.role, test.parentRunID))
		})
	}
}

func TestApplyWorkflowFingerprintScopesByAction(t *testing.T) {
	base := &schema.ErrorOccurrence{Fingerprint: "underlying", FingerprintVersion: ActionFallbackFingerprintVersion}
	checkStyle, checkStyleAgain, tests := *base, *base, *base
	applyWorkflowFingerprint(&checkStyle, "Check style")
	applyWorkflowFingerprint(&checkStyleAgain, "Check  style")
	applyWorkflowFingerprint(&tests, "Test")

	require.Equal(t, checkStyle.Fingerprint, checkStyleAgain.Fingerprint)
	require.NotEqual(t, checkStyle.Fingerprint, tests.Fingerprint)
	require.Equal(t, WorkflowFingerprintVersion, checkStyle.FingerprintVersion)
	require.Equal(t, "workflow_bes", checkStyle.FingerprintSource)
	require.Equal(t, "low", checkStyle.FingerprintConfidence)
}

func TestExtractOccurrenceGroupsVariableIDs(t *testing.T) {
	makeEvent := func(message string) *bepb.BuildEvent {
		return &bepb.BuildEvent{Id: &bepb.BuildEventId{Id: &bepb.BuildEventId_ActionCompleted{ActionCompleted: &bepb.BuildEventId_ActionCompletedId{Label: "//pkg:target"}}}, Payload: &bepb.BuildEvent_Action{Action: &bepb.ActionExecuted{Success: false, Type: "GoCompilePkg", ExitCode: 1, FailureDetail: &fdpb.FailureDetail{Message: message, Category: &fdpb.FailureDetail_Spawn{Spawn: &fdpb.Spawn{Code: fdpb.Spawn_NON_ZERO_EXIT}}}}}}
	}
	a := ExtractOccurrence(makeEvent("process 123 failed for 0123456789abcdef"), "11111111-1111-1111-1111-111111111111", 1, 10, 20)
	b := ExtractOccurrence(makeEvent("process 456 failed for fedcba9876543210"), "22222222-2222-2222-2222-222222222222", 1, 11, 21)
	require.Equal(t, a.Fingerprint, b.Fingerprint)
	require.Equal(t, "//pkg:target", a.TargetLabel)
	require.Contains(t, a.ErrorType, "spawn")
}

func TestBodyDerivedTestFingerprintUsesDiagnosticAndFrame(t *testing.T) {
	base := TestFailure{TargetLabel: "//pkg:test", SuiteName: "suite", ClassName: "Class", TestName: "case", Kind: "failure", Type: "ValueError"}
	left := base
	left.Body = "Traceback (most recent call last):\n  File \"pkg/alpha.py\", line 12, in run\nValueError: expected protocol one"
	right := base
	right.Body = "Traceback (most recent call last):\n  File \"pkg/beta.py\", line 99, in run\nValueError: expected protocol two"

	leftFingerprint, _ := TestFailureFingerprint(left)
	rightFingerprint, _ := TestFailureFingerprint(right)

	require.NotEqual(t, leftFingerprint, rightFingerprint)
}

func TestCompilerFingerprintPreservesSemanticNumbers(t *testing.T) {
	left := occurrenceFingerprint("action/spawn/NON_ZERO_EXIT", "CppCompile", "static assertion failed: expected protocol version 1")
	right := occurrenceFingerprint("action/spawn/NON_ZERO_EXIT", "CppCompile", "static assertion failed: expected protocol version 2")

	require.NotEqual(t, left, right)
}

func TestEnrichOccurrenceGroupsSameDiagnosticAcrossLocations(t *testing.T) {
	makeOccurrence := func(output string) *schema.ErrorOccurrence {
		o := &schema.ErrorOccurrence{ErrorType: "action/spawn/NON_ZERO_EXIT", ActionMnemonic: "GoCompilePkg"}
		EnrichOccurrence(o, output)
		return o
	}
	a := makeOccurrence("compiler wrapper: error running command\nserver/foo/foo.go:13:9: undefined: sharedSymbol\ncompilepkg: exited 2")
	b := makeOccurrence("compiler wrapper: error running command\nenterprise/bar/bar.go:204:17: undefined: sharedSymbol\ncompilepkg: exited 2")
	c := makeOccurrence("compiler wrapper: error running command\nserver/foo/foo.go:13:9: undefined: otherSymbol\ncompilepkg: exited 2")
	require.Equal(t, a.Fingerprint, b.Fingerprint)
	require.NotEqual(t, a.Fingerprint, c.Fingerprint)
	require.Contains(t, a.Message, "foo.go:13:9")
	require.Equal(t, ActionFingerprintVersion, a.FingerprintVersion)
	require.Equal(t, "action_output", a.FingerprintSource)
	require.Equal(t, "high", a.FingerprintConfidence)
}

func TestEnrichOccurrenceUsesMultilineCompilerDiagnostic(t *testing.T) {
	makeOccurrence := func(target, symbol string) *schema.ErrorOccurrence {
		o := &schema.ErrorOccurrence{ErrorType: "action/spawn/NON_ZERO_EXIT", ActionMnemonic: "Javac", TargetLabel: target}
		EnrichOccurrence(o, "Foo.java:10: error: cannot find symbol\n  symbol: class "+symbol+"\n  location: class Foo")
		return o
	}
	alpha := makeOccurrence("//java:alpha", "Alpha")
	beta := makeOccurrence("//java:beta", "Beta")
	relocatedAlpha := &schema.ErrorOccurrence{ErrorType: "action/spawn/NON_ZERO_EXIT", ActionMnemonic: "Javac", TargetLabel: "//other:alpha"}
	EnrichOccurrence(relocatedAlpha, "src/Foo.java:911:27: error: cannot find symbol\n  symbol: class Alpha\n  location: class Foo")
	require.NotEqual(t, alpha.Fingerprint, beta.Fingerprint)
	require.Equal(t, alpha.Fingerprint, relocatedAlpha.Fingerprint)
}

func TestEnrichOccurrenceTargetScopesGenericOutput(t *testing.T) {
	a := &schema.ErrorOccurrence{ErrorType: "action/spawn/NON_ZERO_EXIT", ActionMnemonic: "Javac", TargetLabel: "//java:a"}
	b := &schema.ErrorOccurrence{ErrorType: "action/spawn/NON_ZERO_EXIT", ActionMnemonic: "Javac", TargetLabel: "//java:b"}
	EnrichOccurrence(a, "Foo.java:10: error: compilation failed")
	EnrichOccurrence(b, "Foo.java:99: error: compilation failed")
	require.NotEqual(t, a.Fingerprint, b.Fingerprint)
	require.Equal(t, ActionFallbackFingerprintVersion, a.FingerprintVersion)
	require.Equal(t, "low", a.FingerprintConfidence)
}

func TestExtractOccurrenceScopesGenericActionFallbackByTarget(t *testing.T) {
	makeEvent := func(target string) *bepb.BuildEvent {
		return &bepb.BuildEvent{
			Id: &bepb.BuildEventId{Id: &bepb.BuildEventId_ActionCompleted{ActionCompleted: &bepb.BuildEventId_ActionCompletedId{Label: target}}},
			Payload: &bepb.BuildEvent_Action{Action: &bepb.ActionExecuted{
				Success: false, Type: "GoCompilePkg", ExitCode: 1,
			}},
		}
	}
	a := ExtractOccurrence(makeEvent("//pkg:a"), "inv-a", 1, 1, 1)
	b := ExtractOccurrence(makeEvent("//pkg:b"), "inv-b", 1, 1, 1)
	require.NotEqual(t, a.Fingerprint, b.Fingerprint)
	require.Equal(t, ActionFallbackFingerprintVersion, a.FingerprintVersion)
	require.Equal(t, "action_event_fallback", a.FingerprintSource)
	require.Equal(t, "low", a.FingerprintConfidence)
}

func TestRootOccurrencesPrefersSpecificFailures(t *testing.T) {
	occurrence := func(errorType string) *schema.ErrorOccurrence {
		return &schema.ErrorOccurrence{ErrorType: errorType}
	}
	tests := []struct {
		name  string
		input []*schema.ErrorOccurrence
		want  []string
	}{
		{
			name:  "action removes cascade",
			input: []*schema.ErrorOccurrence{occurrence("action/spawn/NON_ZERO_EXIT"), occurrence("target/unknown"), occurrence("build/unknown"), occurrence("aborted/UNKNOWN")},
			want:  []string{"action/spawn/NON_ZERO_EXIT"},
		},
		{
			name:  "typed build removes loading abort",
			input: []*schema.ErrorOccurrence{occurrence("aborted/LOADING_FAILURE"), occurrence("build/package_loading/BUILD_FILE_MISSING")},
			want:  []string{"build/package_loading/BUILD_FILE_MISSING"},
		},
		{
			name:  "multiple action roots remain",
			input: []*schema.ErrorOccurrence{occurrence("action/spawn/NON_ZERO_EXIT"), occurrence("action/spawn/TIMEOUT"), occurrence("build/unknown")},
			want:  []string{"action/spawn/NON_ZERO_EXIT", "action/spawn/TIMEOUT"},
		},
		{
			name:  "independent action and loading roots remain",
			input: []*schema.ErrorOccurrence{occurrence("action/spawn/NON_ZERO_EXIT"), occurrence("build/package_loading/BUILD_FILE_MISSING")},
			want:  []string{"action/spawn/NON_ZERO_EXIT", "build/package_loading/BUILD_FILE_MISSING"},
		},
		{
			name: "structured test suppresses matching runner action only",
			input: []*schema.ErrorOccurrence{
				{ErrorType: "action/spawn/NON_ZERO_EXIT", TargetLabel: "//pkg:tested"},
				{ErrorType: "action/spawn/TIMEOUT", TargetLabel: "//pkg:tested"},
				{ErrorType: "action/spawn/NON_ZERO_EXIT", TargetLabel: "//pkg:independent"},
				{ErrorType: "test/FAILED/failure", TargetLabel: "//pkg:tested"},
			},
			want: []string{"action/spawn/TIMEOUT", "action/spawn/NON_ZERO_EXIT", "test/FAILED/failure"},
		},
		{
			name:  "unknown abort remains when it is the only failure",
			input: []*schema.ErrorOccurrence{occurrence("aborted/UNKNOWN")},
			want:  []string{"aborted/UNKNOWN"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := RootOccurrences(test.input)
			types := make([]string, 0, len(got))
			for _, occurrence := range got {
				types = append(types, occurrence.ErrorType)
			}
			require.Equal(t, test.want, types)
		})
	}
}

func TestDeduplicateOccurrencesPreservesDistinctTestAttempts(t *testing.T) {
	compiler := &schema.ErrorOccurrence{Fingerprint: "compiler"}
	structuredAttempt1 := &schema.ErrorOccurrence{
		Fingerprint: "test", FingerprintVersion: TestFingerprintVersion,
		TargetLabel: "//pkg:test", TestSuite: "suite", TestClass: "Class", TestName: "case",
		TestFailureKind: "failure", TestFailureType: "AssertionError", TestRun: 1, TestShard: 0, TestAttempt: 1,
	}
	structuredAttempt2 := *structuredAttempt1
	structuredAttempt2.TestAttempt = 2

	got := DeduplicateOccurrences([]*schema.ErrorOccurrence{
		compiler,
		compiler,
		structuredAttempt1,
		structuredAttempt1,
		&structuredAttempt2,
	})
	require.Len(t, got, 3)
	require.Same(t, compiler, got[0])
	require.Same(t, structuredAttempt1, got[1])
	require.Same(t, &structuredAttempt2, got[2])
}

func TestExtractOccurrenceClampsUntrustedEventTime(t *testing.T) {
	event := &bepb.BuildEvent{Payload: &bepb.BuildEvent_Aborted{Aborted: &bepb.Aborted{Reason: bepb.Aborted_INTERNAL, Description: "failed"}}}
	before := time.Now().UnixMicro()
	o := ExtractOccurrence(event, "id", 1, 1, time.Now().Add(365*24*time.Hour).UnixMicro())
	after := time.Now().UnixMicro()
	require.GreaterOrEqual(t, o.EventTimeUsec, before)
	require.LessOrEqual(t, o.EventTimeUsec, after)
}

func TestExtractOccurrenceUnknownFailureCode(t *testing.T) {
	event := &bepb.BuildEvent{Payload: &bepb.BuildEvent_Action{Action: &bepb.ActionExecuted{
		FailureDetail: &fdpb.FailureDetail{
			Message: "future failure",
			Category: &fdpb.FailureDetail_Spawn{Spawn: &fdpb.Spawn{
				Code: fdpb.Spawn_Code(999),
			}},
		},
	}}}
	o := ExtractOccurrence(event, "invocation", 1, 1, 1)
	require.NotNil(t, o)
	require.Contains(t, o.ErrorType, "UNKNOWN_999")
}

func TestExtractOccurrenceIgnoresSuccessAndBoundsMessage(t *testing.T) {
	require.Nil(t, ExtractOccurrence(&bepb.BuildEvent{Payload: &bepb.BuildEvent_Finished{Finished: &bepb.BuildFinished{ExitCode: &bepb.BuildFinished_ExitCode{Code: 0}}}}, "id", 1, 1, 1))
	for _, reason := range []bepb.Aborted_AbortReason{
		bepb.Aborted_USER_INTERRUPTED,
		bepb.Aborted_NO_ANALYZE,
		bepb.Aborted_NO_BUILD,
		bepb.Aborted_LOADING_FAILURE,
		bepb.Aborted_ANALYSIS_FAILURE,
		bepb.Aborted_SKIPPED,
		bepb.Aborted_INCOMPLETE,
	} {
		t.Run("ignores "+reason.String(), func(t *testing.T) {
			require.Nil(t, ExtractOccurrence(&bepb.BuildEvent{Payload: &bepb.BuildEvent_Aborted{Aborted: &bepb.Aborted{
				Reason: reason, Description: "non-actionable abort wrapper",
			}}}, "id", 1, 1, 1))
		})
	}
	e := &bepb.BuildEvent{Payload: &bepb.BuildEvent_Aborted{Aborted: &bepb.Aborted{Reason: bepb.Aborted_INTERNAL, Description: strings.Repeat("é", MaxMessageBytes)}}}
	o := ExtractOccurrence(e, "id", 1, 1, 1)
	require.LessOrEqual(t, len(o.Message), MaxMessageBytes)
	require.True(t, strings.HasPrefix(o.ErrorType, "aborted/"))
}

func TestExtractOccurrenceTypedFailures(t *testing.T) {
	tests := []struct {
		name       string
		event      *bepb.BuildEvent
		typePrefix string
	}{
		{
			name:       "target",
			event:      &bepb.BuildEvent{Id: &bepb.BuildEventId{Id: &bepb.BuildEventId_TargetCompleted{TargetCompleted: &bepb.BuildEventId_TargetCompletedId{Label: "//pkg:target"}}}, Payload: &bepb.BuildEvent_Completed{Completed: &bepb.TargetComplete{Success: false}}},
			typePrefix: "target/",
		},
		{
			name:       "test result",
			event:      &bepb.BuildEvent{Id: &bepb.BuildEventId{Id: &bepb.BuildEventId_TestResult{TestResult: &bepb.BuildEventId_TestResultId{Label: "//pkg:test"}}}, Payload: &bepb.BuildEvent_TestResult{TestResult: &bepb.TestResult{Status: bepb.TestStatus_FAILED, StatusDetails: "assertion failed"}}},
			typePrefix: "test/FAILED",
		},
		{
			name:       "test summary",
			event:      &bepb.BuildEvent{Id: &bepb.BuildEventId{Id: &bepb.BuildEventId_TestSummary{TestSummary: &bepb.BuildEventId_TestSummaryId{Label: "//pkg:test"}}}, Payload: &bepb.BuildEvent_TestSummary{TestSummary: &bepb.TestSummary{OverallStatus: bepb.TestStatus_TIMEOUT}}},
			typePrefix: "test_summary/TIMEOUT",
		},
		{
			name:       "finished",
			event:      &bepb.BuildEvent{Payload: &bepb.BuildEvent_Finished{Finished: &bepb.BuildFinished{ExitCode: &bepb.BuildFinished_ExitCode{Name: "BUILD_FAILURE", Code: 1}}}},
			typePrefix: "build/",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			o := ExtractOccurrence(test.event, "invocation", 1, 1, 1)
			require.NotNil(t, o)
			require.True(t, strings.HasPrefix(o.ErrorType, test.typePrefix), o.ErrorType)
			require.NotEmpty(t, o.Fingerprint)
		})
	}
}
