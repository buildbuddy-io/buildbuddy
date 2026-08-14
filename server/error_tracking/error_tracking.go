package error_tracking

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	etpb "github.com/buildbuddy-io/buildbuddy/proto/error_tracking"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/zeebo/blake3"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	MaxMessageBytes                  = 4096
	MaxOccurrencesPerInvocation      = 100
	MaxRawOccurrencesPerInvocation   = 1000
	MaxInvocationProvenanceBytes     = 1024
	defaultPageSize                  = 50
	maxPageSize                      = 100
	maxDetailPageSize                = 5
	frequencyBucketCount             = 7
	maxLookback                      = 30 * 24 * time.Hour
	maxFutureEventTimeSkew           = time.Hour
	relatedExecutionTimePadding      = 24 * time.Hour
	maxRelatedExecutions             = 5
	relatedExecutionCandidates       = 20
	relatedExecutionMatchersPerQuery = 25
	errorACLWriteTimeout             = 2 * time.Second
	TestFingerprintVersion           = "test:v2"
	TestFallbackFingerprintVersion   = "test_fallback:v2"
	ActionFingerprintVersion         = "compiler:v1"
	ActionFallbackFingerprintVersion = "action_fallback:v1"
	WorkflowFingerprintVersion       = "workflow:v1"
	ErrorOccurrencesUnknown          = int32(0)
	ErrorOccurrencesNone             = int32(1)
	ErrorOccurrencesPresent          = int32(2)
	userInterruptedAbortErrorType    = "aborted/USER_INTERRUPTED"
	noAnalyzeAbortErrorType          = "aborted/NO_ANALYZE"
	noBuildAbortErrorType            = "aborted/NO_BUILD"
	loadingFailureAbortErrorType     = "aborted/LOADING_FAILURE"
	analysisFailureAbortErrorType    = "aborted/ANALYSIS_FAILURE"
	skippedAbortErrorType            = "aborted/SKIPPED"
	incompleteAbortErrorType         = "aborted/INCOMPLETE"
	workflowInvocationRole           = "CI_RUNNER"
	workflowInvocationCommand        = "workflow run"
)

func classifyErrorOrigin(role, parentRunID string) etpb.ErrorOrigin {
	if role == workflowInvocationRole {
		return etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW
	}
	if role == "CI" && parentRunID != "" {
		return etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW_BAZEL_CHILD
	}
	return etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL
}

func applyWorkflowFingerprint(occurrence *schema.ErrorOccurrence, actionName string) {
	if occurrence == nil {
		return
	}
	occurrence.Fingerprint = hashFingerprintBasis(strings.Join([]string{
		WorkflowFingerprintVersion,
		normalizeTestIdentity(actionName),
		occurrence.Fingerprint,
	}, "\x00"))
	occurrence.FingerprintVersion = WorkflowFingerprintVersion
	occurrence.FingerprintSource = "workflow_bes"
	occurrence.FingerprintConfidence = "low"
}

func isNonActionableAbortReason(reason bepb.Aborted_AbortReason) bool {
	switch reason {
	case bepb.Aborted_USER_INTERRUPTED,
		bepb.Aborted_NO_ANALYZE,
		bepb.Aborted_NO_BUILD,
		bepb.Aborted_LOADING_FAILURE,
		bepb.Aborted_ANALYSIS_FAILURE,
		bepb.Aborted_SKIPPED,
		bepb.Aborted_INCOMPLETE:
		return true
	default:
		return false
	}
}

func PendingACLVersion(generation int64) int64   { return generation * 2 }
func CommittedACLVersion(generation int64) int64 { return generation*2 + 1 }

func FlushInvocationACLState(ctx context.Context, env environment.Env, in *tables.Invocation, p int32, version int64, deleted bool) error {
	return env.GetOLAPDBHandle().FlushErrorInvocationACL(ctx, &schema.ErrorInvocationACL{
		GroupID: in.GroupID, InvocationID: in.InvocationID, UserID: in.UserID,
		Perms: p, ACLVersion: version, Deleted: deleted, UpdatedAtUsec: time.Now().UnixMicro(),
	})
}

// FlushInvocationACLStateWithTimeout bounds the derived ClickHouse write so a
// primary-DB transaction cannot be pinned indefinitely by an OLAP outage.
func FlushInvocationACLStateWithTimeout(ctx context.Context, env environment.Env, in *tables.Invocation, p int32, version int64, deleted bool) error {
	writeCtx, cancel := context.WithTimeout(ctx, errorACLWriteTimeout)
	defer cancel()
	return FlushInvocationACLState(writeCtx, env, in, p, version, deleted)
}

// FlushInvocationACLStateForIncarnation publishes a delayed ACL state only if
// the primary invocation still has the incarnation that scheduled the write.
// The primary row remains locked through the ClickHouse write so deletion and
// same-ID reuse cannot interleave between the check and publication.
func FlushInvocationACLStateForIncarnation(ctx context.Context, env environment.Env, invocationID, expectedIncarnation string, p int32, version int64, deleted bool) (bool, error) {
	matched := false
	err := env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		in, ok, err := lockErrorTrackingInvocation(ctx, env, tx, invocationID, expectedIncarnation)
		if err != nil || !ok {
			return err
		}
		if err := FlushInvocationACLStateWithTimeout(ctx, env, &in, p, version, deleted); err != nil {
			return err
		}
		matched = true
		return nil
	})
	return matched, err
}

// FlushErrorOccurrencesWithPrimary reconciles derived ClickHouse rows with the
// canonical invocation ACL and inserts them while the primary invocation row
// remains locked. A durable random incarnation binds queued BES work to one
// primary row, so deleting and reusing an invocation ID cannot publish old
// diagnostics under the replacement row's identity or ACL.
func FlushErrorOccurrencesWithPrimary(ctx context.Context, env environment.Env, invocationID, expectedIncarnation string, occurrences []*schema.ErrorOccurrence) (bool, error) {
	if env.GetOLAPDBHandle() == nil {
		return false, nil
	}
	// Commit the attempted state before beginning any cross-database writes. A
	// ClickHouse timeout is ambiguous: the server may have accepted the rows even
	// though the client saw an error. Keeping this marker durable makes every
	// later ACL restriction or deletion publish a fail-closed ACL state.
	attemptMatched := false
	if err := env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		in, ok, err := lockErrorTrackingInvocation(ctx, env, tx, invocationID, expectedIncarnation)
		if err != nil || !ok {
			return err
		}
		if in.ErrorOccurrencesState != ErrorOccurrencesPresent {
			if err := tx.NewQuery(ctx, "error_tracking_mark_invocation_has_errors").Raw(
				`UPDATE "Invocations" SET error_occurrences_state = ? WHERE invocation_id = ? AND error_tracking_incarnation = ?`, ErrorOccurrencesPresent, invocationID, expectedIncarnation,
			).Exec().Error; err != nil {
				return err
			}
		}
		attemptMatched = true
		return nil
	}); err != nil || !attemptMatched {
		return false, err
	}

	matched := false
	err := env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		in, ok, err := lockErrorTrackingInvocation(ctx, env, tx, invocationID, expectedIncarnation)
		if err != nil || !ok {
			return err
		}
		writeCtx, cancel := context.WithTimeout(ctx, errorACLWriteTimeout)
		defer cancel()
		maxVersion, err := env.GetOLAPDBHandle().GetMaxErrorInvocationACLVersion(writeCtx, in.GroupID, invocationID)
		if err != nil {
			return err
		}
		if maxVersion > CommittedACLVersion(in.ErrorACLVersion) {
			// Invocation IDs are expected to be unique, but a client can reuse one
			// after deletion. Reset only this rare retained incarnation before
			// publishing the replacement; normal deletion remains mutation-free.
			if err := env.GetOLAPDBHandle().ResetErrorTrackingInvocation(writeCtx, in.GroupID, invocationID, in.ErrorTrackingIncarnation); err != nil {
				return err
			}
		}
		if err := FlushInvocationACLState(writeCtx, env, &in, in.Perms, CommittedACLVersion(in.ErrorACLVersion), false); err != nil {
			return err
		}
		runID := truncateUTF8(in.RunID, MaxInvocationProvenanceBytes)
		parentRunID := truncateUTF8(in.ParentRunID, MaxInvocationProvenanceBytes)
		invocationPattern := truncateUTF8(in.Pattern, MaxInvocationProvenanceBytes)
		origin := classifyErrorOrigin(in.Role, parentRunID)
		for _, occurrence := range occurrences {
			occurrence.GroupID = in.GroupID
			occurrence.UserID = in.UserID
			// Keep derived rows unreadable without the independently versioned ACL
			// state published above.
			occurrence.Perms = 0
			occurrence.InvocationUUID = hex.EncodeToString(in.InvocationUUID)
			occurrence.InvocationID = in.InvocationID
			occurrence.InvocationIncarnation = in.ErrorTrackingIncarnation
			occurrence.Origin = int32(origin)
			occurrence.RunID = runID
			occurrence.ParentRunID = parentRunID
			occurrence.InvocationPattern = invocationPattern
			if origin == etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW {
				applyWorkflowFingerprint(occurrence, invocationPattern)
			}
			occurrence.RepoURL = in.RepoURL
			occurrence.BranchName = in.BranchName
			occurrence.CommitSHA = in.CommitSHA
			occurrence.Command = in.Command
			occurrence.User = in.User
		}
		if err := env.GetOLAPDBHandle().FlushErrorOccurrences(writeCtx, occurrences); err != nil {
			return err
		}
		matched = true
		return nil
	})
	return matched, err
}

func lockErrorTrackingInvocation(ctx context.Context, env environment.Env, tx interfaces.DB, invocationID, expectedIncarnation string) (tables.Invocation, bool, error) {
	// Acquire SQLite's database write lock; row-locking databases additionally
	// use SELECT FOR UPDATE below.
	if err := tx.NewQuery(ctx, "error_tracking_lock_invocation_acl").Raw(
		`UPDATE "Invocations" SET perms = perms WHERE invocation_id = ?`, invocationID,
	).Exec().Error; err != nil {
		return tables.Invocation{}, false, err
	}
	var in tables.Invocation
	err := tx.NewQuery(ctx, "error_tracking_get_invocation_acl").Raw(
		`SELECT invocation_id, invocation_uuid, user_id, group_id, perms, error_acl_version, error_occurrences_state, error_tracking_incarnation, created_at_usec, repo_url, branch_name, commit_sha, command, user, role, run_id, parent_run_id, pattern FROM "Invocations" WHERE invocation_id = ? `+env.GetDBHandle().SelectForUpdateModifier(), invocationID,
	).Take(&in)
	if db.IsRecordNotFound(err) {
		return tables.Invocation{}, false, nil
	}
	if err != nil {
		return tables.Invocation{}, false, err
	}
	if in.ErrorTrackingIncarnation == "" || in.ErrorTrackingIncarnation != expectedIncarnation {
		return tables.Invocation{}, false, nil
	}
	return in, true, nil
}

var (
	uuidPattern                  = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	hexPattern                   = regexp.MustCompile(`(?i)\b[0-9a-f]{12,}\b`)
	volatileNumberPattern        = regexp.MustCompile(`(?i)\b(pid|process|attempt|run|shard|worker|port|line|column)\s*(?:#|=|:)?\s*[0-9]+\b`)
	spacePattern                 = regexp.MustCompile(`\s+`)
	locationPattern              = regexp.MustCompile(`(^|[\s(])(?:[a-zA-Z]:)?(?:[^:\s()]*[\\/])?(?:[^:\s()\\/]+\.[A-Za-z0-9_+-]+|BUILD(?:\.bazel)?|WORKSPACE(?:\.bazel)?|MODULE\.bazel):[0-9]+(?::[0-9]+)?`)
	distinctiveDiagnosticPattern = regexp.MustCompile(`(?i)\b(undefined|panic|exception|cannot|invalid|redeclared|expected|actual|assert(?:ion)?|not enough|too many|out of range)\b`)
	genericDiagnosticPattern     = regexp.MustCompile(`(?i)\b(error|fatal|failed|failure|timeout)\b`)
	pythonTestFramePattern       = regexp.MustCompile(`(?m)File "([^"]+)", line [0-9]+, in ([^\s]+)`)
	atTestFramePattern           = regexp.MustCompile(`(?m)^\s*at\s+([^\s(]+)(?:\s+\(([^():]+)(?::[0-9]+){1,2}\))?`)
	goTestFramePattern           = regexp.MustCompile(`(?m)^\s*([A-Za-z0-9_./-]+\.[A-Za-z0-9_<>*]+)\([^\n]*\)`)
	nativeTestFramePattern       = regexp.MustCompile(`(?m)([A-Za-z0-9_./\\-]+\.(?:c|cc|cpp|cxx|rs)):[0-9]+(?::[0-9]+)?`)
	testAttemptPattern           = regexp.MustCompile(`(?i)\b(run|shard|attempt)\s*(?:#|=|:)?\s*[0-9]+\b`)
	testAddressPattern           = regexp.MustCompile(`(?i)\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?::[0-9]+)?\b|\b0x[0-9a-f]{6,}\b`)
	testTimestampPattern         = regexp.MustCompile(`(?i)\b[0-9]{4}-[0-9]{2}-[0-9]{2}[t ][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?z?\b`)
)

// ExtractOccurrence converts typed BES failures into bounded OLAP rows. It
// intentionally does not inspect Progress stdout/stderr.
func ExtractOccurrence(event *bepb.BuildEvent, invocationID string, attempt uint64, sequenceNumber, eventTimeUsec int64) *schema.ErrorOccurrence {
	if event == nil {
		return nil
	}
	var errorType, message, target, mnemonic string
	var genericActionFallback bool
	var exitCode int32
	switch p := event.GetPayload().(type) {
	case *bepb.BuildEvent_Aborted:
		if isNonActionableAbortReason(p.Aborted.GetReason()) {
			return nil
		}
		errorType = "aborted/" + p.Aborted.GetReason().String()
		message = p.Aborted.GetDescription()
	case *bepb.BuildEvent_Action:
		if p.Action.GetSuccess() {
			return nil
		}
		target = event.GetId().GetActionCompleted().GetLabel()
		mnemonic = p.Action.GetType()
		exitCode = p.Action.GetExitCode()
		errorType, message = failureDetail("action", p.Action.GetFailureDetail())
		if message == "" {
			message = fmt.Sprintf("%s action failed with exit code %d", mnemonic, exitCode)
			genericActionFallback = true
		}
	case *bepb.BuildEvent_Completed:
		if p.Completed.GetSuccess() {
			return nil
		}
		target = event.GetId().GetTargetCompleted().GetLabel()
		errorType, message = failureDetail("target", p.Completed.GetFailureDetail())
		if message == "" {
			message = fmt.Sprintf("target %s failed to build", target)
		}
	case *bepb.BuildEvent_TestResult:
		status := p.TestResult.GetStatus()
		if status == bepb.TestStatus_PASSED || status == bepb.TestStatus_FLAKY || status == bepb.TestStatus_NO_STATUS {
			return nil
		}
		target = event.GetId().GetTestResult().GetLabel()
		exitCode = p.TestResult.GetExecutionInfo().GetExitCode()
		errorType = "test/" + status.String()
		message = p.TestResult.GetStatusDetails()
		if message == "" {
			message = fmt.Sprintf("test %s finished with status %s", target, status)
		}
	case *bepb.BuildEvent_TestSummary:
		status := p.TestSummary.GetOverallStatus()
		if status == bepb.TestStatus_PASSED || status == bepb.TestStatus_FLAKY || status == bepb.TestStatus_NO_STATUS {
			return nil
		}
		target = event.GetId().GetTestSummary().GetLabel()
		errorType = "test_summary/" + status.String()
		message = fmt.Sprintf("test %s finished with status %s", target, status)
	case *bepb.BuildEvent_Finished:
		exitCode = p.Finished.GetExitCode().GetCode()
		if exitCode == 0 {
			return nil
		}
		errorType, message = failureDetail("build", p.Finished.GetFailureDetail())
		if message == "" {
			message = p.Finished.GetExitCode().GetName()
		}
	default:
		return nil
	}
	if errorType == "" {
		errorType = "unknown"
	}
	eventTimeUsec = ClampEventTimeUsec(eventTimeUsec)
	message = truncateUTF8(strings.TrimSpace(message), MaxMessageBytes)
	target = truncateUTF8(target, 1024)
	mnemonic = truncateUTF8(mnemonic, 128)
	fingerprint := occurrenceFingerprint(errorType, mnemonic, message)
	fingerprintVersion, fingerprintSource, fingerprintConfidence := "", "", ""
	if strings.HasPrefix(errorType, "action/") {
		_, specific := diagnosticSignatureDetails(message)
		if genericActionFallback || !specific {
			fingerprint = hashFingerprintBasis(strings.Join([]string{ActionFallbackFingerprintVersion, errorType, mnemonic, target, diagnosticSignature(message)}, "\x00"))
			fingerprintVersion, fingerprintSource, fingerprintConfidence = ActionFallbackFingerprintVersion, "action_event_fallback", "low"
		} else {
			fingerprintVersion, fingerprintSource, fingerprintConfidence = ActionFingerprintVersion, "action_failure_detail", "medium"
		}
	}
	return &schema.ErrorOccurrence{
		Fingerprint: fingerprint, FingerprintVersion: fingerprintVersion,
		FingerprintSource: fingerprintSource, FingerprintConfidence: fingerprintConfidence,
		EventTimeUsec: eventTimeUsec,
		InvocationID:  invocationID, Attempt: attempt, SequenceNumber: sequenceNumber,
		ErrorType: truncateUTF8(errorType, 256), Message: message, TargetLabel: target,
		ActionMnemonic: mnemonic, ExitCode: exitCode,
	}
}

// EnrichOccurrence replaces a generic failure detail with bounded output
// attached to the BES event, then recomputes the fingerprint from the more
// useful diagnostic.
func EnrichOccurrence(occurrence *schema.ErrorOccurrence, output string) {
	message := truncateUTF8(strings.TrimSpace(output), MaxMessageBytes)
	if occurrence == nil || message == "" {
		return
	}
	occurrence.Message = message
	signature, specific := diagnosticSignatureDetails(message)
	if specific {
		occurrence.Fingerprint = hashFingerprintBasis(occurrence.ErrorType + "\x00" + occurrence.ActionMnemonic + "\x00" + signature)
		occurrence.FingerprintVersion = ActionFingerprintVersion
		occurrence.FingerprintSource = "action_output"
		occurrence.FingerprintConfidence = "high"
		return
	}
	occurrence.Fingerprint = hashFingerprintBasis(strings.Join([]string{ActionFallbackFingerprintVersion, occurrence.ErrorType, occurrence.ActionMnemonic, occurrence.TargetLabel, signature}, "\x00"))
	occurrence.FingerprintVersion = ActionFallbackFingerprintVersion
	occurrence.FingerprintSource = "action_output_fallback"
	occurrence.FingerprintConfidence = "low"
}

// RootOccurrences removes only secondary failures that can be matched to a
// more specific root event. Independent action, test, loading, and terminal
// failures remain visible.
func RootOccurrences(occurrences []*schema.ErrorOccurrence) []*schema.ErrorOccurrence {
	actionTargets := make(map[string]struct{})
	testTargets := make(map[string]struct{})
	hasSpecificFailure := false
	hasPackageLoadingFailure := false
	for _, occurrence := range occurrences {
		switch {
		case strings.HasPrefix(occurrence.ErrorType, "action/"):
			actionTargets[occurrence.TargetLabel] = struct{}{}
			hasSpecificFailure = true
		case strings.HasPrefix(occurrence.ErrorType, "test/"):
			testTargets[occurrence.TargetLabel] = struct{}{}
			hasSpecificFailure = true
		case strings.HasPrefix(occurrence.ErrorType, "build/package_loading/"):
			hasPackageLoadingFailure = true
			hasSpecificFailure = true
		case occurrence.ErrorType != "build/unknown" && occurrence.ErrorType != "aborted/UNKNOWN":
			hasSpecificFailure = true
		}
	}
	result := make([]*schema.ErrorOccurrence, 0, len(occurrences))
	for _, occurrence := range occurrences {
		switch {
		case strings.HasPrefix(occurrence.ErrorType, "action/"):
			// A failed test action can produce both an ActionExecuted failure and a
			// structured TestResult for the same target. The testcase is the root
			// issue; the action remains available through related executions.
			if _, ok := testTargets[occurrence.TargetLabel]; ok && occurrence.TargetLabel != "" && strings.HasSuffix(occurrence.ErrorType, "/NON_ZERO_EXIT") {
				continue
			}
		case occurrence.ErrorType == "aborted/UNKNOWN" && hasSpecificFailure:
			continue
		case occurrence.ErrorType == "build/unknown" && hasSpecificFailure:
			continue
		case occurrence.ErrorType == "aborted/LOADING_FAILURE" && hasPackageLoadingFailure:
			continue
		case strings.HasPrefix(occurrence.ErrorType, "target/"):
			if _, ok := actionTargets[occurrence.TargetLabel]; ok {
				continue
			}
		case strings.HasPrefix(occurrence.ErrorType, "test_summary/"):
			if _, ok := testTargets[occurrence.TargetLabel]; ok {
				continue
			}
		}
		result = append(result, occurrence)
	}
	return result
}

// DeduplicateOccurrences keeps one representative compiler/build occurrence
// per fingerprint, while retaining distinct test attempt contexts. Query-time
// impact counts use unique invocation IDs; dropping retry/shard evidence here
// would make the detail view unable to explain how a terminal issue occurred.
func DeduplicateOccurrences(occurrences []*schema.ErrorOccurrence) []*schema.ErrorOccurrence {
	seen := make(map[string]struct{}, len(occurrences))
	result := make([]*schema.ErrorOccurrence, 0, len(occurrences))
	for _, occurrence := range occurrences {
		key := occurrence.Fingerprint
		if occurrence.FingerprintVersion == TestFingerprintVersion || occurrence.FingerprintVersion == TestFallbackFingerprintVersion {
			key = strings.Join([]string{
				occurrence.Fingerprint,
				occurrence.TargetLabel,
				occurrence.TestSuite,
				occurrence.TestClass,
				occurrence.TestName,
				occurrence.TestFailureKind,
				occurrence.TestFailureType,
				fmt.Sprint(occurrence.TestRun),
				fmt.Sprint(occurrence.TestShard),
				fmt.Sprint(occurrence.TestAttempt),
			}, "\x00")
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, occurrence)
	}
	return result
}

func occurrenceFingerprint(errorType, mnemonic, message string) string {
	signature := diagnosticSignature(message)
	return hashFingerprintBasis(errorType + "\x00" + mnemonic + "\x00" + signature)

}

// TestFailure identifies one structured failure or error node from a Bazel
// test.xml artifact. Test identity is intentionally part of the canonical
// fingerprint: unlike compiler diagnostics, generic assertion text from two
// different tests is not strong enough evidence to merge them automatically.
type TestFailure struct {
	TargetLabel string
	SuiteName   string
	ClassName   string
	TestName    string
	Kind        string
	Type        string
	Message     string
	Body        string
}

// TestFailureFingerprint returns the stable fingerprint and its explainable
// canonical basis. Run, shard, attempt, source paths, and line/column numbers
// belong to occurrence context and are deliberately excluded.
func TestFailureFingerprint(failure TestFailure) (string, string) {
	message := failure.Message
	bodyDerived := false
	if strings.TrimSpace(message) == "" {
		message = testBodyDiagnostic(failure.Body)
		bodyDerived = true
	}
	normalizedMessage := normalizeTestMessage(message)
	stableFrame := ""
	if bodyDerived || isGenericTestMessage(normalizedMessage, failure.Type) {
		stableFrame = stableTestFrame(failure.Body)
	}
	basis := strings.Join([]string{
		TestFingerprintVersion,
		normalizeTestIdentity(failure.TargetLabel),
		normalizeTestIdentity(failure.SuiteName),
		normalizeTestIdentity(failure.ClassName),
		normalizeTestIdentity(failure.TestName),
		normalizeTestIdentity(failure.Kind),
		normalizeTestIdentity(failure.Type),
		normalizedMessage,
		stableFrame,
	}, "\x00")
	return hashFingerprintBasis(basis), basis
}

// TestFallbackFingerprint is intentionally target-scoped. When no structured
// testcase identity is available, the same generic runner text from two test
// targets must not cause an automatic cross-target merge.
func TestFallbackFingerprint(targetLabel, finalStatus string, diagnostic ...string) (string, string) {
	normalizedDiagnostic := ""
	if len(diagnostic) > 0 {
		normalizedDiagnostic = normalizeTestMessage(diagnostic[0])
	}
	basis := strings.Join([]string{
		TestFallbackFingerprintVersion,
		normalizeTestIdentity(targetLabel),
		normalizeTestIdentity(finalStatus),
		normalizedDiagnostic,
	}, "\x00")
	return hashFingerprintBasis(basis), basis
}

// ClampEventTimeUsec bounds client-controlled BES timestamps to the supported
// retention/query window and prevents future partitions.
func ClampEventTimeUsec(eventTimeUsec int64) int64 {
	nowUsec := time.Now().UnixMicro()
	if eventTimeUsec < nowUsec-maxLookback.Microseconds() || eventTimeUsec > nowUsec+maxFutureEventTimeSkew.Microseconds() {
		return nowUsec
	}
	return eventTimeUsec
}

func hashFingerprintBasis(basis string) string {
	sum := blake3.Sum256([]byte(basis))
	return hex.EncodeToString(sum[:16])
}

func normalizeTestIdentity(value string) string {
	return strings.TrimSpace(spacePattern.ReplaceAllString(value, " "))
}

func normalizeTestMessage(message string) string {
	message = strings.ToLower(message)
	message = uuidPattern.ReplaceAllString(message, "<uuid>")
	message = testTimestampPattern.ReplaceAllString(message, "<timestamp>")
	message = testAddressPattern.ReplaceAllString(message, "<address>")
	message = testAttemptPattern.ReplaceAllString(message, "${1} <n>")
	message = locationPattern.ReplaceAllString(message, "${1}<location>")
	message = hexPattern.ReplaceAllString(message, "<hex>")
	return strings.TrimSpace(spacePattern.ReplaceAllString(message, " "))
}

func isGenericTestMessage(message, failureType string) bool {
	message = strings.Trim(strings.ToLower(strings.TrimSpace(message)), ".:")
	failureType = strings.Trim(strings.ToLower(strings.TrimSpace(failureType)), ".:")
	if message == "" || message == failureType {
		return true
	}
	switch message {
	case "assertion failed", "assertion failure", "error", "failed", "failure", "test failed", "test failure":
		return true
	default:
		return false
	}
}

// stableTestFrame extracts a conservative application-frame tie-breaker for
// otherwise generic failures. It recognizes common Python, JS/Java, Go, and
// native formats while removing volatile source locations and sandbox roots.
func stableTestFrame(body string) string {
	if matches := pythonTestFramePattern.FindAllStringSubmatch(body, -1); len(matches) > 0 {
		for i := len(matches) - 1; i >= 0; i-- {
			frame := canonicalFramePath(matches[i][1]) + ":" + matches[i][2]
			if !isFrameworkTestFrame(frame) {
				return strings.ToLower(frame)
			}
		}
	}
	if matches := atTestFramePattern.FindAllStringSubmatch(body, -1); len(matches) > 0 {
		for _, match := range matches {
			frame := match[1]
			if len(match) > 2 && match[2] != "" {
				frame += ":" + canonicalFramePath(match[2])
			}
			if !isFrameworkTestFrame(frame) {
				return strings.ToLower(frame)
			}
		}
	}
	if matches := goTestFramePattern.FindAllStringSubmatch(body, -1); len(matches) > 0 {
		for _, match := range matches {
			if !isFrameworkTestFrame(match[1]) {
				return strings.ToLower(match[1])
			}
		}
	}
	if matches := nativeTestFramePattern.FindAllStringSubmatch(body, -1); len(matches) > 0 {
		for _, match := range matches {
			frame := canonicalFramePath(match[1])
			if !isFrameworkTestFrame(frame) {
				return strings.ToLower(frame)
			}
		}
	}
	return ""
}

func canonicalFramePath(value string) string {
	parts := strings.Split(strings.ReplaceAll(value, "\\", "/"), "/")
	if len(parts) > 2 {
		parts = parts[len(parts)-2:]
	}
	return strings.Join(parts, "/")
}

func isFrameworkTestFrame(frame string) bool {
	frame = strings.ToLower(frame)
	for _, marker := range []string{"node_modules/", "site-packages/", "/unittest/", "jasmine", "jest", "runtime.", "testing.trunner"} {
		if strings.Contains(frame, marker) {
			return true
		}
	}
	return false
}

func firstNonBlankLine(message string) string {
	for line := range strings.SplitSeq(message, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
	}
	return ""
}

func testBodyDiagnostic(body string) string {
	fallback := firstNonBlankLine(body)
	lines := strings.Split(body, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "traceback (") || strings.HasPrefix(lower, "at ") || strings.HasPrefix(lower, "file \"") || strings.HasPrefix(lower, "goroutine ") {
			continue
		}
		if distinctiveDiagnosticPattern.MatchString(line) || strings.Contains(lower, "panic") || strings.Contains(lower, "assert") {
			return line
		}
		if fallback == "" {
			fallback = line
		}
	}
	return fallback
}

func diagnosticSignature(message string) string {
	signature, _ := diagnosticSignatureDetails(message)
	return signature
}

func diagnosticSignatureDetails(message string) (string, bool) {
	type diagnosticLine struct {
		normalized  string
		located     bool
		distinctive bool
		generic     bool
	}
	lines := make([]diagnosticLine, 0, 8)
	for line := range strings.SplitSeq(message, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if len(lines) < 32 {
			lines = append(lines, diagnosticLine{
				normalized:  normalizeMessage(locationPattern.ReplaceAllString(line, "${1}<location>")),
				located:     locationPattern.MatchString(line),
				distinctive: distinctiveDiagnosticPattern.MatchString(line),
				generic:     genericDiagnosticPattern.MatchString(line),
			})
		}
	}
	if len(lines) == 0 {
		return "", false
	}
	start := -1
	for i, line := range lines {
		if line.located {
			start = i
			break
		}
	}
	if start == -1 {
		for i, line := range lines {
			if line.distinctive {
				start = i
				break
			}
		}
	}
	if start == -1 {
		for i, line := range lines {
			if line.generic {
				start = i
				break
			}
		}
	}
	if start == -1 {
		start = 0
	}
	end := min(start+3, len(lines))
	parts := make([]string, 0, end-start)
	specific := false
	for _, line := range lines[start:end] {
		parts = append(parts, line.normalized)
		specific = specific || line.distinctive
	}
	return strings.Join(parts, "\n"), specific
}

func failureDetail(prefix string, detail interface {
	GetMessage() string
	ProtoReflect() protoreflect.Message
}) (string, string) {
	if detail == nil || !detail.ProtoReflect().IsValid() {
		return prefix + "/unknown", ""
	}
	m := detail.ProtoReflect()
	oneof := m.Descriptor().Oneofs().ByName("category")
	field := m.WhichOneof(oneof)
	if field == nil {
		return prefix + "/unknown", detail.GetMessage()
	}
	category := string(field.Name())
	value := m.Get(field).Message()
	codeField := value.Descriptor().Fields().ByNumber(1)
	if codeField != nil && codeField.Kind() == protoreflect.EnumKind {
		code := value.Get(codeField).Enum()
		if enumValue := codeField.Enum().Values().ByNumber(code); enumValue != nil {
			category += "/" + string(enumValue.Name())
		} else {
			category += fmt.Sprintf("/UNKNOWN_%d", code)
		}
	}
	return prefix + "/" + category, detail.GetMessage()
}

func normalizeMessage(message string) string {
	message = strings.ToLower(message)
	message = uuidPattern.ReplaceAllString(message, "<uuid>")
	message = hexPattern.ReplaceAllString(message, "<hex>")
	message = volatileNumberPattern.ReplaceAllString(message, "${1} #")
	return strings.TrimSpace(spacePattern.ReplaceAllString(message, " "))
}

func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	s = s[:maxBytes]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

type groupRow struct {
	Fingerprint                 string
	LatestErrorType             string
	LatestMessage               string
	LatestTarget                string
	LatestMnemonic              string
	LatestFingerprintVersion    string
	LatestFingerprintSource     string
	LatestFingerprintConfidence string
	LatestTestSuite             string
	LatestTestClass             string
	LatestTestName              string
	LatestTestFailureKind       string
	LatestTestFailureType       string
	LatestOrigin                int32
	LatestInvocationPattern     string
	OccurrenceCount             int64
	FirstSeenUsec               int64
	LastSeenUsec                int64
	FrequencyBucket0            int64 `gorm:"column:frequency_bucket_0"`
	FrequencyBucket1            int64 `gorm:"column:frequency_bucket_1"`
	FrequencyBucket2            int64 `gorm:"column:frequency_bucket_2"`
	FrequencyBucket3            int64 `gorm:"column:frequency_bucket_3"`
	FrequencyBucket4            int64 `gorm:"column:frequency_bucket_4"`
	FrequencyBucket5            int64 `gorm:"column:frequency_bucket_5"`
	FrequencyBucket6            int64 `gorm:"column:frequency_bucket_6"`
}

func errorOriginPlane(origin etpb.ErrorOrigin) etpb.ErrorOrigin {
	if origin == etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW {
		return origin
	}
	return etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL
}

func addErrorOriginFilter(q *query_builder.Query, origin etpb.ErrorOrigin) error {
	switch origin {
	case etpb.ErrorOrigin_ERROR_ORIGIN_UNKNOWN:
		return nil
	case etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL:
		// Rows written before provenance was introduced have origin UNKNOWN.
		// The CI runner's canonical synthetic command preserves the historical
		// Workflow/Bazel split while new rows use the typed origin exclusively.
		q.AddWhereClause(`(eo.origin IN (?, ?) OR (eo.origin = ? AND eo.command != ?))`,
			int32(etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL),
			int32(etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW_BAZEL_CHILD),
			int32(etpb.ErrorOrigin_ERROR_ORIGIN_UNKNOWN),
			workflowInvocationCommand,
		)
		return nil
	case etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW:
		q.AddWhereClause(`(eo.origin = ? OR (eo.origin = ? AND eo.command = ?))`,
			int32(etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW),
			int32(etpb.ErrorOrigin_ERROR_ORIGIN_UNKNOWN),
			workflowInvocationCommand,
		)
		return nil
	case etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW_BAZEL_CHILD:
		q.AddWhereClause("eo.origin = ?", int32(etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW_BAZEL_CHILD))
		return nil
	default:
		return status.InvalidArgumentError("invalid error origin")
	}
}

type frequencyBucketBounds struct {
	startUsec int64
	endUsec   int64
}

func makeFrequencyBucketBounds(startUsec, endUsec int64) []frequencyBucketBounds {
	rangeUsec := endUsec - startUsec + 1
	bucketWidthUsec := max(int64(1), (rangeUsec+frequencyBucketCount-1)/frequencyBucketCount)
	bounds := make([]frequencyBucketBounds, 0, frequencyBucketCount)
	for i := range frequencyBucketCount {
		bucketStartUsec := min(endUsec, startUsec+int64(i)*bucketWidthUsec)
		bucketEndUsec := min(endUsec, bucketStartUsec+bucketWidthUsec-1)
		bounds = append(bounds, frequencyBucketBounds{startUsec: bucketStartUsec, endUsec: bucketEndUsec})
	}
	return bounds
}

func frequencyBucketSelects(bounds []frequencyBucketBounds) string {
	selects := make([]string, 0, len(bounds))
	for i, bound := range bounds {
		selects = append(selects, fmt.Sprintf(
			"uniqExactIf(eo.invocation_id, eo.event_time_usec >= %d AND eo.event_time_usec <= %d) AS frequency_bucket_%d",
			bound.startUsec, bound.endUsec, i))
	}
	return strings.Join(selects, ",\n\t\t")
}

func frequencyBuckets(row *groupRow, bounds []frequencyBucketBounds) []*etpb.ErrorFrequencyBucket {
	counts := []int64{
		row.FrequencyBucket0,
		row.FrequencyBucket1,
		row.FrequencyBucket2,
		row.FrequencyBucket3,
		row.FrequencyBucket4,
		row.FrequencyBucket5,
		row.FrequencyBucket6,
	}
	buckets := make([]*etpb.ErrorFrequencyBucket, 0, len(bounds))
	for i, bound := range bounds {
		buckets = append(buckets, &etpb.ErrorFrequencyBucket{
			StartTimeUsec: bound.startUsec, EndTimeUsec: bound.endUsec,
			AffectedInvocationCount: counts[i],
		})
	}
	return buckets
}

type pageCursor struct {
	Kind   string `json:"k"`
	Time   int64  `json:"t"`
	Score  int64  `json:"v,omitempty"`
	Sort   int32  `json:"s,omitempty"`
	Origin int32  `json:"o,omitempty"`
	ID     string `json:"i"`
}

func encodePageCursor(cursor pageCursor) string {
	b, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodePageCursor(token string) (pageCursor, error) {
	if token == "" {
		return pageCursor{}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(b) > 1024 {
		return pageCursor{}, status.InvalidArgumentError("invalid error tracking page token")
	}
	var cursor pageCursor
	if err := json.Unmarshal(b, &cursor); err != nil || cursor.Time <= 0 || cursor.Score < 0 || cursor.ID == "" {
		return pageCursor{}, status.InvalidArgumentError("invalid error tracking page token")
	}
	return cursor, nil
}

func normalizeErrorGroupSort(sort etpb.ErrorGroupSort) (etpb.ErrorGroupSort, error) {
	switch sort {
	case etpb.ErrorGroupSort_ERROR_GROUP_SORT_UNKNOWN, etpb.ErrorGroupSort_ERROR_GROUP_SORT_AFFECTED_BUILDS:
		return etpb.ErrorGroupSort_ERROR_GROUP_SORT_AFFECTED_BUILDS, nil
	case etpb.ErrorGroupSort_ERROR_GROUP_SORT_LAST_SEEN, etpb.ErrorGroupSort_ERROR_GROUP_SORT_RECENT_FREQUENCY:
		return sort, nil
	default:
		return 0, status.InvalidArgumentError("invalid error group sort")
	}
}

func errorGroupSortScore(row *groupRow, sort etpb.ErrorGroupSort) int64 {
	switch sort {
	case etpb.ErrorGroupSort_ERROR_GROUP_SORT_AFFECTED_BUILDS:
		return row.OccurrenceCount
	case etpb.ErrorGroupSort_ERROR_GROUP_SORT_RECENT_FREQUENCY:
		return row.FrequencyBucket6
	default:
		return row.LastSeenUsec
	}
}

func GetErrorGroups(ctx context.Context, env environment.Env, req *etpb.GetErrorGroupsRequest) (*etpb.GetErrorGroupsResponse, error) {
	if env.GetOLAPDBHandle() == nil {
		return nil, status.FailedPreconditionError("OLAP database is not configured")
	}
	if schema.DataReplicationEnabled() {
		// ACL state writes use insert quorum. Sequentially consistent reads keep a
		// lagging replica from serving an older permissive ACL after a restriction
		// or deletion has committed in the primary DB.
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(map[string]any{
			"select_sequential_consistency": 1,
		}))
	}
	u, err := env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := u.GetGroupID()
	if groupID == "" {
		return nil, status.PermissionDeniedError("organization membership is required")
	}
	nowUsec := time.Now().UnixMicro()
	startUsec, endUsec := req.GetStartTimeUsec(), req.GetEndTimeUsec()
	if endUsec == 0 || endUsec > nowUsec+time.Hour.Microseconds() {
		endUsec = nowUsec
	}
	if startUsec == 0 {
		startUsec = endUsec - 7*24*time.Hour.Microseconds()
	}
	if startUsec > endUsec {
		return nil, status.InvalidArgumentError("start time must not be after end time")
	}
	if startUsec < endUsec-maxLookback.Microseconds() {
		startUsec = endUsec - maxLookback.Microseconds()
	}
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	if req.GetFingerprint() != "" && pageSize > maxDetailPageSize {
		pageSize = maxDetailPageSize
	}
	cursor, err := decodePageCursor(req.GetPageToken())
	if err != nil {
		return nil, err
	}
	expectedCursorKind := "groups"
	if req.GetFingerprint() != "" {
		expectedCursorKind = "detail"
	}
	if cursor.Kind != "" && cursor.Kind != expectedCursorKind {
		return nil, status.InvalidArgumentError("page token does not match error tracking view")
	}
	if cursor.Kind != "" && cursor.Origin != int32(req.GetOrigin()) {
		return nil, status.InvalidArgumentError("page token does not match error origin")
	}
	sort, err := normalizeErrorGroupSort(req.GetSort())
	if err != nil {
		return nil, err
	}
	if cursor.Kind == "groups" {
		cursorSort := etpb.ErrorGroupSort(cursor.Sort)
		if cursorSort == etpb.ErrorGroupSort_ERROR_GROUP_SORT_UNKNOWN {
			// Tokens emitted before sort-aware pagination always used recency.
			cursorSort = etpb.ErrorGroupSort_ERROR_GROUP_SORT_LAST_SEEN
			if cursor.Score == 0 {
				cursor.Score = cursor.Time
			}
		}
		if cursorSort != sort {
			return nil, status.InvalidArgumentError("page token does not match error group sort")
		}
	}

	frequencyBounds := makeFrequencyBucketBounds(startUsec, endUsec)
	aclQuery, aclArgs := currentACLSubquery(groupID, startUsec, endUsec)
	q := query_builder.NewQuery(fmt.Sprintf(`SELECT
		eo.fingerprint,
		argMax(eo.error_type, eo.event_time_usec) AS latest_error_type,
		argMax(eo.message, eo.event_time_usec) AS latest_message,
		argMax(eo.target_label, eo.event_time_usec) AS latest_target,
		argMax(eo.action_mnemonic, eo.event_time_usec) AS latest_mnemonic,
		argMax(eo.fingerprint_version, eo.event_time_usec) AS latest_fingerprint_version,
		argMax(eo.fingerprint_source, eo.event_time_usec) AS latest_fingerprint_source,
		argMax(eo.fingerprint_confidence, eo.event_time_usec) AS latest_fingerprint_confidence,
		argMax(eo.test_suite, eo.event_time_usec) AS latest_test_suite,
		argMax(eo.test_class, eo.event_time_usec) AS latest_test_class,
		argMax(eo.test_name, eo.event_time_usec) AS latest_test_name,
		argMax(eo.test_failure_kind, eo.event_time_usec) AS latest_test_failure_kind,
		argMax(eo.test_failure_type, eo.event_time_usec) AS latest_test_failure_type,
		argMax(eo.origin, eo.event_time_usec) AS latest_origin,
		argMax(eo.invocation_pattern, eo.event_time_usec) AS latest_invocation_pattern,
		uniqExact(eo.invocation_id) AS occurrence_count,
		min(eo.event_time_usec) AS first_seen_usec,
		max(eo.event_time_usec) AS last_seen_usec,
		%s
		FROM ErrorOccurrences AS eo FINAL
		INNER JOIN (%s) AS acl ON eo.invocation_id = acl.invocation_id`, frequencyBucketSelects(frequencyBounds), aclQuery))
	q.AddWhereClause("eo.group_id = ?", groupID)
	if err := addErrorOriginFilter(q, req.GetOrigin()); err != nil {
		return nil, err
	}
	// User-requested stops, analysis/loading wrappers, intentionally skipped
	// targets, and incomplete-build fallout are not actionable root issues.
	// Keep this read-time guard so rows ingested by older servers disappear
	// immediately without a destructive data migration. TIME_OUT,
	// REMOTE_ENVIRONMENT_FAILURE, INTERNAL, OUT_OF_MEMORY, and unknown aborts
	// remain visible because the abort itself carries actionable information.
	q.AddWhereClause("eo.error_type NOT IN (?, ?, ?, ?, ?, ?, ?)",
		userInterruptedAbortErrorType,
		noAnalyzeAbortErrorType,
		noBuildAbortErrorType,
		loadingFailureAbortErrorType,
		analysisFailureAbortErrorType,
		skippedAbortErrorType,
		incompleteAbortErrorType,
	)
	q.AddWhereClause("acl.acl_deleted = 0")
	addPermissionsCheckToQuery(q, groupID, u.GetUserID())
	q.AddWhereClause("eo.event_time_usec >= ?", startUsec)
	q.AddWhereClause("eo.event_time_usec <= ?", endUsec)
	addErrorQueryFilter(q, req.GetQuery())
	if req.GetFingerprint() != "" {
		q.AddWhereClause("eo.fingerprint = ?", truncateUTF8(req.GetFingerprint(), 128))
	}
	q.SetGroupBy("eo.fingerprint")
	sortColumn := "last_seen_usec"
	switch sort {
	case etpb.ErrorGroupSort_ERROR_GROUP_SORT_UNKNOWN, etpb.ErrorGroupSort_ERROR_GROUP_SORT_LAST_SEEN:
		// UNKNOWN is normalized before this point; keep the explicit arm so enum
		// additions cannot silently inherit recency ordering.
	case etpb.ErrorGroupSort_ERROR_GROUP_SORT_AFFECTED_BUILDS:
		sortColumn = "occurrence_count"
	case etpb.ErrorGroupSort_ERROR_GROUP_SORT_RECENT_FREQUENCY:
		sortColumn = fmt.Sprintf("frequency_bucket_%d", frequencyBucketCount-1)
	}
	if cursor.Kind == "groups" {
		q.AddHavingClause(fmt.Sprintf("(%s, last_seen_usec, eo.fingerprint) < (?, ?, ?)", sortColumn), cursor.Score, cursor.Time, cursor.ID)
	}
	q.SetOrderBy(fmt.Sprintf("(%s, last_seen_usec, eo.fingerprint)", sortColumn), false)
	q.SetLimit(int64(pageSize + 1))
	query, args := q.Build()
	args = append(aclArgs, args...)
	rows, err := db.ScanAll(env.GetOLAPDBHandle().NewQuery(ctx, "error_tracking_groups").Raw(query, args...), &groupRow{})
	if err != nil {
		return nil, err
	}
	rsp := &etpb.GetErrorGroupsResponse{}
	if len(rows) > pageSize {
		rows = rows[:pageSize]
		last := rows[len(rows)-1]
		rsp.NextPageToken = encodePageCursor(pageCursor{
			Kind: "groups", Time: last.LastSeenUsec, Score: errorGroupSortScore(last, sort), Sort: int32(sort), Origin: int32(req.GetOrigin()), ID: last.Fingerprint,
		})
	}
	for _, row := range rows {
		groupOrigin := errorOriginPlane(etpb.ErrorOrigin(row.LatestOrigin))
		if req.GetOrigin() == etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW {
			groupOrigin = req.GetOrigin()
		}
		g := &etpb.ErrorGroup{
			Fingerprint: row.Fingerprint, ErrorType: row.LatestErrorType, SampleMessage: row.LatestMessage,
			OccurrenceCount: row.OccurrenceCount, FirstSeenUsec: row.FirstSeenUsec, LastSeenUsec: row.LastSeenUsec,
			SampleTargetLabel: row.LatestTarget, SampleActionMnemonic: row.LatestMnemonic,
			FingerprintVersion: row.LatestFingerprintVersion, FingerprintSource: row.LatestFingerprintSource,
			FingerprintConfidence: row.LatestFingerprintConfidence, SampleTestSuite: row.LatestTestSuite,
			SampleTestClass: row.LatestTestClass, SampleTestName: row.LatestTestName,
			SampleTestFailureKind: row.LatestTestFailureKind, SampleTestFailureType: row.LatestTestFailureType,
			FrequencyBuckets: frequencyBuckets(row, frequencyBounds), Origin: groupOrigin,
			SampleInvocationPattern: row.LatestInvocationPattern,
		}
		if req.GetFingerprint() != "" {
			g.Occurrences, rsp.NextPageToken, err = getOccurrences(ctx, env, groupID, u.GetUserID(), row.Fingerprint, req.GetQuery(), req.GetOrigin(), startUsec, endUsec, pageSize, cursor)
			if err != nil {
				return nil, err
			}
		}
		rsp.Groups = append(rsp.Groups, g)
	}
	return rsp, nil
}

func currentACLSubquery(groupID string, startUsec, endUsec int64) (string, []interface{}) {
	q := query_builder.NewQuery(`SELECT invocation_id, argMax(group_id, acl_version) AS acl_group_id, argMax(user_id, acl_version) AS acl_user_id, argMax(perms, acl_version) AS acl_perms, argMax(deleted, acl_version) AS acl_deleted FROM ErrorInvocationACLs`)
	q.AddWhereClause("group_id = ?", groupID)
	// ACL history is append-only, but only invocations with occurrences in the
	// requested time window can contribute to this query. Narrowing before
	// argMax keeps cost proportional to the bounded occurrence window rather
	// than the organization's all-time invocation count.
	q.AddWhereClause(`invocation_id IN (
		SELECT invocation_id FROM ErrorOccurrences
		WHERE group_id = ? AND event_time_usec >= ? AND event_time_usec <= ?
		GROUP BY invocation_id
	)`, groupID, startUsec, endUsec)
	q.SetGroupBy("invocation_id")
	return q.Build()
}

func addPermissionsCheckToQuery(q *query_builder.Query, groupID, userID string) {
	o := query_builder.OrClauses{}
	o.AddOr("bitAnd(acl.acl_perms, ?) != 0", perms.OTHERS_READ)
	o.AddOr("(bitAnd(acl.acl_perms, ?) != 0 AND acl.acl_group_id = ?)", perms.GROUP_READ, groupID)
	if userID != "" {
		o.AddOr("(bitAnd(acl.acl_perms, ?) != 0 AND acl.acl_user_id = ?)", perms.OWNER_READ, userID)
	}
	orQuery, orArgs := o.Build()
	q.AddWhereClause("("+orQuery+")", orArgs...)
}

func addErrorQueryFilter(q *query_builder.Query, rawQuery string) {
	if rawQuery == "" {
		return
	}
	query := truncateUTF8(rawQuery, 256)
	q.AddWhereClause("(positionCaseInsensitiveUTF8(eo.message, ?) > 0 OR positionCaseInsensitiveUTF8(eo.error_type, ?) > 0 OR positionCaseInsensitiveUTF8(eo.target_label, ?) > 0 OR positionCaseInsensitiveUTF8(eo.test_suite, ?) > 0 OR positionCaseInsensitiveUTF8(eo.test_class, ?) > 0 OR positionCaseInsensitiveUTF8(eo.test_name, ?) > 0 OR positionCaseInsensitiveUTF8(eo.test_failure_type, ?) > 0 OR positionCaseInsensitiveUTF8(eo.invocation_pattern, ?) > 0)", query, query, query, query, query, query, query, query)
}

func getOccurrences(ctx context.Context, env environment.Env, groupID, userID, fingerprint, searchQuery string, origin etpb.ErrorOrigin, startUsec, endUsec int64, limit int, cursor pageCursor) ([]*etpb.ErrorOccurrence, string, error) {
	aclQuery, aclArgs := currentACLSubquery(groupID, startUsec, endUsec)
	type invocationRow struct {
		InvocationID string
		LatestUsec   int64
	}
	invocationQuery := query_builder.NewQuery(fmt.Sprintf(`SELECT eo.invocation_id, max(eo.event_time_usec) AS latest_usec FROM ErrorOccurrences AS eo FINAL INNER JOIN (%s) AS acl ON eo.invocation_id = acl.invocation_id`, aclQuery))
	invocationQuery.AddWhereClause("eo.group_id = ?", groupID)
	if err := addErrorOriginFilter(invocationQuery, origin); err != nil {
		return nil, "", err
	}
	invocationQuery.AddWhereClause("acl.acl_deleted = 0")
	addPermissionsCheckToQuery(invocationQuery, groupID, userID)
	invocationQuery.AddWhereClause("eo.fingerprint = ?", fingerprint)
	invocationQuery.AddWhereClause("eo.event_time_usec >= ?", startUsec)
	invocationQuery.AddWhereClause("eo.event_time_usec <= ?", endUsec)
	addErrorQueryFilter(invocationQuery, searchQuery)
	invocationQuery.SetGroupBy("eo.invocation_id")
	if cursor.Kind == "detail" {
		invocationQuery.AddHavingClause("(latest_usec, eo.invocation_id) < (?, ?)", cursor.Time, cursor.ID)
	}
	invocationQuery.SetOrderBy("(latest_usec, eo.invocation_id)", false)
	invocationQuery.SetLimit(int64(limit + 1))
	query, args := invocationQuery.Build()
	args = append(aclArgs, args...)
	invocations, err := db.ScanAll(env.GetOLAPDBHandle().NewQuery(ctx, "error_tracking_occurrence_invocations").Raw(query, args...), &invocationRow{})
	if err != nil {
		return nil, "", err
	}
	nextPageToken := ""
	if len(invocations) > limit {
		invocations = invocations[:limit]
		last := invocations[len(invocations)-1]
		nextPageToken = encodePageCursor(pageCursor{Kind: "detail", Time: last.LatestUsec, Origin: int32(origin), ID: last.InvocationID})
	}
	invocationIDs := make(map[string]struct{}, len(invocations))
	for _, row := range invocations {
		invocationIDs[row.InvocationID] = struct{}{}
	}
	if len(invocationIDs) == 0 {
		return nil, "", nil
	}

	q := query_builder.NewQuery(fmt.Sprintf(`SELECT eo.* FROM ErrorOccurrences AS eo FINAL INNER JOIN (%s) AS acl ON eo.invocation_id = acl.invocation_id`, aclQuery))
	q.AddWhereClause("eo.group_id = ?", groupID)
	if err := addErrorOriginFilter(q, origin); err != nil {
		return nil, "", err
	}
	q.AddWhereClause("acl.acl_deleted = 0")
	addPermissionsCheckToQuery(q, groupID, userID)
	q.AddWhereClause("eo.fingerprint = ?", fingerprint)
	q.AddWhereClause("eo.event_time_usec >= ?", startUsec)
	q.AddWhereClause("eo.event_time_usec <= ?", endUsec)
	addStringSetWhereClause(q, "eo.invocation_id", invocationIDs)
	addErrorQueryFilter(q, searchQuery)
	q.SetOrderBy("(eo.event_time_usec, eo.sequence_number)", false)
	query, args = q.Build()
	query += fmt.Sprintf(" LIMIT %d BY eo.invocation_id LIMIT %d", MaxOccurrencesPerInvocation, len(invocationIDs)*MaxOccurrencesPerInvocation)
	args = append(aclArgs, args...)
	rows, err := db.ScanAll(env.GetOLAPDBHandle().NewQuery(ctx, "error_tracking_occurrences").Raw(query, args...), &schema.ErrorOccurrence{})
	if err != nil {
		return nil, "", err
	}
	related, err := getRelatedExecutions(ctx, env, groupID, rows)
	if err != nil {
		return nil, "", err
	}
	result := make([]*etpb.ErrorOccurrence, 0, len(rows))
	for _, row := range rows {
		responseOrigin := etpb.ErrorOrigin(row.Origin)
		if responseOrigin == etpb.ErrorOrigin_ERROR_ORIGIN_UNKNOWN {
			if origin == etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW {
				responseOrigin = etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW
			} else if origin == etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL {
				responseOrigin = etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL
			}
		}
		o := &etpb.ErrorOccurrence{
			InvocationId: row.InvocationID, EventTimeUsec: row.EventTimeUsec,
			ErrorType: row.ErrorType, Message: row.Message, TargetLabel: row.TargetLabel,
			ActionMnemonic: row.ActionMnemonic, ExitCode: row.ExitCode, RepoUrl: row.RepoURL,
			BranchName: row.BranchName, CommitSha: row.CommitSHA, Command: row.Command, User: row.User,
			FingerprintVersion: row.FingerprintVersion, FingerprintSource: row.FingerprintSource,
			FingerprintConfidence: row.FingerprintConfidence, TestSuite: row.TestSuite,
			TestClass: row.TestClass, TestName: row.TestName, TestFailureKind: row.TestFailureKind,
			TestFailureType: row.TestFailureType, TestRun: row.TestRun, TestShard: row.TestShard,
			TestAttempt: row.TestAttempt, TestCachedLocally: row.TestCachedLocally,
			TestCachedRemotely: row.TestCachedRemotely, TestStrategy: row.TestStrategy,
			Origin: responseOrigin, RunId: row.RunID, ParentRunId: row.ParentRunID,
			InvocationPattern: row.InvocationPattern,
		}
		o.RelatedExecutions = related[row]
		result = append(result, o)
	}
	return result, nextPageToken, nil
}

func getRelatedExecutions(ctx context.Context, env environment.Env, groupID string, occurrences []*schema.ErrorOccurrence) (map[*schema.ErrorOccurrence][]*etpb.RelatedExecution, error) {
	result := make(map[*schema.ErrorOccurrence][]*etpb.RelatedExecution, len(occurrences))
	if len(occurrences) == 0 {
		return result, nil
	}
	type matcherSpec struct {
		invocationUUID        string
		invocationIncarnation string
		targetLabel           string
		actionMnemonic        string
		minEventUsec          int64
		maxEventUsec          int64
		occurrences           []*schema.ErrorOccurrence
	}
	matcherIDs := make(map[string]int, len(occurrences))
	matchers := make([]matcherSpec, 0, len(occurrences))
	for _, occurrence := range occurrences {
		key := strings.Join([]string{occurrence.InvocationUUID, occurrence.InvocationIncarnation, occurrence.TargetLabel, occurrence.ActionMnemonic}, "\x00")
		if i, ok := matcherIDs[key]; ok {
			matchers[i].minEventUsec = min(matchers[i].minEventUsec, occurrence.EventTimeUsec)
			matchers[i].maxEventUsec = max(matchers[i].maxEventUsec, occurrence.EventTimeUsec)
			matchers[i].occurrences = append(matchers[i].occurrences, occurrence)
			continue
		}
		matcherIDs[key] = len(matchers)
		matchers = append(matchers, matcherSpec{
			invocationUUID:        occurrence.InvocationUUID,
			invocationIncarnation: occurrence.InvocationIncarnation,
			targetLabel:           occurrence.TargetLabel,
			actionMnemonic:        occurrence.ActionMnemonic,
			minEventUsec:          occurrence.EventTimeUsec,
			maxEventUsec:          occurrence.EventTimeUsec,
			occurrences:           []*schema.ErrorOccurrence{occurrence},
		})
	}

	// Query each distinct logical matcher with its own hard candidate limit.
	// This avoids multiplying every execution by every occurrence while still
	// preventing a noisy wildcard matcher from starving an exact target/action
	// pair. Duplicate occurrence contexts share the same bounded matcher result.
	// Keep UNION batches small enough to stay well below ClickHouse's default
	// max_query_size even when a detail page contains the maximum 500 matchers.
	type matchedExecution struct {
		schema.Execution
		MatcherID uint64
	}
	for batchStart := 0; batchStart < len(matchers); batchStart += relatedExecutionMatchersPerQuery {
		batchEnd := min(batchStart+relatedExecutionMatchersPerQuery, len(matchers))
		subqueries := make([]string, 0, batchEnd-batchStart)
		args := make([]interface{}, 0, (batchEnd-batchStart)*8)
		for i := batchStart; i < batchEnd; i++ {
			matcher := matchers[i]
			query := `(
			SELECT toUInt64(?) AS matcher_id,
				group_id, updated_at_usec, invocation_uuid, invocation_incarnation, instance_name,
				execution_uuid, compressor, digest_function, action_digest_hash,
				action_digest_size, target_label, action_mnemonic, status_code,
				status_message, exit_code
			FROM Executions FINAL
			WHERE group_id = ? AND invocation_uuid = ? AND invocation_incarnation = ?
				AND updated_at_usec >= ? AND updated_at_usec <= ?
				AND (status_code != 0 OR exit_code != 0)`
			args = append(args, i, groupID, matcher.invocationUUID, matcher.invocationIncarnation,
				matcher.minEventUsec-relatedExecutionTimePadding.Microseconds(),
				matcher.maxEventUsec+relatedExecutionTimePadding.Microseconds())
			if matcher.targetLabel != "" {
				query += " AND target_label = ?"
				args = append(args, matcher.targetLabel)
			}
			if matcher.actionMnemonic != "" {
				query += " AND action_mnemonic = ?"
				args = append(args, matcher.actionMnemonic)
			}
			query += " ORDER BY updated_at_usec DESC LIMIT " + fmt.Sprint(relatedExecutionCandidates) + ")"
			subqueries = append(subqueries, query)
		}
		query := strings.Join(subqueries, " UNION ALL ")
		rows, err := db.ScanAll(env.GetOLAPDBHandle().NewQuery(ctx, "error_tracking_related_executions").Raw(query, args...), &matchedExecution{})
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row.MatcherID >= uint64(len(matchers)) {
				continue
			}
			rn, err := resourceName(&row.Execution)
			if err != nil {
				continue
			}
			related := &etpb.RelatedExecution{ExecutionId: rn.UploadString(row.ExecutionUUID), TargetLabel: row.TargetLabel, ActionMnemonic: row.ActionMnemonic, StatusCode: row.StatusCode, StatusMessage: row.StatusMessage, ExitCode: row.ExitCode}
			for _, occurrence := range matchers[row.MatcherID].occurrences {
				if len(result[occurrence]) < maxRelatedExecutions {
					result[occurrence] = append(result[occurrence], related)
				}
			}
		}
	}
	return result, nil
}

func addStringSetWhereClause(q *query_builder.Query, field string, values map[string]struct{}) {
	clause, args := stringSetClause(field, values)
	q.AddWhereClause(clause, args...)
}

func stringSetClause(field string, values map[string]struct{}) (string, []interface{}) {
	if len(values) == 0 {
		return "", nil
	}
	args := make([]interface{}, 0, len(values))
	params := make([]string, 0, len(values))
	for value := range values {
		args = append(args, value)
		params = append(params, "?")
	}
	return field + " IN (" + strings.Join(params, ", ") + ")", args
}

func resourceName(in *schema.Execution) (*digest.CASResourceName, error) {
	digestProto := &repb.Digest{Hash: hex.EncodeToString([]byte(in.ActionDigestHash)), SizeBytes: int64(in.ActionDigestSize)}
	df, err := digest.DigestFunctionFromSegment(in.DigestFunction, digestProto)
	if err != nil {
		return nil, err
	}
	rn := digest.NewCASResourceName(digestProto, in.InstanceName, df)
	compressor, err := digest.CompressorFromSegment(in.Compressor)
	if err != nil {
		return nil, err
	}
	rn.SetCompressor(compressor)
	return rn, nil
}
