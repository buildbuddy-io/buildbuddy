package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	etpb "github.com/buildbuddy-io/buildbuddy/proto/error_tracking"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	"github.com/buildbuddy-io/buildbuddy/server/error_tracking"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/require"
)

const relatedExecutionBatchTestSize = 26

func TestGetErrorGroupsClickHouse(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)
	env := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers(
		"US_ERROR_1", "GR_ERROR_1",
		"US_ERROR_OTHER", "GR_ERROR_1",
		"US_ERROR_PRIVATE", "GR_ERROR_1",
		"US_ERROR_2", "GR_ERROR_2",
	))
	env.SetAuthenticator(auth)
	nowUsec := time.Now().UnixMicro()
	invocationUUID := strings.ReplaceAll(uuid.New(), "-", "")
	fingerprint := "same-fingerprint"
	structuredFingerprint := "structured-test-fingerprint"
	frequencyFingerprint := "frequency-fingerprint"
	batchedRelatedFingerprint := "batched-related-executions-fingerprint"
	skippedFingerprint := "skipped-target-fingerprint"
	incompleteFingerprint := "incomplete-build-fingerprint"
	rows := []*schema.ErrorOccurrence{
		{GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ, Fingerprint: fingerprint, EventTimeUsec: nowUsec - 3, InvocationUUID: invocationUUID, InvocationID: uuid.New(), SequenceNumber: 1, ErrorType: "action/spawn/NON_ZERO_EXIT", Message: "process 123 failed", TargetLabel: "//pkg:target", ActionMnemonic: "GoCompilePkg", ExitCode: 1},
		{GroupID: "GR_ERROR_1", UserID: "US_ERROR_OTHER", Perms: perms.GROUP_READ, Fingerprint: fingerprint, EventTimeUsec: nowUsec - 2, InvocationUUID: strings.Repeat("2", 32), InvocationID: uuid.New(), SequenceNumber: 2, ErrorType: "action/spawn/NON_ZERO_EXIT", Message: "process 456 failed", TargetLabel: "//pkg:target", ActionMnemonic: "GoCompilePkg", ExitCode: 1},
		{GroupID: "GR_ERROR_1", UserID: "US_ERROR_PRIVATE", Perms: perms.OWNER_READ, Fingerprint: fingerprint, EventTimeUsec: nowUsec - 1, InvocationUUID: strings.Repeat("4", 32), InvocationID: uuid.New(), SequenceNumber: 3, ErrorType: "action/spawn/NON_ZERO_EXIT", Message: "private process failed", TargetLabel: "//pkg:private", ActionMnemonic: "GoCompilePkg", ExitCode: 1},
		{GroupID: "GR_ERROR_2", UserID: "US_ERROR_2", Perms: perms.GROUP_READ, Fingerprint: fingerprint, EventTimeUsec: nowUsec, InvocationUUID: strings.Repeat("3", 32), InvocationID: uuid.New(), SequenceNumber: 4, ErrorType: "action/spawn/NON_ZERO_EXIT", Message: "other organization", TargetLabel: "//pkg:target", ActionMnemonic: "GoCompilePkg", ExitCode: 1},
	}
	structuredInvocationID := uuid.New()
	structuredInvocationUUID := strings.ReplaceAll(structuredInvocationID, "-", "")
	starvedInvocationID := uuid.New()
	starvedInvocationUUID := strings.ReplaceAll(starvedInvocationID, "-", "")
	batchedRelatedInvocationID := uuid.New()
	batchedRelatedInvocationUUID := strings.ReplaceAll(batchedRelatedInvocationID, "-", "")
	frequencyInvocationID := uuid.New()
	frequencyInvocationUUID := strings.ReplaceAll(frequencyInvocationID, "-", "")
	frequencyOlderInvocationID := uuid.New()
	frequencyOlderInvocationUUID := strings.ReplaceAll(frequencyOlderInvocationID, "-", "")
	frequencyRecentInvocationID := uuid.New()
	frequencyRecentInvocationUUID := strings.ReplaceAll(frequencyRecentInvocationID, "-", "")
	frequencyNewestInvocationID := uuid.New()
	frequencyNewestInvocationUUID := strings.ReplaceAll(frequencyNewestInvocationID, "-", "")
	starvedFingerprint := "exact-execution-match-fingerprint"
	rows = append(rows,
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: structuredFingerprint, FingerprintVersion: error_tracking.TestFingerprintVersion,
			FingerprintSource: "test.xml", FingerprintConfidence: "high", EventTimeUsec: nowUsec - 6,
			InvocationUUID: structuredInvocationUUID, InvocationID: structuredInvocationID, SequenceNumber: 5,
			ErrorType: "test/FAILED", Message: "expected true, got false", TargetLabel: "//pkg:widget_test",
			TestSuite: "widget", TestClass: "WidgetTest", TestName: "renders disabled state",
			TestFailureKind: "failure", TestFailureType: "AssertionError", TestRun: 1, TestShard: 0, TestAttempt: 1,
			TestCachedLocally: true, TestStrategy: "local-cache",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: structuredFingerprint, FingerprintVersion: error_tracking.TestFingerprintVersion,
			FingerprintSource: "test.xml", FingerprintConfidence: "high", EventTimeUsec: nowUsec - 5,
			InvocationUUID: structuredInvocationUUID, InvocationID: structuredInvocationID, SequenceNumber: 6,
			ErrorType: "test/FAILED", Message: "expected true, got false", TargetLabel: "//pkg:widget_test",
			TestSuite: "widget", TestClass: "WidgetTest", TestName: "renders disabled state",
			TestFailureKind: "failure", TestFailureType: "AssertionError", TestRun: 1, TestShard: 0, TestAttempt: 2,
			TestCachedRemotely: true, TestStrategy: "remote-cache",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_OTHER", Perms: perms.GROUP_READ,
			Fingerprint: structuredFingerprint, FingerprintVersion: error_tracking.TestFingerprintVersion,
			FingerprintSource: "test.xml", FingerprintConfidence: "high", EventTimeUsec: nowUsec - 4,
			InvocationUUID: strings.Repeat("5", 32), InvocationID: uuid.New(), SequenceNumber: 7,
			ErrorType: "test/FAILED", Message: "expected true, got false", TargetLabel: "//pkg:widget_test",
			TestSuite: "widget", TestClass: "WidgetTest", TestName: "renders disabled state",
			TestFailureKind: "failure", TestFailureType: "AssertionError", TestRun: 1, TestShard: 1, TestAttempt: 1,
			TestStrategy: "remote",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: starvedFingerprint, EventTimeUsec: nowUsec - 7,
			InvocationUUID: starvedInvocationUUID, InvocationID: starvedInvocationID, SequenceNumber: 8,
			ErrorType: "action/spawn/NON_ZERO_EXIT", Message: "exact execution should remain visible",
			TargetLabel: "//pkg:exact", ActionMnemonic: "GoCompilePkg", ExitCode: 1,
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: starvedFingerprint, EventTimeUsec: nowUsec - 8,
			InvocationUUID: starvedInvocationUUID, InvocationID: starvedInvocationID, SequenceNumber: 9,
			ErrorType: "build/BUILD_FAILURE", Message: "invocation-wide failure",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: frequencyFingerprint, EventTimeUsec: nowUsec - 55*time.Second.Microseconds(),
			InvocationUUID: frequencyInvocationUUID, InvocationID: frequencyInvocationID, SequenceNumber: 10,
			ErrorType: "test/FAILED", Message: "frequency sample older",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: frequencyFingerprint, EventTimeUsec: nowUsec - 3*time.Second.Microseconds(),
			InvocationUUID: frequencyInvocationUUID, InvocationID: frequencyInvocationID, SequenceNumber: 11,
			ErrorType: "test/FAILED", Message: "frequency sample recent",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: frequencyFingerprint, EventTimeUsec: nowUsec - 54*time.Second.Microseconds(),
			InvocationUUID: frequencyOlderInvocationUUID, InvocationID: frequencyOlderInvocationID, SequenceNumber: 12,
			ErrorType: "test/FAILED", Message: "frequency sample older",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: frequencyFingerprint, EventTimeUsec: nowUsec - 2_900*time.Millisecond.Microseconds(),
			InvocationUUID: frequencyRecentInvocationUUID, InvocationID: frequencyRecentInvocationID, SequenceNumber: 13,
			ErrorType: "test/FAILED", Message: "frequency sample recent",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: frequencyFingerprint, EventTimeUsec: nowUsec - 2_800*time.Millisecond.Microseconds(),
			InvocationUUID: frequencyNewestInvocationUUID, InvocationID: frequencyNewestInvocationID, SequenceNumber: 14,
			ErrorType: "test/FAILED", Message: "frequency sample recent",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: skippedFingerprint, EventTimeUsec: nowUsec - 1,
			InvocationUUID: strings.Repeat("6", 32), InvocationID: uuid.New(), SequenceNumber: 15,
			ErrorType: "aborted/SKIPPED", Message: "Target //pkg:incompatible build was skipped.",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: incompleteFingerprint, EventTimeUsec: nowUsec - 1,
			InvocationUUID: strings.Repeat("7", 32), InvocationID: uuid.New(), SequenceNumber: 16,
			ErrorType: "aborted/INCOMPLETE", Message: "build incomplete due to an earlier failure",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: "legacy-analysis-wrapper", EventTimeUsec: nowUsec - 1,
			InvocationUUID: strings.Repeat("8", 32), InvocationID: uuid.New(), SequenceNumber: 17,
			ErrorType: "aborted/ANALYSIS_FAILURE", Message: "analysis failed",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: "legacy-user-interrupted", EventTimeUsec: nowUsec - 1,
			InvocationUUID: strings.Repeat("9", 32), InvocationID: uuid.New(), SequenceNumber: 18,
			ErrorType: "aborted/USER_INTERRUPTED", Message: "build interrupted",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: "workflow-origin-fixture", EventTimeUsec: nowUsec - 2*time.Minute.Microseconds(),
			InvocationUUID: strings.Repeat("a", 32), InvocationID: uuid.New(), SequenceNumber: 19,
			ErrorType: "target/unknown", Message: "origin fixture workflow", Command: "workflow run",
			Origin: int32(etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW), RunID: "workflow-run", InvocationPattern: "Check style",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: "workflow-origin-legacy-fixture", EventTimeUsec: nowUsec - 2*time.Minute.Microseconds() + 1,
			InvocationUUID: strings.Repeat("b", 32), InvocationID: uuid.New(), SequenceNumber: 20,
			ErrorType: "build/unknown", Message: "origin fixture legacy workflow", Command: "workflow run",
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: "bazel-origin-fixture", EventTimeUsec: nowUsec - 2*time.Minute.Microseconds() + 2,
			InvocationUUID: strings.Repeat("c", 32), InvocationID: uuid.New(), SequenceNumber: 21,
			ErrorType: "test/FAILED", Message: "origin fixture standalone bazel", Command: "test",
			Origin: int32(etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL),
		},
		&schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: "workflow-bazel-child-origin-fixture", EventTimeUsec: nowUsec - 2*time.Minute.Microseconds() + 3,
			InvocationUUID: strings.Repeat("d", 32), InvocationID: uuid.New(), SequenceNumber: 22,
			ErrorType: "action/spawn/NON_ZERO_EXIT", Message: "origin fixture workflow bazel child", Command: "build",
			Origin: int32(etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW_BAZEL_CHILD), ParentRunID: "workflow-run",
		},
	)
	for i := 0; i < relatedExecutionBatchTestSize; i++ {
		rows = append(rows, &schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: batchedRelatedFingerprint, EventTimeUsec: nowUsec - 2*time.Minute.Microseconds() - int64(i+1),
			InvocationUUID: batchedRelatedInvocationUUID, InvocationID: batchedRelatedInvocationID,
			SequenceNumber: int64(100 + i), ErrorType: "action/spawn/NON_ZERO_EXIT",
			Message: fmt.Sprintf("batched failure %d", i), TargetLabel: fmt.Sprintf("//batch:target_%d", i),
			ActionMnemonic: "GoCompilePkg", ExitCode: 1,
		})
	}
	// ReplacingMergeTree reconciliation is asynchronous. Insert one logical
	// occurrence twice to prove user-facing queries deduplicate before merges.
	rowsWithRetryDuplicate := append(append([]*schema.ErrorOccurrence(nil), rows...), rows[0])
	require.NoError(t, env.GetOLAPDBHandle().GORM(context.Background(), "insert_error_occurrences").Create(rowsWithRetryDuplicate).Error)
	for _, row := range rows {
		require.NoError(t, env.GetOLAPDBHandle().FlushErrorInvocationACL(context.Background(), &schema.ErrorInvocationACL{
			GroupID: row.GroupID, InvocationID: row.InvocationID, UserID: row.UserID, Perms: row.Perms,
			ACLVersion: error_tracking.CommittedACLVersion(0), UpdatedAtUsec: nowUsec,
		}))
	}
	execution := &schema.Execution{
		GroupID: "GR_ERROR_1", UpdatedAtUsec: nowUsec, InvocationUUID: invocationUUID,
		ExecutionUUID: uuid.New(), ActionDigestHash: strings.Repeat("a", 32), ActionDigestSize: 1,
		TargetLabel: "//pkg:target", ActionMnemonic: "GoCompilePkg", StatusCode: 13, StatusMessage: "compile failed", ExitCode: 1,
	}
	require.NoError(t, env.GetOLAPDBHandle().GORM(context.Background(), "insert_error_execution").Create(execution).Error)
	noisyExecutions := make([]*schema.Execution, 0, 41)
	for i := 0; i < 40; i++ {
		noisyExecutions = append(noisyExecutions, &schema.Execution{
			GroupID: "GR_ERROR_1", UpdatedAtUsec: nowUsec + int64(i) + 100, InvocationUUID: invocationUUID,
			ExecutionUUID: uuid.New(), ActionDigestHash: strings.Repeat("b", 32), ActionDigestSize: 1,
			TargetLabel: "//pkg:target", ActionMnemonic: "GoCompilePkg", StatusCode: 13, StatusMessage: "noisy failure", ExitCode: 1,
		})
	}
	noisyExecutions = append(noisyExecutions, &schema.Execution{
		GroupID: "GR_ERROR_1", UpdatedAtUsec: nowUsec, InvocationUUID: strings.Repeat("2", 32),
		ExecutionUUID: uuid.New(), ActionDigestHash: strings.Repeat("c", 32), ActionDigestSize: 1,
		TargetLabel: "//pkg:target", ActionMnemonic: "GoCompilePkg", StatusCode: 13, StatusMessage: "sparse failure", ExitCode: 1,
	})
	for i := 0; i < 40; i++ {
		noisyExecutions = append(noisyExecutions, &schema.Execution{
			GroupID: "GR_ERROR_1", UpdatedAtUsec: nowUsec + int64(i) + 1_000, InvocationUUID: starvedInvocationUUID,
			ExecutionUUID: uuid.New(), ActionDigestHash: fmt.Sprintf("%032x", i+1), ActionDigestSize: 1,
			TargetLabel: fmt.Sprintf("//pkg:noise_%d", i/20), ActionMnemonic: "GoCompilePkg", StatusCode: 13, StatusMessage: "unrelated newer failure", ExitCode: 1,
		})
	}
	exactExecution := &schema.Execution{
		GroupID: "GR_ERROR_1", UpdatedAtUsec: nowUsec, InvocationUUID: starvedInvocationUUID,
		ExecutionUUID: uuid.New(), ActionDigestHash: strings.Repeat("d", 32), ActionDigestSize: 1,
		TargetLabel: "//pkg:exact", ActionMnemonic: "GoCompilePkg", StatusCode: 13, StatusMessage: "exact older failure", ExitCode: 1,
	}
	noisyExecutions = append(noisyExecutions, exactExecution, exactExecution, &schema.Execution{
		GroupID: "GR_ERROR_1", UpdatedAtUsec: nowUsec - 1, InvocationUUID: starvedInvocationUUID,
		ExecutionUUID: uuid.New(), ActionDigestHash: strings.Repeat("e", 32), ActionDigestSize: 1,
		TargetLabel: "//pkg:exact", ActionMnemonic: "GoCompilePkg", StatusCode: 13, StatusMessage: "second exact failure", ExitCode: 1,
	})
	for i := 0; i < relatedExecutionBatchTestSize; i++ {
		noisyExecutions = append(noisyExecutions, &schema.Execution{
			GroupID: "GR_ERROR_1", UpdatedAtUsec: nowUsec, InvocationUUID: batchedRelatedInvocationUUID,
			ExecutionUUID: uuid.New(), ActionDigestHash: fmt.Sprintf("%032x", 1000+i), ActionDigestSize: 1,
			TargetLabel: fmt.Sprintf("//batch:target_%d", i), ActionMnemonic: "GoCompilePkg",
			StatusCode: 13, StatusMessage: fmt.Sprintf("batched execution %d", i), ExitCode: 1,
		})
	}
	require.NoError(t, env.GetOLAPDBHandle().GORM(context.Background(), "insert_error_execution_candidates").Create(noisyExecutions).Error)

	ctx, err := auth.WithAuthenticatedUser(context.Background(), "US_ERROR_1")
	require.NoError(t, err)
	rsp, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Query: "process"})
	require.NoError(t, err)
	require.Len(t, rsp.GetGroups(), 1)
	require.Equal(t, int64(2), rsp.GetGroups()[0].GetOccurrenceCount())
	require.Len(t, rsp.GetGroups()[0].GetFrequencyBuckets(), 7)
	var frequencyTotal int64
	for _, bucket := range rsp.GetGroups()[0].GetFrequencyBuckets() {
		frequencyTotal += bucket.GetAffectedInvocationCount()
	}
	require.Equal(t, rsp.GetGroups()[0].GetOccurrenceCount(), frequencyTotal)
	require.Equal(t, int64(2), rsp.GetGroups()[0].GetFrequencyBuckets()[6].GetAffectedInvocationCount())

	frequencyRsp, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Query: "frequency sample"})
	require.NoError(t, err)
	require.Len(t, frequencyRsp.GetGroups(), 1)
	require.Equal(t, int64(4), frequencyRsp.GetGroups()[0].GetOccurrenceCount())
	require.Equal(t, int64(2), frequencyRsp.GetGroups()[0].GetFrequencyBuckets()[0].GetAffectedInvocationCount())
	require.Equal(t, int64(3), frequencyRsp.GetGroups()[0].GetFrequencyBuckets()[6].GetAffectedInvocationCount())
	var distributedFrequencyTotal int64
	for _, bucket := range frequencyRsp.GetGroups()[0].GetFrequencyBuckets() {
		distributedFrequencyTotal += bucket.GetAffectedInvocationCount()
	}
	require.Equal(t, int64(5), distributedFrequencyTotal, "one invocation can truthfully appear in multiple time buckets")

	filteredDetail, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Query: "123", Fingerprint: fingerprint})
	require.NoError(t, err)
	require.Len(t, filteredDetail.GetGroups(), 1)
	require.Equal(t, int64(1), filteredDetail.GetGroups()[0].GetOccurrenceCount())
	require.Equal(t, int64(1), filteredDetail.GetGroups()[0].GetFrequencyBuckets()[6].GetAffectedInvocationCount())
	require.Len(t, filteredDetail.GetGroups()[0].GetOccurrences(), 1)
	require.Equal(t, "process 123 failed", filteredDetail.GetGroups()[0].GetOccurrences()[0].GetMessage())

	detail, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Fingerprint: fingerprint})
	require.NoError(t, err)
	require.Len(t, detail.GetGroups(), 1)
	require.Len(t, detail.GetGroups()[0].GetOccurrences(), 2)
	foundNoisy, foundSparse := false, false
	for _, occurrence := range detail.GetGroups()[0].GetOccurrences() {
		switch occurrence.GetMessage() {
		case "process 123 failed":
			require.Len(t, occurrence.GetRelatedExecutions(), 5)
			foundNoisy = true
		case "process 456 failed":
			require.Len(t, occurrence.GetRelatedExecutions(), 1)
			require.Equal(t, "sparse failure", occurrence.GetRelatedExecutions()[0].GetStatusMessage())
			foundSparse = true
		}
	}
	require.True(t, foundNoisy)
	require.True(t, foundSparse)

	exactDetail, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Fingerprint: starvedFingerprint,
	})
	require.NoError(t, err)
	require.Len(t, exactDetail.GetGroups(), 1)
	require.Len(t, exactDetail.GetGroups()[0].GetOccurrences(), 2)
	foundExactContext := false
	for _, occurrence := range exactDetail.GetGroups()[0].GetOccurrences() {
		if occurrence.GetTargetLabel() != "//pkg:exact" {
			continue
		}
		foundExactContext = true
		require.Len(t, occurrence.GetRelatedExecutions(), 2)
		require.ElementsMatch(t, []string{"exact older failure", "second exact failure"}, []string{
			occurrence.GetRelatedExecutions()[0].GetStatusMessage(), occurrence.GetRelatedExecutions()[1].GetStatusMessage(),
		})
	}
	require.True(t, foundExactContext)

	batchedRelatedDetail, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - 3*time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Fingerprint: batchedRelatedFingerprint,
	})
	require.NoError(t, err)
	require.Len(t, batchedRelatedDetail.GetGroups(), 1)
	require.Len(t, batchedRelatedDetail.GetGroups()[0].GetOccurrences(), relatedExecutionBatchTestSize)
	for _, occurrence := range batchedRelatedDetail.GetGroups()[0].GetOccurrences() {
		require.Len(t, occurrence.GetRelatedExecutions(), 1)
	}

	firstGroupPage, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, PageSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, firstGroupPage.GetGroups(), 1)
	require.NotEmpty(t, firstGroupPage.GetNextPageToken())
	secondGroupPage, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, PageSize: 1,
		PageToken: firstGroupPage.GetNextPageToken(),
	})
	require.NoError(t, err)
	require.Len(t, secondGroupPage.GetGroups(), 1)
	require.NotEqual(t, firstGroupPage.GetGroups()[0].GetFingerprint(), secondGroupPage.GetGroups()[0].GetFingerprint())

	for _, test := range []struct {
		name string
		sort etpb.ErrorGroupSort
		want []string
	}{
		{
			name: "affected builds default",
			want: []string{frequencyFingerprint, fingerprint, structuredFingerprint, starvedFingerprint},
		},
		{
			name: "most recently seen",
			sort: etpb.ErrorGroupSort_ERROR_GROUP_SORT_LAST_SEEN,
			want: []string{fingerprint, structuredFingerprint, starvedFingerprint, frequencyFingerprint},
		},
		{
			name: "most frequent recently",
			sort: etpb.ErrorGroupSort_ERROR_GROUP_SORT_RECENT_FREQUENCY,
			want: []string{frequencyFingerprint, fingerprint, structuredFingerprint, starvedFingerprint},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			first, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
				StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
				PageSize: 2, Sort: test.sort,
			})
			require.NoError(t, err)
			require.Len(t, first.GetGroups(), 2)
			require.NotEmpty(t, first.GetNextPageToken())
			second, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
				StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
				PageSize: 2, Sort: test.sort, PageToken: first.GetNextPageToken(),
			})
			require.NoError(t, err)
			require.Len(t, second.GetGroups(), 2)
			require.Empty(t, second.GetNextPageToken())
			got := []string{
				first.GetGroups()[0].GetFingerprint(), first.GetGroups()[1].GetFingerprint(),
				second.GetGroups()[0].GetFingerprint(), second.GetGroups()[1].GetFingerprint(),
			}
			require.Equal(t, test.want, got)

			_, err = error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
				StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
				PageSize: 2, Sort: etpb.ErrorGroupSort_ERROR_GROUP_SORT_RECENT_FREQUENCY,
				PageToken: first.GetNextPageToken(),
			})
			if test.sort == etpb.ErrorGroupSort_ERROR_GROUP_SORT_RECENT_FREQUENCY {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "page token does not match error group sort")
			}
		})
	}

	workflowGroups, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - 3*time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
		Query: "origin fixture", Origin: etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW,
	})
	require.NoError(t, err)
	require.Len(t, workflowGroups.GetGroups(), 2, "typed and legacy workflow rows stay in the workflow plane")
	for _, group := range workflowGroups.GetGroups() {
		require.Equal(t, etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW, group.GetOrigin())
	}
	legacyWorkflowDetail, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - 3*time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
		Fingerprint: "workflow-origin-legacy-fixture", Origin: etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW,
	})
	require.NoError(t, err)
	require.Len(t, legacyWorkflowDetail.GetGroups(), 1)
	require.Len(t, legacyWorkflowDetail.GetGroups()[0].GetOccurrences(), 1)
	require.Equal(t, etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW, legacyWorkflowDetail.GetGroups()[0].GetOccurrences()[0].GetOrigin())

	bazelGroups, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - 3*time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
		Query: "origin fixture", Origin: etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL, PageSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, bazelGroups.GetGroups(), 1)
	require.NotEmpty(t, bazelGroups.GetNextPageToken())
	require.Equal(t, etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL, bazelGroups.GetGroups()[0].GetOrigin())
	_, err = error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - 3*time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
		Query: "origin fixture", Origin: etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW,
		PageSize: 1, PageToken: bazelGroups.GetNextPageToken(),
	})
	require.ErrorContains(t, err, "page token does not match error origin")

	bazelPage2, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - 3*time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
		Query: "origin fixture", Origin: etpb.ErrorOrigin_ERROR_ORIGIN_BAZEL,
		PageSize: 1, PageToken: bazelGroups.GetNextPageToken(),
	})
	require.NoError(t, err)
	require.Len(t, bazelPage2.GetGroups(), 1, "Workflow Bazel children remain in the Bazel plane")
	require.Empty(t, bazelPage2.GetNextPageToken())

	structuredDetail, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
		Fingerprint: structuredFingerprint, PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, structuredDetail.GetGroups(), 1)
	structuredGroup := structuredDetail.GetGroups()[0]
	require.Equal(t, int64(2), structuredGroup.GetOccurrenceCount(), "impact counts unique invocations, not attempts")
	require.Equal(t, error_tracking.TestFingerprintVersion, structuredGroup.GetFingerprintVersion())
	require.Equal(t, "test.xml", structuredGroup.GetFingerprintSource())
	require.Equal(t, "high", structuredGroup.GetFingerprintConfidence())
	require.Equal(t, "widget", structuredGroup.GetSampleTestSuite())
	require.Equal(t, "WidgetTest", structuredGroup.GetSampleTestClass())
	require.Equal(t, "renders disabled state", structuredGroup.GetSampleTestName())
	require.Equal(t, "failure", structuredGroup.GetSampleTestFailureKind())
	require.Equal(t, "AssertionError", structuredGroup.GetSampleTestFailureType())
	require.Len(t, structuredGroup.GetOccurrences(), 3, "detail preserves all retry and invocation contexts")
	var attempts []int32
	for _, occurrence := range structuredGroup.GetOccurrences() {
		require.Equal(t, error_tracking.TestFingerprintVersion, occurrence.GetFingerprintVersion())
		require.Equal(t, "test.xml", occurrence.GetFingerprintSource())
		require.Equal(t, "high", occurrence.GetFingerprintConfidence())
		require.Equal(t, "widget", occurrence.GetTestSuite())
		require.Equal(t, "WidgetTest", occurrence.GetTestClass())
		require.Equal(t, "renders disabled state", occurrence.GetTestName())
		require.Equal(t, "failure", occurrence.GetTestFailureKind())
		require.Equal(t, "AssertionError", occurrence.GetTestFailureType())
		if occurrence.GetInvocationId() == structuredInvocationID {
			attempts = append(attempts, occurrence.GetTestAttempt())
			switch occurrence.GetTestAttempt() {
			case 1:
				require.True(t, occurrence.GetTestCachedLocally())
				require.Equal(t, "local-cache", occurrence.GetTestStrategy())
			case 2:
				require.True(t, occurrence.GetTestCachedRemotely())
				require.Equal(t, "remote-cache", occurrence.GetTestStrategy())
			}
		}
	}
	require.ElementsMatch(t, []int32{1, 2}, attempts)

	firstDetailPage, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
		Fingerprint: structuredFingerprint, PageSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, firstDetailPage.GetGroups()[0].GetOccurrences(), 1)
	require.NotEmpty(t, firstDetailPage.GetNextPageToken())
	secondDetailPage, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
		Fingerprint: structuredFingerprint, PageSize: 1, PageToken: firstDetailPage.GetNextPageToken(),
	})
	require.NoError(t, err)
	require.Len(t, secondDetailPage.GetGroups()[0].GetOccurrences(), 2, "all contexts for the selected invocation stay together")
	require.NotEqual(t, firstDetailPage.GetGroups()[0].GetOccurrences()[0].GetInvocationId(), secondDetailPage.GetGroups()[0].GetOccurrences()[0].GetInvocationId())

	sameGroupCtx, err := auth.WithAuthenticatedUser(context.Background(), "US_ERROR_OTHER")
	require.NoError(t, err)
	sameGroup, err := error_tracking.GetErrorGroups(sameGroupCtx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Fingerprint: fingerprint})
	require.NoError(t, err)
	require.Len(t, sameGroup.GetGroups(), 1)
	require.Equal(t, int64(2), sameGroup.GetGroups()[0].GetOccurrenceCount())
	require.Len(t, sameGroup.GetGroups()[0].GetOccurrences(), 2)

	ownerCtx, err := auth.WithAuthenticatedUser(context.Background(), "US_ERROR_PRIVATE")
	require.NoError(t, err)
	owner, err := error_tracking.GetErrorGroups(ownerCtx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Fingerprint: fingerprint})
	require.NoError(t, err)
	require.Len(t, owner.GetGroups(), 1)
	require.Equal(t, int64(3), owner.GetGroups()[0].GetOccurrenceCount())
	require.Len(t, owner.GetGroups()[0].GetOccurrences(), 3)

	otherCtx, err := auth.WithAuthenticatedUser(context.Background(), "US_ERROR_2")
	require.NoError(t, err)
	other, err := error_tracking.GetErrorGroups(otherCtx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1})
	require.NoError(t, err)
	require.Len(t, other.GetGroups(), 1)
	require.Equal(t, int64(1), other.GetGroups()[0].GetOccurrenceCount())

	group := &tables.Group{GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", SharingEnabled: true}
	require.NoError(t, env.GetDBHandle().GORM(ctx, "insert_error_tracking_group").Create(group).Error)
	lifecycleInvocationID := uuid.New()
	lifecycleInvocationUUIDBytes, err := uuid.StringToBytes(lifecycleInvocationID)
	require.NoError(t, err)
	lifecycleNow := time.Now().Add(-time.Hour)
	env.GetDBHandle().SetNowFunc(func() time.Time { return lifecycleNow })
	lifecycleInvocation := &tables.Invocation{
		InvocationID: lifecycleInvocationID, InvocationUUID: lifecycleInvocationUUIDBytes,
		Role: "CI", RunID: strings.Repeat("r", error_tracking.MaxInvocationProvenanceBytes+1),
		ParentRunID: strings.Repeat("p", error_tracking.MaxInvocationProvenanceBytes+1),
		Pattern:     strings.Repeat("a", error_tracking.MaxInvocationProvenanceBytes+1),
	}
	created, err := env.GetInvocationDB().CreateInvocation(ctx, lifecycleInvocation)
	require.NoError(t, err)
	require.True(t, created)
	lifecycleFingerprint := "acl-lifecycle-fingerprint"
	lifecycleInvocationUUID := strings.ReplaceAll(lifecycleInvocationID, "-", "")
	matched, err := error_tracking.FlushErrorOccurrencesWithPrimary(ctx, env, lifecycleInvocationID, lifecycleInvocation.ErrorTrackingIncarnation, []*schema.ErrorOccurrence{{
		Fingerprint: lifecycleFingerprint, EventTimeUsec: nowUsec, InvocationID: lifecycleInvocationID,
		InvocationUUID: lifecycleInvocationUUID, ErrorType: "aborted/INTERNAL",
		Message: "owner-only after ACL update", TargetLabel: "//pkg:reused", ActionMnemonic: "GoCompilePkg",
	}})
	require.NoError(t, err)
	require.True(t, matched)
	var lifecycleOccurrences []*schema.ErrorOccurrence
	require.NoError(t, env.GetOLAPDBHandle().GORM(ctx, "get_lifecycle_occurrence_provenance").Raw(`
		SELECT * FROM ErrorOccurrences FINAL WHERE group_id = ? AND fingerprint = ?`,
		"GR_ERROR_1", lifecycleFingerprint,
	).Scan(&lifecycleOccurrences).Error)
	require.Len(t, lifecycleOccurrences, 1)
	require.Equal(t, int32(etpb.ErrorOrigin_ERROR_ORIGIN_WORKFLOW_BAZEL_CHILD), lifecycleOccurrences[0].Origin)
	require.Equal(t, strings.Repeat("r", error_tracking.MaxInvocationProvenanceBytes), lifecycleOccurrences[0].RunID)
	require.Equal(t, strings.Repeat("p", error_tracking.MaxInvocationProvenanceBytes), lifecycleOccurrences[0].ParentRunID)
	require.Equal(t, strings.Repeat("a", error_tracking.MaxInvocationProvenanceBytes), lifecycleOccurrences[0].InvocationPattern)

	user, err := auth.AuthenticatedUser(ctx)
	require.NoError(t, err)
	ownerOnlyACL := perms.ToACLProto(&uidpb.UserId{Id: "US_ERROR_1"}, "GR_ERROR_1", perms.OWNER_READ|perms.OWNER_WRITE)
	require.NoError(t, env.GetInvocationDB().UpdateInvocationACL(ctx, &user, lifecycleInvocationID, ownerOnlyACL))
	sameGroupAfterACL, err := error_tracking.GetErrorGroups(sameGroupCtx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Fingerprint: lifecycleFingerprint})
	require.NoError(t, err)
	require.Empty(t, sameGroupAfterACL.GetGroups())
	ownerAfterACL, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Fingerprint: lifecycleFingerprint})
	require.NoError(t, err)
	require.Len(t, ownerAfterACL.GetGroups(), 1)
	require.NoError(t, env.GetOLAPDBHandle().GORM(ctx, "insert_old_incarnation_execution").Create(&schema.Execution{
		GroupID: "GR_ERROR_1", UpdatedAtUsec: nowUsec, InvocationUUID: lifecycleInvocationUUID,
		InvocationIncarnation: lifecycleInvocation.ErrorTrackingIncarnation,
		ExecutionUUID:         uuid.New(), ActionDigestHash: strings.Repeat("f", 32), ActionDigestSize: 1,
		TargetLabel: "//pkg:reused", ActionMnemonic: "GoCompilePkg", StatusCode: 13,
		StatusMessage: "private old incarnation execution", ExitCode: 1,
	}).Error)

	// Simulate a pre-commit ACL state whose primary transaction rolled back.
	// The deletion tombstone must have a strictly greater version.
	require.NoError(t, env.GetOLAPDBHandle().FlushErrorInvocationACL(ctx, &schema.ErrorInvocationACL{
		GroupID: "GR_ERROR_1", InvocationID: lifecycleInvocationID, UserID: "US_ERROR_1",
		Perms: perms.GROUP_READ | perms.GROUP_WRITE, ACLVersion: error_tracking.PendingACLVersion(2),
		UpdatedAtUsec: nowUsec,
	}))
	require.NoError(t, env.GetInvocationDB().DeleteInvocationWithPermsCheck(ctx, &user, lifecycleInvocationID))
	// A delayed BES insert after physical cleanup remains hidden by the retained
	// higher-versioned deletion tombstone.
	require.NoError(t, env.GetOLAPDBHandle().FlushErrorOccurrences(ctx, []*schema.ErrorOccurrence{{
		GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Fingerprint: lifecycleFingerprint,
		EventTimeUsec: nowUsec, InvocationID: lifecycleInvocationID,
		InvocationUUID: strings.ReplaceAll(lifecycleInvocationID, "-", ""), ErrorType: "aborted/INTERNAL",
		Message: "delayed occurrence after deletion",
	}}))
	ownerAfterDelete, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Fingerprint: lifecycleFingerprint})
	require.NoError(t, err)
	require.Empty(t, ownerAfterDelete.GetGroups())

	// A newly created logical invocation may reuse an ID after the prior row was
	// deleted. Its ACL must not be hidden by the retained tombstone, and granting
	// the new incarnation must not reveal occurrences from the deleted one.
	env.GetDBHandle().SetNowFunc(func() time.Time { return lifecycleNow.Add(time.Minute) })
	recreatedInvocation := &tables.Invocation{InvocationID: lifecycleInvocationID, InvocationUUID: lifecycleInvocationUUIDBytes}
	created, err = env.GetInvocationDB().CreateInvocation(ctx, recreatedInvocation)
	require.NoError(t, err)
	require.True(t, created)
	require.NotEqual(t, lifecycleInvocation.CreatedAtUsec, recreatedInvocation.CreatedAtUsec)
	require.NotEqual(t, lifecycleInvocation.ErrorTrackingIncarnation, recreatedInvocation.ErrorTrackingIncarnation)
	require.NoError(t, env.GetOLAPDBHandle().GORM(ctx, "insert_replacement_incarnation_execution").Create(&schema.Execution{
		GroupID: "GR_ERROR_1", UpdatedAtUsec: nowUsec, InvocationUUID: lifecycleInvocationUUID,
		InvocationIncarnation: recreatedInvocation.ErrorTrackingIncarnation,
		ExecutionUUID:         uuid.New(), ActionDigestHash: strings.Repeat("e", 32), ActionDigestSize: 1,
		TargetLabel: "//pkg:reused", ActionMnemonic: "GoCompilePkg", StatusCode: 13,
		StatusMessage: "replacement incarnation execution", ExitCode: 1,
	}).Error)
	recreatedFingerprint := "recreated-invocation-fingerprint"
	matched, err = error_tracking.FlushErrorOccurrencesWithPrimary(ctx, env, lifecycleInvocationID, recreatedInvocation.ErrorTrackingIncarnation, []*schema.ErrorOccurrence{{
		Fingerprint: recreatedFingerprint, EventTimeUsec: nowUsec, ErrorType: "aborted/INTERNAL",
		Message: "new invocation with reused ID", TargetLabel: "//pkg:reused", ActionMnemonic: "GoCompilePkg",
	}})
	require.NoError(t, err)
	require.True(t, matched)
	var remainingExecutions []*schema.Execution
	require.NoError(t, env.GetOLAPDBHandle().GORM(ctx, "get_reused_invocation_executions").Raw(`
		SELECT * FROM Executions FINAL WHERE group_id = ? AND invocation_uuid = ?`,
		"GR_ERROR_1", lifecycleInvocationUUID,
	).Scan(&remainingExecutions).Error)
	require.Len(t, remainingExecutions, 1)
	require.Equal(t, recreatedInvocation.ErrorTrackingIncarnation, remainingExecutions[0].InvocationIncarnation)
	var recreatedOccurrences []*schema.ErrorOccurrence
	require.NoError(t, env.GetOLAPDBHandle().GORM(ctx, "get_recreated_occurrences").Raw(`
		SELECT * FROM ErrorOccurrences FINAL WHERE group_id = ? AND fingerprint = ?`,
		"GR_ERROR_1", recreatedFingerprint,
	).Scan(&recreatedOccurrences).Error)
	require.Len(t, recreatedOccurrences, 1)
	require.Equal(t, recreatedInvocation.ErrorTrackingIncarnation, recreatedOccurrences[0].InvocationIncarnation)
	var matchingExecutions []*schema.Execution
	require.NoError(t, env.GetOLAPDBHandle().GORM(ctx, "get_matching_recreated_executions").Raw(`
		SELECT * FROM Executions FINAL
		WHERE group_id = ? AND invocation_uuid = ? AND invocation_incarnation = ?
			AND updated_at_usec >= ? AND updated_at_usec <= ?
			AND (status_code != 0 OR exit_code != 0)
			AND target_label = ? AND action_mnemonic = ?`,
		"GR_ERROR_1", lifecycleInvocationUUID, recreatedInvocation.ErrorTrackingIncarnation,
		nowUsec-time.Hour.Microseconds(), nowUsec+time.Hour.Microseconds(),
		"//pkg:reused", "GoCompilePkg",
	).Scan(&matchingExecutions).Error)
	require.Len(t, matchingExecutions, 1)
	recreated, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Fingerprint: recreatedFingerprint})
	require.NoError(t, err)
	require.Len(t, recreated.GetGroups(), 1)
	require.Len(t, recreated.GetGroups()[0].GetOccurrences(), 1)
	require.Len(t, recreated.GetGroups()[0].GetOccurrences()[0].GetRelatedExecutions(), 1)
	require.Equal(t, "replacement incarnation execution", recreated.GetGroups()[0].GetOccurrences()[0].GetRelatedExecutions()[0].GetStatusMessage())
	deletedAfterRecreation, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1, Fingerprint: lifecycleFingerprint})
	require.NoError(t, err)
	require.Empty(t, deletedAfterRecreation.GetGroups())

	boundedFingerprint := "bounded-context-fingerprint"
	boundedInvocationID := uuid.New()
	boundedRows := make([]*schema.ErrorOccurrence, 0, error_tracking.MaxOccurrencesPerInvocation+1)
	for i := 1; i <= error_tracking.MaxOccurrencesPerInvocation+1; i++ {
		boundedRows = append(boundedRows, &schema.ErrorOccurrence{
			GroupID: "GR_ERROR_1", UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
			Fingerprint: boundedFingerprint, EventTimeUsec: nowUsec, SequenceNumber: int64(i),
			InvocationID: boundedInvocationID, InvocationUUID: strings.ReplaceAll(boundedInvocationID, "-", ""),
			ErrorType: "test/FAILED/failure", Message: fmt.Sprintf("context-%d", i),
		})
	}
	require.NoError(t, env.GetOLAPDBHandle().FlushErrorOccurrences(ctx, boundedRows))
	require.NoError(t, env.GetOLAPDBHandle().FlushErrorInvocationACL(ctx, &schema.ErrorInvocationACL{
		GroupID: "GR_ERROR_1", InvocationID: boundedInvocationID, UserID: "US_ERROR_1", Perms: perms.GROUP_READ,
		ACLVersion: error_tracking.CommittedACLVersion(0), UpdatedAtUsec: nowUsec,
	}))
	boundedDetail, err := error_tracking.GetErrorGroups(ctx, env, &etpb.GetErrorGroupsRequest{
		StartTimeUsec: nowUsec - time.Minute.Microseconds(), EndTimeUsec: nowUsec + 1,
		Fingerprint: boundedFingerprint, PageSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, boundedDetail.GetGroups()[0].GetOccurrences(), error_tracking.MaxOccurrencesPerInvocation)
	messages := make(map[string]struct{}, error_tracking.MaxOccurrencesPerInvocation)
	for _, occurrence := range boundedDetail.GetGroups()[0].GetOccurrences() {
		messages[occurrence.GetMessage()] = struct{}{}
	}
	require.NotContains(t, messages, "context-1")
	require.Contains(t, messages, fmt.Sprintf("context-%d", error_tracking.MaxOccurrencesPerInvocation+1))
}
