package test_buddy_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	testbuddy "github.com/buildbuddy-io/buildbuddy/server/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
)

type getTestsStream struct {
	tbpb.TestBuddyService_GetTestsServer
	ctx       context.Context
	responses []*tbpb.GetTestsResponse
}

func (s *getTestsStream) Context() context.Context {
	return s.ctx
}

func (s *getTestsStream) Send(response *tbpb.GetTestsResponse) error {
	s.responses = append(s.responses, proto.Clone(response).(*tbpb.GetTestsResponse))
	return nil
}

type getTestTargetsStream struct {
	tbpb.TestBuddyService_GetTestTargetsServer
	ctx       context.Context
	responses []*tbpb.GetTestTargetsResponse
}

type getTestsToSkipStream struct {
	tbpb.TestBuddyService_GetTestsToSkipServer
	ctx       context.Context
	responses []*tbpb.GetTestsToSkipResponse
}

func (s *getTestsToSkipStream) Context() context.Context {
	return s.ctx
}

func (s *getTestsToSkipStream) Send(response *tbpb.GetTestsToSkipResponse) error {
	s.responses = append(s.responses, proto.Clone(response).(*tbpb.GetTestsToSkipResponse))
	return nil
}

func (s *getTestTargetsStream) Context() context.Context {
	return s.ctx
}

func (s *getTestTargetsStream) Send(response *tbpb.GetTestTargetsResponse) error {
	s.responses = append(s.responses, proto.Clone(response).(*tbpb.GetTestTargetsResponse))
	return nil
}

type reportTestResultsStream struct {
	tbpb.TestBuddyService_ReportTestResultsServer
	ctx      context.Context
	requests []*tbpb.ReportTestResultsRequest
	response *tbpb.ReportTestResultsResponse
}

func (s *reportTestResultsStream) Context() context.Context { return s.ctx }

func (s *reportTestResultsStream) Recv() (*tbpb.ReportTestResultsRequest, error) {
	if len(s.requests) == 0 {
		return nil, io.EOF
	}
	req := s.requests[0]
	s.requests = s.requests[1:]
	return req, nil
}

func (s *reportTestResultsStream) SendAndClose(response *tbpb.ReportTestResultsResponse) error {
	s.response = response
	return nil
}

func reportTestResults(
	service *testbuddy.Service,
	ctx context.Context,
	requests ...*tbpb.ReportTestResultsRequest,
) (*tbpb.ReportTestResultsResponse, error) {
	stream := &reportTestResultsStream{ctx: ctx, requests: requests}
	err := service.ReportTestResults(stream)
	return stream.response, err
}

func getTests(t *testing.T, service *testbuddy.Service, ctx context.Context, req *tbpb.GetTestsRequest) []*tbpb.TestCaseSummary {
	t.Helper()
	stream := &getTestsStream{ctx: ctx}
	require.NoError(t, service.GetTests(req, stream))
	tests := make([]*tbpb.TestCaseSummary, 0)
	for _, response := range stream.responses {
		tests = append(tests, response.GetTests()...)
	}
	return tests
}

func caseResult(run, target, name string, outcome tbpb.TestOutcome, durationUsec int64) *tbpb.TestCaseResult {
	return &tbpb.TestCaseResult{
		Identity: &tbpb.TestCaseIdentity{
			Target: &tbpb.TestTargetIdentity{TargetLabel: target}, CaseName: name,
		},
		Result: &tbpb.TestResult{
			Outcome: outcome, DurationUsec: durationUsec,
			SourceUrl:     "https://app.buildbuddy.io/invocation/" + run,
			EventTimeUsec: 1_000_000, ResultId: run,
		},
	}
}

func targetResult(run, target string, outcome tbpb.TestOutcome, durationUsec int64) *tbpb.TestTargetResult {
	return &tbpb.TestTargetResult{
		Identity: &tbpb.TestTargetIdentity{TargetLabel: target},
		Result: &tbpb.TestResult{
			Outcome: outcome, DurationUsec: durationUsec,
			SourceUrl:     "https://app.buildbuddy.io/invocation/" + run,
			EventTimeUsec: 1_000_000, ResultId: run,
		},
	}
}

func getTestTargets(t *testing.T, service *testbuddy.Service, ctx context.Context, req *tbpb.GetTestTargetsRequest) []*tbpb.TestTargetSummary {
	t.Helper()
	stream := &getTestTargetsStream{ctx: ctx}
	require.NoError(t, service.GetTestTargets(req, stream))
	targets := make([]*tbpb.TestTargetSummary, 0)
	for _, response := range stream.responses {
		targets = append(targets, response.GetTargets()...)
	}
	return targets
}

func getTestsToSkip(t *testing.T, service *testbuddy.Service, ctx context.Context, req *tbpb.GetTestsToSkipRequest) ([]*tbpb.TestTargetSummary, []*tbpb.TestCaseSummary) {
	t.Helper()
	stream := &getTestsToSkipStream{ctx: ctx}
	require.NoError(t, service.GetTestsToSkip(req, stream))
	var targets []*tbpb.TestTargetSummary
	var testCases []*tbpb.TestCaseSummary
	for _, response := range stream.responses {
		targets = append(targets, response.GetTargets()...)
		testCases = append(testCases, response.GetTestCases()...)
	}
	return targets, testCases
}

func TestReportAndQueryTests(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	service := testbuddy.New(env)
	repository := "https://github.com/acme/repo"

	report := func(invocationID, target, name string, outcome tbpb.TestOutcome) *tbpb.ReportTestResultsResponse {
		rsp, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl:   repository,
			TestCases: []*tbpb.TestCaseResult{caseResult(invocationID, target, name, outcome, 1_000_000)},
		})
		require.NoError(t, err)
		return rsp
	}

	report("z-last-by-name", "//a/b:unit_test", "TestRequest", tbpb.TestOutcome_TEST_OUTCOME_PASS)
	report("a-first-by-name", "//a/b:unit_test", "TestRequest", tbpb.TestOutcome_TEST_OUTCOME_FAIL)
	report("m-middle-by-name", "//a/b:unit_test", "TestRequest", tbpb.TestOutcome_TEST_OUTCOME_FAIL)
	report("m-middle-by-name", "//a/b:unit_test", "TestRequest", tbpb.TestOutcome_TEST_OUTCOME_FAIL)
	report("sibling", "//a/b2:unit_test", "TestSibling", tbpb.TestOutcome_TEST_OUTCOME_FAIL)

	// Admission stores each subject's own package path, and that column is the
	// cone index. There is no routing row to keep in step with the catalog.
	var targetRow tables.TestTarget
	require.NoError(t, env.GetDBHandle().GORM(ctx, "test_buddy_test_target_catalog").
		Where("repository = ? AND target_label = ?", repository, "//a/b:unit_test").Take(&targetRow).Error)
	require.Equal(t, "a/b", targetRow.PackagePath)
	var caseRow tables.TestCase
	require.NoError(t, env.GetDBHandle().GORM(ctx, "test_buddy_test_case_catalog").
		Where("repository = ? AND target_label = ? AND case_name = ?", repository, "//a/b:unit_test", "TestRequest").
		Take(&caseRow).Error)
	require.Equal(t, targetRow.PackagePath, caseRow.PackagePath)

	tests := getTests(t, service, ctx, &tbpb.GetTestsRequest{
		RepoUrl: repository, PackagePrefix: "a/b",
	})
	require.Len(t, tests, 1)
	got := tests[0]
	require.Equal(t, "//a/b:unit_test", got.GetIdentity().GetTarget().GetTargetLabel())
	require.Equal(t, "TestRequest", got.GetIdentity().GetCaseName())
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_FLAKY, got.GetSummary().GetHealth())
	require.Equal(t, int64(1), got.GetSummary().GetPassCount())
	require.Equal(t, int64(2), got.GetSummary().GetFailCount())
	require.Equal(t, int64(0), got.GetSummary().GetTimeoutCount())
	require.Equal(t, int64(1_000_000), got.GetSummary().GetMeanDurationUsec())
	require.InDelta(t, 1.0/3.0, got.GetSummary().GetPassRate(), 0.0001)

	detail, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
		RepoUrl: repository, Identity: got.GetIdentity(),
	})
	require.NoError(t, err)
	require.Equal(t, got, detail.GetTest())
	require.Len(t, detail.GetRecentResults(), 3)
	require.Equal(t, "https://app.buildbuddy.io/invocation/m-middle-by-name", detail.GetRecentResults()[0].GetSourceUrl())
	require.Equal(t, int64(1_000_000), detail.GetRecentResults()[0].GetEventTimeUsec())
	require.Equal(t, "m-middle-by-name", detail.GetRecentResults()[0].GetResultId())
	require.NotEmpty(t, detail.GetTransitions())
}

func TestExecutionDispositionControlsTestsToSkip(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/repo"
	_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository,
		TestTargets: []*tbpb.TestTargetResult{
			targetResult("bad-target", "//pkg:bad", tbpb.TestOutcome_TEST_OUTCOME_FAIL, 10),
			targetResult("manual-target", "//pkg:manual", tbpb.TestOutcome_TEST_OUTCOME_PASS, 20),
			targetResult("outside-target", "//other:bad", tbpb.TestOutcome_TEST_OUTCOME_FAIL, 30),
		},
		TestCases: []*tbpb.TestCaseResult{
			caseResult("bad-case", "//pkg:bad", "TestBad", tbpb.TestOutcome_TEST_OUTCOME_FAIL, 10),
			caseResult("manual-case", "//pkg:manual", "TestManual", tbpb.TestOutcome_TEST_OUTCOME_PASS, 20),
			caseResult("outside-case", "//other:bad", "TestBad", tbpb.TestOutcome_TEST_OUTCOME_FAIL, 30),
		},
	})
	require.NoError(t, err)

	setTarget := func(target string, disposition tbpb.TestExecutionDisposition) {
		rsp, err := service.SetTestExecutionDisposition(ctx, &tbpb.SetTestExecutionDispositionRequest{
			RepoUrl: repository,
			Subject: &tbpb.SetTestExecutionDispositionRequest_Target{
				Target: &tbpb.TestTargetIdentity{TargetLabel: target},
			},
			Disposition: disposition,
		})
		require.NoError(t, err)
		require.Equal(t, disposition, rsp.GetDisposition())
	}
	setCase := func(target, name string, disposition tbpb.TestExecutionDisposition) {
		rsp, err := service.SetTestExecutionDisposition(ctx, &tbpb.SetTestExecutionDispositionRequest{
			RepoUrl: repository,
			Subject: &tbpb.SetTestExecutionDispositionRequest_TestCase{
				TestCase: &tbpb.TestCaseIdentity{
					Target: &tbpb.TestTargetIdentity{TargetLabel: target}, CaseName: name,
				},
			},
			Disposition: disposition,
		})
		require.NoError(t, err)
		require.Equal(t, disposition, rsp.GetDisposition())
	}

	setTarget("//pkg:bad", tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_ENABLED)
	setTarget("//pkg:manual", tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_DISABLED)
	setCase("//pkg:manual", "TestManual", tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_DISABLED)
	setCase("//pkg:manual", "TestManual", tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_DISABLED)

	targets, testCases := getTestsToSkip(t, service, ctx, &tbpb.GetTestsToSkipRequest{
		RepoUrl: repository, PackagePrefix: "pkg",
	})
	require.Len(t, targets, 1)
	require.Equal(t, "//pkg:manual", targets[0].GetIdentity().GetTargetLabel())
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, targets[0].GetSummary().GetHealth())
	require.Equal(t, tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_DISABLED, targets[0].GetDisposition())
	require.Len(t, testCases, 2)
	require.Equal(t, "TestBad", testCases[0].GetIdentity().GetCaseName())
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_FAILING, testCases[0].GetSummary().GetHealth())
	require.Equal(t, tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_AUTOMATIC, testCases[0].GetDisposition())
	require.Equal(t, "TestManual", testCases[1].GetIdentity().GetCaseName())
	require.Equal(t, tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_DISABLED, testCases[1].GetDisposition())

	targetDetail, err := service.GetTestTarget(ctx, &tbpb.GetTestTargetRequest{
		RepoUrl: repository, Identity: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:bad"},
	})
	require.NoError(t, err)
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_FAILING, targetDetail.GetTarget().GetSummary().GetHealth())
	require.Equal(t, tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_ENABLED, targetDetail.GetTarget().GetDisposition())
	caseDetail, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
		RepoUrl: repository,
		Identity: &tbpb.TestCaseIdentity{
			Target: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:manual"}, CaseName: "TestManual",
		},
	})
	require.NoError(t, err)
	require.Equal(t, tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_DISABLED, caseDetail.GetTest().GetDisposition())

	setCase("//pkg:bad", "TestBad", tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_ENABLED)
	setCase("//pkg:manual", "TestManual", tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_AUTOMATIC)
	targets, testCases = getTestsToSkip(t, service, ctx, &tbpb.GetTestsToSkipRequest{
		RepoUrl: repository, PackagePrefix: "pkg",
	})
	require.Len(t, targets, 1)
	require.Empty(t, testCases)
}

func TestRepositoriesAreGroupScopedAndOrderedByLatestReport(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	users := testauth.TestUsers("user1", "group1", "user2", "group2")
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, users))
	service := testbuddy.New(env)
	group1 := testauth.WithAuthenticatedUserInfo(ctx, users["user1"])
	group2 := testauth.WithAuthenticatedUserInfo(ctx, users["user2"])
	report := func(ctx context.Context, repository, run string) {
		_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestCases: []*tbpb.TestCaseResult{caseResult(
				run, "//pkg:test", "TestCase", tbpb.TestOutcome_TEST_OUTCOME_PASS, 1)},
		})
		require.NoError(t, err)
	}

	report(group1, "https://github.com/acme/older", "older-run")
	report(group1, "https://github.com/acme/newer", "newer-run")
	report(group2, "https://github.com/acme/private", "private-run")
	require.NoError(t, env.GetDBHandle().GORM(ctx, "test_buddy_set_repository_times").
		Model(&tables.TestRepositoryCatalog{}).
		Where("group_id = ? AND repository = ?", "group1", "https://github.com/acme/older").
		UpdateColumn("updated_at_usec", 1).Error)
	require.NoError(t, env.GetDBHandle().GORM(ctx, "test_buddy_set_repository_times").
		Model(&tables.TestRepositoryCatalog{}).
		Where("group_id = ? AND repository = ?", "group1", "https://github.com/acme/newer").
		UpdateColumn("updated_at_usec", 2).Error)

	got, err := service.GetTestRepositories(group1, &tbpb.GetTestRepositoriesRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{
		"https://github.com/acme/newer", "https://github.com/acme/older",
	}, []string{got.GetRepositories()[0].GetRepoUrl(), got.GetRepositories()[1].GetRepoUrl()})
	require.Equal(t, int64(2), got.GetRepositories()[0].GetLastReportedAtUsec())

	// Reporting the older repository again updates the catalog row rather than
	// adding another row, so it becomes the first choice in the selector.
	report(group1, "https://github.com/acme/older", "latest-run")
	got, err = service.GetTestRepositories(group1, &tbpb.GetTestRepositoriesRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{
		"https://github.com/acme/older", "https://github.com/acme/newer",
	}, []string{got.GetRepositories()[0].GetRepoUrl(), got.GetRepositories()[1].GetRepoUrl()})
	require.Len(t, got.GetRepositories(), 2)
}

func TestUnicodeCaseNameRoundTripsThroughStorage(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	service := testbuddy.New(env)
	repository := "https://github.com/acme/repo"
	caseName := "TestTruncateStringSlice/[ツ]/1"
	result := caseResult("unicode-run", "//pkg:unit_test", caseName,
		tbpb.TestOutcome_TEST_OUTCOME_FAIL, 100)

	_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository, TestCases: []*tbpb.TestCaseResult{result},
	})
	require.NoError(t, err)

	tests := getTests(t, service, ctx, &tbpb.GetTestsRequest{RepoUrl: repository})
	require.Len(t, tests, 1)
	require.Equal(t, caseName, tests[0].GetIdentity().GetCaseName())
	detail, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
		RepoUrl: repository, Identity: tests[0].GetIdentity(),
	})
	require.NoError(t, err)
	require.Equal(t, caseName, detail.GetTest().GetIdentity().GetCaseName())
	require.Len(t, detail.GetTransitions(), 1)

	var stored tables.TestCase
	require.NoError(t, env.GetDBHandle().GORM(ctx, "test_buddy_unicode_storage_key").
		Where("repository = ? AND target_label = ?", repository, "//pkg:unit_test").
		Take(&stored).Error)
	require.NotEqual(t, caseName, stored.CaseName)
	require.Equal(t, len(stored.CaseName), len([]byte(stored.CaseName)))
	decoded, err := identity.CaseNameFromKey(stored.CaseName)
	require.NoError(t, err)
	require.Equal(t, caseName, decoded)
}

func TestReportProcessesCasesIndependently(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	cases := make([]*tbpb.TestCaseResult, 32)
	for i := range cases {
		cases[i] = caseResult("one-report", "//a/b:unit_test", fmt.Sprintf("TestCase%d", i),
			tbpb.TestOutcome_TEST_OUTCOME_PASS, 0)
	}
	rsp, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: "https://github.com/acme/repo", TestCases: cases,
	})
	require.NoError(t, err)
	require.Equal(t, int32(len(cases)), rsp.GetAcceptedCount())

	got := getTests(t, service, ctx, &tbpb.GetTestsRequest{
		RepoUrl: "https://github.com/acme/repo", PackagePrefix: "a/b",
	})
	require.Len(t, got, len(cases))
}

func TestReportAggregatesStreamedBatches(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/repo"
	rsp, err := reportTestResults(service, ctx,
		&tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestCases: []*tbpb.TestCaseResult{
				caseResult("run-1", "//pkg:test", "TestCase", tbpb.TestOutcome_TEST_OUTCOME_PASS, 1),
			},
		},
		&tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestCases: []*tbpb.TestCaseResult{
				caseResult("run-2", "//pkg:test", "TestCase", tbpb.TestOutcome_TEST_OUTCOME_FAIL, 1),
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, int32(2), rsp.GetAcceptedCount())
	got, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
		RepoUrl: repository,
		Identity: &tbpb.TestCaseIdentity{
			Target: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"}, CaseName: "TestCase",
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), got.GetTest().GetSummary().GetPassCount())
	require.Equal(t, int64(1), got.GetTest().GetSummary().GetFailCount())
}

func TestReportDeduplicatesRetransmissions(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/repo"
	request := &tbpb.ReportTestResultsRequest{
		RepoUrl: repository,
		TestCases: []*tbpb.TestCaseResult{
			caseResult("case-result", "//pkg:test", "TestCase", tbpb.TestOutcome_TEST_OUTCOME_PASS, 10),
		},
		TestTargets: []*tbpb.TestTargetResult{
			targetResult("target-result", "//pkg:test", tbpb.TestOutcome_TEST_OUTCOME_PASS, 20),
		},
	}
	rsp, err := reportTestResults(service, ctx, request, proto.Clone(request).(*tbpb.ReportTestResultsRequest))
	require.NoError(t, err)
	require.Equal(t, int32(4), rsp.GetAcceptedCount())

	testCase, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
		RepoUrl: repository,
		Identity: &tbpb.TestCaseIdentity{
			Target: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"}, CaseName: "TestCase",
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), testCase.GetTest().GetSummary().GetPassCount())
	require.Len(t, testCase.GetRecentResults(), 1)
	require.Len(t, testCase.GetTransitions(), 1)

	target, err := service.GetTestTarget(ctx, &tbpb.GetTestTargetRequest{
		RepoUrl: repository, Identity: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), target.GetTarget().GetSummary().GetPassCount())
	require.Len(t, target.GetRecentResults(), 1)
	require.Len(t, target.GetTransitions(), 1)
}

func TestReportRejectsConflictingResultID(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/repo"
	first := caseResult("result-1", "//pkg:test", "TestCase", tbpb.TestOutcome_TEST_OUTCOME_PASS, 10)
	_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository, TestCases: []*tbpb.TestCaseResult{first},
	})
	require.NoError(t, err)
	conflict := proto.Clone(first).(*tbpb.TestCaseResult)
	conflict.Result.Outcome = tbpb.TestOutcome_TEST_OUTCOME_FAIL
	_, err = reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository, TestCases: []*tbpb.TestCaseResult{conflict},
	})
	require.True(t, status.IsFailedPreconditionError(err), err)

	got, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
		RepoUrl: repository,
		Identity: &tbpb.TestCaseIdentity{
			Target: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"}, CaseName: "TestCase",
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), got.GetTest().GetSummary().GetPassCount())
	require.Zero(t, got.GetTest().GetSummary().GetFailCount())
}

func TestResultIDDeduplicationIsBounded(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/repo"
	const resultCount = 201
	results := make([]*tbpb.TestCaseResult, resultCount)
	for i := range results {
		results[i] = caseResult(fmt.Sprintf("result-%03d", i), "//pkg:test", "TestCase",
			tbpb.TestOutcome_TEST_OUTCOME_PASS, 1)
	}
	_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository, TestCases: results,
	})
	require.NoError(t, err)
	_, err = reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository, TestCases: []*tbpb.TestCaseResult{proto.Clone(results[resultCount-1]).(*tbpb.TestCaseResult)},
	})
	require.NoError(t, err)
	_, err = reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository, TestCases: []*tbpb.TestCaseResult{proto.Clone(results[0]).(*tbpb.TestCaseResult)},
	})
	require.NoError(t, err)

	got, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
		RepoUrl: repository,
		Identity: &tbpb.TestCaseIdentity{
			Target: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"}, CaseName: "TestCase",
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(resultCount+1), got.GetTest().GetSummary().GetPassCount())
	require.Len(t, got.GetRecentResults(), 50)
}

func TestMaximumLengthAddress(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/repo"
	target := "//" + strings.Repeat("p", identity.MaxPackagePathBytes) + ":" +
		strings.Repeat("t", identity.MaxTargetNameBytes)
	caseName := strings.Repeat("c", identity.MaxCaseNameBytes)

	_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository,
		TestCases: []*tbpb.TestCaseResult{
			caseResult("long-address", target, caseName, tbpb.TestOutcome_TEST_OUTCOME_FAIL, 1),
		},
		TestTargets: []*tbpb.TestTargetResult{
			targetResult("long-address", target, tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT, 1),
		},
	})
	require.NoError(t, err)
	_, err = service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
		RepoUrl: repository,
		Identity: &tbpb.TestCaseIdentity{
			Target: &tbpb.TestTargetIdentity{TargetLabel: target}, CaseName: caseName,
		},
	})
	require.NoError(t, err)
}

func TestTargetStateIsIndependentFromCases(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/repo"
	target := "//a/b:unit_test"
	reportTarget := func(invocationID string, outcome tbpb.TestOutcome) {
		_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl:     repository,
			TestTargets: []*tbpb.TestTargetResult{targetResult(invocationID, target, outcome, 1_000_000)},
		})
		require.NoError(t, err)
	}
	reportCase := func(invocationID string, outcome tbpb.TestOutcome) {
		_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl:   repository,
			TestCases: []*tbpb.TestCaseResult{caseResult(invocationID, target, "TestCase", outcome, 0)},
		})
		require.NoError(t, err)
	}

	for i := 0; i < 4; i++ {
		reportTarget(fmt.Sprintf("target-timeout-%d", i), tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT)
	}
	targetDetail, err := service.GetTestTarget(ctx, &tbpb.GetTestTargetRequest{
		RepoUrl: repository, Identity: &tbpb.TestTargetIdentity{TargetLabel: target},
	})
	require.NoError(t, err)
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, targetDetail.GetTarget().GetSummary().GetHealth())
	require.Equal(t, int64(4), targetDetail.GetTarget().GetSummary().GetTimeoutCount())
	require.Empty(t, getTests(t, service, ctx, &tbpb.GetTestsRequest{
		RepoUrl: repository, Target: &tbpb.TestTargetIdentity{TargetLabel: target},
	}))

	reportCase("case-failure", tbpb.TestOutcome_TEST_OUTCOME_FAIL)
	targetDetail, err = service.GetTestTarget(ctx, &tbpb.GetTestTargetRequest{
		RepoUrl: repository, Identity: &tbpb.TestTargetIdentity{TargetLabel: target},
	})
	require.NoError(t, err)
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, targetDetail.GetTarget().GetSummary().GetHealth())
	targetCases := getTests(t, service, ctx, &tbpb.GetTestsRequest{
		RepoUrl: repository, Target: &tbpb.TestTargetIdentity{TargetLabel: target},
	})
	require.Len(t, targetCases, 1)
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_FAILING, targetCases[0].GetSummary().GetHealth())

	reportTarget("target-timeout-4", tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT)
	targetDetail, err = service.GetTestTarget(ctx, &tbpb.GetTestTargetRequest{
		RepoUrl: repository, Identity: &tbpb.TestTargetIdentity{TargetLabel: target},
	})
	require.NoError(t, err)
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_TIMEOUT, targetDetail.GetTarget().GetSummary().GetHealth())
	require.Equal(t, int64(5), targetDetail.GetTarget().GetSummary().GetTimeoutCount())
	require.Equal(t, int64(0), targetDetail.GetTarget().GetSummary().GetFailCount())
	require.Len(t, targetDetail.GetRecentResults(), 5)
	targetCases = getTests(t, service, ctx, &tbpb.GetTestsRequest{
		RepoUrl: repository, Target: &tbpb.TestTargetIdentity{TargetLabel: target},
	})
	require.Len(t, targetCases, 1)
	require.Equal(t, int64(1), targetCases[0].GetSummary().GetFailCount())

	targets := getTestTargets(t, service, ctx, &tbpb.GetTestTargetsRequest{
		RepoUrl: repository, PackagePrefix: "a/b",
	})
	require.Len(t, targets, 1)
	require.Equal(t, target, targets[0].GetIdentity().GetTargetLabel())
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_TIMEOUT, targets[0].GetSummary().GetHealth())

	unattributedTarget := "//a/b:harness_test"
	target = unattributedTarget
	reportTarget("target-failure", tbpb.TestOutcome_TEST_OUTCOME_FAIL)
	unattributed, err := service.GetTestTarget(ctx, &tbpb.GetTestTargetRequest{
		RepoUrl: repository, Identity: &tbpb.TestTargetIdentity{TargetLabel: unattributedTarget},
	})
	require.NoError(t, err)
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_FAILING, unattributed.GetTarget().GetSummary().GetHealth())
	require.Equal(t, int64(1), unattributed.GetTarget().GetSummary().GetFailCount())
	require.Empty(t, getTests(t, service, ctx, &tbpb.GetTestsRequest{
		RepoUrl: repository, Target: &tbpb.TestTargetIdentity{TargetLabel: unattributedTarget},
	}))

	repositoryHealth, err := service.GetRepositoryHealth(ctx, &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.NoError(t, err)
	require.Equal(t, int64(1), repositoryHealth.GetTargets().GetTimedOutCount())
	require.Equal(t, int64(1), repositoryHealth.GetTargets().GetFailingCount())
}

func TestAnalyzerConfigIsPerRepository(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	configuredRepository := "https://github.com/acme/configured"
	defaultRepository := "https://github.com/acme/default"

	defaultConfig, err := service.GetTestAnalyzerConfig(ctx, &tbpb.GetTestAnalyzerConfigRequest{
		RepoUrl: configuredRepository,
	})
	require.NoError(t, err)
	require.Equal(t, int32(50), defaultConfig.GetConfig().GetLinear().GetWindowSize())
	require.Equal(t, int32(1), defaultConfig.GetConfig().GetLinear().GetFailureThreshold())
	require.Equal(t, int32(5), defaultConfig.GetConfig().GetLinear().GetTargetTimeoutThreshold())
	require.Positive(t, defaultConfig.GetRevision())

	setConfig, err := service.SetTestAnalyzerConfig(ctx, &tbpb.SetTestAnalyzerConfigRequest{
		RepoUrl: configuredRepository,
		Config: &tbpb.TestAnalyzerConfig{Analyzer: &tbpb.TestAnalyzerConfig_Linear{Linear: &tbpb.LinearAnalyzer{
			WindowSize: 100, FailureThreshold: 2, TargetTimeoutThreshold: 10,
		}}},
	})
	require.NoError(t, err)
	configured, err := service.GetTestAnalyzerConfig(ctx, &tbpb.GetTestAnalyzerConfigRequest{
		RepoUrl: configuredRepository,
	})
	require.NoError(t, err)
	require.Equal(t, int32(100), configured.GetConfig().GetLinear().GetWindowSize())
	require.Equal(t, int32(2), configured.GetConfig().GetLinear().GetFailureThreshold())
	require.Equal(t, int32(10), configured.GetConfig().GetLinear().GetTargetTimeoutThreshold())
	require.Equal(t, setConfig.GetRevision(), configured.GetRevision())
	require.NotEqual(t, defaultConfig.GetRevision(), configured.GetRevision())

	reportFailure := func(repository string) {
		_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestCases: []*tbpb.TestCaseResult{caseResult(
				"failure", "//a/b:unit_test", "TestCase", tbpb.TestOutcome_TEST_OUTCOME_FAIL, 0)},
		})
		require.NoError(t, err)
	}
	getDetail := func(repository string) *tbpb.GetTestCaseResponse {
		rsp, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
			RepoUrl: repository,
			Identity: &tbpb.TestCaseIdentity{
				Target:   &tbpb.TestTargetIdentity{TargetLabel: "//a/b:unit_test"},
				CaseName: "TestCase",
			},
		})
		require.NoError(t, err)
		return rsp
	}

	reportFailure(configuredRepository)
	reportFailure(defaultRepository)
	configuredDetail := getDetail(configuredRepository)
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, configuredDetail.GetTest().GetSummary().GetHealth())
	require.Equal(t, configured.GetRevision(), configuredDetail.GetAnalyzerRevision())
	defaultDetail := getDetail(defaultRepository)
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_FAILING, defaultDetail.GetTest().GetSummary().GetHealth())
	require.Equal(t, defaultConfig.GetRevision(), defaultDetail.GetAnalyzerRevision())
}

func TestAnalyzerProvenance(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/provenance"
	configured, err := service.SetTestAnalyzerConfig(ctx, &tbpb.SetTestAnalyzerConfigRequest{
		RepoUrl: repository,
		Config: &tbpb.TestAnalyzerConfig{Analyzer: &tbpb.TestAnalyzerConfig_Linear{Linear: &tbpb.LinearAnalyzer{
			WindowSize: 50, FailureThreshold: 1, TargetTimeoutThreshold: 5,
		}}},
	})
	require.NoError(t, err)
	revision := configured.GetRevision()

	reportCase := func(run, name string, outcome tbpb.TestOutcome) {
		_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestCases: []*tbpb.TestCaseResult{
				caseResult(run, "//pkg:test", name, outcome, 1),
			},
		})
		require.NoError(t, err)
	}
	caseDetail := func(name string) *tbpb.GetTestCaseResponse {
		rsp, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
			RepoUrl: repository,
			Identity: &tbpb.TestCaseIdentity{
				Target: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"}, CaseName: name,
			},
		})
		require.NoError(t, err)
		return rsp
	}
	assertProvenance := func(reason string, eligible int64, revision int64, transition *tbpb.TestHealthTransition) {
		require.Equal(t, reason, transition.GetAnalysisReason())
		require.Equal(t, eligible, transition.GetEligibleSampleCount())
		require.Equal(t, revision, transition.GetAnalyzerRevision())
	}

	reportCase("mixed-fail", "TestMixed", tbpb.TestOutcome_TEST_OUTCOME_FAIL)
	mixed := caseDetail("TestMixed")
	require.Equal(t, "all_failures", mixed.GetAnalysisReason())
	require.Equal(t, int64(1), mixed.GetEligibleSampleCount())
	assertProvenance("all_failures", 1, revision, mixed.GetTransitions()[0])
	reportCase("mixed-pass", "TestMixed", tbpb.TestOutcome_TEST_OUTCOME_PASS)
	mixed = caseDetail("TestMixed")
	require.Equal(t, "failures_in_window", mixed.GetAnalysisReason())
	require.Equal(t, int64(2), mixed.GetEligibleSampleCount())
	assertProvenance("failures_in_window", 2, revision, mixed.GetTransitions()[0])

	reportCase("healthy-1", "TestHealthy", tbpb.TestOutcome_TEST_OUTCOME_PASS)
	reportCase("healthy-2", "TestHealthy", tbpb.TestOutcome_TEST_OUTCOME_PASS)
	healthy := caseDetail("TestHealthy")
	require.Equal(t, "uncertain", healthy.GetAnalysisReason())
	require.Equal(t, int64(2), healthy.GetEligibleSampleCount())
	require.Len(t, healthy.GetTransitions(), 1)
	assertProvenance("uncertain", 1, revision, healthy.GetTransitions()[0])
	reportCase("healthy-3", "TestHealthy", tbpb.TestOutcome_TEST_OUTCOME_PASS)
	healthy = caseDetail("TestHealthy")
	require.Equal(t, "consecutive_passes", healthy.GetAnalysisReason())
	require.Equal(t, int64(3), healthy.GetEligibleSampleCount())
	assertProvenance("consecutive_passes", 3, revision, healthy.GetTransitions()[0])

	timeouts := make([]*tbpb.TestTargetResult, 5)
	for i := range timeouts {
		timeouts[i] = targetResult(
			fmt.Sprintf("timeout-%d", i), "//pkg:timeout_test", tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT, 1)
	}
	_, err = reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository, TestTargets: timeouts,
	})
	require.NoError(t, err)
	target, err := service.GetTestTarget(ctx, &tbpb.GetTestTargetRequest{
		RepoUrl: repository, Identity: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:timeout_test"},
	})
	require.NoError(t, err)
	require.Equal(t, "timeouts_in_window", target.GetAnalysisReason())
	require.Equal(t, int64(5), target.GetEligibleSampleCount())
	assertProvenance("timeouts_in_window", 5, revision, target.GetTransitions()[0])
}

func TestGetRepositoryHealth(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	service := testbuddy.New(env)
	repository := "https://github.com/acme/repo"

	report := func(invocationID, name string, outcome tbpb.TestOutcome, durationUsec int64) {
		_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestCases: []*tbpb.TestCaseResult{caseResult(
				invocationID, "//a/b:unit_test", name, outcome, durationUsec)},
		})
		require.NoError(t, err)
	}

	for _, invocationID := range []string{"run-1", "run-2", "run-3"} {
		report(invocationID, "TestHealthy", tbpb.TestOutcome_TEST_OUTCOME_PASS, 2_000_000)
	}
	report("run-1", "TestFlaky", tbpb.TestOutcome_TEST_OUTCOME_FAIL, 1_000_000)
	report("run-2", "TestFlaky", tbpb.TestOutcome_TEST_OUTCOME_PASS, 1_000_000)
	report("run-1", "TestNew", tbpb.TestOutcome_TEST_OUTCOME_PASS, 3_000_000)
	// Cataloged but never analyzed, so it has no serving state at all.
	report("run-1", "TestNeverAnalyzed", tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN, 0)

	rsp, err := service.GetRepositoryHealth(ctx, &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.NoError(t, err)
	require.Equal(t, int64(1), rsp.GetTargets().GetTotalCount())
	require.Equal(t, int64(1), rsp.GetTargets().GetUnknownCount())
	require.Equal(t, int64(4), rsp.GetCases().GetTotalCount())
	require.Equal(t, int64(1), rsp.GetCases().GetHealthyCount())
	require.Equal(t, int64(1), rsp.GetCases().GetFlakyCount())
	require.Equal(t, int64(1), rsp.GetCases().GetInsufficientDataCount())
	require.Equal(t, int64(1), rsp.GetCases().GetUnknownCount())
	require.Equal(t, int64(5), rsp.GetCases().GetPassCount())
	require.Equal(t, int64(1), rsp.GetCases().GetFailCount())
	require.Equal(t, int64(0), rsp.GetCases().GetTimeoutCount())
	require.InDelta(t, 5.0/6.0, rsp.GetCases().GetPassRate(), 0.0001)
	require.Equal(t, int64(11_000_000/6), rsp.GetCases().GetMeanDurationUsec())

	report("run-1", "TestAfterCache", tbpb.TestOutcome_TEST_OUTCOME_PASS, 1_000_000)
	cached, err := service.GetRepositoryHealth(ctx, &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.NoError(t, err)
	require.Equal(t, int64(4), cached.GetCases().GetTotalCount())
	uncached, err := testbuddy.New(env).GetRepositoryHealth(
		ctx, &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.NoError(t, err)
	require.Equal(t, int64(5), uncached.GetCases().GetTotalCount())

	_, err = service.GetRepositoryHealth(ctx, &tbpb.GetRepositoryHealthRequest{
		RepoUrl: "https://github.com/acme/unreported",
	})
	require.True(t, status.IsNotFoundError(err), "expected NotFound, got %v", err)
}

func TestConeOrdersHealthBySeverity(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/ordered"
	report := func(target, name string, outcomes ...tbpb.TestOutcome) {
		for i, outcome := range outcomes {
			_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
				RepoUrl: repository,
				TestCases: []*tbpb.TestCaseResult{caseResult(
					fmt.Sprintf("case-%s-%d", name, i), target, name, outcome, 1_000)},
				TestTargets: []*tbpb.TestTargetResult{targetResult(
					fmt.Sprintf("target-%s-%d", name, i), target, outcome, 1_000)},
			})
			require.NoError(t, err)
		}
	}
	report("//pkg:failing", "TestFailing", tbpb.TestOutcome_TEST_OUTCOME_FAIL)
	report("//pkg:flaky", "TestFlaky",
		tbpb.TestOutcome_TEST_OUTCOME_PASS, tbpb.TestOutcome_TEST_OUTCOME_FAIL)
	for i, outcome := range []tbpb.TestOutcome{
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
	} {
		_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestCases: []*tbpb.TestCaseResult{caseResult(
				fmt.Sprintf("case-only-%d", i), "//pkg:case_flaky", "TestCaseFlaky", outcome, 1_000)},
		})
		require.NoError(t, err)
	}
	report("//pkg:healthy", "TestHealthy",
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_PASS)
	for i := 0; i < 5; i++ {
		_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestTargets: []*tbpb.TestTargetResult{targetResult(
				fmt.Sprintf("target-timeout-%d", i), "//pkg:timeout", tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT, 1_000)},
		})
		require.NoError(t, err)
	}

	cases := getTests(t, service, ctx, &tbpb.GetTestsRequest{RepoUrl: repository})
	require.Len(t, cases, 4)
	require.Equal(t, []tbpb.TestHealth{
		tbpb.TestHealth_TEST_HEALTH_FAILING,
		tbpb.TestHealth_TEST_HEALTH_FLAKY,
		tbpb.TestHealth_TEST_HEALTH_FLAKY,
		tbpb.TestHealth_TEST_HEALTH_HEALTHY,
	}, []tbpb.TestHealth{
		cases[0].GetSummary().GetHealth(),
		cases[1].GetSummary().GetHealth(),
		cases[2].GetSummary().GetHealth(),
		cases[3].GetSummary().GetHealth(),
	})
	targets := getTestTargets(t, service, ctx, &tbpb.GetTestTargetsRequest{RepoUrl: repository})
	require.Len(t, targets, 5)
	require.Equal(t, []string{
		"//pkg:failing",
		"//pkg:flaky",
		"//pkg:case_flaky",
		"//pkg:timeout",
		"//pkg:healthy",
	}, []string{
		targets[0].GetIdentity().GetTargetLabel(),
		targets[1].GetIdentity().GetTargetLabel(),
		targets[2].GetIdentity().GetTargetLabel(),
		targets[3].GetIdentity().GetTargetLabel(),
		targets[4].GetIdentity().GetTargetLabel(),
	})
	require.Equal(t, []tbpb.TestHealth{
		tbpb.TestHealth_TEST_HEALTH_FAILING,
		tbpb.TestHealth_TEST_HEALTH_FLAKY,
		tbpb.TestHealth_TEST_HEALTH_UNKNOWN,
		tbpb.TestHealth_TEST_HEALTH_TIMEOUT,
		tbpb.TestHealth_TEST_HEALTH_HEALTHY,
	}, []tbpb.TestHealth{
		targets[0].GetSummary().GetHealth(),
		targets[1].GetSummary().GetHealth(),
		targets[2].GetSummary().GetHealth(),
		targets[3].GetSummary().GetHealth(),
		targets[4].GetSummary().GetHealth(),
	})
	require.Equal(t, int64(1), targets[2].GetCases().GetTotalCount())
	require.Equal(t, int64(1), targets[2].GetCases().GetFlakyCount())
}

func TestConeBoundsFollowPackageComponents(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/repo"

	// These packages are chosen so that matching raw characters and matching
	// whole path components disagree: "ab" starts with the characters of "a"
	// and "a/bc" starts with those of "a/b", but neither is inside that cone.
	packages := []string{"", "a", "ab", "a/b", "a/b2", "a/bc", "a/b/c"}
	results := make([]*tbpb.TestCaseResult, 0, len(packages))
	for _, pkg := range packages {
		results = append(results, caseResult("one-run", "//"+pkg+":test", "TestCase",
			tbpb.TestOutcome_TEST_OUTCOME_PASS, 1_000))
	}
	_, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository, TestCases: results,
	})
	require.NoError(t, err)

	for _, test := range []struct {
		prefix string
		want   []string
	}{
		{"", []string{"//:test", "//a:test", "//ab:test", "//a/b:test", "//a/b2:test", "//a/bc:test", "//a/b/c:test"}},
		{"a", []string{"//a:test", "//a/b:test", "//a/b2:test", "//a/bc:test", "//a/b/c:test"}},
		{"a/b", []string{"//a/b:test", "//a/b/c:test"}},
		{"a/b/c", []string{"//a/b/c:test"}},
	} {
		t.Run("cone="+test.prefix, func(t *testing.T) {
			cases := getTests(t, service, ctx, &tbpb.GetTestsRequest{
				RepoUrl: repository, PackagePrefix: test.prefix,
			})
			labels := make([]string, 0, len(cases))
			for _, c := range cases {
				labels = append(labels, c.GetIdentity().GetTarget().GetTargetLabel())
			}
			require.ElementsMatch(t, test.want, labels)

			targets := getTestTargets(t, service, ctx, &tbpb.GetTestTargetsRequest{
				RepoUrl: repository, PackagePrefix: test.prefix,
			})
			targetLabels := make([]string, 0, len(targets))
			for _, target := range targets {
				targetLabels = append(targetLabels, target.GetIdentity().GetTargetLabel())
			}
			require.ElementsMatch(t, test.want, targetLabels)
		})
	}
}

func TestConeQueriesStreamAllResults(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/large"
	const caseCount = 1_050
	cases := make([]*tbpb.TestCaseResult, caseCount)
	failingCount := int64(0)
	for i := range cases {
		outcome := tbpb.TestOutcome_TEST_OUTCOME_PASS
		if i%100 == 0 {
			outcome = tbpb.TestOutcome_TEST_OUTCOME_FAIL
			failingCount++
		}
		cases[i] = caseResult("one-big-run", fmt.Sprintf("//pkg/sub%d:test", i),
			fmt.Sprintf("TestCase%04d", i), outcome, 1_000)
	}
	reported, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: repository, TestCases: cases,
	})
	require.NoError(t, err)
	require.Equal(t, int32(caseCount), reported.GetAcceptedCount())

	stream := &getTestsStream{ctx: ctx}
	require.NoError(t, service.GetTests(&tbpb.GetTestsRequest{RepoUrl: repository}, stream))
	require.Greater(t, len(stream.responses), 1)
	listed := make([]*tbpb.TestCaseSummary, 0, caseCount)
	for _, response := range stream.responses {
		listed = append(listed, response.GetTests()...)
	}
	require.Len(t, listed, caseCount)
	targetStream := &getTestTargetsStream{ctx: ctx}
	require.NoError(t, service.GetTestTargets(
		&tbpb.GetTestTargetsRequest{RepoUrl: repository}, targetStream))
	require.Greater(t, len(targetStream.responses), 1)
	listedTargets := make([]*tbpb.TestTargetSummary, 0, caseCount)
	for _, response := range targetStream.responses {
		listedTargets = append(listedTargets, response.GetTargets()...)
	}
	require.Len(t, listedTargets, caseCount)

	rsp, err := service.GetRepositoryHealth(ctx, &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.NoError(t, err)
	require.Equal(t, int64(caseCount), rsp.GetTargets().GetTotalCount())
	require.Equal(t, int64(caseCount), rsp.GetTargets().GetUnknownCount())
	require.Equal(t, int64(caseCount), rsp.GetCases().GetTotalCount())
	require.Equal(t, failingCount, rsp.GetCases().GetFailingCount())
	require.Equal(t, int64(caseCount)-failingCount, rsp.GetCases().GetInsufficientDataCount())
	require.Equal(t, int64(0), rsp.GetCases().GetHealthyCount())
	require.Equal(t, int64(0), rsp.GetCases().GetUnknownCount())
	require.Equal(t, int64(caseCount)-failingCount, rsp.GetCases().GetPassCount())
	require.Equal(t, failingCount, rsp.GetCases().GetFailCount())
	require.InDelta(t, float64(caseCount-11)/float64(caseCount), rsp.GetCases().GetPassRate(), 0.0001)
	require.Equal(t, int64(1_000), rsp.GetCases().GetMeanDurationUsec())
}

func TestBrowserTransportThroughAppProxy(t *testing.T) {
	ctx := context.Background()
	users := testauth.TestUsers("user1", "group1", "user2", "group2")
	repository := "https://github.com/acme/repo"

	// The TestBuddy service runs as its own process with its own database.
	backendEnv := testenv.GetTestEnv(t)
	backendEnv.SetAuthenticator(testauth.NewTestAuthenticator(t, users))
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	backendServer, runBackend := testenv.GRPCServer(backendEnv, listener)
	testbuddy.RegisterLocal(backendEnv, backendServer)
	go runBackend()
	t.Cleanup(backendServer.Stop)

	// The app proxies to it and exposes the authenticated browser transport.
	flags.Set(t, "test_buddy.backend", "grpc://"+listener.Addr().String())
	appEnv := testenv.GetTestEnv(t)
	appEnv.SetAuthenticator(testauth.NewTestAuthenticator(t, users))
	appGRPCServer, runApp, appConn := testenv.RegisterLocalGRPCServer(t, appEnv)
	require.NoError(t, testbuddy.Register(appEnv))
	tbpb.RegisterTestBuddyServiceServer(appGRPCServer, appEnv.GetTestBuddyServiceServer())
	go runApp()
	handlers, err := protolet.GenerateHTTPHandlers(
		"/rpc/TestBuddyService/", tbpb.TestBuddyService_ServiceDesc.ServiceName,
		appEnv.GetTestBuddyServiceServer(), appEnv.GetGRPCServer())
	require.NoError(t, err)
	handler := interceptors.WrapAuthenticatedExternalProtoletHandler(appEnv, "/rpc/TestBuddyService/", handlers)

	post := func(apiKey, method string, req proto.Message) *httptest.ResponseRecorder {
		body, err := protojson.Marshal(req)
		require.NoError(t, err)
		httpReq := httptest.NewRequest(http.MethodPost, "/rpc/TestBuddyService/"+method, bytes.NewReader(body))
		httpReq.Header.Set("Content-Type", "application/json")
		if apiKey != "" {
			httpReq.Header.Set(authutil.APIKeyHeader, apiKey)
		}
		rsp := httptest.NewRecorder()
		handler.ServeHTTP(rsp, httpReq)
		return rsp
	}

	grpcConn, err := testenv.LocalGRPCConn(ctx, appConn)
	require.NoError(t, err)
	grpcClient := tbpb.NewTestBuddyServiceClient(grpcConn)
	grpcCtx := metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, "user1")
	reportStream, err := grpcClient.ReportTestResults(grpcCtx)
	require.NoError(t, err)
	for _, invocationID := range []string{"run-1", "run-2", "run-3"} {
		require.NoError(t, reportStream.Send(&tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestCases: []*tbpb.TestCaseResult{caseResult(
				invocationID, "//a/b:unit_test", "TestHealthy", tbpb.TestOutcome_TEST_OUTCOME_PASS, 1_000_000)},
		}))
	}
	reported, err := reportStream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, int32(3), reported.GetAcceptedCount())

	healthRsp := post("user1", "GetRepositoryHealth", &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.Equal(t, http.StatusOK, healthRsp.Code, healthRsp.Body.String())
	health := &tbpb.GetRepositoryHealthResponse{}
	require.NoError(t, protojson.Unmarshal(healthRsp.Body.Bytes(), health))
	require.Equal(t, int64(1), health.GetCases().GetTotalCount())
	require.Equal(t, int64(1), health.GetCases().GetHealthyCount())
	require.Equal(t, int64(3), health.GetCases().GetPassCount())
	repositoriesRsp := post("user1", "GetTestRepositories", &tbpb.GetTestRepositoriesRequest{})
	require.Equal(t, http.StatusOK, repositoriesRsp.Code, repositoriesRsp.Body.String())
	repositories := &tbpb.GetTestRepositoriesResponse{}
	require.NoError(t, protojson.Unmarshal(repositoriesRsp.Body.Bytes(), repositories))
	require.Len(t, repositories.GetRepositories(), 1)
	require.Equal(t, repository, repositories.GetRepositories()[0].GetRepoUrl())

	// An unauthenticated browser request is refused before reaching the RPC.
	unauthenticated := post("", "GetRepositoryHealth", &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.Equal(t, http.StatusForbidden, unauthenticated.Code, unauthenticated.Body.String())

	// Another tenant sees nothing, and naming the first tenant's group in the
	// request context cannot cross organizations: scope stays the
	// authenticated group.
	otherTenant := post("user2", "GetRepositoryHealth", &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.NotEqual(t, http.StatusOK, otherTenant.Code)
	require.Contains(t, otherTenant.Body.String(), "was not found")
	tampered := post("user2", "GetRepositoryHealth", &tbpb.GetRepositoryHealthRequest{
		RepoUrl:        repository,
		RequestContext: &ctxpb.RequestContext{GroupId: "group1"},
	})
	require.NotEqual(t, http.StatusOK, tampered.Code)
	require.Contains(t, tampered.Body.String(), "was not found")

	// API-key metadata on the gRPC path reaches the same backend.
	grpcStream, err := grpcClient.GetTests(grpcCtx, &tbpb.GetTestsRequest{RepoUrl: repository})
	require.NoError(t, err)
	grpcRsp, err := grpcStream.Recv()
	require.NoError(t, err)
	require.Len(t, grpcRsp.GetTests(), 1)
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_HEALTHY, grpcRsp.GetTests()[0].GetSummary().GetHealth())
	_, err = grpcStream.Recv()
	require.ErrorIs(t, err, io.EOF)

	// The app has no serving path of its own: once the TestBuddy process is
	// stopped, the same authenticated read fails instead of being answered
	// from the app's database.
	backendServer.Stop()
	stopped := post("user1", "GetRepositoryHealth", &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.NotEqual(t, http.StatusOK, stopped.Code, stopped.Body.String())
}

func TestReportPreservesRepeatedCaseSamples(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	service := testbuddy.New(env)
	cases := make([]*tbpb.TestCaseResult, 100)
	for i := range cases {
		outcome := tbpb.TestOutcome_TEST_OUTCOME_PASS
		if i%10 == 0 {
			outcome = tbpb.TestOutcome_TEST_OUTCOME_FAIL
		}
		cases[i] = caseResult("repeated-runs", "//a/b:unit_test", "TestRepeated", outcome, 0)
		cases[i].Result.ResultId = fmt.Sprintf("repeated-run-%d", i)
	}
	rsp, err := reportTestResults(service, ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: "https://github.com/acme/repo", TestCases: cases,
	})
	require.NoError(t, err)
	require.Equal(t, int32(len(cases)), rsp.GetAcceptedCount())

	got, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
		RepoUrl: "https://github.com/acme/repo",
		Identity: &tbpb.TestCaseIdentity{
			Target:   &tbpb.TestTargetIdentity{TargetLabel: "//a/b:unit_test"},
			CaseName: "TestRepeated",
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(90), got.GetTest().GetSummary().GetPassCount())
	require.Equal(t, int64(10), got.GetTest().GetSummary().GetFailCount())
	require.Len(t, got.GetRecentResults(), 50)
	require.Len(t, got.GetTransitions(), 2)

	var catalogCount int64
	require.NoError(t, env.GetDBHandle().GORM(ctx, "test_buddy_test_case_count").
		Model(&tables.TestCase{}).
		Where("repository = ? AND target_label = ?", "https://github.com/acme/repo", "//a/b:unit_test").
		Count(&catalogCount).Error)
	require.Equal(t, int64(1), catalogCount)
	state := &tables.TestCaseState{}
	require.NoError(t, env.GetDBHandle().GORM(ctx, "test_buddy_test_case_state").
		Where("repository = ? AND target_label = ?", "https://github.com/acme/repo", "//a/b:unit_test").
		Take(state).Error)
	require.Equal(t, int64(100), state.StateVersion)
}
