package test_buddy_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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

func (s *getTestTargetsStream) Context() context.Context {
	return s.ctx
}

func (s *getTestTargetsStream) Send(response *tbpb.GetTestTargetsResponse) error {
	s.responses = append(s.responses, proto.Clone(response).(*tbpb.GetTestTargetsResponse))
	return nil
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
			SourceUrl: "https://app.buildbuddy.io/invocation/" + run,
		},
	}
}

func targetResult(run, target string, outcome tbpb.TestOutcome, durationUsec int64) *tbpb.TestTargetResult {
	return &tbpb.TestTargetResult{
		Identity: &tbpb.TestTargetIdentity{TargetLabel: target},
		Result: &tbpb.TestResult{
			Outcome: outcome, DurationUsec: durationUsec,
			SourceUrl: "https://app.buildbuddy.io/invocation/" + run,
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

func TestReportAndQueryTests(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	service := testbuddy.New(env)
	repository := "https://github.com/acme/repo"

	report := func(invocationID, target, name string, outcome tbpb.TestOutcome) *tbpb.ReportTestResultsResponse {
		rsp, err := service.ReportTestResults(ctx, &tbpb.ReportTestResultsRequest{
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

	var targetRow tables.TestTarget
	require.NoError(t, env.GetDBHandle().GORM(ctx, "test_buddy_test_target_bucket").
		Where("repository = ? AND target_label = ?", repository, "//a/b:unit_test").Take(&targetRow).Error)
	bucketID := identity.BucketForTarget(targetRow.GroupID, identity.TargetAddress{
		Repository: repository, TargetLabel: targetRow.TargetLabel,
	})
	for _, prefix := range []string{"", "a", "a/b"} {
		var count int64
		require.NoError(t, env.GetDBHandle().GORM(ctx, "test_buddy_test_cone_buckets").
			Model(&tables.TestTargetConeBucket{}).
			Where("repository = ? AND package_prefix = ? AND bucket_id = ?", repository, prefix, bucketID).
			Count(&count).Error)
		require.Equal(t, int64(1), count, prefix)
	}
	var caseRow tables.TestCase
	require.NoError(t, env.GetDBHandle().GORM(ctx, "test_buddy_test_case_bucket").
		Where("repository = ? AND target_label = ? AND case_name = ?", repository, "//a/b:unit_test", "TestRequest").
		Take(&caseRow).Error)
	require.Equal(t, bucketID, targetRow.BucketID)
	require.Equal(t, targetRow.BucketID, caseRow.BucketID)

	tests := getTests(t, service, ctx, &tbpb.GetTestsRequest{
		RepoUrl: repository, PackagePrefix: "a/b",
	})
	require.Len(t, tests, 1)
	got := tests[0]
	require.Equal(t, "//a/b:unit_test", got.GetIdentity().GetTarget().GetTargetLabel())
	require.Equal(t, "TestRequest", got.GetIdentity().GetCaseName())
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_FLAKY, got.GetSummary().GetHealth())
	require.Equal(t, int64(1), got.GetSummary().GetPassCount())
	require.Equal(t, int64(3), got.GetSummary().GetFailCount())
	require.Equal(t, int64(0), got.GetSummary().GetTimeoutCount())
	require.Equal(t, int64(1_000_000), got.GetSummary().GetMeanDurationUsec())
	require.InDelta(t, 0.25, got.GetSummary().GetPassRate(), 0.0001)

	detail, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
		RepoUrl: repository, Identity: got.GetIdentity(),
	})
	require.NoError(t, err)
	require.Equal(t, got, detail.GetTest())
	require.Len(t, detail.GetRecentResults(), 4)
	require.Equal(t, "https://app.buildbuddy.io/invocation/m-middle-by-name", detail.GetRecentResults()[0].GetSourceUrl())
	require.NotEmpty(t, detail.GetTransitions())
}

func TestReportProcessesCasesIndependently(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	cases := make([]*tbpb.TestCaseResult, 32)
	for i := range cases {
		cases[i] = caseResult("one-report", "//a/b:unit_test", fmt.Sprintf("TestCase%d", i),
			tbpb.TestOutcome_TEST_OUTCOME_PASS, 0)
	}
	rsp, err := service.ReportTestResults(ctx, &tbpb.ReportTestResultsRequest{
		RepoUrl: "https://github.com/acme/repo", TestCases: cases,
	})
	require.NoError(t, err)
	require.Equal(t, int32(len(cases)), rsp.GetAcceptedCount())

	got := getTests(t, service, ctx, &tbpb.GetTestsRequest{
		RepoUrl: "https://github.com/acme/repo", PackagePrefix: "a/b",
	})
	require.Len(t, got, len(cases))
}

func TestTargetStateIsIndependentFromCases(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/repo"
	target := "//a/b:unit_test"
	reportTarget := func(invocationID string, outcome tbpb.TestOutcome) {
		_, err := service.ReportTestResults(ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl:     repository,
			TestTargets: []*tbpb.TestTargetResult{targetResult(invocationID, target, outcome, 1_000_000)},
		})
		require.NoError(t, err)
	}
	reportCase := func(invocationID string, outcome tbpb.TestOutcome) {
		_, err := service.ReportTestResults(ctx, &tbpb.ReportTestResultsRequest{
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
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_FLAKY, targetCases[0].GetSummary().GetHealth())

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
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_FLAKY, unattributed.GetTarget().GetSummary().GetHealth())
	require.Equal(t, int64(1), unattributed.GetTarget().GetSummary().GetFailCount())
	require.Empty(t, getTests(t, service, ctx, &tbpb.GetTestsRequest{
		RepoUrl: repository, Target: &tbpb.TestTargetIdentity{TargetLabel: unattributedTarget},
	}))

	repositoryHealth, err := service.GetRepositoryHealth(ctx, &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.NoError(t, err)
	require.Equal(t, int64(1), repositoryHealth.GetTargets().GetTimedOutCount())
	require.Equal(t, int64(1), repositoryHealth.GetTargets().GetFlakyCount())
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

	_, err = service.SetTestAnalyzerConfig(ctx, &tbpb.SetTestAnalyzerConfigRequest{
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

	reportFailure := func(repository string) {
		_, err := service.ReportTestResults(ctx, &tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestCases: []*tbpb.TestCaseResult{caseResult(
				"failure", "//a/b:unit_test", "TestCase", tbpb.TestOutcome_TEST_OUTCOME_FAIL, 0)},
		})
		require.NoError(t, err)
	}
	getHealth := func(repository string) tbpb.TestHealth {
		rsp, err := service.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
			RepoUrl: repository,
			Identity: &tbpb.TestCaseIdentity{
				Target:   &tbpb.TestTargetIdentity{TargetLabel: "//a/b:unit_test"},
				CaseName: "TestCase",
			},
		})
		require.NoError(t, err)
		return rsp.GetTest().GetSummary().GetHealth()
	}

	reportFailure(configuredRepository)
	reportFailure(defaultRepository)
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, getHealth(configuredRepository))
	require.Equal(t, tbpb.TestHealth_TEST_HEALTH_FLAKY, getHealth(defaultRepository))
}

func TestGetRepositoryHealth(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	service := testbuddy.New(env)
	repository := "https://github.com/acme/repo"

	report := func(invocationID, name string, outcome tbpb.TestOutcome, durationUsec int64) {
		_, err := service.ReportTestResults(ctx, &tbpb.ReportTestResultsRequest{
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

func TestConeQueriesStreamAllResults(t *testing.T) {
	ctx := context.Background()
	service := testbuddy.New(testenv.GetTestEnv(t))
	repository := "https://github.com/acme/large"
	const caseCount = 1_050
	cases := make([]*tbpb.TestCaseResult, caseCount)
	flakyCount := int64(0)
	for i := range cases {
		outcome := tbpb.TestOutcome_TEST_OUTCOME_PASS
		if i%100 == 0 {
			outcome = tbpb.TestOutcome_TEST_OUTCOME_FAIL
			flakyCount++
		}
		cases[i] = caseResult("one-big-run", fmt.Sprintf("//pkg/sub%d:test", i),
			fmt.Sprintf("TestCase%04d", i), outcome, 1_000)
	}
	reported, err := service.ReportTestResults(ctx, &tbpb.ReportTestResultsRequest{
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
	require.Equal(t, flakyCount, rsp.GetCases().GetFlakyCount())
	require.Equal(t, int64(caseCount)-flakyCount, rsp.GetCases().GetInsufficientDataCount())
	require.Equal(t, int64(0), rsp.GetCases().GetHealthyCount())
	require.Equal(t, int64(0), rsp.GetCases().GetUnknownCount())
	require.Equal(t, int64(caseCount)-flakyCount, rsp.GetCases().GetPassCount())
	require.Equal(t, flakyCount, rsp.GetCases().GetFailCount())
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

	// Reporting and reading work over the authenticated browser transport.
	for _, invocationID := range []string{"run-1", "run-2", "run-3"} {
		rsp := post("user1", "ReportTestResults", &tbpb.ReportTestResultsRequest{
			RepoUrl: repository,
			TestCases: []*tbpb.TestCaseResult{caseResult(
				invocationID, "//a/b:unit_test", "TestHealthy", tbpb.TestOutcome_TEST_OUTCOME_PASS, 1_000_000)},
		})
		require.Equal(t, http.StatusOK, rsp.Code, rsp.Body.String())
	}
	healthRsp := post("user1", "GetRepositoryHealth", &tbpb.GetRepositoryHealthRequest{RepoUrl: repository})
	require.Equal(t, http.StatusOK, healthRsp.Code, healthRsp.Body.String())
	health := &tbpb.GetRepositoryHealthResponse{}
	require.NoError(t, protojson.Unmarshal(healthRsp.Body.Bytes(), health))
	require.Equal(t, int64(1), health.GetCases().GetTotalCount())
	require.Equal(t, int64(1), health.GetCases().GetHealthyCount())
	require.Equal(t, int64(3), health.GetCases().GetPassCount())

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

	// The gRPC path the CLI uses is unchanged: API-key metadata through the
	// app's gRPC server reaches the same backend.
	grpcConn, err := testenv.LocalGRPCConn(ctx, appConn)
	require.NoError(t, err)
	grpcClient := tbpb.NewTestBuddyServiceClient(grpcConn)
	grpcCtx := metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, "user1")
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
	service := testbuddy.New(testenv.GetTestEnv(t))
	cases := make([]*tbpb.TestCaseResult, 100)
	for i := range cases {
		outcome := tbpb.TestOutcome_TEST_OUTCOME_PASS
		if i%10 == 0 {
			outcome = tbpb.TestOutcome_TEST_OUTCOME_FAIL
		}
		cases[i] = caseResult("repeated-runs", "//a/b:unit_test", "TestRepeated", outcome, 0)
	}
	rsp, err := service.ReportTestResults(ctx, &tbpb.ReportTestResultsRequest{
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
}
