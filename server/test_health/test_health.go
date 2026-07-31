// Package test_health implements synchronous test result reporting and reads.
package test_health

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm/clause"

	thpb "github.com/buildbuddy-io/buildbuddy/proto/test_health"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/test_health/analyzer"
	"github.com/buildbuddy-io/buildbuddy/server/test_health/config"
	"github.com/buildbuddy-io/buildbuddy/server/test_health/identity"
	"github.com/buildbuddy-io/buildbuddy/server/test_health/normalize"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"golang.org/x/sync/errgroup"
)

const (
	queryBatchSize  = 500
	reportBatchSize = 500
)

var backendTarget = flag.String("test_buddy.backend", "", "Internal gRPC target for the TestBuddy service.")

type Service struct {
	thpb.UnimplementedTestBuddyServiceServer
	env                   environment.Env
	repositoryHealthCache *repositoryHealthCache
}

func New(env environment.Env) *Service {
	return &Service{
		env: env,
		repositoryHealthCache: &repositoryHealthCache{
			entries: make(map[repositoryHealthCacheKey]*repositoryHealthCacheEntry),
		},
	}
}

type repositoryHealthCacheKey struct {
	groupID    string
	repository string
}

type repositoryHealthCacheEntry struct {
	response  *thpb.GetRepositoryHealthResponse
	expiresAt time.Time
}

type repositoryHealthCache struct {
	mu      sync.Mutex
	entries map[repositoryHealthCacheKey]*repositoryHealthCacheEntry
	refresh singleflight.Group[repositoryHealthCacheKey, *thpb.GetRepositoryHealthResponse]
}

func (c *repositoryHealthCache) lookup(key repositoryHealthCacheKey, now time.Time) (*thpb.GetRepositoryHealthResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil || !now.Before(entry.expiresAt) {
		return nil, false
	}
	return proto.Clone(entry.response).(*thpb.GetRepositoryHealthResponse), true
}

func (c *repositoryHealthCache) store(key repositoryHealthCacheKey, response *thpb.GetRepositoryHealthResponse) {
	jitter := time.Duration(rand.Int64N(int64(time.Minute))) - 30*time.Second
	c.mu.Lock()
	c.entries[key] = &repositoryHealthCacheEntry{
		response:  proto.Clone(response).(*thpb.GetRepositoryHealthResponse),
		expiresAt: time.Now().Add(5*time.Minute + jitter),
	}
	c.mu.Unlock()
}

// Register builds the app's TestBuddy server — the local service, or a
// proxy to the configured backend — and stores it on the environment so both
// the gRPC servers and the browser proto-over-HTTP handlers serve the same
// instance.
func Register(env *real_environment.RealEnv) error {
	if *backendTarget == "" {
		env.SetTestBuddyServiceServer(New(env))
		return nil
	}
	conn, err := grpc_client.DialInternalWithoutPooling(env, *backendTarget)
	if err != nil {
		return err
	}
	env.GetHealthChecker().RegisterShutdownFunction(func(context.Context) error {
		return conn.Close()
	})
	env.SetTestBuddyServiceServer(&proxy{
		client:        thpb.NewTestBuddyServiceClient(conn),
		authenticator: env.GetAuthenticator(),
	})
	return nil
}

func RegisterLocal(env environment.Env, grpcServer *grpc.Server) {
	thpb.RegisterTestBuddyServiceServer(grpcServer, New(env))
}

type proxy struct {
	thpb.UnimplementedTestBuddyServiceServer
	client        thpb.TestBuddyServiceClient
	authenticator interfaces.Authenticator
}

func (p *proxy) ReportTestResults(ctx context.Context, req *thpb.ReportTestResultsRequest) (*thpb.ReportTestResultsResponse, error) {
	return p.client.ReportTestResults(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) GetTests(req *thpb.GetTestsRequest, stream thpb.TestBuddyService_GetTestsServer) error {
	client, err := p.client.GetTests(forwardAuth(stream.Context(), p.authenticator), req)
	if err != nil {
		return err
	}
	for {
		rsp, err := client.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(rsp); err != nil {
			return err
		}
	}
}

func (p *proxy) GetTestTargets(req *thpb.GetTestTargetsRequest, stream thpb.TestBuddyService_GetTestTargetsServer) error {
	client, err := p.client.GetTestTargets(forwardAuth(stream.Context(), p.authenticator), req)
	if err != nil {
		return err
	}
	for {
		rsp, err := client.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(rsp); err != nil {
			return err
		}
	}
}

func (p *proxy) GetTestCase(ctx context.Context, req *thpb.GetTestCaseRequest) (*thpb.GetTestCaseResponse, error) {
	return p.client.GetTestCase(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) GetTestTarget(ctx context.Context, req *thpb.GetTestTargetRequest) (*thpb.GetTestTargetResponse, error) {
	return p.client.GetTestTarget(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) GetRepositoryHealth(ctx context.Context, req *thpb.GetRepositoryHealthRequest) (*thpb.GetRepositoryHealthResponse, error) {
	return p.client.GetRepositoryHealth(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) GetTestAnalyzerConfig(ctx context.Context, req *thpb.GetTestAnalyzerConfigRequest) (*thpb.GetTestAnalyzerConfigResponse, error) {
	return p.client.GetTestAnalyzerConfig(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) SetTestAnalyzerConfig(ctx context.Context, req *thpb.SetTestAnalyzerConfigRequest) (*thpb.SetTestAnalyzerConfigResponse, error) {
	return p.client.SetTestAnalyzerConfig(forwardAuth(ctx, p.authenticator), req)
}

func forwardAuth(ctx context.Context, authenticator interfaces.Authenticator) context.Context {
	outgoing := metadata.MD{}
	incoming, _ := metadata.FromIncomingContext(ctx)
	for _, key := range []string{authutil.APIKeyHeader, authutil.ContextTokenStringKey} {
		if values := incoming.Get(key); len(values) > 0 {
			outgoing.Set(key, values[len(values)-1])
		}
	}
	if jwt := authenticator.TrustedJWTFromAuthContext(ctx); jwt != "" {
		outgoing.Set(authutil.ContextTokenStringKey, jwt)
	}
	return metadata.NewOutgoingContext(ctx, outgoing)
}

func (s *Service) ReportTestResults(ctx context.Context, req *thpb.ReportTestResultsRequest) (*thpb.ReportTestResultsResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentError("request is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	records := make([]normalize.CaseRecord, 0, len(req.GetTestCases()))
	for _, result := range req.GetTestCases() {
		records = append(records, normalize.CaseRecord{
			TargetLabel:    result.GetTargetLabel(),
			CaseName:       result.GetCaseName(),
			Outcome:        result.GetOutcome(),
			DurationUsec:   result.GetDurationUsec(),
			FailureMessage: result.GetFailureMessage(),
		})
	}
	targetRecords := make([]normalize.TargetRecord, 0, len(req.GetTestTargets()))
	for _, result := range req.GetTestTargets() {
		targetRecords = append(targetRecords, normalize.TargetRecord{
			TargetLabel: result.GetTargetLabel(), Outcome: result.GetOutcome(),
			DurationUsec:   result.GetDurationUsec(),
			FailureMessage: result.GetFailureMessage(),
		})
	}
	report, err := normalize.Normalize(normalize.ReportContext{
		RepositoryURL: req.GetRepoUrl(),
		InvocationID:  req.GetInvocationId(),
		Source:        req.GetSource(),
	}, records, targetRecords)
	if err != nil {
		return nil, err
	}
	analyzerConfig, err := s.analyzerConfig(ctx, groupID, report.Context.RepositoryURL)
	if err != nil {
		return nil, err
	}
	if err := admitCatalog(ctx, s.env.GetDBHandle(), groupID, report); err != nil {
		return nil, err
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(max(1, runtime.GOMAXPROCS(0)))
	resultsByCase := make(map[identity.CaseAddress][]*normalize.CaseResult)
	for _, result := range report.CaseResults {
		if analyzer.Eligible(analyzer.Sample{
			InvocationID: result.Result.GetInvocationId(),
			Outcome:      result.Result.GetOutcome(),
			Source:       result.Result.GetSource(),
		}) {
			resultsByCase[result.Identity.Address] = append(resultsByCase[result.Identity.Address], result)
		}
	}
	for _, results := range resultsByCase {
		group.Go(func() error {
			for _, result := range results {
				if err := s.applyCase(groupCtx, groupID, result, analyzerConfig); err != nil {
					return err
				}
			}
			return nil
		})
	}
	resultsByTarget := make(map[identity.TargetAddress][]*normalize.TargetResult)
	for _, result := range report.TargetResults {
		if analyzer.Eligible(analyzer.Sample{
			InvocationID: result.Result.GetInvocationId(),
			Outcome:      result.Result.GetOutcome(),
			Source:       result.Result.GetSource(),
		}) {
			resultsByTarget[result.Target.Address] = append(resultsByTarget[result.Target.Address], result)
		}
	}
	for _, results := range resultsByTarget {
		group.Go(func() error {
			for _, result := range results {
				if err := s.applyTarget(groupCtx, groupID, result, analyzerConfig); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return &thpb.ReportTestResultsResponse{
		AcceptedCount: int32(len(report.CaseResults) + len(report.TargetResults)),
		RejectedCount: int32(report.Rejected.Total()),
	}, nil
}

func admitCatalog(ctx context.Context, database interfaces.DB, groupID string, report *normalize.Report) error {
	repository := report.Context.RepositoryURL
	if err := database.GORM(ctx, "test_health_admit_repository").
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&tables.TestRepositoryCatalog{GroupID: groupID, Repository: repository}).Error; err != nil {
		return err
	}
	targets := make(map[identity.TargetAddress]*tables.TestTarget)
	for _, result := range report.CaseResults {
		target := result.Target
		targets[target.Address] = &tables.TestTarget{
			GroupID: groupID, Repository: repository, TargetLabel: target.Address.TargetLabel,
			PackagePath: target.Target.PackagePath,
		}
	}
	for _, result := range report.TargetResults {
		target := result.Target
		targets[target.Address] = &tables.TestTarget{
			GroupID: groupID, Repository: repository, TargetLabel: target.Address.TargetLabel,
			PackagePath: target.Target.PackagePath,
		}
	}
	targetRows := make([]*tables.TestTarget, 0, len(targets))
	for _, target := range targets {
		targetRows = append(targetRows, target)
	}
	for start := 0; start < len(targetRows); start += reportBatchSize {
		end := min(start+reportBatchSize, len(targetRows))
		if err := database.GORM(ctx, "test_health_admit_targets").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(targetRows[start:end]).Error; err != nil {
			return err
		}
	}
	for start := 0; start < len(report.CaseResults); start += reportBatchSize {
		end := min(start+reportBatchSize, len(report.CaseResults))
		cases := make([]*tables.TestCase, 0, end-start)
		for _, result := range report.CaseResults[start:end] {
			cases = append(cases, &tables.TestCase{
				GroupID: groupID, Repository: repository, TargetLabel: result.Identity.Address.TargetLabel,
				CaseName: result.Identity.Address.CaseName, PackagePath: result.Identity.Target.PackagePath,
			})
		}
		if err := database.GORM(ctx, "test_health_admit_cases").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(cases).Error; err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) applyCase(ctx context.Context, groupID string, result *normalize.CaseResult, analyzerConfig *thpb.TestAnalyzerConfig) error {
	return s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		emptyCheckpoint, err := proto.Marshal(&thpb.TestStateCheckpoint{})
		if err != nil {
			return err
		}
		address := result.Identity.Address
		if err := tx.GORM(ctx, "test_health_admit_state").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(&tables.TestCaseState{
				GroupID: groupID, Repository: address.Repository, TargetLabel: address.TargetLabel,
				CaseName: address.CaseName, Health: thpb.TestHealth_TEST_HEALTH_UNKNOWN.String(),
				Checkpoint: emptyCheckpoint,
			}).Error; err != nil {
			return err
		}
		state := &tables.TestCaseState{}
		query := `SELECT * FROM "TestCaseStates"
			WHERE group_id = ? AND repository = ? AND target_label = ? AND case_name = ?` +
			s.env.GetDBHandle().SelectForUpdateModifier()
		if err := tx.NewQuery(ctx, "test_health_lock_case_state").Raw(
			query, groupID, address.Repository, address.TargetLabel, address.CaseName).Take(state); err != nil {
			return err
		}
		checkpoint := &thpb.TestStateCheckpoint{}
		if err := proto.Unmarshal(state.Checkpoint, checkpoint); err != nil {
			return status.InternalErrorf("decode test state checkpoint: %s", err)
		}
		checkpoint.Samples = append(checkpoint.Samples, &thpb.CheckpointSample{
			InvocationId:   result.Result.GetInvocationId(),
			Outcome:        result.Result.GetOutcome(),
			Source:         result.Result.GetSource(),
			DurationUsec:   result.Result.GetDurationUsec(),
			FailureMessage: result.Result.GetFailureMessage(),
		})
		if extra := len(checkpoint.Samples) - int(analyzerConfig.GetWindowSize()); extra > 0 {
			checkpoint.Samples = checkpoint.Samples[extra:]
		}
		samples := make([]analyzer.Sample, 0, len(checkpoint.Samples))
		for _, sample := range checkpoint.Samples {
			samples = append(samples, analyzer.Sample{
				InvocationID: sample.GetInvocationId(), Outcome: sample.GetOutcome(), Source: sample.GetSource(),
			})
		}
		analysis, err := analyzer.Linear(samples, analyzerConfig)
		if err != nil {
			return err
		}
		previousHealth := state.Health
		state.Health = analysis.Health.String()
		state.StateVersion++
		switch result.Result.GetOutcome() {
		case thpb.TestOutcome_TEST_OUTCOME_PASS:
			state.PassCount++
		case thpb.TestOutcome_TEST_OUTCOME_FAIL:
			state.FailCount++
		case thpb.TestOutcome_TEST_OUTCOME_TIMEOUT:
			state.TimeoutCount++
		default:
			return status.InternalError("unknown outcome reached the analyzer")
		}
		state.TotalDurationUsec += result.Result.GetDurationUsec()
		state.Checkpoint, err = proto.Marshal(checkpoint)
		if err != nil {
			return err
		}
		if err := tx.GORM(ctx, "test_health_update_case_state").Save(state).Error; err != nil {
			return err
		}
		if previousHealth == state.Health {
			return nil
		}
		return tx.NewQuery(ctx, "test_health_create_case_change").Create(&tables.TestCaseStateChange{
			GroupID: groupID, Repository: address.Repository, TargetLabel: address.TargetLabel,
			CaseName: address.CaseName, StateVersion: state.StateVersion,
			PreviousHealth: previousHealth, Health: state.Health,
			PassCount: state.PassCount, FailCount: state.FailCount, TimeoutCount: state.TimeoutCount,
			EventTimeUsec: tx.NowFunc().UnixMicro(),
		})
	})
}

func (s *Service) applyTarget(ctx context.Context, groupID string, result *normalize.TargetResult, analyzerConfig *thpb.TestAnalyzerConfig) error {
	return s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		emptyCheckpoint, err := proto.Marshal(&thpb.TestStateCheckpoint{})
		if err != nil {
			return err
		}
		address := result.Target.Address
		if err := tx.GORM(ctx, "test_health_admit_target_state").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(&tables.TestTargetState{
				GroupID: groupID, Repository: address.Repository, TargetLabel: address.TargetLabel,
				Health: thpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), Checkpoint: emptyCheckpoint,
			}).Error; err != nil {
			return err
		}
		state := &tables.TestTargetState{}
		query := `SELECT * FROM "TestTargetStates"
			WHERE group_id = ? AND repository = ? AND target_label = ?` +
			s.env.GetDBHandle().SelectForUpdateModifier()
		if err := tx.NewQuery(ctx, "test_health_lock_target_state").Raw(
			query, groupID, address.Repository, address.TargetLabel).Take(state); err != nil {
			return err
		}
		checkpoint := &thpb.TestStateCheckpoint{}
		if err := proto.Unmarshal(state.Checkpoint, checkpoint); err != nil {
			return status.InternalErrorf("decode test target state checkpoint: %s", err)
		}
		checkpoint.Samples = append(checkpoint.Samples, &thpb.CheckpointSample{
			InvocationId:   result.Result.GetInvocationId(),
			Outcome:        result.Result.GetOutcome(),
			Source:         result.Result.GetSource(),
			DurationUsec:   result.Result.GetDurationUsec(),
			FailureMessage: result.Result.GetFailureMessage(),
		})
		if extra := len(checkpoint.Samples) - int(analyzerConfig.GetWindowSize()); extra > 0 {
			checkpoint.Samples = checkpoint.Samples[extra:]
		}
		samples := make([]analyzer.Sample, 0, len(checkpoint.Samples))
		for _, sample := range checkpoint.Samples {
			samples = append(samples, analyzer.Sample{
				InvocationID: sample.GetInvocationId(), Outcome: sample.GetOutcome(), Source: sample.GetSource(),
			})
		}
		analysis, err := analyzer.LinearTarget(samples, analyzerConfig)
		if err != nil {
			return err
		}
		previousHealth := state.Health
		state.Health = analysis.Health.String()
		state.StateVersion++
		switch result.Result.GetOutcome() {
		case thpb.TestOutcome_TEST_OUTCOME_PASS:
			state.PassCount++
		case thpb.TestOutcome_TEST_OUTCOME_FAIL:
			state.FailCount++
		case thpb.TestOutcome_TEST_OUTCOME_TIMEOUT:
			state.TimeoutCount++
		default:
			return status.InternalError("unknown target outcome reached the analyzer")
		}
		state.TotalDurationUsec += result.Result.GetDurationUsec()
		state.Checkpoint, err = proto.Marshal(checkpoint)
		if err != nil {
			return err
		}
		if err := tx.GORM(ctx, "test_health_update_target_state").Save(state).Error; err != nil {
			return err
		}
		if previousHealth == state.Health {
			return nil
		}
		return tx.NewQuery(ctx, "test_health_create_target_change").Create(&tables.TestTargetStateChange{
			GroupID: groupID, Repository: address.Repository, TargetLabel: address.TargetLabel,
			StateVersion: state.StateVersion, PreviousHealth: previousHealth, Health: state.Health,
			PassCount: state.PassCount, FailCount: state.FailCount, TimeoutCount: state.TimeoutCount,
			EventTimeUsec: tx.NowFunc().UnixMicro(),
		})
	})
}

func (s *Service) GetTestAnalyzerConfig(ctx context.Context, req *thpb.GetTestAnalyzerConfigRequest) (*thpb.GetTestAnalyzerConfigResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentError("request is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	repository, err := identity.NormalizeRepositoryURL(req.GetRepoUrl())
	if err != nil {
		return nil, err
	}
	analyzerConfig, err := s.analyzerConfig(ctx, groupID, repository)
	if err != nil {
		return nil, err
	}
	return &thpb.GetTestAnalyzerConfigResponse{Config: analyzerConfig}, nil
}

func (s *Service) SetTestAnalyzerConfig(ctx context.Context, req *thpb.SetTestAnalyzerConfigRequest) (*thpb.SetTestAnalyzerConfigResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentError("request is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	repository, err := identity.NormalizeRepositoryURL(req.GetRepoUrl())
	if err != nil {
		return nil, err
	}
	analyzerConfig := &thpb.TestAnalyzerConfig{
		WindowSize:             req.GetWindowSize(),
		FailureThreshold:       req.GetFailureThreshold(),
		TargetTimeoutThreshold: req.GetTargetTimeoutThreshold(),
	}
	if err := config.Validate(analyzerConfig); err != nil {
		return nil, err
	}
	encoded, err := proto.Marshal(analyzerConfig)
	if err != nil {
		return nil, err
	}
	row := &tables.TestAnalyzerConfig{
		GroupID: groupID, Repository: repository,
		Revision: s.env.GetDBHandle().NowFunc().UnixMicro(),
		Config:   encoded,
	}
	if err := s.env.GetDBHandle().GORM(ctx, "test_health_set_analyzer_config").
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "group_id"}, {Name: "repository"}},
			DoUpdates: clause.AssignmentColumns([]string{"revision", "config"}),
		}).
		Create(row).Error; err != nil {
		return nil, err
	}
	return &thpb.SetTestAnalyzerConfigResponse{Config: analyzerConfig}, nil
}

func (s *Service) analyzerConfig(ctx context.Context, groupID, repository string) (*thpb.TestAnalyzerConfig, error) {
	row := &tables.TestAnalyzerConfig{}
	err := s.env.GetDBHandle().NewQuery(ctx, "test_health_get_analyzer_config").Raw(`
		SELECT config
		FROM "TestAnalyzerConfigs"
		WHERE group_id = ? AND repository = ?`,
		groupID, repository).Take(row)
	if db.IsRecordNotFound(err) {
		return config.Default(), nil
	}
	if err != nil {
		return nil, err
	}
	analyzerConfig := &thpb.TestAnalyzerConfig{}
	if err := proto.Unmarshal(row.Config, analyzerConfig); err != nil {
		return nil, status.InternalErrorf("decode test analyzer config: %s", err)
	}
	if err := config.Validate(analyzerConfig); err != nil {
		return nil, status.InternalErrorf("invalid stored test analyzer config: %s", err)
	}
	return analyzerConfig, nil
}

func (s *Service) GetTests(req *thpb.GetTestsRequest, stream thpb.TestBuddyService_GetTestsServer) error {
	if req == nil {
		return status.InvalidArgumentError("request is required")
	}
	ctx := stream.Context()
	groupID, err := s.groupID(ctx)
	if err != nil {
		return err
	}
	repository, err := identity.NormalizeRepositoryURL(req.GetRepoUrl())
	if err != nil {
		return err
	}
	packagePrefix, err := normalizePackagePrefix(req.GetPackagePrefix())
	if err != nil {
		return err
	}
	if packagePrefix != "" && req.GetTargetLabel() != "" {
		return status.InvalidArgumentError("package_prefix and target_label cannot both be set")
	}
	where := `tc.group_id = ? AND tc.repository = ?`
	args := []any{groupID, repository}
	if packagePrefix != "" {
		where += ` AND (tc.package_path = ? OR (tc.package_path >= ? AND tc.package_path < ?))`
		args = append(args, packagePrefix, packagePrefix+"/", packagePrefix+"0")
	}
	if req.GetTargetLabel() != "" {
		target, err := identity.CanonicalizeTargetIdentity(repository, req.GetTargetLabel())
		if err != nil {
			return err
		}
		where += ` AND tc.target_label = ?`
		args = append(args, target.Address.TargetLabel)
	}
	args = append(args, thpb.TestHealth_TEST_HEALTH_FLAKY.String())
	query := fmt.Sprintf(`
		SELECT tc.target_label, tc.case_name,
			COALESCE(s.health, '%s') AS health,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec
		FROM "TestCases" AS tc
		LEFT JOIN "TestCaseStates" AS s
			ON s.group_id = tc.group_id AND s.repository = tc.repository
			AND s.target_label = tc.target_label AND s.case_name = tc.case_name
		WHERE %s
		ORDER BY CASE WHEN s.health = ? THEN 0 ELSE 1 END,
			COALESCE(s.total_duration_usec * 1.0 /
				NULLIF(s.pass_count + s.fail_count + s.timeout_count, 0), 0) DESC,
			COALESCE(s.pass_count * 1.0 /
				NULLIF(s.pass_count + s.fail_count + s.timeout_count, 0), 1) ASC,
			tc.target_label, tc.case_name`, thpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), where)
	type row struct {
		TargetLabel       string
		CaseName          string
		Health            string
		PassCount         int64
		FailCount         int64
		TimeoutCount      int64
		TotalDurationUsec int64
	}
	rsp := &thpb.GetTestsResponse{Tests: make([]*thpb.TestSummary, 0, queryBatchSize)}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_health_get_tests").Raw(query, args...)
	err = db.ScanEach(rq, func(ctx context.Context, r *row) error {
		rsp.Tests = append(rsp.Tests, summary(
			identity.CaseAddress{
				Repository: repository, TargetLabel: r.TargetLabel, CaseName: r.CaseName,
			},
			r.Health, r.PassCount, r.FailCount, r.TimeoutCount, r.TotalDurationUsec))
		if len(rsp.Tests) == queryBatchSize {
			if err := stream.Send(rsp); err != nil {
				return err
			}
			rsp = &thpb.GetTestsResponse{Tests: make([]*thpb.TestSummary, 0, queryBatchSize)}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(rsp.Tests) > 0 {
		return stream.Send(rsp)
	}
	return nil
}

func (s *Service) GetTestTargets(req *thpb.GetTestTargetsRequest, stream thpb.TestBuddyService_GetTestTargetsServer) error {
	if req == nil {
		return status.InvalidArgumentError("request is required")
	}
	ctx := stream.Context()
	groupID, err := s.groupID(ctx)
	if err != nil {
		return err
	}
	repository, err := identity.NormalizeRepositoryURL(req.GetRepoUrl())
	if err != nil {
		return err
	}
	packagePrefix, err := normalizePackagePrefix(req.GetPackagePrefix())
	if err != nil {
		return err
	}
	where := `tt.group_id = ? AND tt.repository = ?`
	args := []any{groupID, repository}
	if packagePrefix != "" {
		where += ` AND (tt.package_path = ? OR (tt.package_path >= ? AND tt.package_path < ?))`
		args = append(args, packagePrefix, packagePrefix+"/", packagePrefix+"0")
	}
	args = append(args,
		thpb.TestHealth_TEST_HEALTH_FLAKY.String(),
		thpb.TestHealth_TEST_HEALTH_TIMEOUT.String(),
	)
	query := fmt.Sprintf(`
		SELECT tt.target_label,
			COALESCE(s.health, '%s') AS health,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec
		FROM "TestTargets" AS tt
		LEFT JOIN "TestTargetStates" AS s
			ON s.group_id = tt.group_id AND s.repository = tt.repository
			AND s.target_label = tt.target_label
		WHERE %s
		ORDER BY CASE WHEN s.health = ? OR s.health = ? THEN 0 ELSE 1 END,
			COALESCE(s.total_duration_usec * 1.0 /
				NULLIF(s.pass_count + s.fail_count + s.timeout_count, 0), 0) DESC,
			COALESCE(s.pass_count * 1.0 /
				NULLIF(s.pass_count + s.fail_count + s.timeout_count, 0), 1) ASC,
			tt.target_label`, thpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), where)
	type row struct {
		TargetLabel       string
		Health            string
		PassCount         int64
		FailCount         int64
		TimeoutCount      int64
		TotalDurationUsec int64
	}
	rsp := &thpb.GetTestTargetsResponse{Targets: make([]*thpb.TestTargetSummary, 0, queryBatchSize)}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_health_get_test_targets").Raw(query, args...)
	err = db.ScanEach(rq, func(ctx context.Context, r *row) error {
		rsp.Targets = append(rsp.Targets, targetSummary(
			identity.TargetAddress{Repository: repository, TargetLabel: r.TargetLabel},
			r.Health, r.PassCount, r.FailCount, r.TimeoutCount, r.TotalDurationUsec))
		if len(rsp.Targets) == queryBatchSize {
			if err := stream.Send(rsp); err != nil {
				return err
			}
			rsp = &thpb.GetTestTargetsResponse{Targets: make([]*thpb.TestTargetSummary, 0, queryBatchSize)}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(rsp.Targets) > 0 {
		return stream.Send(rsp)
	}
	return nil
}

func (s *Service) GetRepositoryHealth(ctx context.Context, req *thpb.GetRepositoryHealthRequest) (*thpb.GetRepositoryHealthResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentError("request is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	repository, err := identity.NormalizeRepositoryURL(req.GetRepoUrl())
	if err != nil {
		return nil, err
	}
	key := repositoryHealthCacheKey{groupID: groupID, repository: repository}
	cached, fresh := s.repositoryHealthCache.lookup(key, time.Now())
	if fresh {
		return cached, nil
	}
	return s.refreshRepositoryHealth(ctx, key)
}

func (s *Service) refreshRepositoryHealth(ctx context.Context, key repositoryHealthCacheKey) (*thpb.GetRepositoryHealthResponse, error) {
	response, _, err := s.repositoryHealthCache.refresh.Do(
		ctx, key, func(ctx context.Context) (*thpb.GetRepositoryHealthResponse, error) {
			if cached, fresh := s.repositoryHealthCache.lookup(key, time.Now()); fresh {
				return cached, nil
			}
			response, err := s.queryRepositoryHealth(ctx, key.groupID, key.repository)
			if err != nil {
				return nil, err
			}
			s.repositoryHealthCache.store(key, response)
			return response, nil
		})
	if err != nil {
		return nil, err
	}
	return proto.Clone(response).(*thpb.GetRepositoryHealthResponse), nil
}

func (s *Service) queryRepositoryHealth(ctx context.Context, groupID, repository string) (*thpb.GetRepositoryHealthResponse, error) {
	summary := func(name, catalogTable, stateTable string, target bool) (*thpb.TestHealthSummary, error) {
		join := `s.group_id = catalog.group_id AND s.repository = catalog.repository
			AND s.target_label = catalog.target_label`
		if !target {
			join += ` AND s.case_name = catalog.case_name`
		}
		query := fmt.Sprintf(`
		SELECT COALESCE(s.health, '') AS health,
			COUNT(*) AS subject_count,
			COALESCE(SUM(s.pass_count), 0) AS pass_count,
			COALESCE(SUM(s.fail_count), 0) AS fail_count,
			COALESCE(SUM(s.timeout_count), 0) AS timeout_count,
			COALESCE(SUM(s.total_duration_usec), 0) AS total_duration_usec
		FROM "%s" AS catalog
		LEFT JOIN "%s" AS s ON %s
		WHERE catalog.group_id = ? AND catalog.repository = ?
		GROUP BY s.health`, catalogTable, stateTable, join)
		type row struct {
			Health            string
			SubjectCount      int64
			PassCount         int64
			FailCount         int64
			TimeoutCount      int64
			TotalDurationUsec int64
		}
		out := &thpb.TestHealthSummary{}
		totalDurationUsec := int64(0)
		rq := s.env.GetDBHandle().NewQuery(ctx, name).Raw(query, groupID, repository)
		err := db.ScanEach(rq, func(ctx context.Context, r *row) error {
			out.TotalCount += r.SubjectCount
			switch health(r.Health) {
			case thpb.TestHealth_TEST_HEALTH_HEALTHY:
				out.HealthyCount += r.SubjectCount
			case thpb.TestHealth_TEST_HEALTH_FLAKY:
				out.FlakyCount += r.SubjectCount
			case thpb.TestHealth_TEST_HEALTH_TIMEOUT:
				out.TimedOutCount += r.SubjectCount
			case thpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA:
				out.InsufficientDataCount += r.SubjectCount
			default:
				out.UnknownCount += r.SubjectCount
			}
			out.PassCount += r.PassCount
			out.FailCount += r.FailCount
			out.TimeoutCount += r.TimeoutCount
			totalDurationUsec += r.TotalDurationUsec
			return nil
		})
		if err != nil {
			return nil, err
		}
		if total := out.GetPassCount() + out.GetFailCount() + out.GetTimeoutCount(); total > 0 {
			out.PassRate = float64(out.GetPassCount()) / float64(total)
			out.MeanDurationUsec = totalDurationUsec / total
		}
		return out, nil
	}
	targets, err := summary("test_health_get_repository_target_health", "TestTargets", "TestTargetStates", true)
	if err != nil {
		return nil, err
	}
	cases, err := summary("test_health_get_repository_case_health", "TestCases", "TestCaseStates", false)
	if err != nil {
		return nil, err
	}
	if targets.GetTotalCount() == 0 && cases.GetTotalCount() == 0 {
		return nil, status.NotFoundErrorf("repository %s was not found", repository)
	}
	return &thpb.GetRepositoryHealthResponse{Targets: targets, Cases: cases}, nil
}

func (s *Service) GetTestTarget(ctx context.Context, req *thpb.GetTestTargetRequest) (*thpb.GetTestTargetResponse, error) {
	if req == nil || req.GetIdentity() == nil {
		return nil, status.InvalidArgumentError("test target identity is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	target, err := identity.CanonicalizeTargetIdentity(
		req.GetIdentity().GetRepoUrl(), req.GetIdentity().GetTargetLabel())
	if err != nil {
		return nil, err
	}
	type targetRow struct {
		Health            string
		Checkpoint        []byte
		PassCount         int64
		FailCount         int64
		TimeoutCount      int64
		TotalDurationUsec int64
	}
	row := &targetRow{}
	err = s.env.GetDBHandle().NewQuery(ctx, "test_health_get_test_target").Raw(`
		SELECT COALESCE(s.health, ?) AS health, s.checkpoint,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec
		FROM "TestTargets" AS tt
		LEFT JOIN "TestTargetStates" AS s
			ON s.group_id = tt.group_id AND s.repository = tt.repository
			AND s.target_label = tt.target_label
		WHERE tt.group_id = ? AND tt.repository = ? AND tt.target_label = ?
		`,
		thpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), groupID,
		target.Address.Repository, target.Address.TargetLabel).Take(row)
	if db.IsRecordNotFound(err) {
		return nil, status.NotFoundErrorf("test target %s was not found", target.Address.String())
	}
	if err != nil {
		return nil, err
	}
	rsp := &thpb.GetTestTargetResponse{
		Target: targetSummary(target.Address, row.Health, row.PassCount, row.FailCount,
			row.TimeoutCount, row.TotalDurationUsec),
	}
	checkpoint := &thpb.TestStateCheckpoint{}
	if len(row.Checkpoint) > 0 {
		if err := proto.Unmarshal(row.Checkpoint, checkpoint); err != nil {
			return nil, status.InternalErrorf("decode test target state checkpoint: %s", err)
		}
	}
	for i := len(checkpoint.GetSamples()) - 1; i >= 0; i-- {
		sample := checkpoint.GetSamples()[i]
		rsp.RecentResults = append(rsp.RecentResults, &thpb.TestTargetResult{
			Identity: target.Proto(), InvocationId: sample.GetInvocationId(),
			Outcome: sample.GetOutcome(), Source: sample.GetSource(),
			DurationUsec: sample.GetDurationUsec(), FailureMessage: sample.GetFailureMessage(),
		})
	}
	type transitionRow struct {
		PreviousHealth string
		Health         string
		EventTimeUsec  int64
	}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_health_get_test_target_transitions").Raw(`
		SELECT previous_health, health, event_time_usec
		FROM "TestTargetStateChanges"
		WHERE group_id = ? AND repository = ? AND target_label = ?
		ORDER BY state_version DESC
		LIMIT 100`,
		groupID, target.Address.Repository, target.Address.TargetLabel)
	if err := db.ScanEach(rq, func(ctx context.Context, r *transitionRow) error {
		rsp.Transitions = append(rsp.Transitions, &thpb.TestHealthTransition{
			PreviousHealth: health(r.PreviousHealth), Health: health(r.Health),
			EventTimeUsec: r.EventTimeUsec,
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return rsp, nil
}

func (s *Service) GetTestCase(ctx context.Context, req *thpb.GetTestCaseRequest) (*thpb.GetTestCaseResponse, error) {
	if req == nil || req.GetIdentity() == nil {
		return nil, status.InvalidArgumentError("test case identity is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	input := identity.CaseAddressFromProto(req.GetIdentity())
	testCase, err := identity.CanonicalizeCase(identity.CaseInput{
		RepositoryURL: input.Repository,
		TargetLabel:   input.TargetLabel,
		CaseName:      input.CaseName,
	})
	if err != nil {
		return nil, err
	}
	type caseRow struct {
		Health            string
		Checkpoint        []byte
		PassCount         int64
		FailCount         int64
		TimeoutCount      int64
		TotalDurationUsec int64
	}
	row := &caseRow{}
	err = s.env.GetDBHandle().NewQuery(ctx, "test_health_get_test_case").Raw(`
		SELECT COALESCE(s.health, ?) AS health, s.checkpoint,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec
		FROM "TestCases" AS tc
		LEFT JOIN "TestCaseStates" AS s
			ON s.group_id = tc.group_id AND s.repository = tc.repository
			AND s.target_label = tc.target_label AND s.case_name = tc.case_name
		WHERE tc.group_id = ? AND tc.repository = ?
			AND tc.target_label = ? AND tc.case_name = ?`,
		thpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), groupID,
		testCase.Address.Repository, testCase.Address.TargetLabel, testCase.Address.CaseName).Take(row)
	if db.IsRecordNotFound(err) {
		return nil, status.NotFoundErrorf("test case %s was not found", testCase.Address.String())
	}
	if err != nil {
		return nil, err
	}
	rsp := &thpb.GetTestCaseResponse{
		Test: summary(testCase.Address, row.Health, row.PassCount, row.FailCount, row.TimeoutCount, row.TotalDurationUsec),
	}
	checkpoint := &thpb.TestStateCheckpoint{}
	if len(row.Checkpoint) > 0 {
		if err := proto.Unmarshal(row.Checkpoint, checkpoint); err != nil {
			return nil, status.InternalErrorf("decode test state checkpoint: %s", err)
		}
	}
	for i := len(checkpoint.GetSamples()) - 1; i >= 0; i-- {
		sample := checkpoint.GetSamples()[i]
		rsp.RecentResults = append(rsp.RecentResults, &thpb.TestCaseResult{
			Identity: testCase.Proto(), InvocationId: sample.GetInvocationId(),
			Outcome: sample.GetOutcome(), Source: sample.GetSource(),
			DurationUsec: sample.GetDurationUsec(), FailureMessage: sample.GetFailureMessage(),
		})
	}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_health_get_test_case_transitions").Raw(`
		SELECT previous_health, health, event_time_usec
		FROM "TestCaseStateChanges"
		WHERE group_id = ? AND repository = ? AND target_label = ? AND case_name = ?
		ORDER BY state_version DESC
		LIMIT 100`,
		groupID, testCase.Address.Repository, testCase.Address.TargetLabel, testCase.Address.CaseName)
	type transitionRow struct {
		PreviousHealth string
		Health         string
		EventTimeUsec  int64
	}
	err = db.ScanEach(rq, func(ctx context.Context, row *transitionRow) error {
		rsp.Transitions = append(rsp.Transitions, &thpb.TestHealthTransition{
			PreviousHealth: health(row.PreviousHealth),
			Health:         health(row.Health),
			EventTimeUsec:  row.EventTimeUsec,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func summary(address identity.CaseAddress, healthValue string, passCount, failCount, timeoutCount, totalDurationUsec int64) *thpb.TestSummary {
	result := &thpb.TestSummary{
		Identity:     identity.CaseProto(address),
		Health:       health(healthValue),
		PassCount:    passCount,
		FailCount:    failCount,
		TimeoutCount: timeoutCount,
	}
	total := passCount + failCount + timeoutCount
	if total > 0 {
		result.MeanDurationUsec = totalDurationUsec / total
		result.PassRate = float64(passCount) / float64(total)
	}
	return result
}

func targetSummary(address identity.TargetAddress, healthValue string, passCount, failCount, timeoutCount, totalDurationUsec int64) *thpb.TestTargetSummary {
	result := &thpb.TestTargetSummary{
		Identity: &thpb.TestTargetIdentity{
			RepoUrl: address.Repository, TargetLabel: address.TargetLabel,
		},
		Health: health(healthValue), PassCount: passCount, FailCount: failCount,
		TimeoutCount: timeoutCount,
	}
	total := passCount + failCount + timeoutCount
	if total > 0 {
		result.MeanDurationUsec = totalDurationUsec / total
		result.PassRate = float64(passCount) / float64(total)
	}
	return result
}

func health(value string) thpb.TestHealth {
	if number, ok := thpb.TestHealth_value[value]; ok {
		return thpb.TestHealth(number)
	}
	return thpb.TestHealth_TEST_HEALTH_UNKNOWN
}

func normalizePackagePrefix(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "//")
	raw = strings.Trim(raw, "/")
	if raw == "" {
		return "", nil
	}
	if strings.Contains(raw, ":") {
		return "", status.InvalidArgumentError("package prefix must be a directory, not a target label")
	}
	target, err := identity.CanonicalizeTarget("//" + raw + ":__test_health_query__")
	if err != nil {
		return "", err
	}
	return target.PackagePath, nil
}

func (s *Service) groupID(ctx context.Context) (string, error) {
	authenticator := s.env.GetAuthenticator()
	user, err := authenticator.AuthenticatedUser(ctx)
	if authutil.IsAnonymousUserError(err) && authenticator.AnonymousUsageEnabled(ctx) {
		return interfaces.AuthAnonymousUser, nil
	}
	if err != nil {
		return "", err
	}
	if user.GetGroupID() == "" {
		return "", status.PermissionDeniedError("authenticated user has no group")
	}
	return user.GetGroupID(), nil
}
