// Package test_buddy implements synchronous test result reporting and reads.
package test_buddy

import (
	"context"
	"encoding/json"
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

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/analyzer"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/config"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/normalize"
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
	tbpb.UnimplementedTestBuddyServiceServer
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
	response  *tbpb.GetRepositoryHealthResponse
	expiresAt time.Time
}

type repositoryHealthCache struct {
	mu      sync.Mutex
	entries map[repositoryHealthCacheKey]*repositoryHealthCacheEntry
	refresh singleflight.Group[repositoryHealthCacheKey, *tbpb.GetRepositoryHealthResponse]
}

func (c *repositoryHealthCache) lookup(key repositoryHealthCacheKey, now time.Time) (*tbpb.GetRepositoryHealthResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil || !now.Before(entry.expiresAt) {
		return nil, false
	}
	return proto.Clone(entry.response).(*tbpb.GetRepositoryHealthResponse), true
}

func (c *repositoryHealthCache) store(key repositoryHealthCacheKey, response *tbpb.GetRepositoryHealthResponse) {
	jitter := time.Duration(rand.Int64N(int64(time.Minute))) - 30*time.Second
	c.mu.Lock()
	c.entries[key] = &repositoryHealthCacheEntry{
		response:  proto.Clone(response).(*tbpb.GetRepositoryHealthResponse),
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
		client:        tbpb.NewTestBuddyServiceClient(conn),
		authenticator: env.GetAuthenticator(),
	})
	return nil
}

func RegisterLocal(env environment.Env, grpcServer *grpc.Server) {
	tbpb.RegisterTestBuddyServiceServer(grpcServer, New(env))
}

type proxy struct {
	tbpb.UnimplementedTestBuddyServiceServer
	client        tbpb.TestBuddyServiceClient
	authenticator interfaces.Authenticator
}

func (p *proxy) ReportTestResults(ctx context.Context, req *tbpb.ReportTestResultsRequest) (*tbpb.ReportTestResultsResponse, error) {
	return p.client.ReportTestResults(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) GetTests(req *tbpb.GetTestsRequest, stream tbpb.TestBuddyService_GetTestsServer) error {
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

func (p *proxy) GetTestTargets(req *tbpb.GetTestTargetsRequest, stream tbpb.TestBuddyService_GetTestTargetsServer) error {
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

func (p *proxy) GetTestCase(ctx context.Context, req *tbpb.GetTestCaseRequest) (*tbpb.GetTestCaseResponse, error) {
	return p.client.GetTestCase(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) GetTestTarget(ctx context.Context, req *tbpb.GetTestTargetRequest) (*tbpb.GetTestTargetResponse, error) {
	return p.client.GetTestTarget(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) GetRepositoryHealth(ctx context.Context, req *tbpb.GetRepositoryHealthRequest) (*tbpb.GetRepositoryHealthResponse, error) {
	return p.client.GetRepositoryHealth(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) GetTestAnalyzerConfig(ctx context.Context, req *tbpb.GetTestAnalyzerConfigRequest) (*tbpb.GetTestAnalyzerConfigResponse, error) {
	return p.client.GetTestAnalyzerConfig(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) SetTestAnalyzerConfig(ctx context.Context, req *tbpb.SetTestAnalyzerConfigRequest) (*tbpb.SetTestAnalyzerConfigResponse, error) {
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

func (s *Service) ReportTestResults(ctx context.Context, req *tbpb.ReportTestResultsRequest) (*tbpb.ReportTestResultsResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentError("request is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	report, err := normalize.Normalize(req.GetRepoUrl(), req.GetTestCases(), req.GetTestTargets())
	if err != nil {
		return nil, err
	}
	analyzerConfig, err := s.analyzerConfig(ctx, groupID, report.RepositoryURL)
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
		if analyzer.Eligible(analyzer.Sample{Outcome: result.Result.GetResult().GetOutcome()}) {
			resultsByCase[result.Address] = append(resultsByCase[result.Address], result)
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
		if analyzer.Eligible(analyzer.Sample{Outcome: result.Result.GetResult().GetOutcome()}) {
			resultsByTarget[result.Address] = append(resultsByTarget[result.Address], result)
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
	return &tbpb.ReportTestResultsResponse{
		AcceptedCount: int32(len(report.CaseResults) + len(report.TargetResults)),
		RejectedCount: int32(report.Rejected.Total()),
	}, nil
}

func admitCatalog(ctx context.Context, database interfaces.DB, groupID string, report *normalize.Report) error {
	repository := report.RepositoryURL
	if err := database.GORM(ctx, "test_buddy_admit_repository").
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&tables.TestRepositoryCatalog{GroupID: groupID, Repository: repository}).Error; err != nil {
		return err
	}
	targets := make(map[identity.TargetAddress]*tables.TestTarget)
	for _, result := range report.CaseResults {
		target := result.Address.Target()
		targets[target] = &tables.TestTarget{
			GroupID: groupID, Repository: repository, TargetLabel: target.Label(),
			BucketID: identity.BucketForTarget(groupID, target), PackagePath: target.PackagePath,
		}
	}
	for _, result := range report.TargetResults {
		target := result.Address
		targets[target] = &tables.TestTarget{
			GroupID: groupID, Repository: repository, TargetLabel: target.Label(),
			BucketID: identity.BucketForTarget(groupID, target), PackagePath: target.PackagePath,
		}
	}
	targetRows := make([]*tables.TestTarget, 0, len(targets))
	for _, target := range targets {
		targetRows = append(targetRows, target)
	}
	for start := 0; start < len(targetRows); start += reportBatchSize {
		end := min(start+reportBatchSize, len(targetRows))
		if err := database.GORM(ctx, "test_buddy_admit_targets").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(targetRows[start:end]).Error; err != nil {
			return err
		}
	}
	type coneKey struct {
		prefix   string
		bucketID int32
	}
	coneRows := make(map[coneKey]*tables.TestTargetConeBucket)
	for _, target := range targetRows {
		for _, prefix := range identity.PackagePrefixes(target.PackagePath) {
			key := coneKey{prefix: prefix, bucketID: target.BucketID}
			coneRows[key] = &tables.TestTargetConeBucket{
				GroupID: groupID, Repository: repository, PackagePrefix: prefix, BucketID: target.BucketID,
			}
		}
	}
	cones := make([]*tables.TestTargetConeBucket, 0, len(coneRows))
	for _, row := range coneRows {
		cones = append(cones, row)
	}
	for start := 0; start < len(cones); start += reportBatchSize {
		end := min(start+reportBatchSize, len(cones))
		if err := database.GORM(ctx, "test_buddy_admit_cone_buckets").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(cones[start:end]).Error; err != nil {
			return err
		}
	}
	for start := 0; start < len(report.CaseResults); start += reportBatchSize {
		end := min(start+reportBatchSize, len(report.CaseResults))
		cases := make([]*tables.TestCase, 0, end-start)
		for _, result := range report.CaseResults[start:end] {
			address := result.Address
			cases = append(cases, &tables.TestCase{
				GroupID: groupID, Repository: repository, TargetLabel: address.Target().Label(),
				CaseName: address.CaseName, BucketID: identity.BucketForTarget(groupID, address.Target()),
				PackagePath: address.PackagePath,
			})
		}
		if err := database.GORM(ctx, "test_buddy_admit_cases").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(cases).Error; err != nil {
			return err
		}
	}
	return nil
}

type retainedSample struct {
	Outcome        tbpb.TestOutcome `json:"o"`
	DurationUsec   int64            `json:"d,omitempty"`
	FailureMessage string           `json:"f,omitempty"`
	SourceURL      string           `json:"u"`
}

func decodeSamples(encoded []byte) ([]retainedSample, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	var samples []retainedSample
	if err := json.Unmarshal(encoded, &samples); err != nil {
		return nil, status.InternalErrorf("decode recent test results: %s", err)
	}
	return samples, nil
}

func appendSample(encoded []byte, result *tbpb.TestResult, windowSize int) ([]retainedSample, []byte, error) {
	samples, err := decodeSamples(encoded)
	if err != nil {
		return nil, nil, err
	}
	samples = append(samples, retainedSample{
		Outcome: result.GetOutcome(), DurationUsec: result.GetDurationUsec(),
		FailureMessage: result.GetFailureMessage(), SourceURL: result.GetSourceUrl(),
	})
	if extra := len(samples) - windowSize; extra > 0 {
		samples = samples[extra:]
	}
	encoded, err = json.Marshal(samples)
	return samples, encoded, err
}

func analysisSamples(samples []retainedSample) []analyzer.Sample {
	out := make([]analyzer.Sample, 0, len(samples))
	for _, sample := range samples {
		out = append(out, analyzer.Sample{Outcome: sample.Outcome})
	}
	return out
}

func (s *Service) applyCase(ctx context.Context, groupID string, result *normalize.CaseResult, analyzerConfig *tbpb.TestAnalyzerConfig) error {
	return s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		address := result.Address
		targetLabel := address.Target().Label()
		if err := tx.GORM(ctx, "test_buddy_admit_state").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(&tables.TestCaseState{
				GroupID: groupID, Repository: address.Repository, TargetLabel: targetLabel,
				CaseName: address.CaseName, Health: tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(),
				RecentResults: []byte("[]"),
			}).Error; err != nil {
			return err
		}
		state := &tables.TestCaseState{}
		query := `SELECT * FROM "TestCaseStates"
			WHERE group_id = ? AND repository = ? AND target_label = ? AND case_name = ?` +
			s.env.GetDBHandle().SelectForUpdateModifier()
		if err := tx.NewQuery(ctx, "test_buddy_lock_case_state").Raw(
			query, groupID, address.Repository, targetLabel, address.CaseName).Take(state); err != nil {
			return err
		}
		resultInfo := result.Result.GetResult()
		samples, encoded, err := appendSample(
			state.RecentResults, resultInfo, int(analyzerConfig.GetLinear().GetWindowSize()))
		if err != nil {
			return err
		}
		analysis, err := analyzer.Linear(analysisSamples(samples), analyzerConfig)
		if err != nil {
			return err
		}
		previousHealth := state.Health
		state.Health = analysis.Health.String()
		state.StateVersion++
		switch resultInfo.GetOutcome() {
		case tbpb.TestOutcome_TEST_OUTCOME_PASS:
			state.PassCount++
		case tbpb.TestOutcome_TEST_OUTCOME_FAIL:
			state.FailCount++
		case tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT:
			state.TimeoutCount++
		default:
			return status.InternalError("unknown outcome reached the analyzer")
		}
		state.TotalDurationUsec += resultInfo.GetDurationUsec()
		state.RecentResults = encoded
		if err := tx.GORM(ctx, "test_buddy_update_case_state").Save(state).Error; err != nil {
			return err
		}
		if previousHealth == state.Health {
			return nil
		}
		return tx.NewQuery(ctx, "test_buddy_create_case_change").Create(&tables.TestCaseStateChange{
			GroupID: groupID, Repository: address.Repository, TargetLabel: targetLabel,
			CaseName: address.CaseName, StateVersion: state.StateVersion,
			PreviousHealth: previousHealth, Health: state.Health,
			PassCount: state.PassCount, FailCount: state.FailCount, TimeoutCount: state.TimeoutCount,
			EventTimeUsec: tx.NowFunc().UnixMicro(),
		})
	})
}

func (s *Service) applyTarget(ctx context.Context, groupID string, result *normalize.TargetResult, analyzerConfig *tbpb.TestAnalyzerConfig) error {
	return s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		address := result.Address
		targetLabel := address.Label()
		if err := tx.GORM(ctx, "test_buddy_admit_target_state").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(&tables.TestTargetState{
				GroupID: groupID, Repository: address.Repository, TargetLabel: targetLabel,
				Health: tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), RecentResults: []byte("[]"),
			}).Error; err != nil {
			return err
		}
		state := &tables.TestTargetState{}
		query := `SELECT * FROM "TestTargetStates"
			WHERE group_id = ? AND repository = ? AND target_label = ?` +
			s.env.GetDBHandle().SelectForUpdateModifier()
		if err := tx.NewQuery(ctx, "test_buddy_lock_target_state").Raw(
			query, groupID, address.Repository, targetLabel).Take(state); err != nil {
			return err
		}
		resultInfo := result.Result.GetResult()
		samples, encoded, err := appendSample(
			state.RecentResults, resultInfo, int(analyzerConfig.GetLinear().GetWindowSize()))
		if err != nil {
			return err
		}
		analysis, err := analyzer.LinearTarget(analysisSamples(samples), analyzerConfig)
		if err != nil {
			return err
		}
		previousHealth := state.Health
		state.Health = analysis.Health.String()
		state.StateVersion++
		switch resultInfo.GetOutcome() {
		case tbpb.TestOutcome_TEST_OUTCOME_PASS:
			state.PassCount++
		case tbpb.TestOutcome_TEST_OUTCOME_FAIL:
			state.FailCount++
		case tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT:
			state.TimeoutCount++
		default:
			return status.InternalError("unknown target outcome reached the analyzer")
		}
		state.TotalDurationUsec += resultInfo.GetDurationUsec()
		state.RecentResults = encoded
		if err := tx.GORM(ctx, "test_buddy_update_target_state").Save(state).Error; err != nil {
			return err
		}
		if previousHealth == state.Health {
			return nil
		}
		return tx.NewQuery(ctx, "test_buddy_create_target_change").Create(&tables.TestTargetStateChange{
			GroupID: groupID, Repository: address.Repository, TargetLabel: targetLabel,
			StateVersion: state.StateVersion, PreviousHealth: previousHealth, Health: state.Health,
			PassCount: state.PassCount, FailCount: state.FailCount, TimeoutCount: state.TimeoutCount,
			EventTimeUsec: tx.NowFunc().UnixMicro(),
		})
	})
}

func (s *Service) GetTestAnalyzerConfig(ctx context.Context, req *tbpb.GetTestAnalyzerConfigRequest) (*tbpb.GetTestAnalyzerConfigResponse, error) {
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
	return &tbpb.GetTestAnalyzerConfigResponse{Config: analyzerConfig}, nil
}

func (s *Service) SetTestAnalyzerConfig(ctx context.Context, req *tbpb.SetTestAnalyzerConfigRequest) (*tbpb.SetTestAnalyzerConfigResponse, error) {
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
	analyzerConfig := req.GetConfig()
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
	if err := s.env.GetDBHandle().GORM(ctx, "test_buddy_set_analyzer_config").
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "group_id"}, {Name: "repository"}},
			DoUpdates: clause.AssignmentColumns([]string{"revision", "config"}),
		}).
		Create(row).Error; err != nil {
		return nil, err
	}
	return &tbpb.SetTestAnalyzerConfigResponse{Config: analyzerConfig}, nil
}

func (s *Service) analyzerConfig(ctx context.Context, groupID, repository string) (*tbpb.TestAnalyzerConfig, error) {
	row := &tables.TestAnalyzerConfig{}
	err := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_analyzer_config").Raw(`
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
	analyzerConfig := &tbpb.TestAnalyzerConfig{}
	if err := proto.Unmarshal(row.Config, analyzerConfig); err != nil {
		return nil, status.InternalErrorf("decode test analyzer config: %s", err)
	}
	if err := config.Validate(analyzerConfig); err != nil {
		return nil, status.InternalErrorf("invalid stored test analyzer config: %s", err)
	}
	return analyzerConfig, nil
}

func (s *Service) GetTests(req *tbpb.GetTestsRequest, stream tbpb.TestBuddyService_GetTestsServer) error {
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
	if packagePrefix != "" && req.GetTarget() != nil {
		return status.InvalidArgumentError("package_prefix and target_label cannot both be set")
	}
	where := `tc.group_id = ? AND tc.repository = ?`
	args := []any{packagePrefix, groupID, repository}
	if packagePrefix != "" {
		where += ` AND (tc.package_path = ? OR (tc.package_path >= ? AND tc.package_path < ?))`
		args = append(args, packagePrefix, packagePrefix+"/", packagePrefix+"0")
	}
	if req.GetTarget() != nil {
		target, err := identity.CanonicalizeTarget(repository, req.GetTarget().GetTargetLabel())
		if err != nil {
			return err
		}
		where += ` AND tc.target_label = ?`
		args = append(args, target.Label())
	}
	args = append(args, tbpb.TestHealth_TEST_HEALTH_FLAKY.String())
	query := fmt.Sprintf(`
		SELECT tc.target_label, tc.case_name,
			COALESCE(s.health, '%s') AS health,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec
		FROM "TestCases" AS tc
		JOIN "TestTargetConeBuckets" AS cone
			ON cone.group_id = tc.group_id AND cone.repository = tc.repository
			AND cone.bucket_id = tc.bucket_id AND cone.package_prefix = ?
		LEFT JOIN "TestCaseStates" AS s
			ON s.group_id = tc.group_id AND s.repository = tc.repository
			AND s.target_label = tc.target_label AND s.case_name = tc.case_name
		WHERE %s
		ORDER BY CASE WHEN s.health = ? THEN 0 ELSE 1 END,
			COALESCE(s.total_duration_usec * 1.0 /
				NULLIF(s.pass_count + s.fail_count + s.timeout_count, 0), 0) DESC,
			COALESCE(s.pass_count * 1.0 /
				NULLIF(s.pass_count + s.fail_count + s.timeout_count, 0), 1) ASC,
			tc.target_label, tc.case_name`, tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), where)
	type row struct {
		TargetLabel       string
		CaseName          string
		Health            string
		PassCount         int64
		FailCount         int64
		TimeoutCount      int64
		TotalDurationUsec int64
	}
	rsp := &tbpb.GetTestsResponse{Tests: make([]*tbpb.TestCaseSummary, 0, queryBatchSize)}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_tests").Raw(query, args...)
	err = db.ScanEach(rq, func(ctx context.Context, r *row) error {
		target, err := identity.CanonicalizeTarget(repository, r.TargetLabel)
		if err != nil {
			return err
		}
		rsp.Tests = append(rsp.Tests, caseSummary(
			identity.CaseAddress{
				TargetAddress: target, CaseName: r.CaseName,
			},
			r.Health, r.PassCount, r.FailCount, r.TimeoutCount, r.TotalDurationUsec))
		if len(rsp.Tests) == queryBatchSize {
			if err := stream.Send(rsp); err != nil {
				return err
			}
			rsp = &tbpb.GetTestsResponse{Tests: make([]*tbpb.TestCaseSummary, 0, queryBatchSize)}
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

func (s *Service) GetTestTargets(req *tbpb.GetTestTargetsRequest, stream tbpb.TestBuddyService_GetTestTargetsServer) error {
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
	args := []any{packagePrefix, groupID, repository}
	if packagePrefix != "" {
		where += ` AND (tt.package_path = ? OR (tt.package_path >= ? AND tt.package_path < ?))`
		args = append(args, packagePrefix, packagePrefix+"/", packagePrefix+"0")
	}
	args = append(args,
		tbpb.TestHealth_TEST_HEALTH_FLAKY.String(),
		tbpb.TestHealth_TEST_HEALTH_TIMEOUT.String(),
	)
	query := fmt.Sprintf(`
		SELECT tt.target_label,
			COALESCE(s.health, '%s') AS health,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec
		FROM "TestTargets" AS tt
		JOIN "TestTargetConeBuckets" AS cone
			ON cone.group_id = tt.group_id AND cone.repository = tt.repository
			AND cone.bucket_id = tt.bucket_id AND cone.package_prefix = ?
		LEFT JOIN "TestTargetStates" AS s
			ON s.group_id = tt.group_id AND s.repository = tt.repository
			AND s.target_label = tt.target_label
		WHERE %s
		ORDER BY CASE WHEN s.health = ? OR s.health = ? THEN 0 ELSE 1 END,
			COALESCE(s.total_duration_usec * 1.0 /
				NULLIF(s.pass_count + s.fail_count + s.timeout_count, 0), 0) DESC,
			COALESCE(s.pass_count * 1.0 /
				NULLIF(s.pass_count + s.fail_count + s.timeout_count, 0), 1) ASC,
			tt.target_label`, tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), where)
	type row struct {
		TargetLabel       string
		Health            string
		PassCount         int64
		FailCount         int64
		TimeoutCount      int64
		TotalDurationUsec int64
	}
	rsp := &tbpb.GetTestTargetsResponse{Targets: make([]*tbpb.TestTargetSummary, 0, queryBatchSize)}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_test_targets").Raw(query, args...)
	err = db.ScanEach(rq, func(ctx context.Context, r *row) error {
		target, err := identity.CanonicalizeTarget(repository, r.TargetLabel)
		if err != nil {
			return err
		}
		rsp.Targets = append(rsp.Targets, targetSummary(
			target,
			r.Health, r.PassCount, r.FailCount, r.TimeoutCount, r.TotalDurationUsec))
		if len(rsp.Targets) == queryBatchSize {
			if err := stream.Send(rsp); err != nil {
				return err
			}
			rsp = &tbpb.GetTestTargetsResponse{Targets: make([]*tbpb.TestTargetSummary, 0, queryBatchSize)}
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

func (s *Service) GetRepositoryHealth(ctx context.Context, req *tbpb.GetRepositoryHealthRequest) (*tbpb.GetRepositoryHealthResponse, error) {
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

func (s *Service) refreshRepositoryHealth(ctx context.Context, key repositoryHealthCacheKey) (*tbpb.GetRepositoryHealthResponse, error) {
	response, _, err := s.repositoryHealthCache.refresh.Do(
		ctx, key, func(ctx context.Context) (*tbpb.GetRepositoryHealthResponse, error) {
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
	return proto.Clone(response).(*tbpb.GetRepositoryHealthResponse), nil
}

func (s *Service) queryRepositoryHealth(ctx context.Context, groupID, repository string) (*tbpb.GetRepositoryHealthResponse, error) {
	summary := func(name, catalogTable, stateTable string, target bool) (*tbpb.TestHealthSummary, error) {
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
		out := &tbpb.TestHealthSummary{}
		totalDurationUsec := int64(0)
		rq := s.env.GetDBHandle().NewQuery(ctx, name).Raw(query, groupID, repository)
		err := db.ScanEach(rq, func(ctx context.Context, r *row) error {
			out.TotalCount += r.SubjectCount
			switch health(r.Health) {
			case tbpb.TestHealth_TEST_HEALTH_HEALTHY:
				out.HealthyCount += r.SubjectCount
			case tbpb.TestHealth_TEST_HEALTH_FLAKY:
				out.FlakyCount += r.SubjectCount
			case tbpb.TestHealth_TEST_HEALTH_TIMEOUT:
				out.TimedOutCount += r.SubjectCount
			case tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA:
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
	targets, err := summary("test_buddy_get_repository_target_health", "TestTargets", "TestTargetStates", true)
	if err != nil {
		return nil, err
	}
	cases, err := summary("test_buddy_get_repository_case_health", "TestCases", "TestCaseStates", false)
	if err != nil {
		return nil, err
	}
	if targets.GetTotalCount() == 0 && cases.GetTotalCount() == 0 {
		return nil, status.NotFoundErrorf("repository %s was not found", repository)
	}
	return &tbpb.GetRepositoryHealthResponse{Targets: targets, Cases: cases}, nil
}

func (s *Service) GetTestTarget(ctx context.Context, req *tbpb.GetTestTargetRequest) (*tbpb.GetTestTargetResponse, error) {
	if req == nil || req.GetIdentity() == nil {
		return nil, status.InvalidArgumentError("test target identity is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	target, err := identity.CanonicalizeTarget(req.GetRepoUrl(), req.GetIdentity().GetTargetLabel())
	if err != nil {
		return nil, err
	}
	type targetRow struct {
		Health            string
		RecentResults     []byte
		PassCount         int64
		FailCount         int64
		TimeoutCount      int64
		TotalDurationUsec int64
	}
	row := &targetRow{}
	err = s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_test_target").Raw(`
		SELECT COALESCE(s.health, ?) AS health, s.recent_results,
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
		tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), groupID,
		target.Repository, target.Label()).Take(row)
	if db.IsRecordNotFound(err) {
		return nil, status.NotFoundErrorf("test target %s was not found", target.String())
	}
	if err != nil {
		return nil, err
	}
	rsp := &tbpb.GetTestTargetResponse{
		Target: targetSummary(target, row.Health, row.PassCount, row.FailCount,
			row.TimeoutCount, row.TotalDurationUsec),
	}
	samples, err := decodeSamples(row.RecentResults)
	if err != nil {
		return nil, err
	}
	for i := len(samples) - 1; i >= 0; i-- {
		sample := samples[i]
		rsp.RecentResults = append(rsp.RecentResults, &tbpb.TestResult{
			Outcome: sample.Outcome, DurationUsec: sample.DurationUsec,
			FailureMessage: sample.FailureMessage, SourceUrl: sample.SourceURL,
		})
	}
	type transitionRow struct {
		PreviousHealth string
		Health         string
		EventTimeUsec  int64
	}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_test_target_transitions").Raw(`
		SELECT previous_health, health, event_time_usec
		FROM "TestTargetStateChanges"
		WHERE group_id = ? AND repository = ? AND target_label = ?
		ORDER BY state_version DESC
		LIMIT 100`,
		groupID, target.Repository, target.Label())
	if err := db.ScanEach(rq, func(ctx context.Context, r *transitionRow) error {
		rsp.Transitions = append(rsp.Transitions, &tbpb.TestHealthTransition{
			PreviousHealth: health(r.PreviousHealth), Health: health(r.Health),
			EventTimeUsec: r.EventTimeUsec,
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return rsp, nil
}

func (s *Service) GetTestCase(ctx context.Context, req *tbpb.GetTestCaseRequest) (*tbpb.GetTestCaseResponse, error) {
	if req == nil || req.GetIdentity() == nil {
		return nil, status.InvalidArgumentError("test case identity is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	testCase, err := identity.CaseAddressFromProto(req.GetRepoUrl(), req.GetIdentity())
	if err != nil {
		return nil, err
	}
	type caseRow struct {
		Health            string
		RecentResults     []byte
		PassCount         int64
		FailCount         int64
		TimeoutCount      int64
		TotalDurationUsec int64
	}
	row := &caseRow{}
	err = s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_test_case").Raw(`
		SELECT COALESCE(s.health, ?) AS health, s.recent_results,
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
		tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), groupID,
		testCase.Repository, testCase.Target().Label(), testCase.CaseName).Take(row)
	if db.IsRecordNotFound(err) {
		return nil, status.NotFoundErrorf("test case %s was not found", testCase.String())
	}
	if err != nil {
		return nil, err
	}
	rsp := &tbpb.GetTestCaseResponse{
		Test: caseSummary(testCase, row.Health, row.PassCount, row.FailCount, row.TimeoutCount, row.TotalDurationUsec),
	}
	samples, err := decodeSamples(row.RecentResults)
	if err != nil {
		return nil, err
	}
	for i := len(samples) - 1; i >= 0; i-- {
		sample := samples[i]
		rsp.RecentResults = append(rsp.RecentResults, &tbpb.TestResult{
			Outcome: sample.Outcome, DurationUsec: sample.DurationUsec,
			FailureMessage: sample.FailureMessage, SourceUrl: sample.SourceURL,
		})
	}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_test_case_transitions").Raw(`
		SELECT previous_health, health, event_time_usec
		FROM "TestCaseStateChanges"
		WHERE group_id = ? AND repository = ? AND target_label = ? AND case_name = ?
		ORDER BY state_version DESC
		LIMIT 100`,
		groupID, testCase.Repository, testCase.Target().Label(), testCase.CaseName)
	type transitionRow struct {
		PreviousHealth string
		Health         string
		EventTimeUsec  int64
	}
	err = db.ScanEach(rq, func(ctx context.Context, row *transitionRow) error {
		rsp.Transitions = append(rsp.Transitions, &tbpb.TestHealthTransition{
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

func testSummary(healthValue string, passCount, failCount, timeoutCount, totalDurationUsec int64) *tbpb.TestSummary {
	result := &tbpb.TestSummary{
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

func caseSummary(address identity.CaseAddress, healthValue string, passCount, failCount, timeoutCount, totalDurationUsec int64) *tbpb.TestCaseSummary {
	return &tbpb.TestCaseSummary{
		Identity: identity.CaseProto(address),
		Summary:  testSummary(healthValue, passCount, failCount, timeoutCount, totalDurationUsec),
	}
}

func targetSummary(address identity.TargetAddress, healthValue string, passCount, failCount, timeoutCount, totalDurationUsec int64) *tbpb.TestTargetSummary {
	return &tbpb.TestTargetSummary{
		Identity: identity.TargetProto(address),
		Summary:  testSummary(healthValue, passCount, failCount, timeoutCount, totalDurationUsec),
	}
}

func health(value string) tbpb.TestHealth {
	if number, ok := tbpb.TestHealth_value[value]; ok {
		return tbpb.TestHealth(number)
	}
	return tbpb.TestHealth_TEST_HEALTH_UNKNOWN
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
	return identity.CanonicalizePackagePath(raw)
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
