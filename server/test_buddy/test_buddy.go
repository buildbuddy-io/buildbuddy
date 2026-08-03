// Package test_buddy implements synchronous test result reporting and reads.
package test_buddy

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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
	queryBatchSize          = 500
	reportBatchSize         = 500
	retainedResultIDLimit   = 200
	defaultAnalyzerRevision = 1
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

func (p *proxy) ReportTestResults(stream tbpb.TestBuddyService_ReportTestResultsServer) error {
	client, err := p.client.ReportTestResults(forwardAuth(stream.Context(), p.authenticator))
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			rsp, err := client.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(rsp)
		}
		if err != nil {
			return err
		}
		if err := client.Send(req); err != nil {
			return err
		}
	}
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

func (p *proxy) GetTestRepositories(ctx context.Context, req *tbpb.GetTestRepositoriesRequest) (*tbpb.GetTestRepositoriesResponse, error) {
	return p.client.GetTestRepositories(forwardAuth(ctx, p.authenticator), req)
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

func (s *Service) reportTestResults(ctx context.Context, req *tbpb.ReportTestResultsRequest) (*tbpb.ReportTestResultsResponse, error) {
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
	analyzerConfig, analyzerRevision, err := s.analyzerConfig(ctx, groupID, report.RepositoryURL)
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
				if err := s.applyCase(groupCtx, groupID, result, analyzerConfig, analyzerRevision); err != nil {
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
				if err := s.applyTarget(groupCtx, groupID, result, analyzerConfig, analyzerRevision); err != nil {
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

func (s *Service) ReportTestResults(stream tbpb.TestBuddyService_ReportTestResultsServer) error {
	rsp := &tbpb.ReportTestResultsResponse{}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(rsp)
		}
		if err != nil {
			return err
		}
		batch, err := s.reportTestResults(stream.Context(), req)
		if err != nil {
			return err
		}
		rsp.AcceptedCount += batch.GetAcceptedCount()
		rsp.RejectedCount += batch.GetRejectedCount()
	}
}

func admitCatalog(ctx context.Context, database interfaces.DB, groupID string, report *normalize.Report) error {
	repository := report.RepositoryURL
	if err := database.GORM(ctx, "test_buddy_admit_repository").
		Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"updated_at_usec"})}).
		Create(&tables.TestRepositoryCatalog{GroupID: groupID, Repository: repository}).Error; err != nil {
		return err
	}
	targets := make(map[identity.TargetAddress]*tables.TestTarget)
	for _, result := range report.CaseResults {
		target := result.Address.Target()
		targets[target] = &tables.TestTarget{
			GroupID: groupID, Repository: repository, TargetLabel: target.Label(),
			PackagePath: target.PackagePath,
		}
	}
	for _, result := range report.TargetResults {
		target := result.Address
		targets[target] = &tables.TestTarget{
			GroupID: groupID, Repository: repository, TargetLabel: target.Label(),
			PackagePath: target.PackagePath,
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
	for start := 0; start < len(report.CaseResults); start += reportBatchSize {
		end := min(start+reportBatchSize, len(report.CaseResults))
		cases := make([]*tables.TestCase, 0, end-start)
		for _, result := range report.CaseResults[start:end] {
			address := result.Address
			caseName, err := identity.CaseNameKey(address.CaseName)
			if err != nil {
				return err
			}
			cases = append(cases, &tables.TestCase{
				GroupID: groupID, Repository: repository, TargetLabel: address.Target().Label(),
				CaseName: caseName, PackagePath: address.PackagePath,
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
	EventTimeUsec  int64            `json:"t"`
	ResultID       string           `json:"i"`
}

type retainedResultID struct {
	ID          string `json:"i"`
	Fingerprint string `json:"f"`
}

type retainedResults struct {
	Samples   []retainedSample   `json:"s"`
	ResultIDs []retainedResultID `json:"i"`
}

func decodeRetainedResults(encoded []byte) (*retainedResults, error) {
	if len(encoded) == 0 {
		return &retainedResults{}, nil
	}
	results := &retainedResults{}
	if err := json.Unmarshal(encoded, results); err != nil {
		return nil, status.InternalErrorf("decode recent test results: %s", err)
	}
	return results, nil
}

func appendSample(encoded []byte, result *tbpb.TestResult, windowSize int) (*retainedResults, []byte, bool, error) {
	results, err := decodeRetainedResults(encoded)
	if err != nil {
		return nil, nil, false, err
	}
	fingerprint := resultFingerprint(result)
	for _, retained := range results.ResultIDs {
		if retained.ID != result.GetResultId() {
			continue
		}
		if retained.Fingerprint != fingerprint {
			return nil, nil, false, status.FailedPreconditionErrorf(
				"result_id %q was reused with different content", result.GetResultId())
		}
		return results, encoded, true, nil
	}
	results.Samples = append(results.Samples, retainedSample{
		Outcome: result.GetOutcome(), DurationUsec: result.GetDurationUsec(),
		FailureMessage: result.GetFailureMessage(), SourceURL: result.GetSourceUrl(),
		EventTimeUsec: result.GetEventTimeUsec(), ResultID: result.GetResultId(),
	})
	if extra := len(results.Samples) - windowSize; extra > 0 {
		results.Samples = results.Samples[extra:]
	}
	results.ResultIDs = append(results.ResultIDs, retainedResultID{
		ID: result.GetResultId(), Fingerprint: fingerprint,
	})
	if extra := len(results.ResultIDs) - retainedResultIDLimit; extra > 0 {
		results.ResultIDs = results.ResultIDs[extra:]
	}
	encoded, err = json.Marshal(results)
	return results, encoded, false, err
}

func resultFingerprint(result *tbpb.TestResult) string {
	h := sha256.New()
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(result.GetOutcome()))
	_, _ = h.Write(encoded[:])
	binary.BigEndian.PutUint64(encoded[:], uint64(result.GetDurationUsec()))
	_, _ = h.Write(encoded[:])
	binary.BigEndian.PutUint64(encoded[:], uint64(result.GetEventTimeUsec()))
	_, _ = h.Write(encoded[:])
	for _, value := range []string{result.GetFailureMessage(), result.GetSourceUrl()} {
		binary.BigEndian.PutUint64(encoded[:], uint64(len(value)))
		_, _ = h.Write(encoded[:])
		_, _ = io.WriteString(h, value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func analysisSamples(samples []retainedSample) []analyzer.Sample {
	out := make([]analyzer.Sample, 0, len(samples))
	for _, sample := range samples {
		out = append(out, analyzer.Sample{Outcome: sample.Outcome})
	}
	return out
}

func (s *Service) applyCase(ctx context.Context, groupID string, result *normalize.CaseResult, analyzerConfig *tbpb.TestAnalyzerConfig, analyzerRevision int64) error {
	return s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		address := result.Address
		targetLabel := address.Target().Label()
		caseName, err := identity.CaseNameKey(address.CaseName)
		if err != nil {
			return err
		}
		if err := tx.GORM(ctx, "test_buddy_admit_state").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(&tables.TestCaseState{
				GroupID: groupID, Repository: address.Repository, TargetLabel: targetLabel,
				CaseName: caseName, Health: tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(),
				RecentResults: []byte("{}"),
			}).Error; err != nil {
			return err
		}
		state := &tables.TestCaseState{}
		query := `SELECT * FROM "TestCaseStates"
			WHERE group_id = ? AND repository = ? AND target_label = ? AND case_name = ?` +
			s.env.GetDBHandle().SelectForUpdateModifier()
		if err := tx.NewQuery(ctx, "test_buddy_lock_case_state").Raw(
			query, groupID, address.Repository, targetLabel, caseName).Take(state); err != nil {
			return err
		}
		resultInfo := result.Result.GetResult()
		retained, encoded, duplicate, err := appendSample(
			state.RecentResults, resultInfo, int(analyzerConfig.GetLinear().GetWindowSize()))
		if err != nil {
			return err
		}
		if duplicate {
			return nil
		}
		analysis, err := analyzer.Linear(analysisSamples(retained.Samples), analyzerConfig)
		if err != nil {
			return err
		}
		previousHealth := state.Health
		state.Health = analysis.Health.String()
		state.StateVersion++
		state.AnalyzerRevision = analyzerRevision
		state.AnalysisReason = string(analysis.Reason)
		state.EligibleSampleCount = int64(analysis.Evidence.EligibleSamples)
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
			CaseName: caseName, StateVersion: state.StateVersion,
			PreviousHealth: previousHealth, Health: state.Health,
			PassCount: state.PassCount, FailCount: state.FailCount, TimeoutCount: state.TimeoutCount,
			EventTimeUsec: tx.NowFunc().UnixMicro(), AnalyzerRevision: state.AnalyzerRevision,
			AnalysisReason: state.AnalysisReason, EligibleSampleCount: state.EligibleSampleCount,
		})
	})
}

func (s *Service) applyTarget(ctx context.Context, groupID string, result *normalize.TargetResult, analyzerConfig *tbpb.TestAnalyzerConfig, analyzerRevision int64) error {
	return s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		address := result.Address
		targetLabel := address.Label()
		if err := tx.GORM(ctx, "test_buddy_admit_target_state").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(&tables.TestTargetState{
				GroupID: groupID, Repository: address.Repository, TargetLabel: targetLabel,
				Health: tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), RecentResults: []byte("{}"),
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
		retained, encoded, duplicate, err := appendSample(
			state.RecentResults, resultInfo, int(analyzerConfig.GetLinear().GetWindowSize()))
		if err != nil {
			return err
		}
		if duplicate {
			return nil
		}
		analysis, err := analyzer.LinearTarget(analysisSamples(retained.Samples), analyzerConfig)
		if err != nil {
			return err
		}
		previousHealth := state.Health
		state.Health = analysis.Health.String()
		state.StateVersion++
		state.AnalyzerRevision = analyzerRevision
		state.AnalysisReason = string(analysis.Reason)
		state.EligibleSampleCount = int64(analysis.Evidence.EligibleSamples)
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
			EventTimeUsec: tx.NowFunc().UnixMicro(), AnalyzerRevision: state.AnalyzerRevision,
			AnalysisReason: state.AnalysisReason, EligibleSampleCount: state.EligibleSampleCount,
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
	analyzerConfig, revision, err := s.analyzerConfig(ctx, groupID, repository)
	if err != nil {
		return nil, err
	}
	return &tbpb.GetTestAnalyzerConfigResponse{Config: analyzerConfig, Revision: revision}, nil
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
	return &tbpb.SetTestAnalyzerConfigResponse{Config: analyzerConfig, Revision: row.Revision}, nil
}

func (s *Service) analyzerConfig(ctx context.Context, groupID, repository string) (*tbpb.TestAnalyzerConfig, int64, error) {
	row := &tables.TestAnalyzerConfig{}
	err := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_analyzer_config").Raw(`
		SELECT revision, config
		FROM "TestAnalyzerConfigs"
		WHERE group_id = ? AND repository = ?`,
		groupID, repository).Take(row)
	if db.IsRecordNotFound(err) {
		return config.Default(), defaultAnalyzerRevision, nil
	}
	if err != nil {
		return nil, 0, err
	}
	analyzerConfig := &tbpb.TestAnalyzerConfig{}
	if err := proto.Unmarshal(row.Config, analyzerConfig); err != nil {
		return nil, 0, status.InternalErrorf("decode test analyzer config: %s", err)
	}
	if err := config.Validate(analyzerConfig); err != nil {
		return nil, 0, status.InternalErrorf("invalid stored test analyzer config: %s", err)
	}
	return analyzerConfig, row.Revision, nil
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
	args := []any{groupID, repository}
	if packagePrefix != "" {
		bound, boundArgs := conePackageBounds("tc.package_path", packagePrefix)
		where += ` AND ` + bound
		args = append(args, boundArgs...)
	}
	if req.GetTarget() != nil {
		target, err := identity.CanonicalizeTarget(repository, req.GetTarget().GetTargetLabel())
		if err != nil {
			return err
		}
		where += ` AND tc.target_label = ?`
		args = append(args, target.Label())
	}
	args = append(args,
		tbpb.TestHealth_TEST_HEALTH_FAILING.String(),
		tbpb.TestHealth_TEST_HEALTH_FLAKY.String(),
		tbpb.TestHealth_TEST_HEALTH_TIMEOUT.String(),
		tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA.String(),
		tbpb.TestHealth_TEST_HEALTH_HEALTHY.String(),
	)
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
		ORDER BY CASE s.health
			WHEN ? THEN 0
			WHEN ? THEN 1
			WHEN ? THEN 2
			WHEN ? THEN 3
			WHEN ? THEN 4
			ELSE 5 END,
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
		caseName, err := identity.CaseNameFromKey(r.CaseName)
		if err != nil {
			return err
		}
		rsp.Tests = append(rsp.Tests, caseSummary(
			identity.CaseAddress{
				TargetAddress: target, CaseName: caseName,
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
	args := []any{groupID, repository}
	if packagePrefix != "" {
		bound, boundArgs := conePackageBounds("tt.package_path", packagePrefix)
		where += ` AND ` + bound
		args = append(args, boundArgs...)
	}
	queryArgs := []any{
		tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(),
		tbpb.TestHealth_TEST_HEALTH_FAILING.String(),
		tbpb.TestHealth_TEST_HEALTH_FLAKY.String(),
		tbpb.TestHealth_TEST_HEALTH_TIMEOUT.String(),
		tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA.String(),
		tbpb.TestHealth_TEST_HEALTH_HEALTHY.String(),
		tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(),
	}
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs,
		tbpb.TestHealth_TEST_HEALTH_FAILING.String(),
		tbpb.TestHealth_TEST_HEALTH_FLAKY.String(),
		tbpb.TestHealth_TEST_HEALTH_TIMEOUT.String(),
		tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA.String(),
		tbpb.TestHealth_TEST_HEALTH_HEALTHY.String(),
	)
	query := fmt.Sprintf(`
		SELECT * FROM (
		SELECT tt.target_label,
			COALESCE(s.health, ?) AS health,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec,
			COUNT(tc.case_name) AS case_total_count,
			SUM(CASE WHEN tc.case_name IS NOT NULL AND cs.health = ? THEN 1 ELSE 0 END) AS case_failing_count,
			SUM(CASE WHEN tc.case_name IS NOT NULL AND cs.health = ? THEN 1 ELSE 0 END) AS case_flaky_count,
			SUM(CASE WHEN tc.case_name IS NOT NULL AND cs.health = ? THEN 1 ELSE 0 END) AS case_timed_out_count,
			SUM(CASE WHEN tc.case_name IS NOT NULL AND cs.health = ? THEN 1 ELSE 0 END) AS case_insufficient_data_count,
			SUM(CASE WHEN tc.case_name IS NOT NULL AND cs.health = ? THEN 1 ELSE 0 END) AS case_healthy_count,
			SUM(CASE WHEN tc.case_name IS NOT NULL AND (cs.health IS NULL OR cs.health = ?) THEN 1 ELSE 0 END) AS case_unknown_count,
			COALESCE(SUM(cs.pass_count), 0) AS case_pass_count,
			COALESCE(SUM(cs.fail_count), 0) AS case_fail_count,
			COALESCE(SUM(cs.timeout_count), 0) AS case_timeout_count,
			COALESCE(SUM(cs.total_duration_usec), 0) AS case_total_duration_usec
		FROM "TestTargets" AS tt
		LEFT JOIN "TestTargetStates" AS s
			ON s.group_id = tt.group_id AND s.repository = tt.repository
			AND s.target_label = tt.target_label
		LEFT JOIN "TestCases" AS tc
			ON tc.group_id = tt.group_id AND tc.repository = tt.repository
			AND tc.target_label = tt.target_label
		LEFT JOIN "TestCaseStates" AS cs
			ON cs.group_id = tc.group_id AND cs.repository = tc.repository
			AND cs.target_label = tc.target_label AND cs.case_name = tc.case_name
		WHERE %s
		GROUP BY tt.target_label, s.health, s.pass_count, s.fail_count,
			s.timeout_count, s.total_duration_usec
		) AS target_health
		ORDER BY CASE
			WHEN health = ? OR case_failing_count > 0 THEN 0
			WHEN health = ? OR case_flaky_count > 0 THEN 1
			WHEN health = ? OR case_timed_out_count > 0 THEN 2
			WHEN health = ? OR case_insufficient_data_count > 0 THEN 3
			WHEN health = ? OR case_healthy_count > 0 THEN 4
			ELSE 5 END,
			COALESCE(total_duration_usec * 1.0 /
				NULLIF(pass_count + fail_count + timeout_count, 0), 0) DESC,
			COALESCE(pass_count * 1.0 /
				NULLIF(pass_count + fail_count + timeout_count, 0), 1) ASC,
			target_label`, where)
	type row struct {
		TargetLabel               string
		Health                    string
		PassCount                 int64
		FailCount                 int64
		TimeoutCount              int64
		TotalDurationUsec         int64
		CaseTotalCount            int64
		CaseFailingCount          int64
		CaseFlakyCount            int64
		CaseTimedOutCount         int64
		CaseInsufficientDataCount int64
		CaseHealthyCount          int64
		CaseUnknownCount          int64
		CasePassCount             int64
		CaseFailCount             int64
		CaseTimeoutCount          int64
		CaseTotalDurationUsec     int64
	}
	rsp := &tbpb.GetTestTargetsResponse{Targets: make([]*tbpb.TestTargetSummary, 0, queryBatchSize)}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_test_targets").Raw(query, queryArgs...)
	err = db.ScanEach(rq, func(ctx context.Context, r *row) error {
		target, err := identity.CanonicalizeTarget(repository, r.TargetLabel)
		if err != nil {
			return err
		}
		cases := &tbpb.TestHealthSummary{
			TotalCount: r.CaseTotalCount, FailingCount: r.CaseFailingCount,
			FlakyCount: r.CaseFlakyCount, TimedOutCount: r.CaseTimedOutCount,
			InsufficientDataCount: r.CaseInsufficientDataCount,
			HealthyCount:          r.CaseHealthyCount, UnknownCount: r.CaseUnknownCount,
			PassCount: r.CasePassCount, FailCount: r.CaseFailCount,
			TimeoutCount: r.CaseTimeoutCount,
		}
		if total := cases.GetPassCount() + cases.GetFailCount() + cases.GetTimeoutCount(); total > 0 {
			cases.PassRate = float64(cases.GetPassCount()) / float64(total)
			cases.MeanDurationUsec = r.CaseTotalDurationUsec / total
		}
		summary := targetSummary(
			target, r.Health, r.PassCount, r.FailCount, r.TimeoutCount, r.TotalDurationUsec)
		summary.Cases = cases
		rsp.Targets = append(rsp.Targets, summary)
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

func (s *Service) GetTestRepositories(ctx context.Context, req *tbpb.GetTestRepositoriesRequest) (*tbpb.GetTestRepositoriesResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentError("request is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	type row struct {
		Repository    string
		UpdatedAtUsec int64
	}
	rsp := &tbpb.GetTestRepositoriesResponse{}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_repositories").Raw(`
		SELECT repository, updated_at_usec
		FROM "TestRepositoryCatalogs"
		WHERE group_id = ?
		ORDER BY updated_at_usec DESC, repository`, groupID)
	if err := db.ScanEach(rq, func(ctx context.Context, r *row) error {
		rsp.Repositories = append(rsp.Repositories, &tbpb.TestRepository{
			RepoUrl: r.Repository, LastReportedAtUsec: r.UpdatedAtUsec,
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return rsp, nil
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
			case tbpb.TestHealth_TEST_HEALTH_FAILING:
				out.FailingCount += r.SubjectCount
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
		Health              string
		RecentResults       []byte
		PassCount           int64
		FailCount           int64
		TimeoutCount        int64
		TotalDurationUsec   int64
		AnalyzerRevision    int64
		AnalysisReason      string
		EligibleSampleCount int64
	}
	row := &targetRow{}
	err = s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_test_target").Raw(`
		SELECT COALESCE(s.health, ?) AS health, s.recent_results,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec,
			COALESCE(s.analyzer_revision, 0) AS analyzer_revision,
			COALESCE(s.analysis_reason, '') AS analysis_reason,
			COALESCE(s.eligible_sample_count, 0) AS eligible_sample_count
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
		AnalyzerRevision: row.AnalyzerRevision, AnalysisReason: row.AnalysisReason,
		EligibleSampleCount: row.EligibleSampleCount,
	}
	retained, err := decodeRetainedResults(row.RecentResults)
	if err != nil {
		return nil, err
	}
	for i := len(retained.Samples) - 1; i >= 0; i-- {
		sample := retained.Samples[i]
		rsp.RecentResults = append(rsp.RecentResults, &tbpb.TestResult{
			Outcome: sample.Outcome, DurationUsec: sample.DurationUsec,
			FailureMessage: sample.FailureMessage, SourceUrl: sample.SourceURL,
			EventTimeUsec: sample.EventTimeUsec, ResultId: sample.ResultID,
		})
	}
	type transitionRow struct {
		PreviousHealth      string
		Health              string
		EventTimeUsec       int64
		AnalyzerRevision    int64
		AnalysisReason      string
		EligibleSampleCount int64
	}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_test_target_transitions").Raw(`
		SELECT previous_health, health, event_time_usec,
			analyzer_revision, analysis_reason, eligible_sample_count
		FROM "TestTargetStateChanges"
		WHERE group_id = ? AND repository = ? AND target_label = ?
		ORDER BY state_version DESC
		LIMIT 100`,
		groupID, target.Repository, target.Label())
	if err := db.ScanEach(rq, func(ctx context.Context, r *transitionRow) error {
		rsp.Transitions = append(rsp.Transitions, &tbpb.TestHealthTransition{
			PreviousHealth: health(r.PreviousHealth), Health: health(r.Health),
			EventTimeUsec: r.EventTimeUsec, AnalyzerRevision: r.AnalyzerRevision,
			AnalysisReason: r.AnalysisReason, EligibleSampleCount: r.EligibleSampleCount,
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
	caseName, err := identity.CaseNameKey(testCase.CaseName)
	if err != nil {
		return nil, err
	}
	type caseRow struct {
		Health              string
		RecentResults       []byte
		PassCount           int64
		FailCount           int64
		TimeoutCount        int64
		TotalDurationUsec   int64
		AnalyzerRevision    int64
		AnalysisReason      string
		EligibleSampleCount int64
	}
	row := &caseRow{}
	err = s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_test_case").Raw(`
		SELECT COALESCE(s.health, ?) AS health, s.recent_results,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec,
			COALESCE(s.analyzer_revision, 0) AS analyzer_revision,
			COALESCE(s.analysis_reason, '') AS analysis_reason,
			COALESCE(s.eligible_sample_count, 0) AS eligible_sample_count
		FROM "TestCases" AS tc
		LEFT JOIN "TestCaseStates" AS s
			ON s.group_id = tc.group_id AND s.repository = tc.repository
			AND s.target_label = tc.target_label AND s.case_name = tc.case_name
		WHERE tc.group_id = ? AND tc.repository = ?
			AND tc.target_label = ? AND tc.case_name = ?`,
		tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), groupID,
		testCase.Repository, testCase.Target().Label(), caseName).Take(row)
	if db.IsRecordNotFound(err) {
		return nil, status.NotFoundErrorf("test case %s was not found", testCase.String())
	}
	if err != nil {
		return nil, err
	}
	rsp := &tbpb.GetTestCaseResponse{
		Test:             caseSummary(testCase, row.Health, row.PassCount, row.FailCount, row.TimeoutCount, row.TotalDurationUsec),
		AnalyzerRevision: row.AnalyzerRevision, AnalysisReason: row.AnalysisReason,
		EligibleSampleCount: row.EligibleSampleCount,
	}
	retained, err := decodeRetainedResults(row.RecentResults)
	if err != nil {
		return nil, err
	}
	for i := len(retained.Samples) - 1; i >= 0; i-- {
		sample := retained.Samples[i]
		rsp.RecentResults = append(rsp.RecentResults, &tbpb.TestResult{
			Outcome: sample.Outcome, DurationUsec: sample.DurationUsec,
			FailureMessage: sample.FailureMessage, SourceUrl: sample.SourceURL,
			EventTimeUsec: sample.EventTimeUsec, ResultId: sample.ResultID,
		})
	}
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_test_case_transitions").Raw(`
		SELECT previous_health, health, event_time_usec,
			analyzer_revision, analysis_reason, eligible_sample_count
		FROM "TestCaseStateChanges"
		WHERE group_id = ? AND repository = ? AND target_label = ? AND case_name = ?
		ORDER BY state_version DESC
		LIMIT 100`,
		groupID, testCase.Repository, testCase.Target().Label(), caseName)
	type transitionRow struct {
		PreviousHealth      string
		Health              string
		EventTimeUsec       int64
		AnalyzerRevision    int64
		AnalysisReason      string
		EligibleSampleCount int64
	}
	err = db.ScanEach(rq, func(ctx context.Context, row *transitionRow) error {
		rsp.Transitions = append(rsp.Transitions, &tbpb.TestHealthTransition{
			PreviousHealth:      health(row.PreviousHealth),
			Health:              health(row.Health),
			EventTimeUsec:       row.EventTimeUsec,
			AnalyzerRevision:    row.AnalyzerRevision,
			AnalysisReason:      row.AnalysisReason,
			EligibleSampleCount: row.EligibleSampleCount,
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

// conePackageBounds returns a predicate and its bind values selecting every
// package at or beneath prefix from an indexed package_path column.
//
// A cone is the prefix's own package plus everything below prefix + "/". In byte
// order that second set is exactly the half-open range [prefix+"/", prefix+"0"),
// because '0' is the byte immediately after '/'. Bounding on the separator is
// what makes the range component-aware: "a/bc" sorts after "a/b0" and so stays
// out of the "a/b" cone, where a plain "package_path LIKE 'a/b%'" would wrongly
// pull it in. Both bounds are index-ordered, so a cone read is a range scan on
// (group_id, repository, package_path) rather than a scan of the repository.
//
// The columns are declared ascii_bin, so SQL compares them in the same byte
// order this bound assumes; under a case-insensitive collation "//a/B" would
// fall inside the "//a/b" cone, which Bazel says is a different package.
func conePackageBounds(column, prefix string) (string, []any) {
	predicate := fmt.Sprintf(`(%s = ? OR (%s >= ? AND %s < ?))`, column, column, column)
	return predicate, []any{prefix, prefix + "/", prefix + "0"}
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
