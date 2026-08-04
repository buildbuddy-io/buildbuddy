// Package test_buddy implements synchronous test observation reporting and reads.
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
	queryBatchSize             = 500
	reportBatchSize            = 500
	retainedObservationIDLimit = 200
	defaultAnalyzerRevision    = 1
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

func (c *repositoryHealthCache) invalidate(key repositoryHealthCacheKey) {
	c.mu.Lock()
	delete(c.entries, key)
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

func (p *proxy) SetTestExecutionDisposition(ctx context.Context, req *tbpb.SetTestExecutionDispositionRequest) (*tbpb.SetTestExecutionDispositionResponse, error) {
	return p.client.SetTestExecutionDisposition(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) SetTestDeleted(ctx context.Context, req *tbpb.SetTestDeletedRequest) (*tbpb.SetTestDeletedResponse, error) {
	return p.client.SetTestDeleted(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) GetTestsToSkip(req *tbpb.GetTestsToSkipRequest, stream tbpb.TestBuddyService_GetTestsToSkipServer) error {
	client, err := p.client.GetTestsToSkip(forwardAuth(stream.Context(), p.authenticator), req)
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

func (p *proxy) GetRepositoryHealth(ctx context.Context, req *tbpb.GetRepositoryHealthRequest) (*tbpb.GetRepositoryHealthResponse, error) {
	return p.client.GetRepositoryHealth(forwardAuth(ctx, p.authenticator), req)
}

func (p *proxy) GetFailureAnalysisProgress(ctx context.Context, req *tbpb.GetFailureAnalysisProgressRequest) (*tbpb.GetFailureAnalysisProgressResponse, error) {
	return p.client.GetFailureAnalysisProgress(forwardAuth(ctx, p.authenticator), req)
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
	report, err := normalize.Normalize(req.GetRepoUrl(), req.GetCaseObservations(), req.GetTargetObservations())
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
	observationsByCase := make(map[identity.CaseAddress][]*normalize.CaseObservation)
	for _, observation := range report.CaseObservations {
		if analyzer.Eligible(analyzer.Sample{Outcome: observation.Observation.GetObservation().GetOutcome()}) {
			observationsByCase[observation.Address] = append(observationsByCase[observation.Address], observation)
		}
	}
	for _, observations := range observationsByCase {
		group.Go(func() error {
			return s.applyCaseObservations(groupCtx, groupID, observations, analyzerConfig, analyzerRevision)
		})
	}
	observationsByTarget := make(map[identity.TargetAddress][]*normalize.TargetObservation)
	for _, observation := range report.TargetObservations {
		if analyzer.Eligible(analyzer.Sample{Outcome: observation.Observation.GetObservation().GetOutcome()}) {
			observationsByTarget[observation.Address] = append(observationsByTarget[observation.Address], observation)
		}
	}
	for _, observations := range observationsByTarget {
		group.Go(func() error {
			return s.applyTargetObservations(groupCtx, groupID, observations, analyzerConfig, analyzerRevision)
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if err := admitFailureClusters(ctx, s.env.GetDBHandle(), groupID, report); err != nil {
		return nil, err
	}
	return &tbpb.ReportTestResultsResponse{
		AcceptedCount: int32(len(report.CaseObservations) + len(report.TargetObservations)),
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
	for _, observation := range report.CaseObservations {
		target := observation.Address.Target()
		targets[target] = &tables.TestTarget{
			GroupID: groupID, Repository: repository, TargetLabel: target.Label(),
			PackagePath: target.PackagePath,
		}
	}
	for _, observation := range report.TargetObservations {
		target := observation.Address
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
			Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"deleted_at_usec"})}).
			Create(targetRows[start:end]).Error; err != nil {
			return err
		}
	}
	cases := make(map[identity.CaseAddress]*tables.TestCase)
	for _, observation := range report.CaseObservations {
		address := observation.Address
		caseName, err := identity.CaseNameKey(address.CaseName)
		if err != nil {
			return err
		}
		cases[address] = &tables.TestCase{
			GroupID: groupID, Repository: repository, TargetLabel: address.Target().Label(),
			CaseName: caseName, PackagePath: address.PackagePath,
		}
	}
	caseRows := make([]*tables.TestCase, 0, len(cases))
	for _, testCase := range cases {
		caseRows = append(caseRows, testCase)
	}
	for start := 0; start < len(caseRows); start += reportBatchSize {
		end := min(start+reportBatchSize, len(caseRows))
		if err := database.GORM(ctx, "test_buddy_admit_cases").
			Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"deleted_at_usec"})}).
			Create(caseRows[start:end]).Error; err != nil {
			return err
		}
	}
	return nil
}

func admitFailureClusters(ctx context.Context, database interfaces.DB, groupID string, report *normalize.Report) error {
	clusters := make(map[string]*tables.TestFailureCluster)
	add := func(observation *tbpb.TestObservation) {
		fingerprint := observation.GetFailureFingerprint()
		if fingerprint == "" {
			return
		}
		clusters[fingerprint] = &tables.TestFailureCluster{
			GroupID: groupID, Repository: report.RepositoryURL, Fingerprint: fingerprint,
			FailureMessage: []byte(observation.GetFailureMessage()), AnalysisSummary: []byte{},
			SuggestedFix: []byte{},
		}
	}
	for _, observation := range report.CaseObservations {
		add(observation.Observation.GetObservation())
	}
	for _, observation := range report.TargetObservations {
		add(observation.Observation.GetObservation())
	}
	rows := make([]*tables.TestFailureCluster, 0, len(clusters))
	for _, cluster := range clusters {
		rows = append(rows, cluster)
	}
	for start := 0; start < len(rows); start += reportBatchSize {
		end := min(start+reportBatchSize, len(rows))
		if err := database.GORM(ctx, "test_buddy_admit_failure_clusters").
			Clauses(clause.OnConflict{DoNothing: true}).Create(rows[start:end]).Error; err != nil {
			return err
		}
	}
	return nil
}

type retainedObservation struct {
	Outcome            tbpb.TestOutcome           `json:"o"`
	Source             tbpb.TestObservationSource `json:"s"`
	CommitSHA          string                     `json:"c"`
	WorkspaceDirty     bool                       `json:"w,omitempty"`
	DurationUsec       int64                      `json:"d,omitempty"`
	FailureMessage     string                     `json:"f,omitempty"`
	SourceURL          string                     `json:"u"`
	EventTimeUsec      int64                      `json:"t"`
	ObservationID      string                     `json:"i"`
	FailureFingerprint string                     `json:"x,omitempty"`
}

type retainedObservationID struct {
	ID          string `json:"i"`
	Fingerprint string `json:"f"`
}

type retainedObservations struct {
	Observations   []retainedObservation   `json:"s"`
	ObservationIDs []retainedObservationID `json:"i"`
}

func decodeRetainedObservations(encoded []byte) (*retainedObservations, error) {
	if len(encoded) == 0 {
		return &retainedObservations{}, nil
	}
	observations := &retainedObservations{}
	if err := json.Unmarshal(encoded, observations); err != nil {
		return nil, status.InternalErrorf("decode recent test observations: %s", err)
	}
	return observations, nil
}

func appendObservation(observations *retainedObservations, observation *tbpb.TestObservation, windowSize int) (bool, error) {
	fingerprint := observationFingerprint(observation)
	for _, retained := range observations.ObservationIDs {
		if retained.ID != observation.GetObservationId() {
			continue
		}
		if retained.Fingerprint != fingerprint {
			return false, status.FailedPreconditionErrorf(
				"observation_id %q was reused with different content", observation.GetObservationId())
		}
		return true, nil
	}
	observations.Observations = append(observations.Observations, retainedObservation{
		Outcome: observation.GetOutcome(), Source: observation.GetSource(), CommitSHA: observation.GetCommitSha(),
		WorkspaceDirty: observation.GetWorkspaceDirty(), DurationUsec: observation.GetDurationUsec(),
		FailureMessage: observation.GetFailureMessage(), SourceURL: observation.GetSourceUrl(),
		EventTimeUsec: observation.GetEventTimeUsec(), ObservationID: observation.GetObservationId(),
		FailureFingerprint: observation.GetFailureFingerprint(),
	})
	if extra := len(observations.Observations) - windowSize; extra > 0 {
		observations.Observations = observations.Observations[extra:]
	}
	observations.ObservationIDs = append(observations.ObservationIDs, retainedObservationID{
		ID: observation.GetObservationId(), Fingerprint: fingerprint,
	})
	if extra := len(observations.ObservationIDs) - retainedObservationIDLimit; extra > 0 {
		observations.ObservationIDs = observations.ObservationIDs[extra:]
	}
	return false, nil
}

func observationFingerprint(observation *tbpb.TestObservation) string {
	h := sha256.New()
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(observation.GetOutcome()))
	_, _ = h.Write(encoded[:])
	binary.BigEndian.PutUint64(encoded[:], uint64(observation.GetDurationUsec()))
	_, _ = h.Write(encoded[:])
	binary.BigEndian.PutUint64(encoded[:], uint64(observation.GetEventTimeUsec()))
	_, _ = h.Write(encoded[:])
	binary.BigEndian.PutUint64(encoded[:], uint64(observation.GetSource()))
	_, _ = h.Write(encoded[:])
	if observation.GetWorkspaceDirty() {
		encoded[0] = 1
	} else {
		encoded[0] = 0
	}
	_, _ = h.Write(encoded[:1])
	for _, value := range []string{observation.GetFailureMessage(), observation.GetSourceUrl(), observation.GetCommitSha()} {
		binary.BigEndian.PutUint64(encoded[:], uint64(len(value)))
		_, _ = h.Write(encoded[:])
		_, _ = io.WriteString(h, value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func analysisSamples(observations []retainedObservation) []analyzer.Sample {
	out := make([]analyzer.Sample, 0, len(observations))
	for _, observation := range observations {
		out = append(out, analyzer.Sample{Outcome: observation.Outcome})
	}
	return out
}

func (s *Service) applyCaseObservations(ctx context.Context, groupID string, observations []*normalize.CaseObservation, analyzerConfig *tbpb.TestAnalyzerConfig, analyzerRevision int64) error {
	if len(observations) == 0 {
		return nil
	}
	return s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		address := observations[0].Address
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
				RecentObservations: []byte("{}"),
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
		retained, err := decodeRetainedObservations(state.RecentObservations)
		if err != nil {
			return err
		}
		changes := make([]*tables.TestCaseStateChange, 0)
		changed := false
		for _, observation := range observations {
			observationInfo := observation.Observation.GetObservation()
			duplicate, err := appendObservation(retained, observationInfo, int(analyzerConfig.GetLinear().GetWindowSize()))
			if err != nil {
				return err
			}
			if duplicate {
				continue
			}
			changed = true
			analysis, err := analyzer.Linear(analysisSamples(retained.Observations), analyzerConfig)
			if err != nil {
				return err
			}
			previousHealth := state.Health
			state.Health = analysis.Health.String()
			state.StateVersion++
			state.AnalyzerRevision = analyzerRevision
			state.AnalysisReason = string(analysis.Reason)
			state.EligibleSampleCount = int64(analysis.Evidence.EligibleSamples)
			switch observationInfo.GetOutcome() {
			case tbpb.TestOutcome_TEST_OUTCOME_PASS:
				state.PassCount++
			case tbpb.TestOutcome_TEST_OUTCOME_FAIL:
				state.FailCount++
			case tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT:
				state.TimeoutCount++
			default:
				return status.InternalError("unknown outcome reached the analyzer")
			}
			state.TotalDurationUsec += observationInfo.GetDurationUsec()
			if previousHealth != state.Health {
				changes = append(changes, &tables.TestCaseStateChange{
					GroupID: groupID, Repository: address.Repository, TargetLabel: targetLabel,
					CaseName: caseName, StateVersion: state.StateVersion,
					PreviousHealth: previousHealth, Health: state.Health,
					PassCount: state.PassCount, FailCount: state.FailCount, TimeoutCount: state.TimeoutCount,
					EventTimeUsec: tx.NowFunc().UnixMicro(), AnalyzerRevision: state.AnalyzerRevision,
					AnalysisReason: state.AnalysisReason, EligibleSampleCount: state.EligibleSampleCount,
				})
			}
		}
		if !changed {
			return nil
		}
		state.RecentObservations, err = json.Marshal(retained)
		if err != nil {
			return err
		}
		if err := tx.GORM(ctx, "test_buddy_update_case_state").Save(state).Error; err != nil {
			return err
		}
		if len(changes) == 0 {
			return nil
		}
		return tx.NewQuery(ctx, "test_buddy_create_case_changes").Create(changes)
	})
}

func (s *Service) applyTargetObservations(ctx context.Context, groupID string, observations []*normalize.TargetObservation, analyzerConfig *tbpb.TestAnalyzerConfig, analyzerRevision int64) error {
	if len(observations) == 0 {
		return nil
	}
	return s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		address := observations[0].Address
		targetLabel := address.Label()
		if err := tx.GORM(ctx, "test_buddy_admit_target_state").
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(&tables.TestTargetState{
				GroupID: groupID, Repository: address.Repository, TargetLabel: targetLabel,
				Health: tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), RecentObservations: []byte("{}"),
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
		retained, err := decodeRetainedObservations(state.RecentObservations)
		if err != nil {
			return err
		}
		changes := make([]*tables.TestTargetStateChange, 0)
		changed := false
		for _, observation := range observations {
			observationInfo := observation.Observation.GetObservation()
			duplicate, err := appendObservation(retained, observationInfo, int(analyzerConfig.GetLinear().GetWindowSize()))
			if err != nil {
				return err
			}
			if duplicate {
				continue
			}
			changed = true
			analysis, err := analyzer.LinearTarget(analysisSamples(retained.Observations), analyzerConfig)
			if err != nil {
				return err
			}
			previousHealth := state.Health
			state.Health = analysis.Health.String()
			state.StateVersion++
			state.AnalyzerRevision = analyzerRevision
			state.AnalysisReason = string(analysis.Reason)
			state.EligibleSampleCount = int64(analysis.Evidence.EligibleSamples)
			switch observationInfo.GetOutcome() {
			case tbpb.TestOutcome_TEST_OUTCOME_PASS:
				state.PassCount++
			case tbpb.TestOutcome_TEST_OUTCOME_FAIL:
				state.FailCount++
			case tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT:
				state.TimeoutCount++
			default:
				return status.InternalError("unknown target outcome reached the analyzer")
			}
			state.TotalDurationUsec += observationInfo.GetDurationUsec()
			if previousHealth != state.Health {
				changes = append(changes, &tables.TestTargetStateChange{
					GroupID: groupID, Repository: address.Repository, TargetLabel: targetLabel,
					StateVersion: state.StateVersion, PreviousHealth: previousHealth, Health: state.Health,
					PassCount: state.PassCount, FailCount: state.FailCount, TimeoutCount: state.TimeoutCount,
					EventTimeUsec: tx.NowFunc().UnixMicro(), AnalyzerRevision: state.AnalyzerRevision,
					AnalysisReason: state.AnalysisReason, EligibleSampleCount: state.EligibleSampleCount,
				})
			}
		}
		if !changed {
			return nil
		}
		state.RecentObservations, err = json.Marshal(retained)
		if err != nil {
			return err
		}
		if err := tx.GORM(ctx, "test_buddy_update_target_state").Save(state).Error; err != nil {
			return err
		}
		if len(changes) == 0 {
			return nil
		}
		return tx.NewQuery(ctx, "test_buddy_create_target_changes").Create(changes)
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
	where := `tc.group_id = ? AND tc.repository = ? AND tc.deleted_at_usec = 0`
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
		SELECT tc.target_label, tc.case_name, tc.disposition,
			COALESCE(s.health, '%s') AS health,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec
		FROM "TestCases" AS tc
		INNER JOIN "TestTargets" AS tt
			ON tt.group_id = tc.group_id AND tt.repository = tc.repository
			AND tt.target_label = tc.target_label AND tt.deleted_at_usec = 0
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
		Disposition       int32
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
			r.Health, r.PassCount, r.FailCount, r.TimeoutCount, r.TotalDurationUsec,
			r.Disposition))
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
	where := `tt.group_id = ? AND tt.repository = ? AND tt.deleted_at_usec = 0`
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
		SELECT tt.target_label, tt.disposition,
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
			AND tc.target_label = tt.target_label AND tc.deleted_at_usec = 0
		LEFT JOIN "TestCaseStates" AS cs
			ON cs.group_id = tc.group_id AND cs.repository = tc.repository
			AND cs.target_label = tc.target_label AND cs.case_name = tc.case_name
		WHERE %s
		GROUP BY tt.target_label, tt.disposition, s.health, s.pass_count, s.fail_count,
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
		Disposition               int32
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
			target, r.Health, r.PassCount, r.FailCount, r.TimeoutCount,
			r.TotalDurationUsec, r.Disposition)
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

func (s *Service) SetTestExecutionDisposition(ctx context.Context, req *tbpb.SetTestExecutionDispositionRequest) (*tbpb.SetTestExecutionDispositionResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentError("request is required")
	}
	if _, ok := tbpb.TestExecutionDisposition_name[int32(req.GetDisposition())]; !ok {
		return nil, status.InvalidArgumentErrorf("unrecognized test execution disposition %d", req.GetDisposition())
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetTarget() != nil {
		target, err := identity.CanonicalizeTarget(req.GetRepoUrl(), req.GetTarget().GetTargetLabel())
		if err != nil {
			return nil, err
		}
		result := s.env.GetDBHandle().GORM(ctx, "test_buddy_set_target_disposition").
			Model(&tables.TestTarget{}).
			Where("group_id = ? AND repository = ? AND target_label = ?",
				groupID, target.Repository, target.Label()).
			Update("disposition", int32(req.GetDisposition()))
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected == 0 {
			var count int64
			err := s.env.GetDBHandle().GORM(ctx, "test_buddy_find_target_disposition").
				Model(&tables.TestTarget{}).
				Where("group_id = ? AND repository = ? AND target_label = ?",
					groupID, target.Repository, target.Label()).Count(&count).Error
			if err != nil {
				return nil, err
			}
			if count == 0 {
				return nil, status.NotFoundErrorf("test target %s was not found", target.String())
			}
		}
	} else if req.GetTestCase() != nil {
		testCase, err := identity.CaseAddressFromProto(req.GetRepoUrl(), req.GetTestCase())
		if err != nil {
			return nil, err
		}
		caseName, err := identity.CaseNameKey(testCase.CaseName)
		if err != nil {
			return nil, err
		}
		result := s.env.GetDBHandle().GORM(ctx, "test_buddy_set_case_disposition").
			Model(&tables.TestCase{}).
			Where("group_id = ? AND repository = ? AND target_label = ? AND case_name = ?",
				groupID, testCase.Repository, testCase.Target().Label(), caseName).
			Update("disposition", int32(req.GetDisposition()))
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected == 0 {
			var count int64
			err := s.env.GetDBHandle().GORM(ctx, "test_buddy_find_case_disposition").
				Model(&tables.TestCase{}).
				Where("group_id = ? AND repository = ? AND target_label = ? AND case_name = ?",
					groupID, testCase.Repository, testCase.Target().Label(), caseName).Count(&count).Error
			if err != nil {
				return nil, err
			}
			if count == 0 {
				return nil, status.NotFoundErrorf("test case %s was not found", testCase.String())
			}
		}
	} else {
		return nil, status.InvalidArgumentError("test target or case identity is required")
	}
	return &tbpb.SetTestExecutionDispositionResponse{Disposition: req.GetDisposition()}, nil
}

func (s *Service) SetTestDeleted(ctx context.Context, req *tbpb.SetTestDeletedRequest) (*tbpb.SetTestDeletedResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentError("request is required")
	}
	groupID, err := s.groupID(ctx)
	if err != nil {
		return nil, err
	}
	deletedAtUsec := int64(0)
	if req.GetDeleted() {
		deletedAtUsec = s.env.GetDBHandle().NowFunc().UnixMicro()
	}
	repository := ""
	if req.GetTarget() != nil {
		target, err := identity.CanonicalizeTarget(req.GetRepoUrl(), req.GetTarget().GetTargetLabel())
		if err != nil {
			return nil, err
		}
		repository = target.Repository
		result := s.env.GetDBHandle().GORM(ctx, "test_buddy_set_target_deleted").
			Model(&tables.TestTarget{}).
			Where("group_id = ? AND repository = ? AND target_label = ?",
				groupID, target.Repository, target.Label()).
			Update("deleted_at_usec", deletedAtUsec)
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected == 0 {
			var count int64
			err := s.env.GetDBHandle().GORM(ctx, "test_buddy_find_target_deleted").
				Model(&tables.TestTarget{}).
				Where("group_id = ? AND repository = ? AND target_label = ?",
					groupID, target.Repository, target.Label()).Count(&count).Error
			if err != nil {
				return nil, err
			}
			if count == 0 {
				return nil, status.NotFoundErrorf("test target %s was not found", target.String())
			}
		}
	} else if req.GetTestCase() != nil {
		testCase, err := identity.CaseAddressFromProto(req.GetRepoUrl(), req.GetTestCase())
		if err != nil {
			return nil, err
		}
		repository = testCase.Repository
		caseName, err := identity.CaseNameKey(testCase.CaseName)
		if err != nil {
			return nil, err
		}
		result := s.env.GetDBHandle().GORM(ctx, "test_buddy_set_case_deleted").
			Model(&tables.TestCase{}).
			Where("group_id = ? AND repository = ? AND target_label = ? AND case_name = ?",
				groupID, testCase.Repository, testCase.Target().Label(), caseName).
			Update("deleted_at_usec", deletedAtUsec)
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected == 0 {
			var count int64
			err := s.env.GetDBHandle().GORM(ctx, "test_buddy_find_case_deleted").
				Model(&tables.TestCase{}).
				Where("group_id = ? AND repository = ? AND target_label = ? AND case_name = ?",
					groupID, testCase.Repository, testCase.Target().Label(), caseName).Count(&count).Error
			if err != nil {
				return nil, err
			}
			if count == 0 {
				return nil, status.NotFoundErrorf("test case %s was not found", testCase.String())
			}
		}
	} else {
		return nil, status.InvalidArgumentError("test target or case identity is required")
	}
	s.repositoryHealthCache.invalidate(repositoryHealthCacheKey{groupID: groupID, repository: repository})
	return &tbpb.SetTestDeletedResponse{Deleted: req.GetDeleted()}, nil
}

func (s *Service) GetTestsToSkip(req *tbpb.GetTestsToSkipRequest, stream tbpb.TestBuddyService_GetTestsToSkipServer) error {
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

	targetWhere := `tt.group_id = ? AND tt.repository = ? AND tt.deleted_at_usec = 0`
	targetArgs := []any{tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), groupID, repository}
	if packagePrefix != "" {
		bound, boundArgs := conePackageBounds("tt.package_path", packagePrefix)
		targetWhere += ` AND ` + bound
		targetArgs = append(targetArgs, boundArgs...)
	}
	targetArgs = append(targetArgs,
		int32(tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_DISABLED),
		int32(tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_AUTOMATIC),
		tbpb.TestHealth_TEST_HEALTH_FAILING.String(),
		tbpb.TestHealth_TEST_HEALTH_FLAKY.String(),
		tbpb.TestHealth_TEST_HEALTH_TIMEOUT.String(),
	)
	targetQuery := fmt.Sprintf(`
		SELECT tt.target_label, tt.disposition,
			COALESCE(s.health, ?) AS health,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec
		FROM "TestTargets" AS tt
		LEFT JOIN "TestTargetStates" AS s
			ON s.group_id = tt.group_id AND s.repository = tt.repository
			AND s.target_label = tt.target_label
		WHERE %s AND (tt.disposition = ? OR
			(tt.disposition = ? AND s.health IN (?, ?, ?)))
		ORDER BY tt.target_label`, targetWhere)
	type targetRow struct {
		TargetLabel       string
		Disposition       int32
		Health            string
		PassCount         int64
		FailCount         int64
		TimeoutCount      int64
		TotalDurationUsec int64
	}
	targetResponse := &tbpb.GetTestsToSkipResponse{
		Targets: make([]*tbpb.TestTargetSummary, 0, queryBatchSize),
	}
	if err := db.ScanEach(
		s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_targets_to_skip").Raw(targetQuery, targetArgs...),
		func(ctx context.Context, row *targetRow) error {
			target, err := identity.CanonicalizeTarget(repository, row.TargetLabel)
			if err != nil {
				return err
			}
			targetResponse.Targets = append(targetResponse.Targets, targetSummary(
				target, row.Health, row.PassCount, row.FailCount, row.TimeoutCount,
				row.TotalDurationUsec, row.Disposition))
			if len(targetResponse.Targets) == queryBatchSize {
				if err := stream.Send(targetResponse); err != nil {
					return err
				}
				targetResponse = &tbpb.GetTestsToSkipResponse{
					Targets: make([]*tbpb.TestTargetSummary, 0, queryBatchSize),
				}
			}
			return nil
		}); err != nil {
		return err
	}
	if len(targetResponse.Targets) > 0 {
		if err := stream.Send(targetResponse); err != nil {
			return err
		}
	}

	caseWhere := `tc.group_id = ? AND tc.repository = ? AND tc.deleted_at_usec = 0`
	caseArgs := []any{tbpb.TestHealth_TEST_HEALTH_UNKNOWN.String(), groupID, repository}
	if packagePrefix != "" {
		bound, boundArgs := conePackageBounds("tc.package_path", packagePrefix)
		caseWhere += ` AND ` + bound
		caseArgs = append(caseArgs, boundArgs...)
	}
	caseArgs = append(caseArgs,
		int32(tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_DISABLED),
		int32(tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_AUTOMATIC),
		tbpb.TestHealth_TEST_HEALTH_FAILING.String(),
		tbpb.TestHealth_TEST_HEALTH_FLAKY.String(),
		tbpb.TestHealth_TEST_HEALTH_TIMEOUT.String(),
	)
	caseQuery := fmt.Sprintf(`
		SELECT tc.target_label, tc.case_name, tc.disposition,
			COALESCE(s.health, ?) AS health,
			COALESCE(s.pass_count, 0) AS pass_count,
			COALESCE(s.fail_count, 0) AS fail_count,
			COALESCE(s.timeout_count, 0) AS timeout_count,
			COALESCE(s.total_duration_usec, 0) AS total_duration_usec
		FROM "TestCases" AS tc
		INNER JOIN "TestTargets" AS tt
			ON tt.group_id = tc.group_id AND tt.repository = tc.repository
			AND tt.target_label = tc.target_label AND tt.deleted_at_usec = 0
		LEFT JOIN "TestCaseStates" AS s
			ON s.group_id = tc.group_id AND s.repository = tc.repository
			AND s.target_label = tc.target_label AND s.case_name = tc.case_name
		WHERE %s AND (tc.disposition = ? OR
			(tc.disposition = ? AND s.health IN (?, ?, ?)))
		ORDER BY tc.target_label, tc.case_name`, caseWhere)
	type caseRow struct {
		TargetLabel       string
		CaseName          string
		Disposition       int32
		Health            string
		PassCount         int64
		FailCount         int64
		TimeoutCount      int64
		TotalDurationUsec int64
	}
	caseResponse := &tbpb.GetTestsToSkipResponse{
		TestCases: make([]*tbpb.TestCaseSummary, 0, queryBatchSize),
	}
	if err := db.ScanEach(
		s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_cases_to_skip").Raw(caseQuery, caseArgs...),
		func(ctx context.Context, row *caseRow) error {
			target, err := identity.CanonicalizeTarget(repository, row.TargetLabel)
			if err != nil {
				return err
			}
			caseName, err := identity.CaseNameFromKey(row.CaseName)
			if err != nil {
				return err
			}
			caseResponse.TestCases = append(caseResponse.TestCases, caseSummary(
				identity.CaseAddress{TargetAddress: target, CaseName: caseName},
				row.Health, row.PassCount, row.FailCount, row.TimeoutCount,
				row.TotalDurationUsec, row.Disposition))
			if len(caseResponse.TestCases) == queryBatchSize {
				if err := stream.Send(caseResponse); err != nil {
					return err
				}
				caseResponse = &tbpb.GetTestsToSkipResponse{
					TestCases: make([]*tbpb.TestCaseSummary, 0, queryBatchSize),
				}
			}
			return nil
		}); err != nil {
		return err
	}
	if len(caseResponse.TestCases) > 0 {
		return stream.Send(caseResponse)
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

func (s *Service) GetFailureAnalysisProgress(ctx context.Context, req *tbpb.GetFailureAnalysisProgressRequest) (*tbpb.GetFailureAnalysisProgressResponse, error) {
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
	type row struct {
		TotalCount     int64
		CompletedCount int64
	}
	result := &row{}
	err = s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_failure_analysis_progress").Raw(`
		SELECT COUNT(*) AS total_count,
			COALESCE(SUM(CASE WHEN analysis_prompt_version > 0 THEN 1 ELSE 0 END), 0) AS completed_count
		FROM "TestFailureClusters"
		WHERE group_id = ? AND repository = ?`, groupID, repository).Take(result)
	if err != nil {
		return nil, err
	}
	return &tbpb.GetFailureAnalysisProgressResponse{
		TotalCount: result.TotalCount, CompletedCount: result.CompletedCount,
	}, nil
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
		extraWhere := ""
		if !target {
			extraWhere = ` AND EXISTS (
				SELECT 1 FROM "TestTargets" AS target_catalog
				WHERE target_catalog.group_id = catalog.group_id
					AND target_catalog.repository = catalog.repository
					AND target_catalog.target_label = catalog.target_label
					AND target_catalog.deleted_at_usec = 0)`
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
			AND catalog.deleted_at_usec = 0%s
		GROUP BY s.health`, catalogTable, stateTable, join, extraWhere)
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
		var repositoryCount int64
		if err := s.env.GetDBHandle().GORM(ctx, "test_buddy_find_repository_health_catalog").
			Model(&tables.TestRepositoryCatalog{}).
			Where("group_id = ? AND repository = ?", groupID, repository).
			Count(&repositoryCount).Error; err != nil {
			return nil, err
		}
		if repositoryCount == 0 {
			return nil, status.NotFoundErrorf("repository %s was not found", repository)
		}
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
		Disposition         int32
		DeletedAtUsec       int64
		Health              string
		RecentObservations  []byte
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
		SELECT tt.disposition, tt.deleted_at_usec,
			COALESCE(s.health, ?) AS health, s.recent_observations,
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
			row.TimeoutCount, row.TotalDurationUsec, row.Disposition),
		AnalyzerRevision: row.AnalyzerRevision, AnalysisReason: row.AnalysisReason,
		EligibleSampleCount: row.EligibleSampleCount,
	}
	rsp.Target.Deleted = row.DeletedAtUsec != 0
	retained, err := decodeRetainedObservations(row.RecentObservations)
	if err != nil {
		return nil, err
	}
	for i := len(retained.Observations) - 1; i >= 0; i-- {
		observation := retained.Observations[i]
		rsp.RecentObservations = append(rsp.RecentObservations, &tbpb.TestObservation{
			Outcome: observation.Outcome, Source: observation.Source, CommitSha: observation.CommitSHA,
			WorkspaceDirty: observation.WorkspaceDirty, DurationUsec: observation.DurationUsec,
			FailureMessage: observation.FailureMessage, SourceUrl: observation.SourceURL,
			EventTimeUsec: observation.EventTimeUsec, ObservationId: observation.ObservationID,
			FailureFingerprint: observation.FailureFingerprint,
		})
	}
	rsp.FailureClusters, err = s.failureClusters(ctx, groupID, target.Repository, retained.Observations)
	if err != nil {
		return nil, err
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
		Disposition         int32
		DeletedAtUsec       int64
		Health              string
		RecentObservations  []byte
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
		SELECT tc.disposition, tc.deleted_at_usec,
			COALESCE(s.health, ?) AS health, s.recent_observations,
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
		Test: caseSummary(testCase, row.Health, row.PassCount, row.FailCount,
			row.TimeoutCount, row.TotalDurationUsec, row.Disposition),
		AnalyzerRevision: row.AnalyzerRevision, AnalysisReason: row.AnalysisReason,
		EligibleSampleCount: row.EligibleSampleCount,
	}
	rsp.Test.Deleted = row.DeletedAtUsec != 0
	retained, err := decodeRetainedObservations(row.RecentObservations)
	if err != nil {
		return nil, err
	}
	for i := len(retained.Observations) - 1; i >= 0; i-- {
		observation := retained.Observations[i]
		rsp.RecentObservations = append(rsp.RecentObservations, &tbpb.TestObservation{
			Outcome: observation.Outcome, Source: observation.Source, CommitSha: observation.CommitSHA,
			WorkspaceDirty: observation.WorkspaceDirty, DurationUsec: observation.DurationUsec,
			FailureMessage: observation.FailureMessage, SourceUrl: observation.SourceURL,
			EventTimeUsec: observation.EventTimeUsec, ObservationId: observation.ObservationID,
			FailureFingerprint: observation.FailureFingerprint,
		})
	}
	rsp.FailureClusters, err = s.failureClusters(ctx, groupID, testCase.Repository, retained.Observations)
	if err != nil {
		return nil, err
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

func (s *Service) failureClusters(ctx context.Context, groupID, repository string, observations []retainedObservation) ([]*tbpb.TestFailureCluster, error) {
	counts := make(map[string]int64)
	order := make([]string, 0)
	for i := len(observations) - 1; i >= 0; i-- {
		fingerprint := observations[i].FailureFingerprint
		if fingerprint == "" {
			continue
		}
		counts[fingerprint]++
		if counts[fingerprint] == 1 {
			order = append(order, fingerprint)
		}
	}
	if len(order) == 0 {
		return nil, nil
	}
	type clusterRow struct {
		Fingerprint        string
		FailureMessage     []byte
		AnalysisModel      string
		AnalysisCategory   string
		AnalysisSummary    []byte
		SuggestedFix       []byte
		AnalysisConfidence string
	}
	rows := make(map[string]*clusterRow, len(order))
	rq := s.env.GetDBHandle().NewQuery(ctx, "test_buddy_get_failure_clusters").Raw(`
		SELECT fingerprint, failure_message, analysis_model, analysis_category,
			analysis_summary, suggested_fix, analysis_confidence
		FROM "TestFailureClusters"
		WHERE group_id = ? AND repository = ? AND fingerprint IN ?`,
		groupID, repository, order)
	if err := db.ScanEach(rq, func(ctx context.Context, row *clusterRow) error {
		rows[row.Fingerprint] = row
		return nil
	}); err != nil {
		return nil, err
	}
	clusters := make([]*tbpb.TestFailureCluster, 0, len(rows))
	for _, fingerprint := range order {
		row := rows[fingerprint]
		if row == nil {
			continue
		}
		clusters = append(clusters, &tbpb.TestFailureCluster{
			Fingerprint: fingerprint, RepresentativeMessage: string(row.FailureMessage),
			OccurrenceCount: counts[fingerprint], Category: row.AnalysisCategory,
			Summary: string(row.AnalysisSummary), SuggestedFix: string(row.SuggestedFix),
			Confidence: row.AnalysisConfidence, Model: row.AnalysisModel,
		})
	}
	return clusters, nil
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

func caseSummary(address identity.CaseAddress, healthValue string, passCount, failCount, timeoutCount, totalDurationUsec int64, disposition int32) *tbpb.TestCaseSummary {
	return &tbpb.TestCaseSummary{
		Identity:    identity.CaseProto(address),
		Summary:     testSummary(healthValue, passCount, failCount, timeoutCount, totalDurationUsec),
		Disposition: testExecutionDisposition(disposition),
	}
}

func targetSummary(address identity.TargetAddress, healthValue string, passCount, failCount, timeoutCount, totalDurationUsec int64, disposition int32) *tbpb.TestTargetSummary {
	return &tbpb.TestTargetSummary{
		Identity:    identity.TargetProto(address),
		Summary:     testSummary(healthValue, passCount, failCount, timeoutCount, totalDurationUsec),
		Disposition: testExecutionDisposition(disposition),
	}
}

func testExecutionDisposition(value int32) tbpb.TestExecutionDisposition {
	if _, ok := tbpb.TestExecutionDisposition_name[value]; ok {
		return tbpb.TestExecutionDisposition(value)
	}
	return tbpb.TestExecutionDisposition_TEST_EXECUTION_DISPOSITION_AUTOMATIC
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
