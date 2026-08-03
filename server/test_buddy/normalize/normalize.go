// Package normalize validates reported test observations.
package normalize

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"regexp"
	"strings"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"
)

var (
	ansiEscapePattern = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)
	uuidPattern       = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	pointerPattern    = regexp.MustCompile(`(?i)\b0x[0-9a-f]+\b`)
	longHexPattern    = regexp.MustCompile(`(?i)\b[0-9a-f]{16,}\b`)
)

const (
	MaxFailureMessageBytes = 512
	MaxSourceURLBytes      = 2048
	MaxObservationIDBytes  = 128
	MaxCommitSHABytes      = 128
	MaxRetainedRejections  = 100
)

type RejectionReason string

const (
	RejectionInvalidIdentity RejectionReason = "invalid_identity"
	RejectionInvalidContent  RejectionReason = "invalid_content"
)

type RecordKind string

const (
	RecordKindCase   RecordKind = "case"
	RecordKindTarget RecordKind = "target"
)

type CaseRecord struct {
	TargetLabel     string
	CaseName        string
	Outcome         tbpb.TestOutcome
	DurationUsec    int64
	FailureMessage  string
	EventTimeUsec   int64
	OccurrenceIndex int
}

type CaseObservation struct {
	Observation *tbpb.TestCaseObservation
	Address     identity.CaseAddress
}

type TargetObservation struct {
	Observation *tbpb.TestTargetObservation
	Address     identity.TargetAddress
}

type Rejection struct {
	Kind        RecordKind
	RecordIndex int
	Reason      RejectionReason
	Message     string
}

type Report struct {
	RepositoryURL      string
	CaseObservations   []*CaseObservation
	TargetObservations []*TargetObservation
	Rejections         []Rejection
	Rejected           Counts
}

type Counts struct {
	Cases   int
	Targets int
}

func (c Counts) Total() int { return c.Cases + c.Targets }

func (c *Counts) add(kind RecordKind) {
	if kind == RecordKindTarget {
		c.Targets++
	} else {
		c.Cases++
	}
}

type Session struct {
	repository string
	targets    map[string]identity.TargetAddress
}

func NewSession(repositoryURL string) (*Session, error) {
	repository, err := identity.NormalizeRepositoryURL(repositoryURL)
	if err != nil {
		return nil, err
	}
	return &Session{repository: repository, targets: make(map[string]identity.TargetAddress)}, nil
}

func Normalize(repositoryURL string, cases []*tbpb.TestCaseObservation, targets []*tbpb.TestTargetObservation) (*Report, error) {
	session, err := NewSession(repositoryURL)
	if err != nil {
		return nil, err
	}
	return session.Normalize(cases, targets), nil
}

func (s *Session) Normalize(cases []*tbpb.TestCaseObservation, targets []*tbpb.TestTargetObservation) *Report {
	report := &Report{RepositoryURL: s.repository}
	for i, record := range cases {
		observation, err := s.normalizeCase(record)
		if err != nil {
			report.reject(RecordKindCase, i, RejectionInvalidContent, err)
			continue
		}
		report.CaseObservations = append(report.CaseObservations, observation)
	}
	for i, record := range targets {
		observation, err := s.normalizeTarget(record)
		if err != nil {
			report.reject(RecordKindTarget, i, RejectionInvalidContent, err)
			continue
		}
		report.TargetObservations = append(report.TargetObservations, observation)
	}
	return report
}

func (s *Session) normalizeCase(record *tbpb.TestCaseObservation) (*CaseObservation, error) {
	if record.GetIdentity().GetTarget() == nil {
		return nil, status.InvalidArgumentError("case identity is required")
	}
	target, err := s.target(record.GetIdentity().GetTarget().GetTargetLabel())
	if err != nil {
		return nil, err
	}
	if err := identity.ValidateCaseName(record.GetIdentity().GetCaseName()); err != nil {
		return nil, err
	}
	address := identity.CaseAddress{
		TargetAddress: target, CaseName: record.GetIdentity().GetCaseName(),
	}
	observation, err := normalizeObservation(record.GetObservation())
	if err != nil {
		return nil, err
	}
	return &CaseObservation{
		Address:     address,
		Observation: &tbpb.TestCaseObservation{Identity: address.Proto(), Observation: observation},
	}, nil
}

func (s *Session) normalizeTarget(record *tbpb.TestTargetObservation) (*TargetObservation, error) {
	if record.GetIdentity() == nil {
		return nil, status.InvalidArgumentError("target identity is required")
	}
	target, err := s.target(record.GetIdentity().GetTargetLabel())
	if err != nil {
		return nil, err
	}
	observation, err := normalizeObservation(record.GetObservation())
	if err != nil {
		return nil, err
	}
	return &TargetObservation{
		Address:     target,
		Observation: &tbpb.TestTargetObservation{Identity: target.Proto(), Observation: observation},
	}, nil
}

func (s *Session) target(label string) (identity.TargetAddress, error) {
	if target, ok := s.targets[label]; ok {
		return target, nil
	}
	target, err := identity.CanonicalizeTarget(s.repository, label)
	if err != nil {
		return identity.TargetAddress{}, err
	}
	s.targets[label] = target
	return target, nil
}

func validateObservation(observation *tbpb.TestObservation) error {
	if observation == nil {
		return status.InvalidArgumentError("observation is required")
	}
	if _, ok := tbpb.TestOutcome_name[int32(observation.GetOutcome())]; !ok {
		return status.InvalidArgumentErrorf("unrecognized outcome %d", observation.GetOutcome())
	}
	if observation.GetSource() == tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_UNKNOWN {
		return status.InvalidArgumentError("source is required")
	}
	if _, ok := tbpb.TestObservationSource_name[int32(observation.GetSource())]; !ok {
		return status.InvalidArgumentErrorf("unrecognized source %d", observation.GetSource())
	}
	if observation.GetCommitSha() == "" {
		return status.InvalidArgumentError("commit_sha is required")
	}
	if err := identity.ValidateBoundedString("commit SHA", observation.GetCommitSha(), MaxCommitSHABytes); err != nil {
		return err
	}
	if observation.GetDurationUsec() < 0 {
		return status.InvalidArgumentError("duration_usec must not be negative")
	}
	if observation.GetEventTimeUsec() <= 0 {
		return status.InvalidArgumentError("event_time_usec must be greater than zero")
	}
	if observation.GetObservationId() == "" {
		return status.InvalidArgumentError("observation_id is required")
	}
	if err := identity.ValidateBoundedString("observation ID", observation.GetObservationId(), MaxObservationIDBytes); err != nil {
		return err
	}
	if err := identity.ValidateBoundedString("failure message", observation.GetFailureMessage(), MaxFailureMessageBytes); err != nil {
		return err
	}
	if err := identity.ValidateBoundedString("source URL", observation.GetSourceUrl(), MaxSourceURLBytes); err != nil {
		return err
	}
	u, err := url.ParseRequestURI(observation.GetSourceUrl())
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return status.InvalidArgumentError("source_url must be an absolute HTTP(S) URL")
	}
	return nil
}

func normalizeObservation(input *tbpb.TestObservation) (*tbpb.TestObservation, error) {
	if err := validateObservation(input); err != nil {
		return nil, err
	}
	observation := proto.Clone(input).(*tbpb.TestObservation)
	observation.FailureFingerprint = ""
	if observation.GetOutcome() == tbpb.TestOutcome_TEST_OUTCOME_FAIL {
		normalized := strings.TrimSpace(observation.GetFailureMessage())
		normalized = ansiEscapePattern.ReplaceAllString(normalized, "")
		normalized = uuidPattern.ReplaceAllString(normalized, "<uuid>")
		normalized = pointerPattern.ReplaceAllString(normalized, "<address>")
		normalized = longHexPattern.ReplaceAllString(normalized, "<hex>")
		normalized = strings.Join(strings.Fields(normalized), " ")
		if normalized != "" {
			digest := sha256.Sum256([]byte(normalized))
			observation.FailureFingerprint = hex.EncodeToString(digest[:])
		}
	}
	return observation, nil
}

func (r *Report) reject(kind RecordKind, index int, reason RejectionReason, err error) {
	r.Rejected.add(kind)
	if len(r.Rejections) < MaxRetainedRejections {
		r.Rejections = append(r.Rejections, Rejection{
			Kind: kind, RecordIndex: index, Reason: reason, Message: err.Error(),
		})
	}
}
