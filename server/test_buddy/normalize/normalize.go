// Package normalize validates reported test results.
package normalize

import (
	"net/url"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"
)

const (
	MaxFailureMessageBytes = 512
	MaxSourceURLBytes      = 2048
	MaxResultIDBytes       = 128
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

type CaseResult struct {
	Result  *tbpb.TestCaseResult
	Address identity.CaseAddress
}

type TargetResult struct {
	Result  *tbpb.TestTargetResult
	Address identity.TargetAddress
}

type Rejection struct {
	Kind        RecordKind
	RecordIndex int
	Reason      RejectionReason
	Message     string
}

type Report struct {
	RepositoryURL string
	CaseResults   []*CaseResult
	TargetResults []*TargetResult
	Rejections    []Rejection
	Rejected      Counts
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

func Normalize(repositoryURL string, cases []*tbpb.TestCaseResult, targets []*tbpb.TestTargetResult) (*Report, error) {
	session, err := NewSession(repositoryURL)
	if err != nil {
		return nil, err
	}
	return session.Normalize(cases, targets), nil
}

func (s *Session) Normalize(cases []*tbpb.TestCaseResult, targets []*tbpb.TestTargetResult) *Report {
	report := &Report{RepositoryURL: s.repository}
	for i, record := range cases {
		result, err := s.normalizeCase(record)
		if err != nil {
			report.reject(RecordKindCase, i, RejectionInvalidContent, err)
			continue
		}
		report.CaseResults = append(report.CaseResults, result)
	}
	for i, record := range targets {
		result, err := s.normalizeTarget(record)
		if err != nil {
			report.reject(RecordKindTarget, i, RejectionInvalidContent, err)
			continue
		}
		report.TargetResults = append(report.TargetResults, result)
	}
	return report
}

func (s *Session) normalizeCase(record *tbpb.TestCaseResult) (*CaseResult, error) {
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
	if err := validateResult(record.GetResult()); err != nil {
		return nil, err
	}
	return &CaseResult{
		Address: address,
		Result:  &tbpb.TestCaseResult{Identity: address.Proto(), Result: proto.Clone(record.GetResult()).(*tbpb.TestResult)},
	}, nil
}

func (s *Session) normalizeTarget(record *tbpb.TestTargetResult) (*TargetResult, error) {
	if record.GetIdentity() == nil {
		return nil, status.InvalidArgumentError("target identity is required")
	}
	target, err := s.target(record.GetIdentity().GetTargetLabel())
	if err != nil {
		return nil, err
	}
	if err := validateResult(record.GetResult()); err != nil {
		return nil, err
	}
	return &TargetResult{
		Address: target,
		Result:  &tbpb.TestTargetResult{Identity: target.Proto(), Result: proto.Clone(record.GetResult()).(*tbpb.TestResult)},
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

func validateResult(result *tbpb.TestResult) error {
	if result == nil {
		return status.InvalidArgumentError("result is required")
	}
	if _, ok := tbpb.TestOutcome_name[int32(result.GetOutcome())]; !ok {
		return status.InvalidArgumentErrorf("unrecognized outcome %d", result.GetOutcome())
	}
	if result.GetDurationUsec() < 0 {
		return status.InvalidArgumentError("duration_usec must not be negative")
	}
	if result.GetEventTimeUsec() <= 0 {
		return status.InvalidArgumentError("event_time_usec must be greater than zero")
	}
	if result.GetResultId() == "" {
		return status.InvalidArgumentError("result_id is required")
	}
	if err := identity.ValidateBoundedString("result ID", result.GetResultId(), MaxResultIDBytes); err != nil {
		return err
	}
	if err := identity.ValidateBoundedString("failure message", result.GetFailureMessage(), MaxFailureMessageBytes); err != nil {
		return err
	}
	if err := identity.ValidateBoundedString("source URL", result.GetSourceUrl(), MaxSourceURLBytes); err != nil {
		return err
	}
	u, err := url.ParseRequestURI(result.GetSourceUrl())
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return status.InvalidArgumentError("source_url must be an absolute HTTP(S) URL")
	}
	return nil
}

func (r *Report) reject(kind RecordKind, index int, reason RejectionReason, err error) {
	r.Rejected.add(kind)
	if len(r.Rejections) < MaxRetainedRejections {
		r.Rejections = append(r.Rejections, Rejection{
			Kind: kind, RecordIndex: index, Reason: reason, Message: err.Error(),
		})
	}
}
