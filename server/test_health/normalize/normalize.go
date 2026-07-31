// Package normalize validates reported test results.
package normalize

import (
	thpb "github.com/buildbuddy-io/buildbuddy/proto/test_health"
	"github.com/buildbuddy-io/buildbuddy/server/test_health/identity"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	MaxInvocationIDBytes   = 1024
	MaxFailureMessageBytes = 512
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

type ReportContext struct {
	RepositoryURL string
	InvocationID  string
	Source        thpb.ResultSource
}

type CaseRecord struct {
	TargetLabel    string
	CaseName       string
	Outcome        thpb.TestOutcome
	DurationUsec   int64
	FailureMessage string
}

type TargetRecord struct {
	TargetLabel    string
	Outcome        thpb.TestOutcome
	DurationUsec   int64
	FailureMessage string
}

type CaseResult struct {
	Result   *thpb.TestCaseResult
	Identity *identity.Identity
	Target   *identity.TargetIdentity
}

type TargetResult struct {
	Result *thpb.TestTargetResult
	Target *identity.TargetIdentity
}

type Rejection struct {
	Kind        RecordKind
	RecordIndex int
	Reason      RejectionReason
	Message     string
}

type Report struct {
	Context       ReportContext
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
	ctx     ReportContext
	targets map[string]*identity.TargetIdentity
}

func NewSession(ctx ReportContext) (*Session, error) {
	normalized, err := identity.NormalizeRepositoryURL(ctx.RepositoryURL)
	if err != nil {
		return nil, err
	}
	if err := identity.ValidateBoundedString("invocation ID", ctx.InvocationID, MaxInvocationIDBytes); err != nil {
		return nil, err
	}
	if ctx.InvocationID == "" {
		return nil, status.InvalidArgumentError("invocation ID is required")
	}
	if ctx.Source != thpb.ResultSource_RESULT_SOURCE_PRESUBMIT && ctx.Source != thpb.ResultSource_RESULT_SOURCE_POSTSUBMIT {
		return nil, status.InvalidArgumentErrorf("unsupported result source %d", ctx.Source)
	}
	ctx.RepositoryURL = normalized
	return &Session{
		ctx:     ctx,
		targets: make(map[string]*identity.TargetIdentity),
	}, nil
}

func Normalize(ctx ReportContext, cases []CaseRecord, targets []TargetRecord) (*Report, error) {
	session, err := NewSession(ctx)
	if err != nil {
		return nil, err
	}
	return session.Normalize(cases, targets), nil
}

func (s *Session) Normalize(cases []CaseRecord, targets []TargetRecord) *Report {
	report := &Report{Context: s.ctx}
	for i := range cases {
		result, err := s.normalizeCase(&cases[i])
		if err != nil {
			report.reject(RecordKindCase, i, RejectionInvalidContent, err)
			continue
		}
		report.CaseResults = append(report.CaseResults, result)
	}
	for i := range targets {
		result, err := s.normalizeTarget(&targets[i])
		if err != nil {
			report.reject(RecordKindTarget, i, RejectionInvalidContent, err)
			continue
		}
		report.TargetResults = append(report.TargetResults, result)
	}
	return report
}

func (s *Session) normalizeCase(record *CaseRecord) (*CaseResult, error) {
	id, err := identity.CanonicalizeCase(identity.CaseInput{
		RepositoryURL: s.ctx.RepositoryURL,
		TargetLabel:   record.TargetLabel,
		CaseName:      record.CaseName,
	})
	if err != nil {
		return nil, err
	}
	if err := validateResult(record.Outcome, record.DurationUsec, record.FailureMessage); err != nil {
		return nil, err
	}
	target, err := s.target(record.TargetLabel)
	if err != nil {
		return nil, err
	}
	return &CaseResult{
		Identity: id,
		Target:   target,
		Result: &thpb.TestCaseResult{
			Identity:       id.Proto(),
			InvocationId:   s.ctx.InvocationID,
			Outcome:        record.Outcome,
			Source:         s.ctx.Source,
			DurationUsec:   record.DurationUsec,
			FailureMessage: record.FailureMessage,
		},
	}, nil
}

func (s *Session) normalizeTarget(record *TargetRecord) (*TargetResult, error) {
	target, err := s.target(record.TargetLabel)
	if err != nil {
		return nil, err
	}
	if err := validateResult(record.Outcome, record.DurationUsec, record.FailureMessage); err != nil {
		return nil, err
	}
	return &TargetResult{
		Target: target,
		Result: &thpb.TestTargetResult{
			Identity:       target.Proto(),
			InvocationId:   s.ctx.InvocationID,
			Outcome:        record.Outcome,
			Source:         s.ctx.Source,
			DurationUsec:   record.DurationUsec,
			FailureMessage: record.FailureMessage,
		},
	}, nil
}

func (s *Session) target(label string) (*identity.TargetIdentity, error) {
	if target := s.targets[label]; target != nil {
		return target, nil
	}
	target, err := identity.CanonicalizeTargetIdentity(s.ctx.RepositoryURL, label)
	if err != nil {
		return nil, err
	}
	s.targets[label] = target
	return target, nil
}

func validateResult(outcome thpb.TestOutcome, durationUsec int64, failureMessage string) error {
	if _, ok := thpb.TestOutcome_name[int32(outcome)]; !ok {
		return status.InvalidArgumentErrorf("unrecognized outcome %d", outcome)
	}
	if durationUsec < 0 {
		return status.InvalidArgumentError("duration_usec must not be negative")
	}
	return identity.ValidateBoundedString("failure message", failureMessage, MaxFailureMessageBytes)
}

func (r *Report) reject(kind RecordKind, index int, reason RejectionReason, err error) {
	r.Rejected.add(kind)
	if len(r.Rejections) < MaxRetainedRejections {
		r.Rejections = append(r.Rejections, Rejection{
			Kind: kind, RecordIndex: index, Reason: reason, Message: err.Error(),
		})
	}
}
