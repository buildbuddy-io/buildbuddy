// Package normalize validates reported test observations.
package normalize

import (
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
)

const (
	maxFailureMessageBytes = 512
	maxSourceURLBytes      = 2048
	maxIdempotencyBytes    = 128
	maxCommitSHABytes      = 128
	maxRetainedRejections  = 100
)

type Observation struct {
	Address     identity.Address
	Observation *tbpb.TestObservation
}

type Rejection struct {
	RecordIndex      int
	Identity         *tbpb.TestIdentity
	IdempotencyToken string
	Message          string
}

type Report struct {
	RepositoryURL string
	Source        tbpb.TestObservationSource
	SourceURL     string
	CommitSHA     string
	Observations  []*Observation
	Rejections    []Rejection
	RejectedCount int
}

func Normalize(request *tbpb.ReportTestResultsRequest) (*Report, error) {
	if request == nil {
		return nil, status.InvalidArgumentError("report is required")
	}
	repository, err := identity.NormalizeRepositoryURL(request.GetRepoUrl())
	if err != nil {
		return nil, err
	}
	if request.GetSource() != tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR {
		return nil, status.InvalidArgumentError("source must be MONITOR")
	}
	if err := validateBoundedString("commit SHA", request.GetCommitSha(), maxCommitSHABytes); err != nil {
		return nil, err
	}
	if request.GetCommitSha() == "" {
		return nil, status.InvalidArgumentError("commit_sha is required")
	}
	if err := validateSourceURL(request.GetSourceUrl()); err != nil {
		return nil, err
	}

	report := &Report{
		RepositoryURL: repository,
		Source:        request.GetSource(),
		SourceURL:     request.GetSourceUrl(),
		CommitSHA:     request.GetCommitSha(),
	}
	for i, input := range request.GetObservations() {
		observation, err := normalizeObservation(repository, input)
		if err != nil {
			report.reject(i, input, err)
			continue
		}
		report.Observations = append(report.Observations, observation)
	}
	return report, nil
}

func normalizeObservation(repository string, input *tbpb.TestObservation) (*Observation, error) {
	if input == nil {
		return nil, status.InvalidArgumentError("observation is required")
	}
	address, err := identity.Canonicalize(repository, input.GetIdentity())
	if err != nil {
		return nil, err
	}
	switch input.GetOutcome() {
	case tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_BROKEN:
	default:
		return nil, status.InvalidArgumentErrorf("invalid outcome %d", input.GetOutcome())
	}
	if input.GetDurationUsec() < 0 {
		return nil, status.InvalidArgumentError("duration_usec must not be negative")
	}
	if input.GetEventTimeUsec() <= 0 {
		return nil, status.InvalidArgumentError("event_time_usec must be greater than zero")
	}
	if input.GetIdempotencyToken() == "" {
		return nil, status.InvalidArgumentError("idempotency_token is required")
	}
	if err := validateBoundedString("idempotency token", input.GetIdempotencyToken(), maxIdempotencyBytes); err != nil {
		return nil, err
	}
	if err := validateBoundedString("failure message", input.GetFailureMessage(), maxFailureMessageBytes); err != nil {
		return nil, err
	}

	normalized := proto.Clone(input).(*tbpb.TestObservation)
	normalized.Identity = address.Proto()
	return &Observation{Address: address, Observation: normalized}, nil
}

func validateSourceURL(raw string) error {
	if err := validateBoundedString("source URL", raw, maxSourceURLBytes); err != nil {
		return err
	}
	u, err := url.ParseRequestURI(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return status.InvalidArgumentError("source_url must be an absolute HTTP(S) URL")
	}
	return nil
}

func validateBoundedString(name, value string, maxBytes int) error {
	if len(value) > maxBytes {
		return status.InvalidArgumentErrorf("%s exceeds %d bytes", name, maxBytes)
	}
	if !utf8.ValidString(value) {
		return status.InvalidArgumentErrorf("%s is not valid UTF-8", name)
	}
	if strings.ContainsRune(value, '\x00') {
		return status.InvalidArgumentErrorf("%s contains NUL", name)
	}
	return nil
}

func (r *Report) reject(index int, observation *tbpb.TestObservation, err error) {
	r.RejectedCount++
	if len(r.Rejections) >= maxRetainedRejections {
		return
	}
	r.Rejections = append(r.Rejections, Rejection{
		RecordIndex:      index,
		Identity:         observation.GetIdentity(),
		IdempotencyToken: observation.GetIdempotencyToken(),
		Message:          err.Error(),
	})
}
