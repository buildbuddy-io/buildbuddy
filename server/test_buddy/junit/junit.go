// Package junit parses JUnit XML into test case observations.
package junit

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/normalize"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	DefaultMaxXMLBytes     = 64 << 20
	DefaultMaxCases        = 100_000
	DefaultMaxDepth        = 64
	DefaultMaxElements     = 1_000_000
	DefaultMaxDiagnostics  = 100
	maxFailureMessageBytes = normalize.MaxFailureMessageBytes
)

type Limits struct {
	MaxXMLBytes    int64
	MaxCases       int
	MaxDepth       int
	MaxElements    int
	MaxDiagnostics int
}

type Options struct {
	TargetLabel string
	Limits      Limits
}

type DiagnosticCode string

const (
	DiagnosticMissingName      DiagnosticCode = "missing_name"
	DiagnosticInvalidIdentity  DiagnosticCode = "invalid_identity"
	DiagnosticInvalidDuration  DiagnosticCode = "invalid_duration"
	DiagnosticInvalidTimestamp DiagnosticCode = "invalid_timestamp"
	DiagnosticUnknownStatus    DiagnosticCode = "unknown_status"
	DiagnosticInvalidDisabled  DiagnosticCode = "invalid_disabled"
	DiagnosticInvalidUTF8      DiagnosticCode = "invalid_utf8"
)

// Diagnostic records one thing the parser could not use. CaseName is empty
// when the case had no usable name, which is the reason for the diagnostic.
type Diagnostic struct {
	Code      DiagnosticCode
	CaseIndex int
	CaseName  string
}

// DropsCase reports whether this diagnostic means the case was not reported at
// all. A case needs a usable name to be addressable, so those two codes drop
// it; the rest ignore one field and still report the case.
func (c DiagnosticCode) DropsCase() bool {
	return c == DiagnosticMissingName || c == DiagnosticInvalidIdentity
}

type Report struct {
	TargetLabel         string
	Cases               []normalize.CaseRecord
	DurationUsec        int64
	EventTimeUsec       int64
	UnattributedFailure bool
	EncounteredCases    int
	Diagnostics         []Diagnostic
	DiagnosticCount     int
	DroppedDiagnostics  int
}

type parser struct {
	ctx           context.Context
	options       Options
	limits        Limits
	report        *Report
	reader        *countingReader
	elements      int
	suiteFailures int
	suiteTimes    []int64
}

type caseState struct {
	name           string
	time           string
	status         string
	disabled       string
	timestamp      string
	hasFailure     bool
	hasError       bool
	hasSkipped     bool
	failureMessage string
	failureDepth   int
	failureAttr    string
	failureBody    []byte
}

func Parse(ctx context.Context, r io.Reader, options Options) (*Report, error) {
	if r == nil {
		return nil, status.InvalidArgumentError("JUnit XML reader is required")
	}
	targetLabel, err := identity.CanonicalizeTargetLabel(options.TargetLabel)
	if err != nil {
		return nil, err
	}
	limits, err := normalizeLimits(options.Limits)
	if err != nil {
		return nil, err
	}
	options.TargetLabel = targetLabel
	p := &parser{
		ctx: ctx, options: options, limits: limits,
		report: &Report{TargetLabel: targetLabel},
	}
	p.reader = &countingReader{r: io.LimitReader(&contextReader{ctx: ctx, r: r}, limits.MaxXMLBytes+1)}
	utf8Reader := &utf8SanitizingReader{r: p.reader}
	decoder := xml.NewDecoder(utf8Reader)
	depth := 0
	root := ""
	for {
		token, tokenErr := decoder.Token()
		if err := p.checkXMLBytes(); err != nil {
			return nil, err
		}
		if tokenErr == io.EOF {
			break
		}
		if tokenErr != nil {
			if ctx.Err() != nil {
				return nil, status.FromContextError(ctx)
			}
			return nil, p.malformedXMLError(tokenErr)
		}
		switch token := token.(type) {
		case xml.StartElement:
			if depth == 0 && root != "" {
				return nil, p.malformedXMLError(nil)
			}
			depth++
			if err := p.observeElement(depth); err != nil {
				return nil, err
			}
			if root == "" {
				root = token.Name.Local
				if root != "testsuites" && root != "testsuite" {
					return nil, status.InvalidArgumentErrorf("%s has unsupported root element", p.subject())
				}
			}
			switch token.Name.Local {
			case "testsuite":
				suiteTime, timeErr := eventTimeUsec(attribute(token, "timestamp"))
				if timeErr != nil {
					p.addDiagnostic(Diagnostic{Code: DiagnosticInvalidTimestamp, CaseIndex: -1})
				}
				if suiteTime == 0 {
					suiteTime = p.currentSuiteTime()
				}
				p.suiteTimes = append(p.suiteTimes, suiteTime)
				if p.report.EventTimeUsec == 0 {
					p.report.EventTimeUsec = suiteTime
				}
				duration, err := durationUsec(attribute(token, "time"))
				if err == nil && duration > p.report.DurationUsec {
					p.report.DurationUsec = duration
				}
				failures, failureErr := nonnegativeInt(attribute(token, "failures"))
				errors, errorErr := nonnegativeInt(attribute(token, "errors"))
				if failureErr == nil && errorErr == nil && failures+errors > p.suiteFailures {
					p.suiteFailures = failures + errors
				}
			case "testcase":
				p.report.EncounteredCases++
				if p.report.EncounteredCases > limits.MaxCases {
					return nil, status.ResourceExhaustedErrorf("%s exceeds %d cases", p.subject(), limits.MaxCases)
				}
				record, diagnostics, err := p.parseCase(decoder, token, depth)
				if err != nil {
					return nil, err
				}
				depth--
				for _, diagnostic := range diagnostics {
					p.addDiagnostic(diagnostic)
				}
				if record != nil {
					p.report.Cases = append(p.report.Cases, *record)
				}
			}
		case xml.EndElement:
			if token.Name.Local == "testsuite" && len(p.suiteTimes) > 0 {
				p.suiteTimes = p.suiteTimes[:len(p.suiteTimes)-1]
			}
			depth--
			if depth < 0 {
				return nil, p.malformedXMLError(nil)
			}
		case xml.CharData:
			if depth == 0 && len(bytes.TrimSpace(token)) > 0 {
				return nil, p.malformedXMLError(nil)
			}
		}
	}
	if root == "" {
		return nil, status.InvalidArgumentErrorf("%s has no root element", p.subject())
	}
	attributedFailures := 0
	for _, testCase := range p.report.Cases {
		if testCase.Outcome == tbpb.TestOutcome_TEST_OUTCOME_FAIL || testCase.Outcome == tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT {
			attributedFailures++
		}
	}
	p.report.UnattributedFailure = p.suiteFailures > attributedFailures
	if utf8Reader.invalid {
		p.addDiagnostic(Diagnostic{Code: DiagnosticInvalidUTF8, CaseIndex: -1})
	}
	p.report.DroppedDiagnostics = p.report.DiagnosticCount - len(p.report.Diagnostics)
	return p.report, nil
}

func (p *parser) parseCase(decoder *xml.Decoder, start xml.StartElement, startDepth int) (*normalize.CaseRecord, []Diagnostic, error) {
	index := p.report.EncounteredCases - 1
	state := caseState{
		name: attribute(start, "name"),
		time: attribute(start, "time"), status: attribute(start, "status"),
		disabled: attribute(start, "disabled"), timestamp: attribute(start, "timestamp"),
	}
	depth := startDepth
	for {
		token, err := decoder.Token()
		if sizeErr := p.checkXMLBytes(); sizeErr != nil {
			return nil, nil, sizeErr
		}
		if err == io.EOF {
			return nil, nil, p.malformedXMLError(io.ErrUnexpectedEOF)
		}
		if err != nil {
			if p.ctx.Err() != nil {
				return nil, nil, status.FromContextError(p.ctx)
			}
			return nil, nil, p.malformedXMLError(err)
		}
		switch token := token.(type) {
		case xml.StartElement:
			depth++
			if err := p.observeElement(depth); err != nil {
				return nil, nil, err
			}
			if token.Name.Space != start.Name.Space {
				continue
			}
			if token.Name.Local == "testcase" {
				return nil, nil, status.InvalidArgumentErrorf("%s contains a nested testcase element", p.subject())
			}
			if depth != startDepth+1 {
				continue
			}
			switch token.Name.Local {
			case "failure":
				state.hasFailure = true
				state.startFailure(depth, attribute(token, "message"))
			case "error":
				state.hasError = true
				state.startFailure(depth, attribute(token, "message"))
			case "skipped", "disabled":
				state.hasSkipped = true
			}
		case xml.EndElement:
			if depth == state.failureDepth {
				state.finishFailure()
			}
			if depth == startDepth {
				return p.caseRecord(index, &state)
			}
			depth--
		case xml.CharData:
			if state.failureDepth != 0 {
				state.appendFailureBody(token)
			}
		}
	}
}

func (p *parser) caseRecord(index int, state *caseState) (*normalize.CaseRecord, []Diagnostic, error) {
	if state.name == "" {
		return nil, []Diagnostic{{Code: DiagnosticMissingName, CaseIndex: index}}, nil
	}
	if err := identity.ValidateCaseName(state.name); err != nil {
		// The name is unusable as an address but still identifies the case to
		// a human reading the XML, so it is worth carrying.
		return nil, []Diagnostic{
			{Code: DiagnosticInvalidIdentity, CaseIndex: index, CaseName: state.name},
		}, nil
	}
	var diagnostics []Diagnostic
	duration, err := durationUsec(state.time)
	if err != nil {
		diagnostics = append(diagnostics,
			Diagnostic{Code: DiagnosticInvalidDuration, CaseIndex: index, CaseName: state.name})
	}
	outcome, outcomeDiagnostics := state.outcome(index)
	diagnostics = append(diagnostics, outcomeDiagnostics...)
	eventTime := p.currentSuiteTime()
	if state.timestamp != "" {
		parsed, err := eventTimeUsec(state.timestamp)
		if err != nil {
			diagnostics = append(diagnostics,
				Diagnostic{Code: DiagnosticInvalidTimestamp, CaseIndex: index, CaseName: state.name})
		} else {
			eventTime = parsed
		}
	}
	return &normalize.CaseRecord{
		TargetLabel: p.options.TargetLabel, CaseName: state.name,
		Outcome: outcome, DurationUsec: duration, FailureMessage: state.failureMessage,
		EventTimeUsec: eventTime, OccurrenceIndex: index,
	}, diagnostics, nil
}

func (s *caseState) outcome(index int) (tbpb.TestOutcome, []Diagnostic) {
	var diagnostics []Diagnostic
	outcome := tbpb.TestOutcome_TEST_OUTCOME_PASS
	switch strings.ToLower(strings.TrimSpace(s.status)) {
	case "", "run", "passed", "pass", "success":
	case "failed", "failure", "error":
		outcome = tbpb.TestOutcome_TEST_OUTCOME_FAIL
	case "timeout", "timedout", "timed_out":
		outcome = tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT
	case "notrun", "skipped", "skip", "disabled", "ignored":
		outcome = tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN
	default:
		outcome = tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN
		diagnostics = append(diagnostics,
			Diagnostic{Code: DiagnosticUnknownStatus, CaseIndex: index, CaseName: s.name})
	}
	if s.disabled != "" {
		switch strings.ToLower(strings.TrimSpace(s.disabled)) {
		case "true", "1", "yes":
			outcome = tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN
		case "false", "0", "no":
		default:
			diagnostics = append(diagnostics,
				Diagnostic{Code: DiagnosticInvalidDisabled, CaseIndex: index, CaseName: s.name})
		}
	}
	switch {
	case s.hasError || s.hasFailure:
		outcome = tbpb.TestOutcome_TEST_OUTCOME_FAIL
	case s.hasSkipped:
		outcome = tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN
	}
	return outcome, diagnostics
}

func (s *caseState) startFailure(depth int, message string) {
	if s.failureMessage != "" || s.failureDepth != 0 {
		return
	}
	s.failureDepth = depth
	s.failureAttr = message
	s.failureBody = s.failureBody[:0]
}

func (s *caseState) appendFailureBody(body []byte) {
	remaining := maxFailureMessageBytes - len(s.failureBody)
	if remaining <= 0 {
		return
	}
	if len(body) > remaining {
		body = body[:remaining]
		for !utf8.Valid(body) {
			body = body[:len(body)-1]
		}
	}
	s.failureBody = append(s.failureBody, body...)
}

func (s *caseState) finishFailure() {
	message := strings.TrimSpace(s.failureAttr)
	body := strings.TrimSpace(string(s.failureBody))
	if message == "" {
		message = body
	} else if body != "" && body != message {
		message += "\n" + body
	}
	if len(message) > maxFailureMessageBytes {
		message = message[:maxFailureMessageBytes]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	s.failureMessage = message
	s.failureDepth = 0
	s.failureAttr = ""
	s.failureBody = s.failureBody[:0]
}

func nonnegativeInt(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, status.InvalidArgumentError("invalid nonnegative integer")
	}
	return value, nil
}

func durationUsec(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 || seconds > float64(math.MaxInt64/1_000_000) {
		return 0, status.InvalidArgumentError("invalid JUnit duration")
	}
	return int64(math.Round(seconds * 1_000_000)), nil
}

func eventTimeUsec(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		if parsed.UnixMicro() > 0 {
			return parsed.UnixMicro(), nil
		}
		return 0, status.InvalidArgumentError("invalid JUnit timestamp")
	}
	parsed, err := time.ParseInLocation("2006-01-02T15:04:05.999999999", value, time.UTC)
	if err != nil {
		return 0, status.InvalidArgumentError("invalid JUnit timestamp")
	}
	if parsed.UnixMicro() <= 0 {
		return 0, status.InvalidArgumentError("invalid JUnit timestamp")
	}
	return parsed.UnixMicro(), nil
}

func (p *parser) currentSuiteTime() int64 {
	if len(p.suiteTimes) == 0 {
		return 0
	}
	return p.suiteTimes[len(p.suiteTimes)-1]
}

func attribute(element xml.StartElement, localName string) string {
	for _, attribute := range element.Attr {
		if attribute.Name.Space == "" && attribute.Name.Local == localName {
			return attribute.Value
		}
	}
	return ""
}

func (p *parser) observeElement(depth int) error {
	p.elements++
	if depth > p.limits.MaxDepth {
		return status.ResourceExhaustedErrorf("%s exceeds nesting depth %d", p.subject(), p.limits.MaxDepth)
	}
	if p.elements > p.limits.MaxElements {
		return status.ResourceExhaustedErrorf("%s exceeds %d elements", p.subject(), p.limits.MaxElements)
	}
	return nil
}

func (p *parser) checkXMLBytes() error {
	if p.reader.n <= p.limits.MaxXMLBytes {
		return nil
	}
	return status.ResourceExhaustedErrorf("%s exceeds %d bytes", p.subject(), p.limits.MaxXMLBytes)
}

func (p *parser) addDiagnostic(diagnostic Diagnostic) {
	p.report.DiagnosticCount++
	if len(p.report.Diagnostics) < p.limits.MaxDiagnostics {
		p.report.Diagnostics = append(p.report.Diagnostics, diagnostic)
	}
}

func normalizeLimits(l Limits) (Limits, error) {
	if l.MaxXMLBytes == 0 {
		l.MaxXMLBytes = DefaultMaxXMLBytes
	}
	if l.MaxCases == 0 {
		l.MaxCases = DefaultMaxCases
	}
	if l.MaxDepth == 0 {
		l.MaxDepth = DefaultMaxDepth
	}
	if l.MaxElements == 0 {
		l.MaxElements = DefaultMaxElements
	}
	if l.MaxDiagnostics == 0 {
		l.MaxDiagnostics = DefaultMaxDiagnostics
	}
	if l.MaxXMLBytes < 0 || l.MaxCases < 0 || l.MaxDepth < 0 || l.MaxElements < 0 || l.MaxDiagnostics < 0 {
		return Limits{}, status.InvalidArgumentError("JUnit parser limits must not be negative")
	}
	if l.MaxXMLBytes > DefaultMaxXMLBytes || l.MaxCases > DefaultMaxCases || l.MaxDepth > DefaultMaxDepth || l.MaxElements > DefaultMaxElements || l.MaxDiagnostics > DefaultMaxDiagnostics {
		return Limits{}, status.InvalidArgumentError("JUnit parser limits must not exceed the hard maximums")
	}
	return l, nil
}

func (p *parser) subject() string {
	return "JUnit report for " + p.options.TargetLabel
}

func (p *parser) malformedXMLError(err error) error {
	if err == nil {
		return status.InvalidArgumentErrorf("%s is malformed XML", p.subject())
	}
	return status.InvalidArgumentErrorf("%s is malformed XML: %s", p.subject(), err)
}

type countingReader struct {
	r io.Reader
	n int64
}

type utf8SanitizingReader struct {
	r         io.Reader
	input     [32 << 10]byte
	carry     [utf8.UTFMax]byte
	carryLen  int
	output    []byte
	sanitized []byte
	err       error
	invalid   bool
}

func (r *utf8SanitizingReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.output) == 0 && r.err == nil {
		r.fill()
	}
	if len(r.output) == 0 {
		return 0, r.err
	}
	n := copy(p, r.output)
	r.output = r.output[n:]
	return n, nil
}

func (r *utf8SanitizingReader) fill() {
	copy(r.input[:], r.carry[:r.carryLen])
	n, err := r.r.Read(r.input[r.carryLen:])
	data := r.input[:r.carryLen+n]
	r.carryLen = 0
	if len(data) == 0 {
		r.err = err
		return
	}
	if n == 0 && err == nil {
		r.carryLen = copy(r.carry[:], data)
		return
	}
	r.err = err
	if utf8.Valid(data) {
		r.output = data
		return
	}
	r.sanitized = r.sanitized[:0]
	for len(data) > 0 {
		if data[0] < utf8.RuneSelf {
			i := 1
			for i < len(data) && data[i] < utf8.RuneSelf {
				i++
			}
			r.sanitized = append(r.sanitized, data[:i]...)
			data = data[i:]
			continue
		}
		if !utf8.FullRune(data) && err == nil {
			r.carryLen = copy(r.carry[:], data)
			break
		}
		runeValue, size := utf8.DecodeRune(data)
		if runeValue == utf8.RuneError && size == 1 {
			r.sanitized = utf8.AppendRune(r.sanitized, utf8.RuneError)
			r.invalid = true
			data = data[1:]
			continue
		}
		r.sanitized = append(r.sanitized, data[:size]...)
		data = data[size:]
	}
	r.output = r.sanitized
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
