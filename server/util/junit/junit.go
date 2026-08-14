// Package junit parses bounded JUnit-style XML test reports.
package junit

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ErrResultLimit indicates that Parse returned a safe bounded prefix of the
// structured failures while continuing would exceed a result-count limit.
var ErrResultLimit = errors.New("JUnit result limit exceeded")

// Limits bounds the resources consumed while parsing a report. All limits
// must be positive.
type Limits struct {
	MaxInputBytes int64
	MaxDepth      int
	MaxTokens     int
	// TolerateMalformedXML enables encoding/xml's legacy best-effort recovery.
	// Server ingestion must leave this false; it exists for CLI compatibility
	// with reports that the previous interactive parser accepted.
	TolerateMalformedXML bool
	// MaxFailedTestCases bounds returned testcases. Passing testcases are
	// streamed through but are not retained or counted against this limit.
	MaxFailedTestCases int
	MaxFailures        int
	MaxFieldBytes      int
}

// DefaultLimits returns conservative limits suitable for Bazel test.xml
// artifacts.
func DefaultLimits() Limits {
	return Limits{
		MaxInputBytes:      1 << 20,
		MaxDepth:           64,
		MaxTokens:          100_000,
		MaxFailedTestCases: 100,
		MaxFailures:        100,
		MaxFieldBytes:      64 << 10,
	}
}

// TestCase is one testcase from a JUnit report. SuiteName is the name of its
// immediately enclosing testsuite, if any.
type TestCase struct {
	SuiteName string
	ClassName string
	Name      string
	Failures  []Failure
}

// Failure is one failure or error element contained in a testcase.
type Failure struct {
	Kind    string
	Type    string
	Message string
	Body    string
}

// Parse reads and parses the failed testcases in a JUnit report using a
// streaming XML token decoder. Passing testcases are omitted from the result.
// Invalid UTF-8 is replaced and XML 1.0-illegal control characters are
// discarded before decoding. DTDs and other directives are rejected; the XML
// decoder never resolves external entities.
func Parse(r io.Reader, limits Limits) ([]TestCase, error) {
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	data, err := readBounded(r, limits.MaxInputBytes)
	if err != nil {
		return nil, err
	}
	data = sanitizeXML(data)

	dec := xml.NewDecoder(bytes.NewReader(data))
	dec.Strict = !limits.TolerateMalformedXML
	// Bazel test reports occasionally claim a legacy encoding while containing
	// UTF-8. sanitizeXML has already converted the input to valid UTF-8.
	dec.CharsetReader = func(_ string, input io.Reader) (io.Reader, error) { return input, nil }

	type suite struct {
		name  string
		depth int
	}
	var (
		cases        []TestCase
		suites       []suite
		current      *TestCase
		currentDepth int
		failure      *Failure
		failureDepth int
		body         strings.Builder
		depth        int
		tokens       int
		failures     int
	)

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse JUnit XML: %w", err)
		}
		tokens++
		if tokens > limits.MaxTokens {
			return nil, limitError("tokens", limits.MaxTokens)
		}

		switch t := tok.(type) {
		case xml.Directive:
			return nil, fmt.Errorf("parse JUnit XML: directives are not allowed")
		case xml.ProcInst:
			if !strings.EqualFold(t.Target, "xml") {
				return nil, fmt.Errorf("parse JUnit XML: processing instructions are not allowed")
			}
		case xml.StartElement:
			depth++
			if depth > limits.MaxDepth {
				return nil, limitError("depth", limits.MaxDepth)
			}
			switch t.Name.Local {
			case "testsuite":
				name, err := boundedAttr(t.Attr, "name", limits.MaxFieldBytes)
				if err != nil {
					return nil, err
				}
				suites = append(suites, suite{name: name, depth: depth})
			case "testcase":
				if current != nil {
					return nil, fmt.Errorf("parse JUnit XML: nested testcase elements are not allowed")
				}
				name, err := boundedAttr(t.Attr, "name", limits.MaxFieldBytes)
				if err != nil {
					return nil, err
				}
				className, err := boundedAttr(t.Attr, "classname", limits.MaxFieldBytes)
				if err != nil {
					return nil, err
				}
				suiteName := ""
				if len(suites) > 0 {
					suiteName = suites[len(suites)-1].name
				}
				current = &TestCase{SuiteName: suiteName, ClassName: className, Name: name}
				currentDepth = depth
			case "failure", "error":
				if current == nil {
					continue
				}
				if failure != nil {
					return nil, fmt.Errorf("parse JUnit XML: nested failure elements are not allowed")
				}
				if failures >= limits.MaxFailures {
					if len(current.Failures) > 0 && len(cases) < limits.MaxFailedTestCases {
						cases = append(cases, *current)
					}
					return cases, resultLimitError("failures", limits.MaxFailures)
				}
				typ, err := boundedAttr(t.Attr, "type", limits.MaxFieldBytes)
				if err != nil {
					return nil, err
				}
				message, err := boundedAttr(t.Attr, "message", limits.MaxFieldBytes)
				if err != nil {
					return nil, err
				}
				failure = &Failure{Kind: t.Name.Local, Type: typ, Message: message}
				failureDepth = depth
				body.Reset()
				failures++
			}
		case xml.CharData:
			if failure != nil {
				if body.Len()+len(t) > limits.MaxFieldBytes {
					return nil, limitError("field bytes", limits.MaxFieldBytes)
				}
				body.Write(t)
			}
		case xml.EndElement:
			if failure != nil && depth == failureDepth && (t.Name.Local == "failure" || t.Name.Local == "error") {
				failure.Body = body.String()
				current.Failures = append(current.Failures, *failure)
				failure = nil
				body.Reset()
			}
			if current != nil && depth == currentDepth && t.Name.Local == "testcase" {
				if len(current.Failures) > 0 {
					if len(cases) >= limits.MaxFailedTestCases {
						return cases, resultLimitError("failed test cases", limits.MaxFailedTestCases)
					}
					cases = append(cases, *current)
				}
				current = nil
			}
			if len(suites) > 0 && depth == suites[len(suites)-1].depth && t.Name.Local == "testsuite" {
				suites = suites[:len(suites)-1]
			}
			depth--
		}
	}
	return cases, nil
}

func resultLimitError(name string, value int) error {
	return fmt.Errorf("%w: %s limit exceeded (%d)", ErrResultLimit, name, value)
}

func validateLimits(l Limits) error {
	if l.MaxInputBytes <= 0 || l.MaxDepth <= 0 || l.MaxTokens <= 0 || l.MaxFailedTestCases <= 0 || l.MaxFailures <= 0 || l.MaxFieldBytes <= 0 {
		return fmt.Errorf("parse JUnit XML: all limits must be positive")
	}
	return nil
}

func readBounded(r io.Reader, max int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, fmt.Errorf("read JUnit XML: %w", err)
	}
	if int64(len(data)) > max {
		return nil, limitError("input bytes", max)
	}
	return data, nil
}

func boundedAttr(attrs []xml.Attr, localName string, max int) (string, error) {
	for _, attr := range attrs {
		if attr.Name.Local != localName {
			continue
		}
		if len(attr.Value) > max {
			return "", limitError("field bytes", max)
		}
		return attr.Value, nil
	}
	return "", nil
}

func limitError(resource string, max any) error {
	return fmt.Errorf("parse JUnit XML: %s limit exceeded (max %v)", resource, max)
}

func sanitizeXML(data []byte) []byte {
	valid := strings.ToValidUTF8(string(data), "\uFFFD")
	var b strings.Builder
	b.Grow(len(valid))
	for _, r := range valid {
		if isLegalXMLChar(r) {
			b.WriteRune(r)
		}
	}
	return []byte(b.String())
}

func isLegalXMLChar(r rune) bool {
	return r == '\t' || r == '\n' || r == '\r' ||
		(r >= 0x20 && r <= 0xD7FF) ||
		(r >= 0xE000 && r <= 0xFFFD) ||
		(r >= 0x10000 && r <= 0x10FFFF)
}
