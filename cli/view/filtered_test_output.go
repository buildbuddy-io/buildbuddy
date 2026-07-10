package view

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/util/download"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
)

// failingTestStatuses are the target statuses that can carry failed test cases.
var failingTestStatuses = []cmpb.Status{cmpb.Status_FAILED, cmpb.Status_FLAKY, cmpb.Status_TIMED_OUT}

// ViewFilteredTestOutput writes the output of failed test cases to w.
//
// If one or more target labels are given, only those targets are inspected.
// Within each target, only failed test cases whose
// name matches testFilter (a regular expression) are printed; an empty
// testFilter matches every failed case.
func ViewFilteredTestOutput(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, downloader download.Downloader, w io.Writer, invocationID string, targets []string, testFilter string) (int, error) {
	re, err := regexp.Compile(testFilter)
	if err != nil {
		return 1, fmt.Errorf("invalid --test_filter %q: %w", testFilter, err)
	}

	targetsSpecified := len(targets) > 0
	if !targetsSpecified {
		log.Warnf("No target specified; searching all failed test targets in the invocation. This is slow — pass one or more target labels to improve performance.")
		targets, err = failedTestTargets(ctx, bbClient, invocationID)
		if err != nil {
			return -1, err
		}
	}

	matches := 0
	noResults := 0
	for _, t := range targets {
		t = normalizeTarget(t)
		events, err := testResultEvents(ctx, bbClient, invocationID, t)
		if err != nil {
			if targetsSpecified && status.IsNotFoundError(err) {
				return 1, fmt.Errorf("target %s not found in invocation %s", t, invocationID)
			}
			return -1, err
		}
		if len(events) == 0 {
			// The target resolved but produced no test results (e.g. it isn't a
			// test target, or wasn't tested) — distinct from a test that ran and
			// passed. Only worth calling out for targets the user named.
			if targetsSpecified {
				log.Printf("Target %s has no test results in invocation %s.", t, invocationID)
				noResults++
			}
			continue
		}
		matches += printFailedTestCases(ctx, w, downloader, t, events, re)
	}
	// Summarize when nothing was printed, unless every named target simply had
	// no test results (already reported per-target above).
	if matches == 0 && !(targetsSpecified && noResults == len(targets)) {
		if testFilter != "" {
			log.Printf("No failed test cases matching %q found.", testFilter)
		} else {
			log.Printf("No failed test cases found.")
		}
	}
	return 0, nil
}

// normalizeTarget ensures a target label has the leading "//" that the GetTarget
// API expects.
func normalizeTarget(target string) string {
	if strings.HasPrefix(target, "//") || strings.HasPrefix(target, "@") {
		return target
	}
	return "//" + target
}

// failedTestTargets returns every failing test target in the invocation.
func failedTestTargets(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID string) ([]string, error) {
	var targets []string
	for _, s := range failingTestStatuses {
		s := s
		pageToken := ""
		for {
			resp, err := bbClient.GetTarget(ctx, &trpb.GetTargetRequest{
				InvocationId: invocationID,
				Status:       &s,
				PageToken:    pageToken,
			})
			if err != nil {
				return nil, err
			}
			nextPageToken := ""
			for _, g := range resp.GetTargetGroups() {
				for _, t := range g.GetTargets() {
					targets = append(targets, t.GetMetadata().GetLabel())
				}
				if g.GetNextPageToken() != "" {
					nextPageToken = g.GetNextPageToken()
				}
			}
			if nextPageToken == "" {
				break
			}
			pageToken = nextPageToken
		}
	}
	return targets, nil
}

// testResultEvents fetches the TestResult events (one per run/shard/attempt)
// for a single test target.
func testResultEvents(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID, label string) ([]*bespb.BuildEvent, error) {
	resp, err := bbClient.GetTarget(ctx, &trpb.GetTargetRequest{
		InvocationId: invocationID,
		TargetLabel:  label,
	})
	if err != nil {
		return nil, err
	}
	for _, g := range resp.GetTargetGroups() {
		for _, t := range g.GetTargets() {
			if t.GetMetadata().GetLabel() == label {
				return t.GetTestResultEvents(), nil
			}
		}
	}
	return nil, nil
}

// printFailedTestCases downloads and parses the test.xml for each failing
// attempt of a target and prints the failed test cases whose name matches re,
// returning the number of matching test cases printed.
func printFailedTestCases(ctx context.Context, w io.Writer, downloader download.Downloader, label string, events []*bespb.BuildEvent, re *regexp.Regexp) int {
	matches := 0
	for _, event := range events {
		tr := event.GetTestResult()
		if tr.GetStatus() == bespb.TestStatus_PASSED || tr.GetStatus() == bespb.TestStatus_NO_STATUS {
			continue
		}
		uri := testXMLURI(tr)
		if uri == "" {
			continue
		}
		var buf bytes.Buffer
		if err := downloader.GetBytestreamFile(ctx, uri, &buf); err != nil {
			log.Warnf("Failed to download test.xml for %s: %s", label, err)
			continue
		}
		cases, err := parseTestCases(buf.Bytes())
		if err != nil {
			log.Debugf("Failed to parse test.xml for %s: %s", label, err)
			continue
		}
		for _, tc := range cases {
			if len(tc.Failures) == 0 && len(tc.Errors) == 0 {
				continue
			}
			if !re.MatchString(tc.Name) {
				continue
			}
			printTestCase(w, label, event.GetId().GetTestResult(), tc)
			matches++
		}
	}
	return matches
}

// testXMLURI returns the bytestream URI of the test.xml artifact for a test
// result, or "" if there isn't one.
func testXMLURI(tr *bespb.TestResult) string {
	for _, f := range tr.GetTestActionOutput() {
		if f.GetName() == "test.xml" {
			return f.GetUri()
		}
	}
	return ""
}

type junitTestCase struct {
	Name     string             `xml:"name,attr"`
	Failures []junitFailureNode `xml:"failure"`
	Errors   []junitFailureNode `xml:"error"`
}

type junitFailureNode struct {
	Message string `xml:"message,attr"`
	Type    string `xml:"type,attr"`
	Body    string `xml:",chardata"`
}

// parseTestCases extracts all <testcase> elements from a JUnit test.xml.
func parseTestCases(data []byte) ([]junitTestCase, error) {
	dec := xml.NewDecoder(bytes.NewReader(sanitizeXML(data)))
	// Test XML may declare a non-UTF-8 charset or contain minor malformations;
	// be lenient and pass bytes through as-is.
	dec.Strict = false
	dec.CharsetReader = func(_ string, input io.Reader) (io.Reader, error) { return input, nil }

	var cases []junitTestCase
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "testcase" {
			continue
		}
		var tc junitTestCase
		if err := dec.DecodeElement(&tc, &se); err != nil {
			return nil, err
		}
		cases = append(cases, tc)
	}
	return cases, nil
}

// printTestCase writes a single failed test case to w.
func printTestCase(w io.Writer, label string, id *bespb.BuildEventId_TestResultId, tc junitTestCase) {
	fmt.Fprintf(w, "===================== %s%s =====================\n", label, runSuffix(id))
	nodes := append(append([]junitFailureNode{}, tc.Failures...), tc.Errors...)
	for _, n := range nodes {
		if msg := strings.TrimSpace(n.Message); msg != "" {
			fmt.Fprintln(w, cleanTerminalText(msg))
		}
		if body := strings.TrimRight(cleanTerminalText(n.Body), "\n"); strings.TrimSpace(body) != "" {
			fmt.Fprintln(w, body)
		}
	}
	fmt.Fprintln(w)
}

// runSuffix formats the run/shard/attempt of a test result for display, e.g.
// " (run 2, attempt 1)".
func runSuffix(id *bespb.BuildEventId_TestResultId) string {
	if id == nil {
		return ""
	}
	var parts []string
	if id.GetRun() > 0 {
		parts = append(parts, fmt.Sprintf("run %d", id.GetRun()))
	}
	if id.GetShard() > 0 {
		parts = append(parts, fmt.Sprintf("shard %d", id.GetShard()))
	}
	if id.GetAttempt() > 0 {
		parts = append(parts, fmt.Sprintf("attempt %d", id.GetAttempt()))
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + strings.Join(parts, ", ") + ")"
}

// cleanTerminalText restores ANSI escape sequences that get mangled when test
// output is stored in XML, mirroring what the web UI does.
func cleanTerminalText(s string) string {
	esc := string(rune(27)) // ANSI escape (0x1b)
	return strings.NewReplacer(
		"�[", esc+"[",
		"#x1b[", esc+"[",
		"#x1B[", esc+"[",
	).Replace(s)
}

// sanitizeXML removes bytes that are not legal in XML 1.0, which test output
// (e.g. raw ANSI escape codes) sometimes contains and which would otherwise
// cause the XML decoder to error.
func sanitizeXML(b []byte) []byte {
	var sb strings.Builder
	sb.Grow(len(b))
	for _, r := range string(b) {
		if isLegalXMLChar(r) {
			sb.WriteRune(r)
		}
	}
	return []byte(sb.String())
}

func isLegalXMLChar(r rune) bool {
	return r == '\t' || r == '\n' || r == '\r' ||
		(r >= 0x20 && r <= 0xD7FF) ||
		(r >= 0xE000 && r <= 0xFFFD) ||
		(r >= 0x10000 && r <= 0x10FFFF)
}
