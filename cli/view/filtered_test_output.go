package view

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/util/download"
	"github.com/buildbuddy-io/buildbuddy/server/util/junit"
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
		if err != nil && !errors.Is(err, junit.ErrResultLimit) {
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
		cases, err := junit.Parse(bytes.NewReader(buf.Bytes()), cliJUnitLimits(buf.Len()))
		if err != nil {
			// The shared parser is intentionally strict about malformed XML and
			// rejects directives. Skipping an unsafe report is preferable to the
			// legacy parser's best-effort recovery from mismatched markup.
			log.Debugf("Failed to parse test.xml for %s: %s", label, err)
			continue
		}
		for _, tc := range cases {
			if !re.MatchString(tc.Name) {
				continue
			}
			printTestCase(w, label, event.GetId().GetTestResult(), tc)
			matches++
		}
	}
	return matches
}

// cliJUnitLimits preserves the CLI's historical ability to inspect a complete
// already-downloaded report. Server-side ingestion uses stricter fixed limits,
// but every XML node and field here is still bounded by the input buffer.
func cliJUnitLimits(inputBytes int) junit.Limits {
	bound := max(inputBytes+1, 1)
	maxInt := int(^uint(0) >> 1)
	fieldBound := maxInt
	if bound <= maxInt/3 {
		// strings.ToValidUTF8 can replace each invalid input byte with one
		// three-byte replacement rune.
		fieldBound = bound * 3
	}
	return junit.Limits{
		MaxInputBytes:        int64(max(inputBytes, 1)),
		MaxDepth:             bound,
		MaxTokens:            bound,
		TolerateMalformedXML: true,
		MaxFailedTestCases:   bound,
		MaxFailures:          bound,
		MaxFieldBytes:        fieldBound,
	}
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

// printTestCase writes a single failed test case to w.
func printTestCase(w io.Writer, label string, id *bespb.BuildEventId_TestResultId, tc junit.TestCase) {
	fmt.Fprintf(w, "===================== %s%s =====================\n", label, runSuffix(id))
	// Preserve the historical display order: all <failure> nodes followed by
	// all <error> nodes, regardless of their order in the report.
	for _, kind := range []string{"failure", "error"} {
		for _, n := range tc.Failures {
			if n.Kind != kind {
				continue
			}
			if msg := strings.TrimSpace(n.Message); msg != "" {
				fmt.Fprintln(w, cleanTerminalText(msg))
			}
			if body := strings.TrimRight(cleanTerminalText(n.Body), "\n"); strings.TrimSpace(body) != "" {
				fmt.Fprintln(w, body)
			}
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
