// Package testbuddy implements the test-report and get-tests CLI commands.
package testbuddy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/flaghistory"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/junit"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var bazelTestOutputLayout = regexp.MustCompile(
	`^(?:run_[1-9][0-9]*_of_([1-9][0-9]*)|shard_[1-9][0-9]*_of_([1-9][0-9]*)(?:_run_[1-9][0-9]*_of_([1-9][0-9]*))?)$`,
)

// ReportRequestTargetBytes bounds one client-stream message.
//
// The gRPC receive limit is 50 MB, so this only has to be under that — but
// filling the limit would make both sides hold a 50 MB message, and a single
// failed send would cost the whole 50 MB again on retry. 8 MiB keeps peak
// memory per in-flight message small on the CLI and on the server, which
// processes each message synchronously before reading the next.
//
// A report larger than the budget spans several messages; it is never
// truncated. One result cannot exceed the budget on its own: normalization
// bounds every component of a result — a failure message to 512 bytes and a
// source URL to 2,048 — so a result is kilobytes, not megabytes.
const ReportRequestTargetBytes = 8 << 20

// DiagnosticLog accumulates what the JUnit parser could not use, across every
// file in one report.
//
// The counts distinguish dropped cases, ignored fields, and reports whose text
// had to be normalized. Only dropped cases are invisible downstream.
//
// detail is called once per diagnostic and may be nil. The CLI passes the debug
// logger, which discards unless verbose, so the listing costs nothing to leave
// wired up; the parser caps diagnostics per file, and anything past that cap is
// counted in Truncated rather than silently lost.
type DiagnosticLog struct {
	Dropped    int
	Ignored    int
	Normalized int
	Truncated  int
	detail     func(string)
}

func NewDiagnosticLog(detail func(string)) *DiagnosticLog {
	return &DiagnosticLog{detail: detail}
}

func (l *DiagnosticLog) Add(xmlPath, targetLabel string, report *junit.Report) {
	l.Truncated += report.DroppedDiagnostics
	for _, diagnostic := range report.Diagnostics {
		if diagnostic.Code == junit.DiagnosticInvalidUTF8 {
			l.Normalized++
		} else if diagnostic.Code.DropsCase() {
			l.Dropped++
		} else {
			l.Ignored++
		}
		if l.detail != nil {
			l.detail(diagnosticLine(xmlPath, targetLabel, diagnostic))
		}
	}
	// The cap is per file, so an over-cap file still reports its own count.
	if report.DroppedDiagnostics > 0 && l.detail != nil {
		l.detail(fmt.Sprintf("%d more diagnostics in %s were not recorded (per-file cap)",
			report.DroppedDiagnostics, xmlPath))
	}
}

func diagnosticLine(xmlPath, targetLabel string, diagnostic junit.Diagnostic) string {
	if diagnostic.Code == junit.DiagnosticInvalidUTF8 {
		return fmt.Sprintf("normalized report %s %s (%s)", targetLabel, diagnostic.Code, xmlPath)
	}
	kind := "ignored field"
	if diagnostic.Code.DropsCase() {
		kind = "dropped case"
	}
	name := diagnostic.CaseName
	if name == "" {
		name = "<unnamed>"
	}
	where := xmlPath
	if diagnostic.CaseIndex >= 0 {
		where = fmt.Sprintf("%s case %d", xmlPath, diagnostic.CaseIndex)
	}
	return fmt.Sprintf("%s %s %q %s (%s)", kind, targetLabel, name, diagnostic.Code, where)
}

// Summary is the one line printed without verbose. It is empty when the parser
// used everything.
func (l *DiagnosticLog) Summary() string {
	total := l.Dropped + l.Ignored + l.Normalized
	if total == 0 && l.Truncated == 0 {
		return ""
	}
	summary := fmt.Sprintf("%d JUnit diagnostics: %d cases dropped, %d fields ignored",
		total, l.Dropped, l.Ignored)
	if l.Normalized > 0 {
		summary += fmt.Sprintf(", normalized reports: %d", l.Normalized)
	}
	if l.Truncated > 0 {
		summary += fmt.Sprintf(", %d beyond the per-file cap", l.Truncated)
	}
	// The flag is a bare boolean: "--verbose=1" is rejected by the parser.
	return summary + "\nrerun with --verbose (or BB_VERBOSE=1) to list them"
}

// ReportBatcher packs results into client-stream messages that stay within
// ReportRequestTargetBytes, sending each one as soon as it is full.
//
// Sizing is exact rather than estimated: a result contributes its own encoded
// size plus the tag and length prefix that carry it in the enclosing message,
// which is what the receiver measures against its limit.
type ReportBatcher struct {
	repository string
	send       func(*tbpb.ReportTestResultsRequest) error
	batch      *tbpb.ReportTestResultsRequest
	size       int
	count      int
}

func NewReportBatcher(repository string, send func(*tbpb.ReportTestResultsRequest) error) *ReportBatcher {
	b := &ReportBatcher{repository: repository, send: send}
	b.reset()
	return b
}

func (b *ReportBatcher) reset() {
	b.batch = &tbpb.ReportTestResultsRequest{RepoUrl: b.repository}
	b.size = protowire.SizeTag(1) + protowire.SizeBytes(len(b.repository))
	b.count = 0
}

// makeRoom flushes the pending message if result would push it over budget.
// A result is never split, so an empty message always accepts one.
func (b *ReportBatcher) makeRoom(field protowire.Number, result proto.Message) (int, error) {
	wireSize := protowire.SizeTag(field) + protowire.SizeBytes(proto.Size(result))
	if b.count > 0 && b.size+wireSize > ReportRequestTargetBytes {
		if err := b.Flush(); err != nil {
			return 0, err
		}
	}
	return wireSize, nil
}

func (b *ReportBatcher) AddTarget(result *tbpb.TestTargetResult) error {
	wireSize, err := b.makeRoom(3, result)
	if err != nil {
		return err
	}
	b.batch.TestTargets = append(b.batch.TestTargets, result)
	b.size += wireSize
	b.count++
	return nil
}

func (b *ReportBatcher) AddCase(result *tbpb.TestCaseResult) error {
	wireSize, err := b.makeRoom(2, result)
	if err != nil {
		return err
	}
	b.batch.TestCases = append(b.batch.TestCases, result)
	b.size += wireSize
	b.count++
	return nil
}

// Flush sends the pending message, if any. Sending an empty message would
// charge the server a round trip to learn nothing.
func (b *ReportBatcher) Flush() error {
	if b.count == 0 {
		return nil
	}
	if err := b.send(b.batch); err != nil {
		return err
	}
	b.reset()
	return nil
}

var (
	TestReportFlags = flag.NewFlagSet("test-report", flag.ContinueOnError)
	reportTarget    = TestReportFlags.String("target", "", "BuildBuddy gRPC target; defaults to grpc://127.0.0.1:1985.")
	reportRepo      = TestReportFlags.String("repo_url", "", "Repository URL; defaults to git remote.origin.url.")
	reportSourceURL = TestReportFlags.String(
		"source_url", "", "Result URL; defaults to the most recent bb test invocation.")
	reportSource = TestReportFlags.String(
		"source", "monitor", "Observation source: presubmit, postsubmit, or monitor.")
	reportTargetLabel = TestReportFlags.String("target_label", "", "Bazel target label; inferred from bazel-testlogs paths by default.")

	GetTestsFlags  = flag.NewFlagSet("get-tests", flag.ContinueOnError)
	getTarget      = GetTestsFlags.String("target", "", "BuildBuddy gRPC target; defaults to grpc://127.0.0.1:1985.")
	getRepo        = GetTestsFlags.String("repo_url", "", "Repository URL; defaults to git remote.origin.url.")
	getPath        = GetTestsFlags.String("path", "", "Repository directory prefix.")
	getLimit       = GetTestsFlags.Int("limit", 100, "Maximum number of tests to print.")
	getTargetLabel = GetTestsFlags.String("target_label", "", "Exact Bazel target label.")
	getCaseName    = GetTestsFlags.String("case_name", "", "Exact test case name.")
)

func HandleTestReport(args []string) (int, error) {
	if err := arg.ParseFlagSet(TestReportFlags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print("usage: bb test-report [flags] [test.xml or directory ...]")
			return 0, nil
		}
		return 1, err
	}
	workspacePath, err := workspace.Path()
	if err != nil {
		return 1, err
	}
	commitSHA, workspaceDirty, err := WorkspaceRevision(workspacePath)
	if err != nil {
		return 1, err
	}
	repository, err := repositoryURL(workspacePath, *reportRepo)
	if err != nil {
		return 1, err
	}
	sourceURL, err := resultURL(*reportSourceURL)
	if err != nil {
		return 1, err
	}
	source, err := ParseObservationSource(*reportSource)
	if err != nil {
		return 1, err
	}
	metadata := ObservationMetadata{
		SourceURL: sourceURL, Source: source,
		CommitSHA: commitSHA, WorkspaceDirty: workspaceDirty,
	}
	inputs := TestReportFlags.Args()
	if len(inputs) == 0 {
		inputs = []string{"bazel-testlogs"}
	}
	paths, err := FindTestXMLFiles(inputs)
	if err != nil {
		return 1, err
	}
	if len(paths) == 0 {
		return 1, status.InvalidArgumentError("the supplied paths contain no test.xml files")
	}
	target, err := backend(*reportTarget)
	if err != nil {
		return 1, err
	}
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		return 1, err
	}
	defer conn.Close()
	ctx, err := authenticatedContext(context.Background(), target)
	if err != nil {
		return 1, err
	}
	stream, err := tbpb.NewTestBuddyServiceClient(conn).ReportTestResults(ctx)
	if err != nil {
		return 1, err
	}
	defer stream.CloseSend()
	batcher := NewReportBatcher(repository, stream.Send)
	diagnostics := NewDiagnosticLog(func(line string) { log.Debug(line) })
	for _, path := range paths {
		targetLabel := *reportTargetLabel
		if targetLabel == "" {
			targetLabel, err = TargetLabelFromXMLPath(workspacePath, path)
			if err != nil {
				return 1, err
			}
		}
		file, err := os.Open(path)
		if err != nil {
			return 1, err
		}
		report, parseErr := junit.Parse(context.Background(), file, junit.Options{TargetLabel: targetLabel})
		closeErr := file.Close()
		if parseErr != nil {
			return 1, parseErr
		}
		if closeErr != nil {
			return 1, closeErr
		}
		diagnostics.Add(path, targetLabel, report)
		targetResult, testCases, err := ResultsForReport(path, targetLabel, metadata, report)
		if err != nil {
			return 1, err
		}
		if err := batcher.AddTarget(targetResult); err != nil {
			return 1, err
		}
		for _, testCase := range testCases {
			if err := batcher.AddCase(testCase); err != nil {
				return 1, err
			}
		}
	}
	if err := batcher.Flush(); err != nil {
		return 1, err
	}
	rsp, err := stream.CloseAndRecv()
	if err != nil {
		return 1, err
	}
	fmt.Printf("reported %d test results (%d rejected) from %d XML files\n",
		rsp.GetAcceptedCount(), rsp.GetRejectedCount(), len(paths))
	fmt.Printf("source %s\n", sourceURL)
	if summary := diagnostics.Summary(); summary != "" {
		fmt.Println(summary)
	}
	if rsp.GetAcceptedCount() == 0 {
		return 1, nil
	}
	return 0, nil
}

func ParseObservationSource(value string) (tbpb.TestObservationSource, error) {
	switch strings.ToLower(value) {
	case "presubmit":
		return tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_PRESUBMIT, nil
	case "postsubmit":
		return tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_POSTSUBMIT, nil
	case "monitor":
		return tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR, nil
	default:
		return tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_UNKNOWN,
			status.InvalidArgumentError("--source must be presubmit, postsubmit, or monitor")
	}
}

type ObservationMetadata struct {
	SourceURL      string
	Source         tbpb.TestObservationSource
	CommitSHA      string
	WorkspaceDirty bool
}

func WorkspaceRevision(workspacePath string) (string, bool, error) {
	head := exec.Command("git", "rev-parse", "HEAD")
	head.Dir = workspacePath
	output, err := head.Output()
	if err != nil {
		return "", false, status.FailedPreconditionErrorf("could not read git HEAD: %s", err)
	}
	commitSHA := strings.TrimSpace(string(output))
	if commitSHA == "" {
		return "", false, status.FailedPreconditionError("git HEAD is empty")
	}
	statusCommand := exec.Command("git", "status", "--porcelain=v1", "--untracked-files=normal")
	statusCommand.Dir = workspacePath
	output, err = statusCommand.Output()
	if err != nil {
		return "", false, status.FailedPreconditionErrorf("could not inspect git status: %s", err)
	}
	return commitSHA, len(output) > 0, nil
}

func ResultsForReport(xmlPath, targetLabel string, metadata ObservationMetadata, report *junit.Report) (*tbpb.TestTargetResult, []*tbpb.TestCaseResult, error) {
	targetOutcome, err := BazelTargetOutcome(xmlPath)
	if err != nil {
		return nil, nil, err
	}
	eventTimeUsec := report.EventTimeUsec
	if eventTimeUsec <= 0 {
		info, err := os.Stat(xmlPath)
		if err != nil {
			return nil, nil, err
		}
		eventTimeUsec = info.ModTime().UnixMicro()
	}
	if eventTimeUsec <= 0 {
		return nil, nil, status.InvalidArgumentError("a positive report event time is required")
	}
	executionContext := resultContext(xmlPath)
	target := &tbpb.TestTargetResult{
		Identity: &tbpb.TestTargetIdentity{TargetLabel: targetLabel},
		Result: &tbpb.TestResult{
			Outcome: targetOutcome, DurationUsec: report.DurationUsec, SourceUrl: metadata.SourceURL,
			EventTimeUsec:  eventTimeUsec,
			ResultId:       resultID("target", metadata.SourceURL, targetLabel, executionContext),
			Source:         metadata.Source,
			CommitSha:      metadata.CommitSHA,
			WorkspaceDirty: metadata.WorkspaceDirty,
		},
	}
	if targetOutcome == tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT {
		target.Result.FailureMessage = "Bazel test target timed out"
		return target, nil, nil
	}
	if report.UnattributedFailure {
		target.Result.Outcome = tbpb.TestOutcome_TEST_OUTCOME_FAIL
		target.Result.FailureMessage = "test target failed without an attributable test case"
	}
	testCases := make([]*tbpb.TestCaseResult, 0, len(report.Cases))
	for _, testCase := range report.Cases {
		caseEventTimeUsec := testCase.EventTimeUsec
		if caseEventTimeUsec <= 0 {
			caseEventTimeUsec = eventTimeUsec
		}
		testCases = append(testCases, &tbpb.TestCaseResult{
			Identity: &tbpb.TestCaseIdentity{
				Target: &tbpb.TestTargetIdentity{TargetLabel: testCase.TargetLabel}, CaseName: testCase.CaseName,
			},
			Result: &tbpb.TestResult{
				Outcome: testCase.Outcome, DurationUsec: testCase.DurationUsec,
				FailureMessage: testCase.FailureMessage, SourceUrl: metadata.SourceURL,
				EventTimeUsec: caseEventTimeUsec,
				ResultId: resultID("case", metadata.SourceURL, targetLabel, testCase.CaseName,
					executionContext, strconv.Itoa(testCase.OccurrenceIndex)),
				Source:         metadata.Source,
				CommitSha:      metadata.CommitSHA,
				WorkspaceDirty: metadata.WorkspaceDirty,
			},
		})
	}
	return target, testCases, nil
}

func resultContext(path string) string {
	path = filepath.ToSlash(filepath.Clean(path))
	for _, marker := range []string{"bazel-testlogs/", "testlogs/"} {
		if index := strings.LastIndex(path, marker); index >= 0 {
			return path[index+len(marker):]
		}
	}
	return path
}

func resultID(parts ...string) string {
	h := sha256.New()
	var size [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = h.Write(size[:])
		_, _ = io.WriteString(h, part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func BazelTargetOutcome(xmlPath string) (tbpb.TestOutcome, error) {
	logPath := filepath.Join(filepath.Dir(xmlPath), "test.log")
	file, err := os.Open(logPath)
	if os.IsNotExist(err) {
		return tbpb.TestOutcome_TEST_OUTCOME_PASS, nil
	}
	if err != nil {
		return tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN, err
	}
	const tailBytes = 64 << 10
	if info.Size() > tailBytes {
		if _, err := file.Seek(-tailBytes, io.SeekEnd); err != nil {
			return tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN, err
		}
	}
	tail, err := io.ReadAll(file)
	if err != nil {
		return tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN, err
	}
	if bytes.Contains(tail, []byte("-- Test timed out at ")) {
		return tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT, nil
	}
	return tbpb.TestOutcome_TEST_OUTCOME_PASS, nil
}

func HandleGetTests(args []string) (int, error) {
	if err := arg.ParseFlagSet(GetTestsFlags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print("usage: bb get-tests [--path=directory] [--limit=100]")
			return 0, nil
		}
		return 1, err
	}
	if GetTestsFlags.NArg() != 0 {
		return 1, status.InvalidArgumentError("get-tests does not accept positional arguments")
	}
	workspacePath, err := workspace.Path()
	if err != nil {
		return 1, err
	}
	repository, err := repositoryURL(workspacePath, *getRepo)
	if err != nil {
		return 1, err
	}
	target, err := backend(*getTarget)
	if err != nil {
		return 1, err
	}
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		return 1, err
	}
	defer conn.Close()
	ctx, err := authenticatedContext(context.Background(), target)
	if err != nil {
		return 1, err
	}
	client := tbpb.NewTestBuddyServiceClient(conn)
	if *getTargetLabel != "" || *getCaseName != "" {
		if *getTargetLabel == "" || *getCaseName == "" {
			return 1, status.InvalidArgumentError("--target_label and --case_name must be specified together")
		}
		rsp, err := client.GetTestCase(ctx, &tbpb.GetTestCaseRequest{
			RepoUrl: repository,
			Identity: &tbpb.TestCaseIdentity{
				Target:   &tbpb.TestTargetIdentity{TargetLabel: *getTargetLabel},
				CaseName: *getCaseName,
			},
		})
		if err != nil {
			return 1, err
		}
		printSummary(rsp.GetTest())
		fmt.Println("RECENT RESULTS")
		fmt.Println("TYPE\tRESULT\tOUTCOME\tDURATION\tFAILURE")
		for _, result := range rsp.GetRecentResults() {
			fmt.Printf("%s\t%s\t%s\t%s\t%s\n",
				strings.TrimPrefix(result.GetSource().String(), "TEST_OBSERVATION_SOURCE_"),
				result.GetSourceUrl(),
				strings.TrimPrefix(result.GetOutcome().String(), "TEST_OUTCOME_"),
				time.Duration(result.GetDurationUsec())*time.Microsecond,
				result.GetFailureMessage())
		}
		fmt.Println("STATE CHANGES")
		fmt.Println("TIME\tPREVIOUS\tCURRENT")
		for _, transition := range rsp.GetTransitions() {
			fmt.Printf("%s\t%s\t%s\n",
				time.UnixMicro(transition.GetEventTimeUsec()).Format(time.RFC3339),
				strings.TrimPrefix(transition.GetPreviousHealth().String(), "TEST_HEALTH_"),
				strings.TrimPrefix(transition.GetHealth().String(), "TEST_HEALTH_"))
		}
		return 0, nil
	}
	if *getLimit <= 0 {
		return 1, status.InvalidArgumentError("--limit must be greater than zero")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.GetTests(ctx, &tbpb.GetTestsRequest{
		RepoUrl: repository, PackagePrefix: *getPath,
	})
	if err != nil {
		return 1, err
	}
	fmt.Println("HEALTH\tPASS RATE\tMEAN\tTARGET\tCASE")
	printed := 0
	for printed < *getLimit {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 1, err
		}
		for _, test := range rsp.GetTests() {
			summary := test.GetSummary()
			health := strings.TrimPrefix(summary.GetHealth().String(), "TEST_HEALTH_")
			fmt.Printf("%s\t%.1f%%\t%s\t%s\t%s\n",
				health, summary.GetPassRate()*100,
				time.Duration(summary.GetMeanDurationUsec())*time.Microsecond,
				test.GetIdentity().GetTarget().GetTargetLabel(), test.GetIdentity().GetCaseName())
			printed++
			if printed == *getLimit {
				cancel()
				break
			}
		}
	}
	return 0, nil
}

func printSummary(test *tbpb.TestCaseSummary) {
	summary := test.GetSummary()
	fmt.Println("HEALTH\tPASS RATE\tMEAN\tTARGET\tCASE")
	fmt.Printf("%s\t%.1f%%\t%s\t%s\t%s\n",
		strings.TrimPrefix(summary.GetHealth().String(), "TEST_HEALTH_"),
		summary.GetPassRate()*100,
		time.Duration(summary.GetMeanDurationUsec())*time.Microsecond,
		test.GetIdentity().GetTarget().GetTargetLabel(),
		test.GetIdentity().GetCaseName())
}

func TargetLabelFromXMLPath(workspacePath, path string) (string, error) {
	absolute := path
	if !filepath.IsAbs(absolute) {
		absolute = filepath.Join(workspacePath, path)
	}
	slashed := filepath.ToSlash(absolute)
	marker := "/bazel-testlogs/"
	index := strings.LastIndex(slashed, marker)
	if index < 0 {
		marker = "/testlogs/"
		index = strings.LastIndex(slashed, marker)
	}
	if index < 0 || !strings.HasSuffix(slashed, "/test.xml") {
		return "", status.InvalidArgumentErrorf(
			"cannot infer a Bazel target from %q; pass --target_label", path)
	}
	relative := strings.TrimSuffix(slashed[index+len(marker):], "/test.xml")
	parts := strings.Split(relative, "/")
	if _, ok := testOutputLayout(parts[len(parts)-1]); len(parts) > 1 && ok {
		parts = parts[:len(parts)-1]
	}
	if len(parts) == 0 || parts[len(parts)-1] == "" {
		return "", status.InvalidArgumentErrorf("cannot infer a Bazel target from %q", path)
	}
	targetName := parts[len(parts)-1]
	packagePath := strings.Join(parts[:len(parts)-1], "/")
	targetLabel, err := identity.CanonicalizeTargetLabel("//" + packagePath + ":" + targetName)
	if err != nil {
		return "", status.InvalidArgumentErrorf(
			"cannot infer a Bazel target from %q: %s", path, err)
	}
	return targetLabel, nil
}

type testXMLLayout struct {
	paths  map[string]struct{}
	newest time.Time
}

func FindTestXMLFiles(inputs []string) ([]string, error) {
	explicit := make(map[string]struct{})
	layouts := make(map[string]map[string]*testXMLLayout)
	addDiscovered := func(path string) error {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		parent := filepath.Dir(path)
		targetRoot := parent
		layoutName := "direct"
		if layout, ok := testOutputLayout(filepath.Base(parent)); ok {
			targetRoot = filepath.Dir(parent)
			layoutName = layout
		}
		if layouts[targetRoot] == nil {
			layouts[targetRoot] = make(map[string]*testXMLLayout)
		}
		layout := layouts[targetRoot][layoutName]
		if layout == nil {
			layout = &testXMLLayout{paths: make(map[string]struct{})}
			layouts[targetRoot][layoutName] = layout
		}
		layout.paths[path] = struct{}{}
		if info.ModTime().After(layout.newest) {
			layout.newest = info.ModTime()
		}
		return nil
	}
	for _, input := range inputs {
		info, err := os.Stat(input)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			explicit[input] = struct{}{}
			continue
		}
		root, err := filepath.EvalSymlinks(input)
		if err != nil {
			return nil, err
		}
		err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && entry.Name() == "test.xml" {
				return addDiscovered(path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	paths := explicit
	for _, targetLayouts := range layouts {
		var selectedName string
		var selected *testXMLLayout
		for name, layout := range targetLayouts {
			if selected == nil || layout.newest.After(selected.newest) ||
				(layout.newest.Equal(selected.newest) && name < selectedName) {
				selectedName = name
				selected = layout
			}
		}
		for path := range selected.paths {
			paths[path] = struct{}{}
		}
	}
	sorted := make([]string, 0, len(paths))
	for path := range paths {
		sorted = append(sorted, path)
	}
	sort.Strings(sorted)
	return sorted, nil
}

func testOutputLayout(directory string) (string, bool) {
	match := bazelTestOutputLayout.FindStringSubmatch(directory)
	if match == nil {
		return "", false
	}
	if match[1] != "" {
		return "runs:" + match[1], true
	}
	layout := "shards:" + match[2]
	if match[3] != "" {
		layout += ":runs:" + match[3]
	}
	return layout, true
}

func repositoryURL(workspacePath, override string) (string, error) {
	if override != "" {
		return override, nil
	}
	cmd := exec.Command("git", "config", "--get", "remote.origin.url")
	cmd.Dir = workspacePath
	output, err := cmd.Output()
	if err != nil {
		return "", status.FailedPreconditionError(
			"could not read git remote.origin.url; pass --repo_url")
	}
	repository := gitutil.StripRepoURLCredentials(strings.TrimSpace(string(output)))
	if repository == "" {
		return "", status.FailedPreconditionError("git remote.origin.url is empty; pass --repo_url")
	}
	return repository, nil
}

func resultURL(override string) (string, error) {
	if override != "" {
		return override, nil
	}
	invocationID, err := flaghistory.GetPreviousFlag(flaghistory.InvocationIDFlagName)
	if err != nil {
		return "", err
	}
	base, err := flaghistory.GetPreviousFlag(flaghistory.BesResultsUrlFlagName)
	if err != nil {
		return "", err
	}
	if invocationID == "" || base == "" {
		return "", status.FailedPreconditionError(
			"could not determine the most recent invocation URL; pass --source_url")
	}
	if strings.Contains(base, invocationID) {
		return base, nil
	}
	return base + invocationID, nil
}

func backend(override string) (string, error) {
	if override != "" {
		return override, nil
	}
	return "grpc://127.0.0.1:1985", nil
}

func authenticatedContext(ctx context.Context, target string) (context.Context, error) {
	if strings.Contains(target, "localhost") || strings.Contains(target, "127.0.0.1") ||
		strings.HasPrefix(target, "unix://") {
		return ctx, nil
	}
	apiKey, err := login.GetAPIKey()
	if err != nil {
		return nil, err
	}
	if apiKey == "" {
		return nil, status.UnauthenticatedError("not logged in; run bb login")
	}
	return metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey), nil
}
