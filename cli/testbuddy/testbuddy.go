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

var bazelTestOutputDirectory = regexp.MustCompile(
	`^(?:run_[1-9][0-9]*_of_[1-9][0-9]*|shard_[1-9][0-9]*_of_[1-9][0-9]*(?:_run_[1-9][0-9]*_of_[1-9][0-9]*)?)$`,
)

// Leave headroom below BuildBuddy's 50 MB gRPC receive limit.
const reportRequestTargetBytes = 49_000_000

var (
	TestReportFlags = flag.NewFlagSet("test-report", flag.ContinueOnError)
	reportTarget    = TestReportFlags.String("target", "", "BuildBuddy gRPC target; defaults to grpc://127.0.0.1:1985.")
	reportRepo      = TestReportFlags.String("repo_url", "", "Repository URL; defaults to git remote.origin.url.")
	reportSourceURL = TestReportFlags.String(
		"source_url", "", "Result URL; defaults to the most recent bb test invocation.")
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
	repository, err := repositoryURL(workspacePath, *reportRepo)
	if err != nil {
		return 1, err
	}
	sourceURL, err := resultURL(*reportSourceURL)
	if err != nil {
		return 1, err
	}
	inputs := TestReportFlags.Args()
	if len(inputs) == 0 {
		inputs = []string{"bazel-testlogs"}
	}
	paths, err := testXMLPaths(inputs)
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
	batch := &tbpb.ReportTestResultsRequest{RepoUrl: repository}
	batchSize := protowire.SizeTag(1) + protowire.SizeBytes(len(repository))
	batchCount := 0
	sendBatch := func() error {
		if batchCount == 0 {
			return nil
		}
		if err := stream.Send(batch); err != nil {
			return err
		}
		batch = &tbpb.ReportTestResultsRequest{RepoUrl: repository}
		batchSize = protowire.SizeTag(1) + protowire.SizeBytes(len(repository))
		batchCount = 0
		return nil
	}
	makeRoom := func(field protowire.Number, result proto.Message) (int, error) {
		resultSize := proto.Size(result)
		wireSize := protowire.SizeTag(field) + protowire.SizeBytes(resultSize)
		if batchCount > 0 && batchSize+wireSize > reportRequestTargetBytes {
			if err := sendBatch(); err != nil {
				return 0, err
			}
		}
		return wireSize, nil
	}
	diagnostics := 0
	fallbackEventTimeUsec := time.Now().UnixMicro()
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
		diagnostics += report.DiagnosticCount
		targetResult, testCases, err := ResultsForReport(
			path, targetLabel, sourceURL, fallbackEventTimeUsec, report)
		if err != nil {
			return 1, err
		}
		wireSize, err := makeRoom(3, targetResult)
		if err != nil {
			return 1, err
		}
		batch.TestTargets = append(batch.TestTargets, targetResult)
		batchSize += wireSize
		batchCount++
		for _, testCase := range testCases {
			wireSize, err := makeRoom(2, testCase)
			if err != nil {
				return 1, err
			}
			batch.TestCases = append(batch.TestCases, testCase)
			batchSize += wireSize
			batchCount++
		}
	}
	if err := sendBatch(); err != nil {
		return 1, err
	}
	rsp, err := stream.CloseAndRecv()
	if err != nil {
		return 1, err
	}
	fmt.Printf("reported %d test results (%d rejected) from %d XML files\n",
		rsp.GetAcceptedCount(), rsp.GetRejectedCount(), len(paths))
	fmt.Printf("source %s\n", sourceURL)
	if diagnostics > 0 {
		fmt.Printf("%d JUnit diagnostics were ignored\n", diagnostics)
	}
	if rsp.GetAcceptedCount() == 0 {
		return 1, nil
	}
	return 0, nil
}

func ResultsForReport(xmlPath, targetLabel, sourceURL string, fallbackEventTimeUsec int64, report *junit.Report) (*tbpb.TestTargetResult, []*tbpb.TestCaseResult, error) {
	targetOutcome, err := BazelTargetOutcome(xmlPath)
	if err != nil {
		return nil, nil, err
	}
	eventTimeUsec := report.EventTimeUsec
	if eventTimeUsec <= 0 {
		eventTimeUsec = fallbackEventTimeUsec
	}
	if eventTimeUsec <= 0 {
		return nil, nil, status.InvalidArgumentError("a positive report event time is required")
	}
	executionContext := resultContext(xmlPath)
	target := &tbpb.TestTargetResult{
		Identity: &tbpb.TestTargetIdentity{TargetLabel: targetLabel},
		Result: &tbpb.TestResult{
			Outcome: targetOutcome, DurationUsec: report.DurationUsec, SourceUrl: sourceURL,
			EventTimeUsec: eventTimeUsec,
			ResultId:      resultID("target", sourceURL, targetLabel, executionContext),
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
				FailureMessage: testCase.FailureMessage, SourceUrl: sourceURL,
				EventTimeUsec: caseEventTimeUsec,
				ResultId: resultID("case", sourceURL, targetLabel, testCase.CaseName,
					executionContext, strconv.Itoa(testCase.OccurrenceIndex)),
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
		fmt.Println("SOURCE\tOUTCOME\tDURATION\tFAILURE")
		for _, result := range rsp.GetRecentResults() {
			fmt.Printf("%s\t%s\t%s\t%s\n",
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
	if len(parts) > 1 && bazelTestOutputDirectory.MatchString(parts[len(parts)-1]) {
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

func testXMLPaths(inputs []string) ([]string, error) {
	paths := make(map[string]struct{})
	for _, input := range inputs {
		info, err := os.Stat(input)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			paths[input] = struct{}{}
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
				paths[path] = struct{}{}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sorted := make([]string, 0, len(paths))
	for path := range paths {
		sorted = append(sorted, path)
	}
	sort.Strings(sorted)
	return sorted, nil
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
