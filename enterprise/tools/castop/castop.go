// castop shows the largest CAS downloads found across recent invocations.
package main

import (
	"container/heap"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/paging"
	"golang.org/x/term"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pgpb "github.com/buildbuddy-io/buildbuddy/proto/pagination"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	tea "github.com/charmbracelet/bubbletea"
)

const (
	clientAll      = "all"
	clientBazel    = "bazel"
	clientExecutor = "executor"

	truncationEllipsis = "…"
	chevron            = "›"
	ansiReset          = "\x1b[0m"
	ansiBold           = "\x1b[1m"
	ansiDimGray        = "\x1b[38;2;102;102;102m"
)

var errExecutionDedupeSetTooLarge = errors.New("execution dedupe set too large")

var ignoredRPCErrorLogMu sync.Mutex

var (
	target                       = flag.String("target", "remote.buildbuddy.io", "BuildBuddy gRPC target.")
	apiKey                       = flag.String("api_key", "", "BuildBuddy API key for the org whose invocations should be scanned.", flag.Secret)
	groupID                      = flag.String("group_id", "", "Group ID whose latest invocations should be scanned.")
	invocationID                 = flag.String("invocation_id", "", "Invocation ID to inspect directly. If set, castop skips SearchInvocation.")
	clientFilter                 = flag.String("client", clientAll, "Client downloads to include. One of: all, executor, bazel.")
	errorLog                     = flag.String("error_log", "", "If set, redirect stderr to this file.")
	lookback                     = flag.Duration("lookback", 7*24*time.Hour, "How far back to search for updated invocations.")
	searchInvocationsPageSize    = flag.Int("search_invocations_page_size", 1000, "Number of invocations to request per SearchInvocation page.")
	executionDownloadsPageSize   = flag.Int("get_execution_downloads_page_size", 10_000, "Number of downloads to request per GetExecutionDownloads page.")
	cacheScorecardPageSize       = flag.Int("get_cache_scorecard_page_size", 10_000, "Number of downloads to request per GetCacheScoreCard page.")
	getExecutionsWorkers         = flag.Int("get_executions_workers", 10, "Number of workers calling GetExecution for invocations returned by SearchInvocation.")
	getExecutionDownloadsWorkers = flag.Int("get_execution_downloads_workers", 100, "Number of workers calling GetExecutionDownloads for executions returned by GetExecution.")
	getCacheScorecardWorkers     = flag.Int("get_cache_scorecard_workers", 20, "Number of workers calling GetCacheScoreCard for invocations returned by SearchInvocation.")
	refreshInterval              = flag.Duration("refresh_interval", time.Second, "How often to redraw the live table.")
	maxExecutionSetBytes         = flag.Int64("max_execution_set_bytes", 1<<30, "Approximate memory limit for the compact execution dedupe set.")
	queuedInvocationLimit        = flag.Int("queued_invocation_limit", 1000, "Maximum queued invocation or scorecard requests before slowing SearchInvocation.")
	queuedExecutionLimit         = flag.Int("queued_execution_limit", 1000, "Maximum queued execution download requests before slowing GetExecution.")
	queueBackoffDelay            = flag.Duration("queue_backoff_delay", 50*time.Millisecond, "Delay before fetching another invocation or execution page while queues are over their limits.")
)

type progressCounters struct {
	invocationsFetched            atomic.Int64
	executionsFetched             atomic.Int64
	duplicateExecutionsSkipped    atomic.Int64
	executorDownloadsFetched      atomic.Int64
	bazelDownloadsFetched         atomic.Int64
	executionDownloadPagesFetched atomic.Int64
	scorecardPagesFetched         atomic.Int64
	rpcErrorsIgnored              atomic.Int64
	getExecutionsTasksDone        atomic.Int64
	getExecutionsTasksTotal       atomic.Int64
	executionDownloadTasksDone    atomic.Int64
	executionDownloadTasksTotal   atomic.Int64
	scorecardTasksDone            atomic.Int64
	scorecardTasksTotal           atomic.Int64
}

type invocationProgressTracker struct {
	mu          sync.Mutex
	invocations map[string]*invocationProgress
}

type invocationProgress struct {
	invocationID          string
	updatedAtUsec         int64
	totalDownloadSize     int64
	accountedDownloadSize int64
	scorecardPagesFetched int64
	scorecardDone         bool
	getExecutionsDone     bool
	executionsTotal       int64
	executionsFetched     int64
}

type invocationProgressSnapshot struct {
	invocationID          string
	updatedAtUsec         int64
	totalDownloadSize     int64
	accountedDownloadSize int64
	scorecardPagesFetched int64
	scorecardDone         bool
	getExecutionsDone     bool
	executionsTotal       int64
	executionsFetched     int64
}

func newInvocationProgressTracker() *invocationProgressTracker {
	return &invocationProgressTracker{
		invocations: make(map[string]*invocationProgress),
	}
}

func (t *invocationProgressTracker) addInvocation(invocationID string, updatedAtUsec int64, totalDownloadSize int64) {
	if invocationID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	progress := t.ensureLocked(invocationID)
	if updatedAtUsec != 0 {
		progress.updatedAtUsec = updatedAtUsec
	}
	if totalDownloadSize > 0 {
		progress.totalDownloadSize = totalDownloadSize
	}
}

func (t *invocationProgressTracker) accountDownloadBytes(invocationID string, downloadSize int64) {
	if invocationID == "" || downloadSize <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	progress := t.ensureLocked(invocationID)
	progress.accountedDownloadSize += downloadSize
}

func (t *invocationProgressTracker) unaccountedDownloadBytes(invocationID string) int64 {
	if invocationID == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	progress := t.ensureLocked(invocationID)
	return max(progress.totalDownloadSize-progress.accountedDownloadSize, 0)
}

func (t *invocationProgressTracker) markGetExecutionsDone(invocationID string, executionsTotal int64) {
	if invocationID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	progress := t.ensureLocked(invocationID)
	progress.getExecutionsDone = true
	progress.executionsTotal = max(executionsTotal, progress.executionsFetched)
}

func (t *invocationProgressTracker) markExecutionFetched(invocationID string) {
	if invocationID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	progress := t.ensureLocked(invocationID)
	progress.executionsFetched++
	if progress.getExecutionsDone && progress.executionsFetched > progress.executionsTotal {
		progress.executionsTotal = progress.executionsFetched
	}
}

func (t *invocationProgressTracker) markScorecardPageFetched(invocationID string, done bool) {
	if invocationID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	progress := t.ensureLocked(invocationID)
	progress.scorecardPagesFetched++
	if done {
		progress.scorecardDone = true
	}
}

func (t *invocationProgressTracker) snapshot() []invocationProgressSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	snapshots := make([]invocationProgressSnapshot, 0, len(t.invocations))
	for _, progress := range t.invocations {
		snapshots = append(snapshots, invocationProgressSnapshot{
			invocationID:          progress.invocationID,
			updatedAtUsec:         progress.updatedAtUsec,
			totalDownloadSize:     progress.totalDownloadSize,
			accountedDownloadSize: progress.accountedDownloadSize,
			scorecardPagesFetched: progress.scorecardPagesFetched,
			scorecardDone:         progress.scorecardDone,
			getExecutionsDone:     progress.getExecutionsDone,
			executionsTotal:       progress.executionsTotal,
			executionsFetched:     progress.executionsFetched,
		})
	}
	return snapshots
}

func (t *invocationProgressTracker) ensureLocked(invocationID string) *invocationProgress {
	progress := t.invocations[invocationID]
	if progress == nil {
		progress = &invocationProgress{invocationID: invocationID}
		t.invocations[invocationID] = progress
	}
	return progress
}

type scorecardTask struct {
	invocationID string
	offset       int64
}

type executionDownloadTask struct {
	invocationID       string
	executionID        string
	pageToken          string
	fetchedCount       int64
	fetchedSize        int64
	totalDownloadCount int64
	totalDownloadSize  int64
}

type downloadResult struct {
	name   string
	client string
	size   int64
}

type downloadBatch struct {
	results []downloadResult
}

type aggregateKey struct {
	name   string
	client string
}

type aggregateRow struct {
	name       string
	client     string
	count      int64
	totalBytes int64
}

type executionDedupeSet struct {
	mu       sync.Mutex
	ids      map[[16]byte]struct{}
	maxBytes int64
}

func newExecutionDedupeSet(maxBytes int64) *executionDedupeSet {
	return &executionDedupeSet{
		ids:      make(map[[16]byte]struct{}),
		maxBytes: maxBytes,
	}
}

func (s *executionDedupeSet) add(executionID string) (bool, error) {
	id, err := compactExecutionID(executionID)
	if err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ids[id]; ok {
		return false, nil
	}
	s.ids[id] = struct{}{}
	if int64(len(s.ids))*16 > s.maxBytes {
		return false, fmt.Errorf("%w: exceeded %s", errExecutionDedupeSetTooLarge, formatBytes(s.maxBytes))
	}
	return true, nil
}

func compactExecutionID(executionID string) ([16]byte, error) {
	_, uploadID, err := digest.ParseUploadResourceNameWithUUID(executionID)
	if err != nil {
		return [16]byte{}, err
	}
	b, err := hex.DecodeString(strings.ReplaceAll(uploadID, "-", ""))
	if err != nil {
		return [16]byte{}, err
	}
	if len(b) != 16 {
		return [16]byte{}, fmt.Errorf("execution upload ID has %d bytes", len(b))
	}
	var compact [16]byte
	copy(compact[:], b)
	return compact, nil
}

func main() {
	flag.CommandLine.Usage = usage
	flag.Parse()

	errorLogFile, fatalOutput, err := redirectStderr()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
	if errorLogFile != nil {
		defer errorLogFile.Close()
	}
	if fatalOutput != os.Stderr {
		defer fatalOutput.Close()
	}

	parentCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancelCause(parentCtx)
	defer cancel(nil)

	if err := run(ctx, cancel); err != nil {
		printFatalError(fatalOutput, err)
		if errorLogFile != nil {
			printFatalError(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func usage() {
	out := flag.CommandLine.Output()
	fmt.Fprintf(out, "Usage: castop --api_key=KEY (--group_id=GROUP_ID | --invocation_id=INVOCATION_ID) [options]\n\n")
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		defaultText := ""
		if f.DefValue != "" {
			defaultText = fmt.Sprintf(" (default %s)", f.DefValue)
		}
		fmt.Fprintf(out, "  -%s value\n    %s%s\n", f.Name, f.Usage, defaultText)
	})
}

func redirectStderr() (*os.File, *os.File, error) {
	if *errorLog == "" {
		return nil, os.Stderr, nil
	}
	originalStderrFD, err := syscall.Dup(int(os.Stderr.Fd()))
	if err != nil {
		return nil, nil, fmt.Errorf("dup stderr: %w", err)
	}
	originalStderr := os.NewFile(uintptr(originalStderrFD), os.Stderr.Name())
	f, err := os.OpenFile(*errorLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		originalStderr.Close()
		return nil, nil, fmt.Errorf("open error log: %w", err)
	}
	if err := syscall.Dup2(int(f.Fd()), int(os.Stderr.Fd())); err != nil {
		f.Close()
		originalStderr.Close()
		return nil, nil, fmt.Errorf("redirect stderr: %w", err)
	}
	os.Stderr = f
	return f, originalStderr, nil
}

func printFatalError(out *os.File, err error) {
	fmt.Fprintf(out, "Fatal error: %s\n", err)
}

func run(ctx context.Context, cancel context.CancelCauseFunc) error {
	if *groupID == "" && *invocationID == "" {
		return fmt.Errorf("missing --group_id or --invocation_id")
	}
	*clientFilter = strings.ToLower(*clientFilter)
	switch *clientFilter {
	case clientAll, clientExecutor, clientBazel:
	default:
		return fmt.Errorf("--client must be one of: %s, %s, %s", clientAll, clientExecutor, clientBazel)
	}
	if *lookback <= 0 {
		return fmt.Errorf("--lookback must be positive")
	}
	if *searchInvocationsPageSize <= 0 {
		return fmt.Errorf("--search_invocations_page_size must be positive")
	}
	if *executionDownloadsPageSize <= 0 {
		return fmt.Errorf("--get_execution_downloads_page_size must be positive")
	}
	if *cacheScorecardPageSize <= 0 {
		return fmt.Errorf("--get_cache_scorecard_page_size must be positive")
	}
	if *getExecutionsWorkers <= 0 {
		return fmt.Errorf("--get_executions_workers must be positive")
	}
	if *getExecutionDownloadsWorkers <= 0 {
		return fmt.Errorf("--get_execution_downloads_workers must be positive")
	}
	if *getCacheScorecardWorkers <= 0 {
		return fmt.Errorf("--get_cache_scorecard_workers must be positive")
	}
	if *refreshInterval <= 0 {
		return fmt.Errorf("--refresh_interval must be positive")
	}
	if *maxExecutionSetBytes <= 0 {
		return fmt.Errorf("--max_execution_set_bytes must be positive")
	}
	if *queuedInvocationLimit <= 0 {
		return fmt.Errorf("--queued_invocation_limit must be positive")
	}
	if *queuedExecutionLimit <= 0 {
		return fmt.Errorf("--queued_execution_limit must be positive")
	}
	if *queueBackoffDelay <= 0 {
		return fmt.Errorf("--queue_backoff_delay must be positive")
	}

	conn, err := grpc_client.DialSimpleWithoutPooling(*target)
	if err != nil {
		return fmt.Errorf("dial %s: %w", *target, err)
	}
	defer conn.Close()

	client := bbspb.NewBuildBuddyServiceClient(conn)
	counters := &progressCounters{}
	progressTracker := newInvocationProgressTracker()
	executionSet := newExecutionDedupeSet(*maxExecutionSetBytes)
	results := make(chan downloadBatch, 1024)

	getExecutionsQueue := newPriorityWorkQueue[*inpb.Invocation]()
	scorecardQueue := newPriorityWorkQueue[scorecardTask]()
	executionDownloadsQueue := newPriorityWorkQueue[executionDownloadTask]()

	go func() {
		<-ctx.Done()
		getExecutionsQueue.close()
		scorecardQueue.close()
		executionDownloadsQueue.close()
	}()

	var scorecardTasks sync.WaitGroup
	enqueueGetExecutionsTask := func(inv *inpb.Invocation, secondaryPriority float64) {
		priority := float64(progressTracker.unaccountedDownloadBytes(inv.GetInvocationId()))
		if ok := getExecutionsQueue.pushWithSecondary(inv, priority, secondaryPriority); ok {
			counters.getExecutionsTasksTotal.Add(1)
		}
	}
	enqueueScorecardTask := func(task scorecardTask, secondaryPriority float64) {
		scorecardTasks.Add(1)
		priority := float64(progressTracker.unaccountedDownloadBytes(task.invocationID))
		if ok := scorecardQueue.pushWithSecondary(task, priority, secondaryPriority); !ok {
			scorecardTasks.Done()
			return
		}
		counters.scorecardTasksTotal.Add(1)
	}
	var executionDownloadTasks sync.WaitGroup
	enqueueExecutionDownloadTask := func(task executionDownloadTask, secondaryPriority float64) {
		executionDownloadTasks.Add(1)
		priority := float64(progressTracker.unaccountedDownloadBytes(task.invocationID))
		if ok := executionDownloadsQueue.pushWithSecondary(task, priority, secondaryPriority); !ok {
			executionDownloadTasks.Done()
			return
		}
		counters.executionDownloadTasksTotal.Add(1)
	}

	inputDone := make(chan struct{})
	go func() {
		defer close(inputDone)
		defer getExecutionsQueue.close()
		if *invocationID != "" {
			seedInvocation(counters, progressTracker, enqueueGetExecutionsTask, enqueueScorecardTask)
			return
		}
		searchInvocationsWorker(ctx, cancel, client, counters, progressTracker, getExecutionsQueue, scorecardQueue, executionDownloadsQueue, enqueueGetExecutionsTask, enqueueScorecardTask)
	}()

	go func() {
		<-inputDone
		scorecardTasks.Wait()
		scorecardQueue.close()
	}()

	var getExecutionsWG sync.WaitGroup
	for range *getExecutionsWorkers {
		getExecutionsWG.Add(1)
		go func() {
			defer getExecutionsWG.Done()
			getExecutionsWorker(ctx, cancel, client, counters, progressTracker, executionSet, getExecutionsQueue, executionDownloadsQueue, enqueueExecutionDownloadTask)
		}()
	}

	go func() {
		getExecutionsWG.Wait()
		executionDownloadTasks.Wait()
		executionDownloadsQueue.close()
	}()

	var scorecardWG sync.WaitGroup
	for range *getCacheScorecardWorkers {
		scorecardWG.Add(1)
		go func() {
			defer scorecardWG.Done()
			scorecardWorker(ctx, cancel, client, counters, progressTracker, scorecardQueue, &scorecardTasks, enqueueScorecardTask, results)
		}()
	}

	var executionDownloadsWG sync.WaitGroup
	for range *getExecutionDownloadsWorkers {
		executionDownloadsWG.Add(1)
		go func() {
			defer executionDownloadsWG.Done()
			executionDownloadsWorker(ctx, cancel, client, counters, progressTracker, executionDownloadsQueue, &executionDownloadTasks, enqueueExecutionDownloadTask, results)
		}()
	}

	go func() {
		scorecardWG.Wait()
		executionDownloadsWG.Wait()
		close(results)
	}()

	if err := aggregateResults(ctx, cancel, counters, progressTracker, getExecutionsQueue, scorecardQueue, executionDownloadsQueue, results); err != nil {
		return err
	}
	if cause := context.Cause(ctx); cause != nil && cause != context.Canceled {
		return cause
	}
	return nil
}

func rpcContext(ctx context.Context) context.Context {
	if *apiKey == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
}

func recordIgnoredRPCError(counters *progressCounters, rpcName string, err error, details ...string) {
	counters.rpcErrorsIgnored.Add(1)
	if *errorLog == "" {
		return
	}
	detail := ""
	if len(details) > 0 {
		detail = " " + strings.Join(details, " ")
	}
	ignoredRPCErrorLogMu.Lock()
	defer ignoredRPCErrorLogMu.Unlock()
	fmt.Fprintf(os.Stderr, "%s ignored %s error%s: %s\n", time.Now().Format(time.RFC3339), rpcName, detail, err)
}

func cancelForFatalRPCError(cancel context.CancelCauseFunc, rpcName string, err error, details ...string) bool {
	if !isFatalRPCError(err) {
		return false
	}
	detail := ""
	if len(details) > 0 {
		detail = " " + strings.Join(details, " ")
	}
	cancel(fmt.Errorf("%s%s: %w", rpcName, detail, err))
	return true
}

func isFatalRPCError(err error) bool {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return true
	default:
		return false
	}
}

type queueBackpressureCondition struct {
	limit  int
	queued func() int
}

func throttleForQueueBackpressure(ctx context.Context, delay time.Duration, conditions []queueBackpressureCondition) bool {
	for _, condition := range conditions {
		if condition.queued() < condition.limit {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		return true
	}
	return ctx.Err() == nil
}

func queuedInvocationWork(
	getExecutionsQueue *priorityWorkQueue[*inpb.Invocation],
	scorecardQueue *priorityWorkQueue[scorecardTask],
) int {
	queued := 0
	if shouldFetchClient(clientExecutor) {
		queued += getExecutionsQueue.len()
	}
	if shouldFetchClient(clientBazel) {
		queued += scorecardQueue.len()
	}
	return queued
}

func seedInvocation(
	counters *progressCounters,
	progressTracker *invocationProgressTracker,
	enqueueGetExecutionsTask func(*inpb.Invocation, float64),
	enqueueScorecardTask func(scorecardTask, float64),
) {
	counters.invocationsFetched.Add(1)
	progressTracker.addInvocation(*invocationID, 0, 0)
	if shouldFetchClient(clientExecutor) {
		enqueueGetExecutionsTask(&inpb.Invocation{InvocationId: *invocationID}, 0)
	}
	if shouldFetchClient(clientBazel) {
		enqueueScorecardTask(scorecardTask{invocationID: *invocationID}, 0)
	}
}

func searchInvocationsWorker(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	client bbspb.BuildBuddyServiceClient,
	counters *progressCounters,
	progressTracker *invocationProgressTracker,
	getExecutionsQueue *priorityWorkQueue[*inpb.Invocation],
	scorecardQueue *priorityWorkQueue[scorecardTask],
	executionDownloadsQueue *priorityWorkQueue[executionDownloadTask],
	enqueueGetExecutionsTask func(*inpb.Invocation, float64),
	enqueueScorecardTask func(scorecardTask, float64),
) {
	pageToken := ""
	updatedAfter := timestamppb.New(time.Now().Add(-*lookback))
	for ctx.Err() == nil {
		if !throttleForQueueBackpressure(ctx, *queueBackoffDelay, []queueBackpressureCondition{
			{
				limit: *queuedInvocationLimit,
				queued: func() int {
					return queuedInvocationWork(getExecutionsQueue, scorecardQueue)
				},
			},
			{limit: *queuedExecutionLimit, queued: executionDownloadsQueue.len},
		}) {
			return
		}
		rsp, err := client.SearchInvocation(rpcContext(ctx), &inpb.SearchInvocationRequest{
			Query: &inpb.InvocationQuery{
				GroupId:      *groupID,
				UpdatedAfter: updatedAfter,
			},
			Sort: &inpb.InvocationSort{
				SortField: inpb.InvocationSort_CACHE_DOWNLOADED_SORT_FIELD,
				Ascending: false,
			},
			Count:     int32(*searchInvocationsPageSize),
			PageToken: pageToken,
		})
		if err != nil {
			cancel(fmt.Errorf("SearchInvocation page_token=%q: %w", pageToken, err))
			return
		}
		for _, inv := range rsp.GetInvocation() {
			counters.invocationsFetched.Add(1)
			progressTracker.addInvocation(inv.GetInvocationId(), inv.GetUpdatedAtUsec(), invocationDownloadSize(inv))
			priority := float64(invocationDownloadSize(inv))
			if shouldFetchClient(clientExecutor) {
				enqueueGetExecutionsTask(inv, priority)
			}
			if shouldFetchClient(clientBazel) {
				enqueueScorecardTask(scorecardTask{invocationID: inv.GetInvocationId()}, priority)
			}
		}
		pageToken = rsp.GetNextPageToken()
		if pageToken == "" {
			return
		}
	}
}

func shouldFetchClient(client string) bool {
	return *clientFilter == clientAll || *clientFilter == client
}

func getExecutionsWorker(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	client bbspb.BuildBuddyServiceClient,
	counters *progressCounters,
	progressTracker *invocationProgressTracker,
	executionSet *executionDedupeSet,
	getExecutionsQueue *priorityWorkQueue[*inpb.Invocation],
	executionDownloadsQueue *priorityWorkQueue[executionDownloadTask],
	enqueueExecutionDownloadTask func(executionDownloadTask, float64),
) {
	for {
		inv, ok := getExecutionsQueue.popReprioritized(ctx, func(inv *inpb.Invocation) float64 {
			return float64(progressTracker.unaccountedDownloadBytes(inv.GetInvocationId()))
		})
		if !ok {
			return
		}
		func() {
			defer counters.getExecutionsTasksDone.Add(1)
			if !throttleForQueueBackpressure(ctx, *queueBackoffDelay, []queueBackpressureCondition{
				{limit: *queuedExecutionLimit, queued: executionDownloadsQueue.len},
			}) {
				return
			}
			rsp, err := client.GetExecution(rpcContext(ctx), &espb.GetExecutionRequest{
				ExecutionLookup: &espb.ExecutionLookup{
					InvocationId: inv.GetInvocationId(),
				},
			})
			if err != nil {
				detail := fmt.Sprintf("invocation_id=%q", inv.GetInvocationId())
				if cancelForFatalRPCError(cancel, "GetExecution", err, detail) {
					return
				}
				recordIgnoredRPCError(counters, "GetExecution", err, detail)
				return
			}
			executions := rsp.GetExecution()
			sort.SliceStable(executions, func(i, j int) bool {
				return executionDownloadSize(executions[i]) > executionDownloadSize(executions[j])
			})
			totalExecutionsToFetch := int64(0)
			for _, execution := range executions {
				counters.executionsFetched.Add(1)
				executionID := execution.GetExecutionId()
				if executionID == "" {
					continue
				}
				isNew, err := executionSet.add(executionID)
				if err != nil {
					if errors.Is(err, errExecutionDedupeSetTooLarge) {
						cancel(err)
						return
					}
					continue
				}
				if !isNew {
					counters.duplicateExecutionsSkipped.Add(1)
					continue
				}

				totalDownloadSize := executionDownloadSize(execution)
				totalDownloadCount := executionDownloadCount(execution)
				if totalDownloadSize <= 0 && totalDownloadCount <= 0 {
					continue
				}
				totalExecutionsToFetch++
				enqueueExecutionDownloadTask(executionDownloadTask{
					invocationID:       inv.GetInvocationId(),
					executionID:        executionID,
					totalDownloadCount: totalDownloadCount,
					totalDownloadSize:  totalDownloadSize,
				}, float64(totalDownloadSize))
			}
			progressTracker.markGetExecutionsDone(inv.GetInvocationId(), totalExecutionsToFetch)
		}()
	}
}

func executionDownloadsWorker(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	client bbspb.BuildBuddyServiceClient,
	counters *progressCounters,
	progressTracker *invocationProgressTracker,
	queue *priorityWorkQueue[executionDownloadTask],
	tasks *sync.WaitGroup,
	enqueueExecutionDownloadTask func(executionDownloadTask, float64),
	results chan<- downloadBatch,
) {
	for {
		task, ok := queue.popReprioritized(ctx, func(task executionDownloadTask) float64 {
			return float64(progressTracker.unaccountedDownloadBytes(task.invocationID))
		})
		if !ok {
			return
		}
		func() {
			defer tasks.Done()
			defer counters.executionDownloadTasksDone.Add(1)
			rsp, err := client.GetExecutionDownloads(rpcContext(ctx), &capb.GetExecutionDownloadsRequest{
				InvocationId: task.invocationID,
				ExecutionId:  task.executionID,
				PageSize:     int32(*executionDownloadsPageSize),
				PageToken:    task.pageToken,
			})
			if err != nil {
				details := []string{
					fmt.Sprintf("invocation_id=%q", task.invocationID),
					fmt.Sprintf("execution_id=%q", task.executionID),
					fmt.Sprintf("page_token=%q", task.pageToken),
				}
				if cancelForFatalRPCError(cancel, "GetExecutionDownloads", err, details...) {
					return
				}
				recordIgnoredRPCError(counters, "GetExecutionDownloads", err, details...)
				return
			}
			counters.executionDownloadPagesFetched.Add(1)
			downloads := rsp.GetDownloads()
			counters.executorDownloadsFetched.Add(int64(len(downloads)))
			downloadSize := executionDownloadBatchSize(downloads)
			progressTracker.accountDownloadBytes(task.invocationID, downloadSize)
			nextPageToken := rsp.GetNextPageToken()
			if nextPageToken == "" {
				progressTracker.markExecutionFetched(task.invocationID)
			}

			batch := downloadBatch{results: make([]downloadResult, 0, len(downloads))}
			for _, download := range downloads {
				name := download.GetPath()
				if name == "" {
					name = digestName(download.GetDigest().GetHash())
				}
				batch.results = append(batch.results, downloadResult{
					name:   name,
					client: clientExecutor,
					size:   download.GetDigest().GetSizeBytes(),
				})
			}
			sendDownloadBatch(ctx, results, batch)

			if nextPageToken == "" {
				return
			}
			fetchedCount := task.fetchedCount + int64(len(downloads))
			fetchedSize := task.fetchedSize + downloadSize
			task.pageToken = nextPageToken
			task.fetchedCount = fetchedCount
			task.fetchedSize = fetchedSize
			enqueueExecutionDownloadTask(task, nextExecutionDownloadPagePriority(downloads, task.totalDownloadCount, fetchedCount, task.totalDownloadSize, fetchedSize))
		}()
	}
}

func scorecardWorker(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	client bbspb.BuildBuddyServiceClient,
	counters *progressCounters,
	progressTracker *invocationProgressTracker,
	queue *priorityWorkQueue[scorecardTask],
	tasks *sync.WaitGroup,
	enqueueScorecardTask func(scorecardTask, float64),
	results chan<- downloadBatch,
) {
	for {
		task, ok := queue.popReprioritized(ctx, func(task scorecardTask) float64 {
			return float64(progressTracker.unaccountedDownloadBytes(task.invocationID))
		})
		if !ok {
			return
		}
		func() {
			defer tasks.Done()
			defer counters.scorecardTasksDone.Add(1)
			pageToken, err := scorecardPageToken(task.offset)
			if err != nil {
				cancel(fmt.Errorf("GetCacheScoreCard invocation_id=%q offset=%d page_size=%d: %w", task.invocationID, task.offset, *cacheScorecardPageSize, err))
				return
			}
			rsp, err := client.GetCacheScoreCard(rpcContext(ctx), &capb.GetCacheScoreCardRequest{
				InvocationId: task.invocationID,
				PageToken:    pageToken,
				Filter: &capb.GetCacheScoreCardRequest_Filter{
					Mask: &fieldmaskpb.FieldMask{
						Paths: []string{"cache_type", "request_type", "response_type"},
					},
					CacheType:    rspb.CacheType_CAS,
					RequestType:  capb.RequestType_READ,
					ResponseType: capb.ResponseType_OK,
				},
				OrderBy:    capb.GetCacheScoreCardRequest_ORDER_BY_SIZE,
				Descending: true,
			})
			if err != nil {
				details := []string{
					fmt.Sprintf("invocation_id=%q", task.invocationID),
					fmt.Sprintf("offset=%d", task.offset),
					fmt.Sprintf("page_size=%d", *cacheScorecardPageSize),
				}
				if cancelForFatalRPCError(cancel, "GetCacheScoreCard", err, details...) {
					return
				}
				recordIgnoredRPCError(counters, "GetCacheScoreCard", err, details...)
				return
			}
			counters.scorecardPagesFetched.Add(1)
			scorecardResults := rsp.GetResults()
			counters.bazelDownloadsFetched.Add(int64(len(scorecardResults)))
			progressTracker.accountDownloadBytes(task.invocationID, scorecardResultBatchSize(scorecardResults))
			nextPageToken := rsp.GetNextPageToken()
			progressTracker.markScorecardPageFetched(task.invocationID, nextPageToken == "")

			batch := downloadBatch{results: make([]downloadResult, 0, len(scorecardResults))}
			for _, result := range scorecardResults {
				batch.results = append(batch.results, downloadResult{
					name:   scorecardResultName(result),
					client: clientBazel,
					size:   scorecardResultSize(result),
				})
			}
			sendDownloadBatch(ctx, results, batch)

			if nextPageToken == "" {
				return
			}
			if len(scorecardResults) == 0 {
				return
			}
			task.offset += int64(len(scorecardResults))
			enqueueScorecardTask(task, nextScorecardPagePriority(scorecardResults))
		}()
	}
}

func sendDownloadBatch(ctx context.Context, results chan<- downloadBatch, batch downloadBatch) {
	if len(batch.results) == 0 {
		return
	}
	select {
	case results <- batch:
	case <-ctx.Done():
	}
}

func aggregateResults(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	counters *progressCounters,
	progressTracker *invocationProgressTracker,
	getExecutionsQueue *priorityWorkQueue[*inpb.Invocation],
	scorecardQueue *priorityWorkQueue[scorecardTask],
	executionDownloadsQueue *priorityWorkQueue[executionDownloadTask],
	results <-chan downloadBatch,
) error {
	if !term.IsTerminal(int(os.Stdout.Fd())) {
		return aggregateResultsPlain(ctx, counters, progressTracker, getExecutionsQueue, scorecardQueue, executionDownloadsQueue, results)
	}

	model := &aggregateModel{
		ctx:                     ctx,
		cancel:                  cancel,
		counters:                counters,
		progressTracker:         progressTracker,
		getExecutionsQueue:      getExecutionsQueue,
		scorecardQueue:          scorecardQueue,
		executionDownloadsQueue: executionDownloadsQueue,
		results:                 results,
		rows:                    make(map[aggregateKey]*aggregateRow),
	}
	p := tea.NewProgram(model, tea.WithAltScreen())
	finalModel, err := p.Run()
	finalAggregateModel := model
	if model, ok := finalModel.(*aggregateModel); ok {
		finalAggregateModel = model
	}
	if errors.Is(err, tea.ErrInterrupted) {
		cancel(nil)
		printAggregateSnapshot(finalAggregateModel)
		return nil
	}
	if err != nil {
		return fmt.Errorf("run UI: %w", err)
	}
	if finalAggregateModel.err != nil {
		return finalAggregateModel.err
	}
	printAggregateSnapshot(finalAggregateModel)
	return nil
}

func aggregateResultsPlain(
	ctx context.Context,
	counters *progressCounters,
	progressTracker *invocationProgressTracker,
	getExecutionsQueue *priorityWorkQueue[*inpb.Invocation],
	scorecardQueue *priorityWorkQueue[scorecardTask],
	executionDownloadsQueue *priorityWorkQueue[executionDownloadTask],
	results <-chan downloadBatch,
) error {
	rows := make(map[aggregateKey]*aggregateRow)
	for {
		select {
		case batch, ok := <-results:
			if !ok {
				width, height := terminalSize()
				fmt.Fprint(os.Stdout, renderTable(width, height, counters, progressTracker, rows))
				if cause := context.Cause(ctx); cause != nil && cause != context.Canceled {
					return cause
				}
				return nil
			}
			addDownloadBatch(rows, batch)
		case <-ctx.Done():
			if cause := context.Cause(ctx); cause != nil && cause != context.Canceled {
				return cause
			}
			return nil
		}
	}
}

type aggregateModel struct {
	ctx                     context.Context
	cancel                  context.CancelCauseFunc
	counters                *progressCounters
	progressTracker         *invocationProgressTracker
	getExecutionsQueue      *priorityWorkQueue[*inpb.Invocation]
	scorecardQueue          *priorityWorkQueue[scorecardTask]
	executionDownloadsQueue *priorityWorkQueue[executionDownloadTask]
	results                 <-chan downloadBatch
	rows                    map[aggregateKey]*aggregateRow
	width                   int
	height                  int
	err                     error
	interrupted             bool
}

type aggregateResultMsg struct {
	batch downloadBatch
	ok    bool
}

type aggregateContextDoneMsg struct {
	cause error
}

type aggregateRefreshMsg time.Time

func (m *aggregateModel) Init() tea.Cmd {
	return tea.Batch(waitForAggregateResult(m.ctx, m.results), aggregateRefreshCmd())
}

func (m *aggregateModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			m.interrupted = true
			m.cancel(nil)
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case aggregateResultMsg:
		if !msg.ok {
			if cause := context.Cause(m.ctx); cause != nil && cause != context.Canceled {
				m.err = cause
			}
			return m, tea.Quit
		}
		addDownloadBatch(m.rows, msg.batch)
		return m, waitForAggregateResult(m.ctx, m.results)
	case aggregateContextDoneMsg:
		if msg.cause != nil && msg.cause != context.Canceled {
			m.err = msg.cause
		}
		return m, tea.Quit
	case aggregateRefreshMsg:
		return m, aggregateRefreshCmd()
	}
	return m, nil
}

func (m *aggregateModel) View() string {
	return m.renderTable()
}

func (m *aggregateModel) renderTable() string {
	width := m.width
	height := m.height
	if width <= 0 || height <= 0 {
		width, height = terminalSize()
	}
	return renderTable(width, height, m.counters, m.progressTracker, m.rows)
}

func printAggregateSnapshot(model *aggregateModel) {
	fmt.Fprint(os.Stdout, model.renderTable())
}

func waitForAggregateResult(ctx context.Context, results <-chan downloadBatch) tea.Cmd {
	return func() tea.Msg {
		select {
		case batch, ok := <-results:
			return aggregateResultMsg{batch: batch, ok: ok}
		case <-ctx.Done():
			return aggregateContextDoneMsg{cause: context.Cause(ctx)}
		}
	}
}

func aggregateRefreshCmd() tea.Cmd {
	return tea.Tick(*refreshInterval, func(t time.Time) tea.Msg {
		return aggregateRefreshMsg(t)
	})
}

func addDownloadBatch(rows map[aggregateKey]*aggregateRow, batch downloadBatch) {
	for _, result := range batch.results {
		key := aggregateKey{name: result.name, client: result.client}
		row := rows[key]
		if row == nil {
			row = &aggregateRow{name: result.name, client: result.client}
			rows[key] = row
		}
		row.count++
		row.totalBytes += result.size
	}
}

func renderTable(
	width int,
	height int,
	counters *progressCounters,
	progressTracker *invocationProgressTracker,
	rows map[aggregateKey]*aggregateRow,
) string {
	if width <= 0 {
		width = 120
	}
	if height <= 0 {
		height = 40
	}

	out := &strings.Builder{}
	fmt.Fprintln(out, truncateEnd(summaryLine(counters), width))
	fmt.Fprintln(out, truncateEnd(progressLine(counters), width))
	fmt.Fprintln(out, renderInvocationProgressBar(width, progressTracker.snapshot()))

	clientWidth := 8
	countWidth := 5
	sizeWidth := 8
	spacingWidth := 6
	nameWidth := width - clientWidth - countWidth - sizeWidth - spacingWidth
	if nameWidth < 24 {
		nameWidth = 24
	}

	header := renderHeader(nameWidth, clientWidth, countWidth, sizeWidth)
	fmt.Fprintln(out, truncateEnd(header, width))
	fmt.Fprintln(out, styleDimGray(strings.Repeat("─", min(width, nameWidth+clientWidth+countWidth+sizeWidth+spacingWidth))))

	sortedRows := make([]*aggregateRow, 0, len(rows))
	for _, row := range rows {
		sortedRows = append(sortedRows, row)
	}
	sort.SliceStable(sortedRows, func(i, j int) bool {
		if sortedRows[i].totalBytes == sortedRows[j].totalBytes {
			return sortedRows[i].count > sortedRows[j].count
		}
		return sortedRows[i].totalBytes > sortedRows[j].totalBytes
	})

	const fixedRows = 5
	maxRows := height - fixedRows - 1
	if maxRows < 0 {
		maxRows = 0
	}
	if maxRows > len(sortedRows) {
		maxRows = len(sortedRows)
	}
	for _, row := range sortedRows[:maxRows] {
		line := renderRow(row, nameWidth, clientWidth, countWidth, sizeWidth)
		fmt.Fprintln(out, truncateEnd(line, width))
	}
	return out.String()
}

func renderInvocationProgressBar(width int, snapshots []invocationProgressSnapshot) string {
	if width <= 0 {
		return ""
	}
	if len(snapshots) == 0 {
		return styleGrayBrightness(0.12, strings.Repeat("░", width))
	}
	sort.SliceStable(snapshots, func(i, j int) bool {
		if snapshots[i].updatedAtUsec == snapshots[j].updatedAtUsec {
			return snapshots[i].invocationID > snapshots[j].invocationID
		}
		return snapshots[i].updatedAtUsec > snapshots[j].updatedAtUsec
	})

	cells := min(width, len(snapshots))
	out := strings.Builder{}
	for i := range cells {
		start := i * len(snapshots) / cells
		end := (i + 1) * len(snapshots) / cells
		brightness := averageInvocationProgress(snapshots[start:end])
		out.WriteString(styleGrayBrightness(brightness, "█"))
	}
	return out.String()
}

func averageInvocationProgress(snapshots []invocationProgressSnapshot) float64 {
	if len(snapshots) == 0 {
		return 0
	}
	total := 0.0
	for _, snapshot := range snapshots {
		total += invocationProgressBrightness(snapshot)
	}
	return total / float64(len(snapshots))
}

func invocationProgressBrightness(snapshot invocationProgressSnapshot) float64 {
	fetchBazel := shouldFetchClient(clientBazel)
	fetchExecutor := shouldFetchClient(clientExecutor)
	if fetchBazel && !fetchExecutor {
		if snapshot.scorecardDone {
			return 1
		}
		if snapshot.scorecardPagesFetched > 0 {
			return 0.5
		}
		return 0
	}
	if fetchExecutor && !fetchBazel {
		return executorInvocationProgress(snapshot)
	}

	progress := 0.0
	if snapshot.scorecardPagesFetched > 0 {
		progress += 0.5
	}
	progress += 0.5 * executorInvocationProgress(snapshot)
	if progress > 1 {
		return 1
	}
	return progress
}

func executorInvocationProgress(snapshot invocationProgressSnapshot) float64 {
	if !snapshot.getExecutionsDone {
		return 0
	}
	if snapshot.executionsTotal <= 0 {
		return 1
	}
	if snapshot.executionsFetched >= snapshot.executionsTotal {
		return 1
	}
	return float64(snapshot.executionsFetched) / float64(snapshot.executionsTotal)
}

func renderHeader(nameWidth, clientWidth, countWidth, sizeWidth int) string {
	return strings.Join([]string{
		padRightVisible(styleBold("Name"), nameWidth),
		padRightVisible(styleBold("Client"), clientWidth),
		padLeftVisible(styleBold("Count"), countWidth),
		padLeftVisible(styleBold("Total"), sizeWidth),
	}, "  ")
}

func renderRow(row *aggregateRow, nameWidth, clientWidth, countWidth, sizeWidth int) string {
	return strings.Join([]string{
		padRightVisible(truncateDownloadName(row.name, nameWidth), nameWidth),
		padRightVisible(truncateEnd(row.client, clientWidth), clientWidth),
		padLeftVisible(formatCount(row.count), countWidth),
		padLeftVisible(formatBytes(row.totalBytes), sizeWidth),
	}, "  ")
}

func summaryLine(counters *progressCounters) string {
	return styleEquals(fmt.Sprintf(
		"Fetched   invocations=%s executions=%s executor_downloads=%s bazel_downloads=%s pages(executor=%s scorecard=%s) rpc_errors_ignored=%s",
		formatCount(counters.invocationsFetched.Load()),
		formatCount(counters.executionsFetched.Load()),
		formatCount(counters.executorDownloadsFetched.Load()),
		formatCount(counters.bazelDownloadsFetched.Load()),
		formatCount(counters.executionDownloadPagesFetched.Load()),
		formatCount(counters.scorecardPagesFetched.Load()),
		formatCount(counters.rpcErrorsIgnored.Load()),
	))
}

func progressLine(counters *progressCounters) string {
	return styleEquals(fmt.Sprintf(
		"Progress  GetExecutions=%s GetExecutionDownloads=%s GetCacheScoreCard=%s",
		formatProgress(counters.getExecutionsTasksDone.Load(), counters.getExecutionsTasksTotal.Load()),
		formatProgress(counters.executionDownloadTasksDone.Load(), counters.executionDownloadTasksTotal.Load()),
		formatProgress(counters.scorecardTasksDone.Load(), counters.scorecardTasksTotal.Load()),
	))
}

func terminalSize() (int, int) {
	width, height, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 120, 40
	}
	return width, height
}

func invocationDownloadSize(inv *inpb.Invocation) int64 {
	return inv.GetCacheStats().GetTotalDownloadSizeBytes()
}

func executionDownloadSize(execution *espb.Execution) int64 {
	return execution.GetExecutedActionMetadata().GetIoStats().GetFileDownloadSizeBytes()
}

func executionDownloadCount(execution *espb.Execution) int64 {
	return execution.GetExecutedActionMetadata().GetIoStats().GetFileDownloadCount()
}

func nextExecutionDownloadPagePriority(
	downloads []*capb.ExecutionDownload,
	totalDownloadCount int64,
	fetchedCount int64,
	totalDownloadSize int64,
	fetchedSize int64,
) float64 {
	if len(downloads) == 0 {
		return 0
	}
	currentPageSmallestSize := downloads[len(downloads)-1].GetDigest().GetSizeBytes()
	return nextPagePriority(
		currentPageSmallestSize,
		int64(*executionDownloadsPageSize),
		totalDownloadCount,
		fetchedCount,
		totalDownloadSize,
		fetchedSize,
	)
}

func executionDownloadBatchSize(downloads []*capb.ExecutionDownload) int64 {
	var total int64
	for _, download := range downloads {
		total += download.GetDigest().GetSizeBytes()
	}
	return total
}

func nextScorecardPagePriority(results []*capb.ScoreCard_Result) float64 {
	if len(results) == 0 {
		return 0
	}
	currentPageSmallestSize := scorecardResultSize(results[len(results)-1])
	return nextPagePriority(currentPageSmallestSize, int64(*cacheScorecardPageSize), 0, 0, 0, 0)
}

func scorecardResultBatchSize(results []*capb.ScoreCard_Result) int64 {
	var total int64
	for _, result := range results {
		total += scorecardResultSize(result)
	}
	return total
}

func nextPagePriority(
	currentPageSmallestSize int64,
	pageSize int64,
	totalCount int64,
	fetchedCount int64,
	totalSize int64,
	fetchedSize int64,
) float64 {
	remaining := pageSize
	if totalCount > fetchedCount {
		remaining = min(remaining, totalCount-fetchedCount)
	}
	estimatedNextPageSize := currentPageSmallestSize * remaining
	if totalSize > fetchedSize && estimatedNextPageSize > 0 {
		estimatedNextPageSize = min(estimatedNextPageSize, totalSize-fetchedSize)
	}
	return float64(estimatedNextPageSize)
}

func scorecardPageToken(offset int64) (string, error) {
	return paging.EncodeOffsetLimit(&pgpb.OffsetLimit{
		Offset: offset,
		Limit:  int64(*cacheScorecardPageSize),
	})
}

func scorecardResultSize(result *capb.ScoreCard_Result) int64 {
	if size := result.GetDigest().GetSizeBytes(); size > 0 {
		return size
	}
	return result.GetTransferredSizeBytes()
}

func scorecardResultName(result *capb.ScoreCard_Result) string {
	target := result.GetTargetId()
	mnemonic := result.GetActionMnemonic()
	switch {
	case target != "" && mnemonic != "":
		return target + " " + chevron + " " + scorecardMnemonicName(mnemonic)
	case target != "":
		return target
	case mnemonic != "":
		return scorecardMnemonicName(mnemonic)
	}
	if name := strings.Trim(strings.TrimSuffix(result.GetPathPrefix(), "/")+"/"+result.GetName(), "/"); name != "" {
		return name
	}
	if actionID := result.GetActionId(); actionID != "" {
		return "action " + shortHash(actionID)
	}
	return digestName(result.GetDigest().GetHash())
}

func digestName(hash string) string {
	if hash == "" {
		return "(unknown)"
	}
	return "digest " + shortHash(hash)
}

func shortHash(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}

func truncateDownloadName(name string, maxWidth int) string {
	if visibleLen(name) <= maxWidth {
		return styleDownloadName(name)
	}
	return styleDownloadName(truncateMiddle(shortenPathForTruncation(name), maxWidth))
}

func scorecardMnemonicName(mnemonic string) string {
	return mnemonic + " (outs)"
}

func shortenPathForTruncation(name string) string {
	if !strings.Contains(name, "/") {
		return name
	}
	if after, ok := strings.CutPrefix(name, "bazel-out/"); ok {
		name = truncationEllipsis + "/" + after
	}
	name = shortenSecondPathSegment(name)
	name = strings.ReplaceAll(name, "/runfiles/", "/run"+truncationEllipsis+"/")
	name = strings.ReplaceAll(name, ".runfiles/", ".run"+truncationEllipsis+"/")
	return strings.Replace(name, "/bin/", "/"+truncationEllipsis+"/", 1)
}

func shortenSecondPathSegment(name string) string {
	segments := strings.Split(name, "/")
	if len(segments) < 2 {
		return name
	}
	segments[1] = strings.ReplaceAll(segments[1], "fastbuild", "fast"+truncationEllipsis)
	segments[1] = strings.ReplaceAll(segments[1], "x86_64", "x"+truncationEllipsis+"64")
	return strings.Join(segments, "/")
}

func truncateEnd(s string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	if visibleLen(s) <= maxWidth {
		return s
	}
	if maxWidth == 1 {
		return styleDimGray(truncationEllipsis)
	}
	return takeVisiblePrefix(s, maxWidth-1) + styleDimGray(truncationEllipsis)
}

func truncateMiddle(s string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	if visibleLen(s) <= maxWidth {
		return s
	}
	if maxWidth == 1 {
		return truncationEllipsis
	}
	remaining := maxWidth - 1
	left := remaining / 2
	right := remaining - left
	runes := []rune(s)
	return string(runes[:left]) + truncationEllipsis + string(runes[len(runes)-right:])
}

func styleTruncations(s string) string {
	return strings.ReplaceAll(s, truncationEllipsis, styleDimGray(truncationEllipsis))
}

func stylePathSeparators(s string) string {
	return strings.ReplaceAll(s, "/", styleDimGray("/"))
}

func styleDownloadName(s string) string {
	s = styleTruncations(s)
	s = stylePathSeparators(s)
	s = strings.ReplaceAll(s, chevron, styleDimGray(chevron))
	return strings.ReplaceAll(s, "(outs)", styleDimGray("(outs)"))
}

func styleEquals(s string) string {
	return strings.ReplaceAll(s, "=", styleDimGray("="))
}

func styleBold(s string) string {
	return ansiBold + s + ansiReset
}

func styleDimGray(s string) string {
	return ansiDimGray + s + ansiReset
}

func styleGrayBrightness(brightness float64, s string) string {
	brightness = min(max(brightness, 0), 1)
	value := int(32 + brightness*223)
	return fmt.Sprintf("\x1b[38;2;%d;%d;%dm%s%s", value, value, value, s, ansiReset)
}

func padRightVisible(s string, width int) string {
	if padding := width - visibleLen(s); padding > 0 {
		return s + strings.Repeat(" ", padding)
	}
	return s
}

func padLeftVisible(s string, width int) string {
	if padding := width - visibleLen(s); padding > 0 {
		return strings.Repeat(" ", padding) + s
	}
	return s
}

func visibleLen(s string) int {
	n := 0
	for i := 0; i < len(s); {
		if next, ok := skipANSISequence(s, i); ok {
			i = next
			continue
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		n++
		i += size
	}
	return n
}

func takeVisiblePrefix(s string, width int) string {
	if width <= 0 {
		return ""
	}
	out := strings.Builder{}
	visible := 0
	for i := 0; i < len(s) && visible < width; {
		if next, ok := skipANSISequence(s, i); ok {
			out.WriteString(s[i:next])
			i = next
			continue
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		out.WriteString(s[i : i+size])
		visible++
		i += size
	}
	return out.String()
}

func skipANSISequence(s string, i int) (int, bool) {
	if i >= len(s) || s[i] != '\x1b' {
		return i, false
	}
	i++
	if i >= len(s) || s[i] != '[' {
		return i, true
	}
	i++
	for i < len(s) {
		c := s[i]
		i++
		if c >= '@' && c <= '~' {
			break
		}
	}
	return i, true
}

func formatCount(n int64) string {
	if n < 100_000 {
		return fmt.Sprint(n)
	}
	return fmt.Sprintf("%.0fK", float64(n)/1000)
}

func formatProgress(done int64, total int64) string {
	return formatCount(done) + "/" + formatCount(total)
}

func formatBytes(n int64) string {
	if n < 0 {
		return "-" + formatBytes(-n)
	}
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	value := float64(n)
	unit := units[0]
	for _, nextUnit := range units[1:] {
		if value < 1024 {
			break
		}
		value /= 1024
		unit = nextUnit
	}
	if unit == "B" {
		return fmt.Sprintf("%d B", n)
	}
	if value >= 10 {
		return fmt.Sprintf("%.0f %s", value, unit)
	}
	return fmt.Sprintf("%.1f %s", value, unit)
}

type priorityWorkQueue[T any] struct {
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	nextID uint64
	items  priorityHeap[T]
}

func newPriorityWorkQueue[T any]() *priorityWorkQueue[T] {
	q := &priorityWorkQueue[T]{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *priorityWorkQueue[T]) push(value T, priority float64) bool {
	return q.pushWithSecondary(value, priority, 0)
}

func (q *priorityWorkQueue[T]) pushWithSecondary(value T, priority float64, secondaryPriority float64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	heap.Push(&q.items, &priorityItem[T]{
		value:             value,
		priority:          priority,
		secondaryPriority: secondaryPriority,
		id:                q.nextID,
	})
	q.nextID++
	q.cond.Signal()
	return true
}

func (q *priorityWorkQueue[T]) pop(ctx context.Context) (T, bool) {
	return q.popReprioritized(ctx, nil)
}

func (q *priorityWorkQueue[T]) popReprioritized(ctx context.Context, priority func(T) float64) (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		for len(q.items) == 0 && !q.closed {
			if ctx.Err() != nil {
				var zero T
				return zero, false
			}
			q.cond.Wait()
		}
		if ctx.Err() != nil || len(q.items) == 0 {
			var zero T
			return zero, false
		}

		item := heap.Pop(&q.items).(*priorityItem[T])
		if priority == nil {
			return item.value, true
		}
		refreshedPriority := priority(item.value)
		if len(q.items) == 0 || priorityPrecedes(refreshedPriority, item.secondaryPriority, item.id, q.items[0]) {
			item.priority = refreshedPriority
			return item.value, true
		}
		item.priority = refreshedPriority
		heap.Push(&q.items, item)
	}
}

func (q *priorityWorkQueue[T]) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

func (q *priorityWorkQueue[T]) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

type priorityItem[T any] struct {
	value             T
	priority          float64
	secondaryPriority float64
	id                uint64
	index             int
}

type priorityHeap[T any] []*priorityItem[T]

func (h priorityHeap[T]) Len() int {
	return len(h)
}

func (h priorityHeap[T]) Less(i, j int) bool {
	return priorityPrecedes(h[i].priority, h[i].secondaryPriority, h[i].id, h[j])
}

func priorityPrecedes[T any](priority float64, secondaryPriority float64, id uint64, other *priorityItem[T]) bool {
	if priority != other.priority {
		return priority > other.priority
	}
	if secondaryPriority != other.secondaryPriority {
		return secondaryPriority > other.secondaryPriority
	}
	return id < other.id
}

func (h priorityHeap[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *priorityHeap[T]) Push(x any) {
	item := x.(*priorityItem[T])
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *priorityHeap[T]) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}
