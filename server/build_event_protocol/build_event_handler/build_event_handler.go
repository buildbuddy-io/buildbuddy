package build_event_handler

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Masterminds/semver/v3"
	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_status_reporter"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/target_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/cache_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/error_tracking"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/features"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/olapdbconfig"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/scorecard"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/junit"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/paging"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/protofile"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/util/urlutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/google/shlex"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	csinpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	pgpb "github.com/buildbuddy-io/buildbuddy/proto/pagination"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
	api_common "github.com/buildbuddy-io/buildbuddy/server/api/common"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	gstatus "google.golang.org/grpc/status"
)

const (
	defaultChunkFileSizeBytes    = 1000 * 100 // 100KB
	maxTestAttemptsPerInvocation = 100
	maxTestXMLBytes              = 1 << 20
	maxTestArtifactBytes         = 8 << 20
	maxQueuedErrorBytes          = 64 << 20
	maxLiveErrorBytes            = 64 << 20
	liveErrorStreamReservation   = 16 << 20
	maxTestLogBytes              = 4 << 10
	maxArtifactURIBytes          = 2 << 10
	maxArtifactNameBytes         = 512
	maxTestTargetBytes           = 1024
	maxTestConfigurationBytes    = 512
	maxTestStatusDetailsBytes    = 4 << 10
	maxTestStrategyBytes         = 128
	testArtifactFetchTimeout     = 2 * time.Second

	// How many workers to spin up for writing cache stats to the DB.
	numStatsRecorderWorkers = 8

	// How many workers to spin up for looking up invocations before
	// webhooks are notified.
	numWebhookInvocationLookupWorkers = 8
	// How many workers to spin up for notifying webhooks.
	numWebhookNotifyWorkers = 16

	// How long to wait before giving up on webhook requests.
	webhookNotifyTimeout = 1 * time.Minute

	// Default number of actions shown by bazel
	defaultActionsShown = 8

	// Exit code in Finished event indicating that the build was interrupted
	// (i.e. killed by user).
	InterruptedExitCode = 8

	// First sequence number that we expect to see in the ordered build
	// event stream.
	firstExpectedSequenceNumber = 1

	// Max total pattern length to include in the Expanded event returned to the
	// UI.
	maxPatternLengthBytes = 10_000

	// Rather than immediately deleting executions data from Redis after flushing
	// finalized data to Clickhouse, expire it after this TTL so that even if Clickhouse
	// has replication lag, clients will still be able to read the data from Redis.
	expireRedisExecutionsTTL = 5 * time.Minute
)

var (
	chunkFileSizeBytes      = flag.Int("storage.chunk_file_size_bytes", 3_000_000 /* 3 MB */, "How many bytes to buffer in memory before flushing a chunk of build protocol data to disk.")
	enableChunkedEventLogs  = flag.Bool("storage.enable_chunked_event_logs", true, "If true, Event logs will be stored separately from the invocation proto in chunks.")
	disablePersistArtifacts = flag.Bool("storage.disable_persist_cache_artifacts", false, "If disabled, buildbuddy will not persist cache artifacts in the blobstore. This may make older invocations not display properly.")
	writeToOLAPDBEnabled    = flag.Bool("app.enable_write_to_olap_db", true, "If enabled, complete invocations will be flushed to OLAP DB")

	buildEventFilterStartThreshold = flag.Int("app.build_event_filter_start_threshold", 100_000, "When looking up an invocation, start filtering out unimportant events after this many events have been processed.")
	cacheStatsFinalizationDelay    = flag.Duration("cache_stats_finalization_delay", 500*time.Millisecond, "The time allowed for all metrics collectors across all apps to flush their local cache stats to the backing storage, before finalizing stats in the DB.")
)

var cacheArtifactsBlobstorePath = path.Join("artifacts", "cache")

type PersistArtifacts struct {
	URIs              []*url.URL
	TestActionOutputs bool
}

type BuildEventHandler struct {
	env              environment.Env
	statsRecorder    *statsRecorder
	webhookNotifier  *webhookNotifier
	openChannels     *sync.WaitGroup
	cancelFnsByInvID sync.Map // map of string invocationID => context.CancelFunc
	liveErrorBytes   atomic.Int64

	mu           sync.Mutex
	shuttingDown bool
}

func NewBuildEventHandler(env environment.Env) *BuildEventHandler {
	openChannels := &sync.WaitGroup{}
	onStatsRecorded := make(chan *invocationInfo, 4096)
	statsRecorder := newStatsRecorder(env, openChannels, onStatsRecorded)
	webhookNotifier := newWebhookNotifier(env, onStatsRecorded)

	statsRecorder.Start()
	webhookNotifier.Start()

	h := &BuildEventHandler{
		env:              env,
		statsRecorder:    statsRecorder,
		webhookNotifier:  webhookNotifier,
		openChannels:     openChannels,
		cancelFnsByInvID: sync.Map{},
	}
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		h.Stop()
		return nil
	})
	return h
}

func (b *BuildEventHandler) OpenChannel(ctx context.Context, iid string) (interfaces.BuildEventChannel, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shuttingDown {
		return nil, status.UnavailableErrorf("Server shutting down, cannot open channel for %s", iid)
	}

	invocation := &inpb.Invocation{InvocationId: iid}
	buildEventAccumulator := accumulator.NewBEValues(invocation)
	val, ok := b.cancelFnsByInvID.Load(iid)
	if ok {
		cancelFn := val.(context.CancelFunc)
		cancelFn()
	}

	ctx, cancel := context.WithCancel(ctx)
	b.cancelFnsByInvID.Store(iid, cancel)

	b.openChannels.Add(1)
	onClose := func() {
		b.openChannels.Done()
		b.cancelFnsByInvID.Delete(iid)
	}

	return &EventChannel{
		env:            b.env,
		statsRecorder:  b.statsRecorder,
		ctx:            ctx,
		pw:             nil,
		beValues:       buildEventAccumulator,
		redactor:       redact.NewStreamingRedactor(),
		statusReporter: build_status_reporter.NewBuildStatusReporter(b.env, buildEventAccumulator),
		targetTracker:  target_tracker.NewTargetTracker(b.env, buildEventAccumulator),
		collector:      b.env.GetMetricsCollector(),
		apiTargetMap:   api_common.NewTargetMap( /* TargetSelector */ nil),

		hasReceivedEventWithOptions:      false,
		hasReceivedStartedEvent:          false,
		bufferedEvents:                   make([]*inpb.InvocationEvent, 0),
		requestedTerminalColumns:         eventlog.DefaultTerminalLineLength,
		logWriter:                        nil,
		onClose:                          onClose,
		attempt:                          1,
		groupIDForMetrics:                getGroupIDForMetrics(ctx, b.env),
		errorOccurrenceFingerprintCounts: make(map[string]int),
		errorOutputFiles:                 make(map[*schema.ErrorOccurrence]*build_event_stream.File),
		testSummaries:                    make(map[testTargetKey]build_event_stream.TestStatus),
		liveErrorBytes:                   &b.liveErrorBytes,
	}, nil
}

func getGroupIDForMetrics(ctx context.Context, env environment.Env) string {
	userInfo, err := env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return userInfo.GetGroupID()
}

func (b *BuildEventHandler) Stop() {
	b.mu.Lock()
	b.shuttingDown = true
	b.mu.Unlock()
	b.cancelFnsByInvID.Range(func(key, val any) bool {
		iid := key.(string)
		cancelFn := val.(context.CancelFunc)
		log.Infof("Cancelling invocation %q because server received shutdown signal", iid)
		cancelFn()
		return true
	})
	b.statsRecorder.Stop()
	b.webhookNotifier.Stop()
}

// invocationInfo represents an invocation ID as well as the JWT granting access
// to it. It should only be used for background tasks that need access to the
// JWT after the build event stream is already closed.
type invocationInfo struct {
	id          string
	jwt         string
	attempt     uint64
	incarnation string
}

// recordStatsTask contains the info needed to record the stats for an
// invocation. These tasks are enqueued to statsRecorder and executed in the
// background.
type recordStatsTask struct {
	*invocationInfo
	// createdAt is the time at which this task was created.
	createdAt time.Time
	// files contains a mapping of file digests to file name metadata for files
	// referenced in the BEP.
	files                    map[string]*build_event_stream.File
	persist                  *PersistArtifacts
	kytheSSTableResourceName *rspb.ResourceName
	invocationStatus         inspb.InvocationStatus
	// Git fetch stats reported by the remote runner, if any. These are stored
	// only in the OLAP DB, so they are carried here rather than read back from
	// the primary DB at flush time.
	gitFetchTotalBytes    int64
	gitFetchDurationUsec  int64
	gitFetchRetryCount    int64
	errorOccurrences      []*schema.ErrorOccurrence
	errorOccurrenceLoader func(context.Context) []*schema.ErrorOccurrence
	errorOccurrenceBytes  int64
}

// statsRecorder listens for finalized invocations and copies cache stats from
// the metrics collector to the DB.
type statsRecorder struct {
	env          environment.Env
	openChannels *sync.WaitGroup
	// onStatsRecorded is a channel for this statsRecorder to notify after
	// recording stats for each invocation. Invocations sent on this channel are
	// considered "finalized".
	onStatsRecorded chan<- *invocationInfo
	eg              errgroup.Group

	mu               sync.Mutex // protects(tasks, stopped, queuedErrorBytes)
	tasks            chan *recordStatsTask
	queuedErrorBytes int64
	stopped          bool
}

func newStatsRecorder(env environment.Env, openChannels *sync.WaitGroup, onStatsRecorded chan<- *invocationInfo) *statsRecorder {
	return &statsRecorder{
		env:             env,
		openChannels:    openChannels,
		onStatsRecorded: onStatsRecorded,
		tasks:           make(chan *recordStatsTask, 4096),
	}
}

// Enqueue enqueues a task for the given invocation's stats to be recorded
// once they are available.
func (r *statsRecorder) Enqueue(ctx context.Context, beValues *accumulator.BEValues, invocationIncarnation string, errorOccurrenceLoader func(context.Context) []*schema.ErrorOccurrence, errorOccurrenceBytes int64) {
	persist := &PersistArtifacts{}
	if !*disablePersistArtifacts {
		persist.URIs = slices.Concat(
			beValues.BuildToolLogURIs(),
			beValues.FailedTestOutputURIs(),
			beValues.PassedTestOutputURIs(),
		)
	}

	invocation := beValues.Invocation()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		alert.UnexpectedEvent(
			"stats_recorder_finalize_after_shutdown",
			"Invocation %q was marked finalized after the stats recorder was shut down.",
			invocation.GetInvocationId())
		return
	}
	if errorOccurrenceBytes > 0 && r.queuedErrorBytes+errorOccurrenceBytes > maxQueuedErrorBytes {
		alert.UnexpectedEvent("error_tracking_queue_byte_budget_exceeded", "Dropping error artifact work for invocation %q because the global queue byte budget is full.", invocation.GetInvocationId())
		errorOccurrenceLoader = nil
		errorOccurrenceBytes = 0
	}
	jwt := r.env.GetAuthenticator().TrustedJWTFromAuthContext(ctx)
	req := &recordStatsTask{
		invocationInfo: &invocationInfo{
			id:          invocation.GetInvocationId(),
			attempt:     invocation.GetAttempt(),
			jwt:         jwt,
			incarnation: invocationIncarnation,
		},
		createdAt:                time.Now(),
		files:                    beValues.OutputFiles(),
		invocationStatus:         invocation.GetInvocationStatus(),
		persist:                  persist,
		kytheSSTableResourceName: beValues.KytheSSTableResourceName(),
		gitFetchTotalBytes:       beValues.GitFetchTotalBytes(),
		gitFetchDurationUsec:     beValues.GitFetchDuration().Microseconds(),
		gitFetchRetryCount:       beValues.GitFetchRetryCount(),
		errorOccurrenceLoader:    errorOccurrenceLoader,
		errorOccurrenceBytes:     errorOccurrenceBytes,
	}
	select {
	case r.tasks <- req:
		r.queuedErrorBytes += errorOccurrenceBytes
		break
	default:
		alert.UnexpectedEvent(
			"stats_recorder_channel_buffer_full",
			"Failed to write cache stats: stats recorder task buffer is full")
	}
}

func (r *statsRecorder) Start() {
	ctx := r.env.GetServerContext()
	for range numStatsRecorderWorkers {
		metrics.StatsRecorderWorkers.Inc()
		r.eg.Go(func() error {
			defer metrics.StatsRecorderWorkers.Dec()
			for task := range r.tasks {
				r.handleTask(ctx, task)
			}
			return nil
		})
	}
}

func (r *statsRecorder) flushInvocationStatsToOLAPDB(ctx context.Context, task *recordStatsTask) error {
	if r.env.GetOLAPDBHandle() == nil || !*writeToOLAPDBEnabled {
		return nil
	}
	inv, matched, err := r.currentInvocation(ctx, task)
	if err != nil {
		return err
	}
	if !matched {
		log.CtxWarningf(ctx, "Skipped stale invocation and execution stats for a deleted or replaced invocation")
		return nil
	}
	// Git fetch stats are stored only in the OLAP DB, so they are carried on
	// the task instead of being read back from the primary DB.
	inv.GitFetchTotalBytes = task.gitFetchTotalBytes
	inv.GitFetchDurationUsec = task.gitFetchDurationUsec
	inv.GitFetchRetryCount = task.gitFetchRetryCount
	if err := r.env.GetOLAPDBHandle().FlushInvocationStats(ctx, inv); err != nil {
		return err
	}
	// Temporary logging for debugging clickhouse missing data.
	log.CtxInfo(ctx, "Successfully wrote invocation to clickhouse")
	if *features.ErrorTrackingEnabled && len(task.errorOccurrences) > 0 {
		// Recheck the immutable primary-row creation identity and keep the row
		// locked through ACL publication and occurrence insertion. This prevents
		// delayed work from an invocation whose ID was deleted and reused from
		// being published under the replacement invocation's ACL.
		if matched, err := error_tracking.FlushErrorOccurrencesWithPrimary(ctx, r.env, inv.InvocationID, task.incarnation, task.errorOccurrences); err != nil {
			log.CtxWarningf(ctx, "Failed to synchronize and flush BES error occurrences; skipping occurrences: %s", err)
		} else if !matched {
			log.CtxWarningf(ctx, "Skipped stale BES error occurrences for a deleted or replaced invocation")
		}
	}

	if r.env.GetExecutionCollector() == nil {
		return nil
	}
	inv, matched, err = r.currentInvocation(ctx, task)
	if err != nil {
		return err
	}
	if !matched {
		log.CtxWarningf(ctx, "Skipped stale execution stats for a deleted or replaced invocation")
		return nil
	}
	return r.flushExecutionsToOLAPDB(ctx, inv)
}

func (r *statsRecorder) currentInvocation(ctx context.Context, task *recordStatsTask) (*tables.Invocation, bool, error) {
	inv := &tables.Invocation{}
	err := r.env.GetDBHandle().NewQuery(ctx, "stats_recorder_get_invocation_incarnation").Raw(
		`SELECT * FROM "Invocations" WHERE invocation_id = ? AND error_tracking_incarnation = ?`, task.id, task.incarnation,
	).Take(inv)
	if db.IsRecordNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return inv, true, nil
}

func (r *statsRecorder) flushExecutionsToOLAPDB(ctx context.Context, inv *tables.Invocation) error {
	const batchSize = 50_000
	var startIndex int64 = 0
	var endIndex int64 = batchSize - 1

	// Always clean up executions in Collector because we are not retrying
	defer func() {
		// Clickhouse ReplicatedMergeTree tables can have replication lag, which can cause
		// reads of executions data to fail.
		// Rather than immediately deleting executions data from Redis after flushing
		// finalized data to Clickhouse, keep the data in Redis a little bit
		// longer, so clients can read executions data from Redis in these cases.
		err := r.env.GetExecutionCollector().ExpireExecutions(ctx, inv.InvocationID, expireRedisExecutionsTTL)
		if err != nil {
			log.CtxErrorf(ctx, "failed to soft delete executions in collector: %s", err)
		}
	}()

	if !olapdbconfig.WriteExecutionsToOLAPDBEnabled() {
		return nil
	}

	// Add the invocation to redis to signal to the executors that they can flush
	// complete Executions into clickhouse directly, in case the PublishOperation
	// is received after the Invocation is complete.
	storedInv := toStoredInvocation(inv)
	if err := r.env.GetExecutionCollector().AddInvocation(ctx, storedInv); err != nil {
		log.CtxErrorf(ctx, "failed to write the complete Invocation to redis: %s", err)
	} else {
		log.CtxInfo(ctx, "Successfully wrote invocation to redis")
	}

	for {
		endIndex = startIndex + batchSize - 1
		executions, err := r.env.GetExecutionCollector().GetExecutions(ctx, inv.InvocationID, int64(startIndex), int64(endIndex))
		if err != nil {
			return status.InternalErrorf("failed to read executions for invocation_id = %q, startIndex = %d, endIndex = %d from Redis: %s", inv.InvocationID, startIndex, endIndex, err)
		}
		if len(executions) == 0 {
			break
		}
		if err := r.env.GetOLAPDBHandle().FlushExecutionStats(ctx, storedInv, executions); err != nil {
			log.CtxErrorf(ctx, "Failed to flush executions to OLAP DB: %s", err)
			break
		}
		log.CtxInfof(ctx, "successfully wrote %d executions", len(executions))
		// Flush executions to OLAP
		size := len(executions)
		if size < batchSize {
			break
		}
		startIndex += batchSize
	}

	return nil
}

func (r *statsRecorder) maybeIngestKytheSST(ctx context.Context, ij *invocationInfo, sstableResource *rspb.ResourceName) error {
	// first check that css is enabled
	codesearchService := r.env.GetCodesearchService()
	if codesearchService == nil {
		return nil
	}

	if sstableResource == nil {
		return nil
	}

	ctx = r.env.GetAuthenticator().AuthContextFromTrustedJWT(ctx, ij.jwt)
	_, err := codesearchService.IngestAnnotations(ctx, &csinpb.IngestAnnotationsRequest{
		SstableName: sstableResource,
		Async:       true, // don't wait for an answer.
	})
	return err
}

func (r *statsRecorder) handleTask(ctx context.Context, task *recordStatsTask) {
	r.mu.Lock()
	r.queuedErrorBytes -= task.errorOccurrenceBytes
	r.mu.Unlock()
	start := time.Now()
	defer func() {
		metrics.StatsRecorderDuration.Observe(float64(time.Since(start).Microseconds()))
	}()

	// Apply the finalization delay relative to when the invocation was marked
	// finalized, rather than relative to now. Otherwise each worker would be
	// unnecessarily throttled.
	time.Sleep(time.Until(task.createdAt.Add(*cacheStatsFinalizationDelay)))
	ti := &tables.Invocation{
		InvocationID:             task.invocationInfo.id,
		Attempt:                  task.invocationInfo.attempt,
		ErrorTrackingIncarnation: task.invocationInfo.incarnation,
	}
	ctx = log.EnrichContext(ctx, log.InvocationIDKey, task.invocationInfo.id)
	if stats := hit_tracker.CollectCacheStats(ctx, r.env, task.invocationInfo.id); stats != nil {
		fillInvocationFromCacheStats(stats, ti)
	} else {
		log.CtxInfo(ctx, "cache stats is not available.")
	}
	updated, err := r.env.GetInvocationDB().UpdateInvocation(ctx, ti)
	if err != nil {
		log.CtxErrorf(ctx, "Failed to write cache stats to primaryDB: %s", err)
		return
	}
	if !updated {
		log.CtxWarningf(ctx, "Attempt %d of invocation pre-empted by a more recent attempt or invocation incarnation; skipping stale stats task.", task.invocationInfo.attempt)
		return
	}

	if sc := hit_tracker.ScoreCard(ctx, r.env, task.invocationInfo.id); sc != nil {
		scorecard.FillBESMetadata(sc, task.files)
		if err := scorecard.Write(ctx, r.env, task.invocationInfo.id, task.invocationInfo.attempt, sc); err != nil {
			log.CtxErrorf(ctx, "Error writing scorecard blob: %s", err)
		}
	}

	if err := r.maybeIngestKytheSST(ctx, task.invocationInfo, task.kytheSSTableResourceName); err != nil {
		log.CtxWarningf(ctx, "Failed to ingest kythe sst: %s", err)
	}

	if task.invocationStatus == inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS {
		// only flush complete invocation to clickhouse.
		if *features.ErrorTrackingEnabled && task.errorOccurrenceLoader != nil && r.env.GetOLAPDBHandle() != nil && *writeToOLAPDBEnabled {
			artifactCtx := r.env.GetAuthenticator().AuthContextFromTrustedJWT(ctx, task.invocationInfo.jwt)
			task.errorOccurrences = task.errorOccurrenceLoader(artifactCtx)
			task.errorOccurrenceLoader = nil
		}
		err = r.flushInvocationStatsToOLAPDB(ctx, task)
		if err != nil {
			log.CtxErrorf(ctx, "Failed to flush stats to clickhouse: %s", err)
		}
	} else {
		log.CtxInfof(ctx, "skipped writing stats to clickhouse, invocationStatus = %s", task.invocationStatus)
	}
	// Cleanup regardless of whether the stats are flushed successfully to
	// the DB (since we won't retry the flush and we don't need these stats
	// for any other purpose).
	hit_tracker.CleanupCacheStats(ctx, r.env, task.invocationInfo.id)
	// Once cache stats are populated, notify the onStatsRecorded channel in
	// a non-blocking fashion.
	select {
	case r.onStatsRecorded <- task.invocationInfo:
		break
	default:
		alert.UnexpectedEvent(
			"webhook_channel_buffer_full",
			"Failed to notify webhook: channel buffer is full",
		)
	}

	ctx = r.env.GetAuthenticator().AuthContextFromTrustedJWT(ctx, task.invocationInfo.jwt)
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(50) // Max concurrency when copying files from cache->blobstore.

	artifactsUploaded := make(map[string]struct{}, 0)
	for _, uri := range task.persist.URIs {
		// Only persist artifacts from caches that are hosted on the BuildBuddy
		// domain (but only if we know it).
		if cache_api_url.String() != "" && urlutil.GetDomain(uri.Hostname()) != urlutil.GetDomain(cache_api_url.WithPath("").Hostname()) {
			continue
		}
		rn, err := digest.ParseDownloadResourceName(strings.TrimPrefix(uri.Path, "/"))
		if err != nil {
			log.CtxErrorf(ctx, "Unparseable artifact URI: %s", err)
			continue
		}
		if rn.IsEmpty() {
			continue
		}
		if _, seen := artifactsUploaded[rn.GetDigest().GetHash()]; seen {
			continue
		}
		artifactsUploaded[rn.GetDigest().GetHash()] = struct{}{}
		eg.Go(func() error {
			// When persisting artifacts, make sure we associate the cache
			// requests with the app, not bazel.
			ctx := usageutil.WithLocalServerLabels(ctx)

			fullPath := path.Join(task.invocationInfo.id, cacheArtifactsBlobstorePath, uri.Path)
			if err := persistArtifact(ctx, r.env, uri, fullPath); err != nil {
				log.CtxError(ctx, err.Error())
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		log.CtxErrorf(ctx, "Failed to persist cache artifacts to blobstore: %s", err)
	}
}

func (r *statsRecorder) Stop() {
	// Wait for all EventHandler channels to be closed to ensure there will be no
	// more calls to Enqueue.
	// TODO(bduffany): This has a race condition where the server can be shutdown
	// just after the stream request is accepted by the server but before calling
	// openChannels.Add(1). Can fix this by explicitly waiting for the gRPC server
	// shutdown to finish, which ensures all streaming requests have terminated.
	log.Info("StatsRecorder: waiting for EventChannels to be closed before shutting down")
	r.openChannels.Wait()

	log.Info("StatsRecorder: shutting down")
	r.mu.Lock()
	r.stopped = true
	close(r.tasks)
	r.mu.Unlock()

	if err := r.eg.Wait(); err != nil {
		log.Error(err.Error())
	}

	close(r.onStatsRecorded)
}

func persistArtifact(ctx context.Context, env environment.Env, uri *url.URL, path string) error {
	w, err := env.GetBlobstore().Writer(ctx, path)
	if err != nil {
		return status.WrapErrorf(
			err,
			"Failed to open writer to blobstore for path %s to persist cache artifact at %s",
			path,
			uri.String(),
		)
	}
	if err := env.GetPooledByteStreamClient().StreamBytestreamFile(ctx, uri, w); err != nil {
		w.Close()
		return status.WrapErrorf(
			err,
			"Failed to stream to blobstore for path %s to persist cache artifact at %s",
			path,
			uri.String(),
		)
	}
	if err := w.Commit(); err != nil {
		w.Close()
		return status.WrapErrorf(
			err,
			"Failed to commit to blobstore for path %s to persist cache artifact at %s",
			path,
			uri.String(),
		)
	}
	if err := w.Close(); err != nil {
		return status.WrapErrorf(
			err,
			"Failed to close blobstore writer for path %s to persist cache artifact at %s",
			path,
			uri.String(),
		)
	}
	return nil
}

type notifyWebhookTask struct {
	// hook is the webhook to notify of a completed invocation.
	hook interfaces.Webhook
	// invocationInfo contains the invocation ID and JWT for the invocation.
	*invocationInfo
	// invocation is the complete invocation looked up from the invocationInfo.
	invocation *inpb.Invocation
}

func notifyWithTimeout(ctx context.Context, env environment.Env, t *notifyWebhookTask) error {
	start := time.Now()
	defer func() {
		metrics.WebhookNotifyDuration.Observe(float64(time.Since(start).Microseconds()))
	}()

	ctx, cancel := context.WithTimeout(ctx, webhookNotifyTimeout)
	defer cancel()
	// Run the webhook using the authenticated user from the build event stream.
	ij := t.invocationInfo
	ctx = env.GetAuthenticator().AuthContextFromTrustedJWT(ctx, ij.jwt)
	return t.hook.NotifyComplete(ctx, t.invocation)
}

// webhookNotifier listens for invocations to be finalized (including stats)
// and notifies webhooks.
type webhookNotifier struct {
	env environment.Env
	// invocations is a channel of finalized invocations. On each invocation
	// sent to this channel, we notify all configured webhooks.
	invocations <-chan *invocationInfo

	tasks       chan *notifyWebhookTask
	lookupGroup errgroup.Group
	notifyGroup errgroup.Group
}

func newWebhookNotifier(env environment.Env, invocations <-chan *invocationInfo) *webhookNotifier {
	return &webhookNotifier{
		env:         env,
		invocations: invocations,
		tasks:       make(chan *notifyWebhookTask, 4096),
	}
}

func (w *webhookNotifier) Start() {
	ctx := w.env.GetServerContext()

	w.lookupGroup = errgroup.Group{}
	for range numWebhookInvocationLookupWorkers {
		metrics.WebhookInvocationLookupWorkers.Inc()
		w.lookupGroup.Go(func() error {
			defer metrics.WebhookInvocationLookupWorkers.Dec()
			// Listen for invocations that have been finalized and start a notify
			// webhook task for each webhook.
			for ij := range w.invocations {
				if err := w.lookupAndCreateTask(ctx, ij); err != nil {
					log.Warningf("Failed to lookup invocation before notifying webhook: %s", err)
				}
			}
			return nil
		})
	}

	w.notifyGroup = errgroup.Group{}
	for range numWebhookNotifyWorkers {
		metrics.WebhookNotifyWorkers.Inc()
		w.notifyGroup.Go(func() error {
			defer metrics.WebhookNotifyWorkers.Dec()
			for task := range w.tasks {
				ctx := log.EnrichContext(ctx, log.InvocationIDKey, task.invocation.GetInvocationId())
				if err := notifyWithTimeout(ctx, w.env, task); err != nil {
					log.CtxWarningf(ctx, "Failed to notify invocation webhook: %s", err)
				}
			}
			return nil
		})
	}
}

func (w *webhookNotifier) lookupAndCreateTask(ctx context.Context, ij *invocationInfo) error {
	start := time.Now()
	defer func() {
		metrics.WebhookInvocationLookupDuration.Observe(float64(time.Since(start).Microseconds()))
	}()

	invocation, err := w.lookupInvocation(ctx, ij)
	if err != nil {
		return err
	}

	// Don't call webhooks for disconnected invocations.
	if invocation.GetInvocationStatus() == inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS {
		return nil
	}

	for _, hook := range w.env.GetWebhooks() {
		w.tasks <- &notifyWebhookTask{
			hook:           hook,
			invocationInfo: ij,
			invocation:     invocation,
		}
	}

	return nil
}

func (w *webhookNotifier) Stop() {
	// Make sure we are done sending tasks on the task channel before we close it.
	if err := w.lookupGroup.Wait(); err != nil {
		log.Error(err.Error())
	}
	close(w.tasks)

	if err := w.notifyGroup.Wait(); err != nil {
		log.Error(err.Error())
	}
}

func (w *webhookNotifier) lookupInvocation(ctx context.Context, ij *invocationInfo) (*inpb.Invocation, error) {
	ctx = w.env.GetAuthenticator().AuthContextFromTrustedJWT(ctx, ij.jwt)
	inv, err := LookupInvocation(w.env, ctx, ij.id)
	if err != nil {
		return nil, err
	}
	// If detailed cache stats are enabled, the invocation will be missing the
	// scorecard misses field (with only AC misses) that we used to populate.
	// Populate these here for backwards compatibility.
	if hit_tracker.DetailedStatsEnabled() {
		tok, err := paging.EncodeOffsetLimit(&pgpb.OffsetLimit{Limit: hit_tracker.CacheMissScoreCardLimit})
		if err != nil {
			return nil, status.InternalErrorf("failed to encode page token: %s", err)
		}
		req := &capb.GetCacheScoreCardRequest{
			InvocationId: ij.id,
			PageToken:    tok,
			Filter: &capb.GetCacheScoreCardRequest_Filter{
				Mask: &fieldmaskpb.FieldMask{
					Paths: []string{
						"cache_type",
						"request_type",
						"response_type",
					},
				},
				CacheType:    rspb.CacheType_AC,
				RequestType:  capb.RequestType_READ,
				ResponseType: capb.ResponseType_NOT_FOUND,
			},
		}
		sc, err := scorecard.GetCacheScoreCard(ctx, w.env, req)
		if err != nil {
			log.Warningf("Failed to read cache scorecard for invocation %q: %s", req.GetInvocationId(), err)
		} else {
			inv.ScoreCard = &capb.ScoreCard{Misses: sc.GetResults()}
		}
	}
	return inv, nil
}

func isFinalEvent(obe *pepb.OrderedBuildEvent) bool {
	switch obe.GetEvent().GetEvent().(type) {
	case *bepb.BuildEvent_ComponentStreamFinished:
		return true
	}
	return false
}

func (e *EventChannel) isFirstStartedEvent(bazelBuildEvent *build_event_stream.BuildEvent) bool {
	if e.hasReceivedStartedEvent {
		return false
	}
	_, ok := bazelBuildEvent.GetPayload().(*build_event_stream.BuildEvent_Started)
	return ok
}

func (e *EventChannel) isFirstEventWithOptions(bazelBuildEvent *build_event_stream.BuildEvent) bool {
	switch p := bazelBuildEvent.GetPayload().(type) {
	case *build_event_stream.BuildEvent_Started:
		return p.Started.GetOptionsDescription() != "" && !e.hasReceivedEventWithOptions
	case *build_event_stream.BuildEvent_OptionsParsed:
		return !e.hasReceivedEventWithOptions
	}
	return false
}

func isWorkspaceStatusEvent(bazelBuildEvent *build_event_stream.BuildEvent) bool {
	switch bazelBuildEvent.GetPayload().(type) {
	case *build_event_stream.BuildEvent_WorkspaceStatus:
		return true
	}
	return false
}

func isChildInvocationsConfiguredEvent(bazelBuildEvent *build_event_stream.BuildEvent) bool {
	switch bazelBuildEvent.GetPayload().(type) {
	case *build_event_stream.BuildEvent_ChildInvocationsConfigured:
		return true
	}
	return false
}

func readBazelEvent(obe *pepb.OrderedBuildEvent, out *build_event_stream.BuildEvent) error {
	switch buildEvent := obe.GetEvent().GetEvent().(type) {
	case *bepb.BuildEvent_BazelEvent:
		return buildEvent.BazelEvent.UnmarshalTo(out)
	case *bepb.BuildEvent_ExperimentalBuildToolEvent:
		// TODO(sluongng): implement support for generic build tool events (i.e. BuckEvent)
	}
	return fmt.Errorf("Not a bazel event %s", obe)
}

type EventChannel struct {
	ctx            context.Context
	env            environment.Env
	pw             *protofile.BufferedProtoWriter
	beValues       *accumulator.BEValues
	redactor       *redact.StreamingRedactor
	statusReporter *build_status_reporter.BuildStatusReporter
	targetTracker  *target_tracker.TargetTracker
	statsRecorder  *statsRecorder
	collector      interfaces.MetricsCollector
	apiTargetMap   *api_common.TargetMap

	startedEvent                     *build_event_stream.BuildEvent_Started
	bufferedEvents                   []*inpb.InvocationEvent
	wroteBuildMetadata               bool
	numDroppedEventsBeforeProcessing uint64
	initialSequenceNumber            int64
	hasReceivedEventWithOptions      bool
	hasReceivedStartedEvent          bool
	requestedTerminalColumns         int
	requestedTerminalLines           int
	logWriter                        *eventlog.EventLogWriter
	onClose                          func()
	attempt                          uint64
	groupIDForMetrics                string
	errorOccurrences                 []*schema.ErrorOccurrence
	errorOccurrenceFingerprintCounts map[string]int
	errorOutputFiles                 map[*schema.ErrorOccurrence]*build_event_stream.File
	testAttempts                     []*testAttempt
	testSummaries                    map[testTargetKey]build_event_stream.TestStatus
	testArtifactBytes                int
	invocationIncarnation            string
	liveErrorBytes                   *atomic.Int64
	errorTrackingReserved            bool
	errorTrackingBudgetRejected      bool

	// isVoid determines whether all EventChannel operations are NOPs. This is set
	// when we're retrying an invocation that is already complete, or is
	// incomplete but was created too far in the past.
	isVoid bool

	// lastDBUpdateTime is when the invocation row was last written to the DB.
	// It is used to periodically update the row while events are streaming.
	lastDBUpdateTime time.Time
}

type testTargetKey struct {
	targetLabel     string
	configurationID string
}

type testAttempt struct {
	key            testTargetKey
	run            int32
	shard          int32
	attempt        int32
	status         build_event_stream.TestStatus
	statusDetails  string
	cachedLocally  bool
	cachedRemotely bool
	strategy       string
	exitCode       int32
	sequenceNumber int64
	eventTimeUsec  int64
	testXML        *build_event_stream.File
	testLog        *build_event_stream.File
	artifactBytes  int
}

func (e *EventChannel) Context() context.Context {
	return e.ctx
}

func (e *EventChannel) Close() {
	e.releaseErrorTrackingReservation()
	e.onClose()
}

func (e *EventChannel) reserveErrorTracking() bool {
	if e.errorTrackingReserved {
		return true
	}
	if e.errorTrackingBudgetRejected || e.liveErrorBytes == nil {
		return false
	}
	for {
		used := e.liveErrorBytes.Load()
		if used+liveErrorStreamReservation > maxLiveErrorBytes {
			e.errorTrackingBudgetRejected = true
			alert.UnexpectedEvent("error_tracking_live_byte_budget_exceeded", "Dropping error tracking collection for a live BES stream because the global live-state byte budget is full.")
			return false
		}
		if e.liveErrorBytes.CompareAndSwap(used, used+liveErrorStreamReservation) {
			e.errorTrackingReserved = true
			return true
		}
	}
}

func (e *EventChannel) releaseErrorTrackingReservation() {
	if !e.errorTrackingReserved || e.liveErrorBytes == nil {
		return
	}
	e.liveErrorBytes.Add(-liveErrorStreamReservation)
	e.errorTrackingReserved = false
}

func (e *EventChannel) FinalizeInvocation(iid string) error {
	if e.isVoid || !e.hasReceivedEventWithOptions {
		return nil
	}

	ctx, cancel := background.ExtendContextForFinalization(e.ctx, 10*time.Second)
	defer cancel()

	e.beValues.Finalize(ctx)

	invocation := e.beValues.Invocation()
	invocation.Attempt = e.attempt
	invocation.HasChunkedEventLogs = e.logWriter != nil

	disconnected := invocation.GetInvocationStatus() == inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS

	// Flush/close blobstore writers (raw event protos and build logs).
	if e.pw != nil {
		if err := e.pw.Flush(ctx); err != nil {
			// Return the error so that the client can retry sending events,
			// giving us another chance to write them to blobstore. If the
			// client disconnected, just log the error since they won't get the
			// error that we return here. This also ensures that we properly
			// mark the invocation disconnected below.
			if disconnected {
				log.CtxWarningf(ctx, "Failed to flush invocation events to blobstore: %s", err)
			} else {
				return err
			}
		}
	}
	if e.logWriter != nil {
		if err := e.logWriter.Close(ctx); err != nil {
			// Return the error so that the client can retry sending events,
			// giving us another chance to write them to blobstore. If the
			// client disconnected, just log the error since they won't get the
			// error that we return here. This also ensures that we properly
			// mark the invocation disconnected in the DB below.
			if disconnected {
				log.CtxWarningf(ctx, "Failed to flush invocation logs to blobstore: %s", err)
			} else {
				return err
			}
		}
		invocation.LastChunkId = e.logWriter.GetLastChunkId(ctx)
	}

	ti, err := e.tableInvocationFromProto(invocation, iid)
	if err != nil {
		return err
	}

	e.recordInvocationMetrics(ti)
	updated, err := e.env.GetInvocationDB().UpdateInvocation(ctx, ti)
	if err != nil {
		return err
	}
	if !updated {
		e.isVoid = true
		return status.CanceledErrorf("Attempt %d of invocation %s pre-empted by more recent attempt, invocation not finalized.", e.attempt, iid)
	}

	e.flushAPIFacets(iid)

	// Report a disconnect only if we successfully updated the invocation.
	// This reduces the likelihood that the disconnected invocation's status
	// will overwrite any statuses written by a more recent attempt.
	if disconnected {
		log.CtxWarning(ctx, "Reporting disconnected status for invocation")
		e.statusReporter.ReportDisconnect(ctx)
	}

	var errorOccurrenceLoader func(context.Context) []*schema.ErrorOccurrence
	var errorOccurrenceBytes int64
	if *features.ErrorTrackingEnabled {
		errorFinalizer := e.snapshotErrorFinalizer()
		errorOccurrenceLoader = func(backgroundCtx context.Context) []*schema.ErrorOccurrence {
			return errorFinalizer.finalizeErrorOccurrences(backgroundCtx, iid)
		}
		errorOccurrenceBytes = errorFinalizer.retainedErrorBytes()
	}
	e.statsRecorder.Enqueue(ctx, e.beValues, e.invocationIncarnation, errorOccurrenceLoader, errorOccurrenceBytes)
	log.CtxInfof(ctx, "Finalized invocation in primary DB and enqueued for stats recording (status: %s)", invocation.GetInvocationStatus())
	return nil
}

// snapshotErrorFinalizer copies only the bounded error-tracking state needed
// by the stats worker. Artifact reads must not delay BES acknowledgements.
func (e *EventChannel) snapshotErrorFinalizer() *EventChannel {
	// Only final user-visible candidates need to cross the queue boundary. This
	// caps retained diagnostics at roughly 400 KiB per queued invocation rather
	// than retaining the raw 1,000-occurrence collection buffer.
	rootCandidates := error_tracking.RootOccurrences(e.errorOccurrences)
	candidates := make([]*schema.ErrorOccurrence, 0, min(len(rootCandidates), error_tracking.MaxOccurrencesPerInvocation))
	selected := make(map[*schema.ErrorOccurrence]struct{}, error_tracking.MaxOccurrencesPerInvocation)
	seenFingerprints := make(map[string]struct{}, error_tracking.MaxOccurrencesPerInvocation)
	// Preserve one representative of every already-distinct fingerprint before
	// spending the remaining budget on URI-backed candidates whose provisional
	// fingerprints may change after enrichment.
	for _, occurrence := range rootCandidates {
		if _, ok := seenFingerprints[occurrence.Fingerprint]; ok {
			continue
		}
		seenFingerprints[occurrence.Fingerprint] = struct{}{}
		candidates = append(candidates, occurrence)
		selected[occurrence] = struct{}{}
		if len(candidates) == error_tracking.MaxOccurrencesPerInvocation {
			break
		}
	}
	for _, occurrence := range rootCandidates {
		if len(candidates) == error_tracking.MaxOccurrencesPerInvocation {
			break
		}
		if _, ok := selected[occurrence]; ok || e.errorOutputFiles[occurrence] == nil {
			continue
		}
		candidates = append(candidates, occurrence)
		selected[occurrence] = struct{}{}
	}
	occurrences := make([]*schema.ErrorOccurrence, 0, len(candidates))
	outputFiles := make(map[*schema.ErrorOccurrence]*build_event_stream.File, len(candidates))
	fingerprintCounts := make(map[string]int, len(candidates))
	remainingArtifactBytes := maxTestArtifactBytes
	for _, occurrence := range candidates {
		clone := *occurrence
		occurrences = append(occurrences, &clone)
		fingerprintCounts[clone.Fingerprint]++
		if file := e.errorOutputFiles[occurrence]; file != nil {
			if retained := cloneQueuedTestArtifact(file, maxTestLogBytes, &remainingArtifactBytes); retained.GetUri() != "" || len(retained.GetContents()) > 0 {
				outputFiles[&clone] = retained
			}
		}
	}
	testAttempts := make([]*testAttempt, 0, len(e.testAttempts))
	for _, attempt := range e.testAttempts {
		clone := *attempt
		clone.testXML = cloneQueuedTestArtifact(attempt.testXML, maxTestXMLBytes, &remainingArtifactBytes)
		clone.testLog = cloneQueuedTestArtifact(attempt.testLog, maxTestLogBytes, &remainingArtifactBytes)
		testAttempts = append(testAttempts, &clone)
	}
	return &EventChannel{
		env:                              e.env,
		attempt:                          e.attempt,
		errorOccurrences:                 occurrences,
		errorOccurrenceFingerprintCounts: fingerprintCounts,
		errorOutputFiles:                 outputFiles,
		testAttempts:                     testAttempts,
		testSummaries:                    maps.Clone(e.testSummaries),
		testArtifactBytes:                maxTestArtifactBytes - remainingArtifactBytes,
	}
}

func (e *EventChannel) retainedErrorBytes() int64 {
	bytes := int64(e.testArtifactBytes)
	for _, occurrence := range e.errorOccurrences {
		bytes += int64(len(occurrence.Message) + len(occurrence.ErrorType) + len(occurrence.TargetLabel) + len(occurrence.ActionMnemonic))
	}
	for _, attempt := range e.testAttempts {
		bytes += int64(len(attempt.key.targetLabel) + len(attempt.key.configurationID) + len(attempt.statusDetails) + len(attempt.strategy))
	}
	return bytes
}

func cloneQueuedTestArtifact(file *build_event_stream.File, perArtifactLimit int, remainingBytes *int) *build_event_stream.File {
	if file == nil {
		return nil
	}
	clone := &build_event_stream.File{}
	clone.Name = retainArtifactString(file.GetName(), maxArtifactNameBytes, remainingBytes)
	if uri := file.GetUri(); uri != "" {
		if retained := retainArtifactString(uri, maxArtifactURIBytes, remainingBytes); retained != "" {
			clone.File = &build_event_stream.File_Uri{Uri: retained}
		}
		return clone
	}
	contents := file.GetContents()
	if len(contents) == 0 || *remainingBytes <= 0 {
		return clone
	}
	retained := min(len(contents), perArtifactLimit+1, *remainingBytes)
	clone.File = &build_event_stream.File_Contents{Contents: append([]byte(nil), contents[:retained]...)}
	*remainingBytes -= retained
	return clone
}

func retainArtifactString(value string, perValueLimit int, remainingBytes *int) string {
	if value == "" || *remainingBytes <= 0 {
		return ""
	}
	retained := min(len(value), perValueLimit, *remainingBytes)
	value = boundUTF8(value, retained)
	*remainingBytes -= len(value)
	return value
}

func (e *EventChannel) finalizeErrorOccurrences(ctx context.Context, invocationID string) []*schema.ErrorOccurrence {
	ctx, cancel := context.WithTimeout(ctx, testArtifactFetchTimeout)
	defer cancel()
	e.enrichErrorOccurrences(ctx)
	for _, occurrence := range e.testErrorOccurrences(ctx, invocationID) {
		e.addErrorOccurrence(occurrence, nil)
	}
	rootOccurrences := error_tracking.RootOccurrences(e.errorOccurrences)
	uniqueOccurrences := error_tracking.DeduplicateOccurrences(rootOccurrences)
	if len(uniqueOccurrences) > error_tracking.MaxOccurrencesPerInvocation {
		uniqueOccurrences = uniqueOccurrences[:error_tracking.MaxOccurrencesPerInvocation]
	}
	return uniqueOccurrences
}

func fillInvocationFromCacheStats(cacheStats *capb.CacheStats, ti *tables.Invocation) {
	ti.ActionCacheHits = cacheStats.GetActionCacheHits()
	ti.ActionCacheMisses = cacheStats.GetActionCacheMisses()
	ti.ActionCacheUploads = cacheStats.GetActionCacheUploads()
	ti.CasCacheHits = cacheStats.GetCasCacheHits()
	ti.CasCacheMisses = cacheStats.GetCasCacheMisses()
	ti.CasCacheUploads = cacheStats.GetCasCacheUploads()
	ti.TotalDownloadSizeBytes = cacheStats.GetTotalDownloadSizeBytes()
	ti.TotalUploadSizeBytes = cacheStats.GetTotalUploadSizeBytes()
	ti.TotalDownloadTransferredSizeBytes = cacheStats.GetTotalDownloadTransferredSizeBytes()
	ti.TotalUploadTransferredSizeBytes = cacheStats.GetTotalUploadTransferredSizeBytes()
	ti.TotalDownloadUsec = cacheStats.GetTotalDownloadUsec()
	ti.TotalUploadUsec = cacheStats.GetTotalUploadUsec()
	ti.DownloadThroughputBytesPerSecond = cacheStats.GetDownloadThroughputBytesPerSecond()
	ti.UploadThroughputBytesPerSecond = cacheStats.GetUploadThroughputBytesPerSecond()
	ti.TotalCachedActionExecUsec = cacheStats.GetTotalCachedActionExecUsec()
	ti.TotalUncachedActionExecUsec = cacheStats.GetTotalUncachedActionExecUsec()
}

func invocationStatusLabel(ti *tables.Invocation) string {
	if ti.InvocationStatus == int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS) {
		if ti.Success {
			return "success"
		}
		return "failure"
	}
	if ti.InvocationStatus == int64(inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS) {
		return "disconnected"
	}
	return "unknown"
}

func (e *EventChannel) recordInvocationMetrics(ti *tables.Invocation) {
	statusLabel := invocationStatusLabel(ti)
	metrics.InvocationCount.With(prometheus.Labels{
		metrics.InvocationStatusLabel: statusLabel,
		metrics.BazelExitCode:         ti.BazelExitCode,
		metrics.BazelCommand:          ti.Command,
	}).Inc()
	metrics.InvocationDurationUs.With(prometheus.Labels{
		metrics.InvocationStatusLabel: statusLabel,
		metrics.BazelCommand:          ti.Command,
	}).Observe(float64(ti.DurationUsec))
	metrics.InvocationDurationUsExported.With(prometheus.Labels{
		metrics.InvocationStatusLabel: statusLabel,
		metrics.GroupID:               e.groupIDForMetrics,
	}).Observe(float64(ti.DurationUsec))
}

func (e *EventChannel) HandleEvent(event *pepb.PublishBuildToolEventStreamRequest) error {
	tStart := time.Now()
	err := e.handleEvent(event)
	duration := time.Since(tStart)
	labels := prometheus.Labels{
		metrics.StatusLabel: fmt.Sprintf("%d", gstatus.Code(err)),
	}
	metrics.BuildEventCount.With(labels).Inc()
	metrics.BuildEventHandlerDurationUs.With(labels).Observe(float64(duration.Microseconds()))
	return err
}

func (e *EventChannel) handleEvent(event *pepb.PublishBuildToolEventStreamRequest) error {
	if e.isVoid {
		return nil
	}

	if event.GetOrderedBuildEvent() == nil {
		return status.InvalidArgumentError("Missing OrderedBuildEvent")
	}

	seqNo := event.GetOrderedBuildEvent().GetSequenceNumber()
	streamID := event.GetOrderedBuildEvent().GetStreamId()
	iid := streamID.GetInvocationId()

	if e.initialSequenceNumber == 0 {
		e.initialSequenceNumber = seqNo
	}
	// We only allow initial sequence numbers greater than one in the case where
	// Bazel failed to receive all of our ACKs after we finalized an invocation
	// (marking it complete). In that case we just void the channel and ACK all
	// events without doing any work.
	if e.initialSequenceNumber > firstExpectedSequenceNumber {
		// TODO: once https://github.com/bazelbuild/bazel/pull/18437 lands in
		// Bazel, log an error if the client attempt number is 1 in this case,
		// since today we're relying on Bazel to always start sending events
		// starting from sequence number 1 in the first attempt.
		log.Infof("Voiding EventChannel for invocation %s: build event stream starts with sequence number > %d (%d), which likely means Bazel is retrying an invocation that we already finalized.", iid, firstExpectedSequenceNumber, e.initialSequenceNumber)
		e.isVoid = true
		return nil
	}

	if isFinalEvent(event.GetOrderedBuildEvent()) {
		return nil
	}

	var bazelBuildEvent build_event_stream.BuildEvent
	if err := readBazelEvent(event.GetOrderedBuildEvent(), &bazelBuildEvent); err != nil {
		log.CtxWarningf(e.ctx, "error reading bazel event: %s", err)
		return err
	}

	invocationEvent := &inpb.InvocationEvent{
		EventTime:      event.GetOrderedBuildEvent().GetEvent().GetEventTime(),
		BuildEvent:     &bazelBuildEvent,
		SequenceNumber: event.GetOrderedBuildEvent().GetSequenceNumber(),
	}

	// Bazel sends an Interrupted exit code in the finished event if the user cancelled the build.
	// Use that signal to cancel any actions that are currently in the remote execution system.
	if f, ok := bazelBuildEvent.GetPayload().(*build_event_stream.BuildEvent_Finished); ok {
		if f.Finished.GetExitCode().GetCode() == InterruptedExitCode && e.env.GetRemoteExecutionService() != nil {
			if err := e.env.GetRemoteExecutionService().Cancel(e.ctx, iid); err != nil {
				log.CtxWarningf(e.ctx, "Could not cancel executions for invocation %q: %s", iid, err)
			}
		}
	}
	if seqNo == 1 {
		log.CtxDebugf(e.ctx, "First event! sequence: %d invocation_id: %s, project_id: %s, notification_keywords: %s", seqNo, iid, event.GetProjectId(), event.GetNotificationKeywords())
	}

	if e.isFirstStartedEvent(&bazelBuildEvent) {
		started, _ := bazelBuildEvent.GetPayload().(*build_event_stream.BuildEvent_Started)

		parsedVersion, err := semver.NewVersion(started.Started.GetBuildToolVersion())
		version := "unknown"
		if err == nil {
			version = fmt.Sprintf("%d.%d", parsedVersion.Major(), parsedVersion.Minor())
		}
		metrics.InvocationsByBazelVersionCount.With(
			prometheus.Labels{metrics.BazelVersion: version}).Inc()

		e.hasReceivedStartedEvent = true
		e.beValues.SetExpectedMetadataEvents(bazelBuildEvent.GetChildren())
	}
	// If this is the first event with options, keep track of the project ID and save any notification keywords.
	if e.isFirstEventWithOptions(&bazelBuildEvent) {
		e.hasReceivedEventWithOptions = true
		log.CtxDebugf(e.ctx, "Received options! sequence: %d invocation_id: %s", seqNo, iid)

		authenticated, err := e.authenticateEvent(&bazelBuildEvent)
		if err != nil {
			return err
		}

		if authenticated {
			if irs := e.env.GetIPRulesEnforcer(); irs != nil {
				if _, err := irs.Authorize(e.ctx); err != nil {
					return err
				}
			}
			baseBBURL, err := subdomain.ReplaceURLSubdomain(e.ctx, e.env, build_buddy_url.String())
			if err != nil {
				return err
			}
			e.statusReporter.SetBaseBuildBuddyURL(baseBBURL)
		}

		invocationUUID, err := uuid.StringToBytes(iid)
		if err != nil {
			return err
		}
		ti := &tables.Invocation{
			InvocationID:     iid,
			InvocationUUID:   invocationUUID,
			InvocationStatus: int64(inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS),
			RedactionFlags:   redact.RedactionFlagStandardRedactions,
			Attempt:          e.attempt,
		}
		if *enableChunkedEventLogs {
			ti.LastChunkId = eventlog.EmptyId
		}

		created, err := e.env.GetInvocationDB().CreateInvocation(e.ctx, ti)
		if err != nil {
			return err
		}
		if !created {
			// We failed to retry an existing invocation
			log.CtxWarningf(e.ctx, "Voiding EventChannel for invocation %s: invocation already exists and is either completed or past its reconnect window, so may not be retried.", iid)
			e.isVoid = true
			return nil
		}
		e.lastDBUpdateTime = e.env.GetClock().Now()
		e.attempt = ti.Attempt
		e.invocationIncarnation = ti.ErrorTrackingIncarnation
		e.ctx = log.EnrichContext(e.ctx, "invocation_attempt", fmt.Sprintf("%d", e.attempt))
		log.CtxInfof(e.ctx, "Created invocation %q, attempt %d", ti.InvocationID, ti.Attempt)
		chunkFileSizeBytes := *chunkFileSizeBytes
		if chunkFileSizeBytes == 0 {
			chunkFileSizeBytes = defaultChunkFileSizeBytes
		}
		e.pw = protofile.NewBufferedProtoWriter(
			e.env.GetBlobstore(),
			GetStreamIdFromInvocationIdAndAttempt(iid, e.attempt),
			chunkFileSizeBytes,
		)
		if *enableChunkedEventLogs {
			e.requestedTerminalLines = getNumActionsFromOptions(&bazelBuildEvent)
			if e.requestedTerminalLines != 0 {
				// the number of lines curses can overwrite is 4 + the ui_actions shown:
				// 2 for the progress tracker, 1 for each action, and 2 blank lines.
				// 0 indicates that curses is not being used.
				e.requestedTerminalLines += 4
			}
		}
		// Since this is the first event with options and we just parsed the API key,
		// now is a good time to record invocation usage for the group. Check that
		// this is the first attempt of this invocation, to guarantee that we
		// don't increment the usage on invocation retries.
		if ut := e.env.GetUsageTracker(); ut != nil && ti.Attempt == 1 {
			incrementInvocationUsage(e.ctx, ut)
		}
	} else if !e.hasReceivedEventWithOptions || !e.hasReceivedStartedEvent {
		e.bufferedEvents = append(e.bufferedEvents, invocationEvent)
		if len(e.bufferedEvents) > 100 {
			e.numDroppedEventsBeforeProcessing++
			e.bufferedEvents = e.bufferedEvents[1:]
		}
		return nil
	}

	// Process buffered events.
	for _, event := range e.bufferedEvents {
		if err := e.processSingleEvent(event, iid); err != nil {
			return err
		}
	}
	e.bufferedEvents = nil

	// Process regular events.
	return e.processSingleEvent(invocationEvent, iid)
}

func (e *EventChannel) authenticateEvent(bazelBuildEvent *build_event_stream.BuildEvent) (bool, error) {
	auth := e.env.GetAuthenticator()
	if user, err := auth.AuthenticatedUser(e.ctx); err == nil && user != nil {
		return true, nil
	}
	options, err := extractOptions(bazelBuildEvent)
	if err != nil {
		return false, err
	}
	apiKey, err := authutil.ParseAPIKeyFromString(options)
	if err != nil {
		return false, err
	}
	if apiKey == "" {
		return false, nil
	}
	e.ctx = auth.AuthContextFromAPIKey(e.ctx, apiKey)
	e.groupIDForMetrics = getGroupIDForMetrics(e.ctx, e.env)
	authError := e.ctx.Value(interfaces.AuthContextUserErrorKey)
	if authError != nil {
		if err, ok := authError.(error); ok {
			return false, err
		}
		return false, status.UnknownError(fmt.Sprintf("%v", authError))
	}
	return true, nil
}

func (e *EventChannel) InitializeLogWriter(iid string) error {
	// Attach the invocation ID to the context so experiments evaluated by the
	// log writer can target and bucket by invocation.
	ctx := bazel_request.OverrideRequestMetadata(e.ctx, &repb.RequestMetadata{ToolInvocationId: iid})
	var err error
	e.logWriter, err = eventlog.NewEventLogWriter(
		ctx,
		e.env.GetBlobstore(),
		e.env.GetKeyValStore(),
		e.env.GetPubSub(),
		e.env.GetExperimentFlagProvider(),
		eventlog.GetEventLogPubSubChannel(iid),
		eventlog.GetEventLogPathFromInvocationIdAndAttempt(iid, e.attempt),
		e.requestedTerminalColumns,
		e.requestedTerminalLines,
	)
	return err
}

func (e *EventChannel) processSingleEvent(event *inpb.InvocationEvent, iid string) error {
	if err := e.redactor.RedactAPIKey(e.ctx, event.GetBuildEvent()); err != nil {
		return err
	}
	if err := e.redactor.RedactMetadata(event.GetBuildEvent()); err != nil {
		return err
	}
	// Accumulate a subset of invocation fields in memory.
	if err := e.beValues.AddEvent(event.GetBuildEvent()); err != nil {
		return err
	}
	eventTimeUsec := time.Now().UnixMicro()
	if event.GetEventTime() != nil {
		eventTimeUsec = event.GetEventTime().AsTime().UnixMicro()
	}
	bazelEvent := event.GetBuildEvent()
	if *features.ErrorTrackingEnabled {
		switch bazelEvent.GetPayload().(type) {
		case *build_event_stream.BuildEvent_TestResult:
			if result := bazelEvent.GetTestResult(); result != nil && result.GetStatus() != build_event_stream.TestStatus_PASSED && result.GetStatus() != build_event_stream.TestStatus_FLAKY && result.GetStatus() != build_event_stream.TestStatus_NO_STATUS && e.reserveErrorTracking() {
				e.collectTestAttempt(bazelEvent, event.GetSequenceNumber(), eventTimeUsec)
			}
		case *build_event_stream.BuildEvent_TestSummary:
			if e.errorTrackingReserved {
				e.collectTestSummary(bazelEvent)
			}
		default:
			occurrence := error_tracking.ExtractOccurrence(bazelEvent, iid, e.attempt, event.GetSequenceNumber(), eventTimeUsec)
			if occurrence != nil && e.reserveErrorTracking() {
				output, file := errorOutput(bazelEvent)
				error_tracking.EnrichOccurrence(occurrence, sanitizeErrorOutput(output))
				if output != "" {
					file = nil
				}
				e.addErrorOccurrence(occurrence, file)
			}
		}
	}

	switch p := event.GetBuildEvent().GetPayload().(type) {
	case *build_event_stream.BuildEvent_StructuredCommandLine:
		if e.logWriter == nil {
			// best effort to reduce memory usage when possible by using the value of
			// the `terminal_columns` option to determine the width of the ANSI
			// window, but if we need to write logs before we get a
			// `structuredCommandLine` build event, we have to just initialize with
			// default values, which is a little less efficient in some cases.
			for _, section := range p.StructuredCommandLine.GetSections() {
				if section.SectionLabel == "command options" {
					switch s := section.SectionType.(type) {
					case *command_line.CommandLineSection_ChunkList:
						// don't care about these
						continue
					case *command_line.CommandLineSection_OptionList:
						for _, option := range s.OptionList.Option {
							if option.GetOptionName() == "terminal_columns" {
								terminalColumns, err := strconv.ParseInt(option.OptionValue, 10, strconv.IntSize)
								if err != nil {
									terminalColumns = math.MaxInt
								}
								e.requestedTerminalColumns = int(terminalColumns)
							}
						}
					}
				}
			}
		}
	case *build_event_stream.BuildEvent_Progress:
		if *enableChunkedEventLogs {
			if e.logWriter == nil {
				if err := e.InitializeLogWriter(iid); err != nil {
					return err
				}
			}
			n, err := e.logWriter.Write(e.ctx, append([]byte(p.Progress.GetStderr()), []byte(p.Progress.GetStdout())...))
			if err == nil {
				if n > 0 {
					metrics.EventLogBytesWritten.With(map[string]string{
						metrics.EventName: "build_log",
						metrics.GroupID:   e.groupIDForMetrics,
					}).Add(float64(n))
				}
			} else if err != context.Canceled {
				log.CtxWarningf(e.ctx, "Failed to write build logs for event: %s", err)
			}
			// Don't store the log in the protostream if we're
			// writing it separately to blobstore
			p.Progress.Stderr = ""
			p.Progress.Stdout = ""
		}
	}

	e.targetTracker.TrackTargetsForEvent(e.ctx, event.GetBuildEvent())
	e.statusReporter.ReportStatusForEvent(e.ctx, event.GetBuildEvent())

	if err := e.collectAPIFacets(iid, event.GetBuildEvent()); err != nil {
		log.CtxWarningf(e.ctx, "Error collecting API facets: %s", err)
	}

	// For everything else, just save the event to our buffer and keep on chugging.
	if e.pw != nil {
		if err := e.pw.WriteProtoToStream(e.ctx, event); err != nil {
			return err
		}

		// Small optimization: For certain event types, flush the event stream
		// immediately to show things to the user faster when fetching status
		// of an incomplete build.
		/// Also flush if we haven't in over a minute.
		if shouldFlushImmediately(event.GetBuildEvent()) || e.pw.TimeSinceLastWrite().Minutes() > 1 {
			if err := e.pw.Flush(e.ctx); err != nil {
				return err
			}
		}
	}

	// When we have processed all invocation-level metadata events, update the
	// invocation in the DB so that it can be searched by its commit SHA, user
	// name, etc. even while the invocation is still in progress.
	if !e.wroteBuildMetadata && e.beValues.MetadataIsLoaded() {
		if err := e.writeBuildMetadata(e.ctx, iid); err != nil {
			return err
		}
		e.wroteBuildMetadata = true
	}

	// While events are still streaming, periodically update the invocation
	// row. The row is otherwise only updated at creation, when metadata is
	// loaded, and at finalization, so an invocation that runs longer than the
	// reconnect window would look abandoned and could never be retried if it
	// got disconnected.
	updatePeriod := e.env.GetInvocationDB().GetInvocationReconnectWindow() / 2
	if e.env.GetClock().Since(e.lastDBUpdateTime) >= updatePeriod {
		ti := &tables.Invocation{
			InvocationID:             iid,
			Attempt:                  e.attempt,
			ErrorTrackingIncarnation: e.invocationIncarnation,
		}
		if updated, err := e.env.GetInvocationDB().UpdateInvocation(e.ctx, ti); err != nil {
			log.CtxErrorf(e.ctx, "Error updating invocation row while streaming events: %s", err)
			return status.UnavailableErrorf("write periodic metadata update: %s", err)
		} else if !updated {
			e.isVoid = true
			return status.CanceledErrorf("Attempt %d of invocation %s pre-empted by more recent attempt.", e.attempt, iid)
		} else {
			e.lastDBUpdateTime = e.env.GetClock().Now()
		}
	}

	return nil
}

func errorOutput(event *build_event_stream.BuildEvent) (string, *build_event_stream.File) {
	var candidates []*build_event_stream.File
	switch payload := event.GetPayload().(type) {
	case *build_event_stream.BuildEvent_Action:
		candidates = []*build_event_stream.File{payload.Action.GetStderr(), payload.Action.GetStdout()}
	case *build_event_stream.BuildEvent_TestResult:
		for _, file := range payload.TestResult.GetTestActionOutput() {
			if strings.HasSuffix(file.GetName(), "test.log") {
				candidates = append(candidates, file)
			}
		}
	}
	for _, file := range candidates {
		if len(file.GetContents()) > 0 {
			contents := file.GetContents()
			// Inline action output is client-controlled and processed synchronously
			// on the BES stream. Bound it before conversion, UTF-8 repair, redaction,
			// and fingerprinting. Invalid UTF-8 may expand during repair, but this
			// keeps that expansion bounded by a small multiple of MaxMessageBytes.
			contents = contents[:min(len(contents), error_tracking.MaxMessageBytes)]
			return string(contents), nil
		}
	}
	for _, file := range candidates {
		if file.GetUri() != "" {
			return "", file
		}
	}
	return "", nil
}

func (e *EventChannel) collectTestAttempt(event *build_event_stream.BuildEvent, sequenceNumber, eventTimeUsec int64) {
	id := event.GetId().GetTestResult()
	result := event.GetTestResult()
	if id == nil || result == nil {
		return
	}
	status := result.GetStatus()
	if status == build_event_stream.TestStatus_PASSED || status == build_event_stream.TestStatus_FLAKY || status == build_event_stream.TestStatus_NO_STATUS {
		return
	}
	attempt := &testAttempt{
		key: testTargetKey{
			targetLabel:     retainTestMetadata(id.GetLabel(), maxTestTargetBytes),
			configurationID: retainTestMetadata(id.GetConfiguration().GetId(), maxTestConfigurationBytes),
		},
		run:            id.GetRun(),
		shard:          id.GetShard(),
		attempt:        id.GetAttempt(),
		status:         status,
		statusDetails:  retainTestMetadata(result.GetStatusDetails(), maxTestStatusDetailsBytes),
		cachedLocally:  result.GetCachedLocally(),
		cachedRemotely: result.GetExecutionInfo().GetCachedRemotely(),
		strategy:       retainTestMetadata(result.GetExecutionInfo().GetStrategy(), maxTestStrategyBytes),
		exitCode:       result.GetExecutionInfo().GetExitCode(),
		sequenceNumber: sequenceNumber,
		eventTimeUsec:  eventTimeUsec,
	}
	replaceIndex := -1
	if len(e.testAttempts) >= maxTestAttemptsPerInvocation {
		// Preserve a later distinct terminal candidate by replacing an older
		// duplicate attempt from the same target/configuration family.
		counts := make(map[testTargetKey]int, len(e.testAttempts))
		for _, existing := range e.testAttempts {
			counts[existing.key]++
		}
		for i, existing := range e.testAttempts {
			if counts[existing.key] > 1 {
				replaceIndex = i
				break
			}
		}
		if replaceIndex == -1 {
			// Decide the drop before cloning or charging any unretained artifact.
			return
		}
		e.testArtifactBytes = max(0, e.testArtifactBytes-e.testAttempts[replaceIndex].artifactBytes)
	}
	for _, file := range result.GetTestActionOutput() {
		remainingBytes := maxTestArtifactBytes - e.testArtifactBytes
		switch path.Base(file.GetName()) {
		case "test.xml":
			if attempt.testXML == nil {
				attempt.testXML = cloneQueuedTestArtifact(file, maxTestXMLBytes, &remainingBytes)
				attempt.artifactBytes += retainedArtifactBytes(attempt.testXML)
			}
		case "test.log":
			if attempt.testLog == nil {
				attempt.testLog = cloneQueuedTestArtifact(file, maxTestLogBytes, &remainingBytes)
				attempt.artifactBytes += retainedArtifactBytes(attempt.testLog)
			}
		}
		e.testArtifactBytes = maxTestArtifactBytes - remainingBytes
	}
	if replaceIndex >= 0 {
		e.testAttempts[replaceIndex] = attempt
		return
	}
	e.testAttempts = append(e.testAttempts, attempt)
}

func (e *EventChannel) collectTestSummary(event *build_event_stream.BuildEvent) {
	id := event.GetId().GetTestSummary()
	summary := event.GetTestSummary()
	if id == nil || summary == nil {
		return
	}
	key := testTargetKey{
		targetLabel:     retainTestMetadata(id.GetLabel(), maxTestTargetBytes),
		configurationID: retainTestMetadata(id.GetConfiguration().GetId(), maxTestConfigurationBytes),
	}
	finalStatus := summary.GetOverallStatus()
	if finalStatus == build_event_stream.TestStatus_PASSED || finalStatus == build_event_stream.TestStatus_FLAKY {
		retained := e.testAttempts[:0]
		for _, attempt := range e.testAttempts {
			if attempt.key == key {
				e.testArtifactBytes = max(0, e.testArtifactBytes-attempt.artifactBytes)
				continue
			}
			retained = append(retained, attempt)
		}
		e.testAttempts = retained
		delete(e.testSummaries, key)
		return
	}
	for _, attempt := range e.testAttempts {
		if attempt.key == key {
			e.testSummaries[key] = finalStatus
			return
		}
	}
}

func retainTestMetadata(value string, maxBytes int) string {
	return strings.Clone(boundUTF8(value, maxBytes))
}

type testArtifacts struct {
	xml []byte
	log []byte
}

func (e *EventChannel) testErrorOccurrences(ctx context.Context, invocationID string) []*schema.ErrorOccurrence {
	if len(e.testAttempts) == 0 {
		return nil
	}
	cacheURL, _ := url.Parse(cache_api_url.String())
	fetchCtx, cancel := context.WithTimeout(ctx, testArtifactFetchTimeout)
	defer cancel()

	artifacts := make([]testArtifacts, len(e.testAttempts))
	var aggregateBytes atomic.Int64
	group, groupCtx := errgroup.WithContext(fetchCtx)
	group.SetLimit(8)
	for i, attempt := range e.testAttempts {
		finalStatus, ok := e.testSummaries[attempt.key]
		if ok && (finalStatus == build_event_stream.TestStatus_PASSED || finalStatus == build_event_stream.TestStatus_FLAKY) {
			continue
		}
		group.Go(func() error {
			artifacts[i].xml, _ = e.readTestArtifact(groupCtx, cacheURL, attempt.testXML, maxTestXMLBytes, false, &aggregateBytes)
			artifacts[i].log, _ = e.readTestArtifact(groupCtx, cacheURL, attempt.testLog, maxTestLogBytes, true, &aggregateBytes)
			return nil
		})
	}
	_ = group.Wait()

	var occurrences []*schema.ErrorOccurrence
	limits := junit.DefaultLimits()
	limits.MaxInputBytes = maxTestXMLBytes
	for i, attempt := range e.testAttempts {
		finalStatus := attempt.status
		if status, ok := e.testSummaries[attempt.key]; ok {
			if status == build_event_stream.TestStatus_PASSED || status == build_event_stream.TestStatus_FLAKY {
				continue
			}
			finalStatus = status
		}
		cases, err := junit.Parse(bytes.NewReader(artifacts[i].xml), limits)
		if (err == nil || errors.Is(err, junit.ErrResultLimit)) && len(cases) > 0 {
			for _, testCase := range leafFailedTestCases(cases) {
				for _, failure := range testCase.Failures {
					fingerprintFailure := error_tracking.TestFailure{
						TargetLabel: attempt.key.targetLabel,
						SuiteName:   testCase.SuiteName,
						ClassName:   testCase.ClassName,
						TestName:    testCase.Name,
						Kind:        failure.Kind,
						Type:        failure.Type,
						Message:     sanitizeErrorOutput(failure.Message),
						Body:        sanitizeErrorOutput(failure.Body),
					}
					fingerprint, _ := error_tracking.TestFailureFingerprint(fingerprintFailure)
					message := boundedTestMessage(fingerprintFailure.Message, fingerprintFailure.Body)
					if message == "" {
						message = boundedTestMessage(testCase.Name, failure.Type, failure.Kind)
					}
					occurrences = append(occurrences, e.newTestOccurrence(invocationID, attempt, &schema.ErrorOccurrence{
						Fingerprint:           fingerprint,
						ErrorType:             "test/" + finalStatus.String() + "/" + failure.Kind,
						Message:               message,
						FingerprintVersion:    error_tracking.TestFingerprintVersion,
						FingerprintSource:     "test_xml",
						FingerprintConfidence: "high",
						TestSuite:             boundUTF8(testCase.SuiteName, 1024),
						TestClass:             boundUTF8(testCase.ClassName, 1024),
						TestName:              boundUTF8(testCase.Name, 1024),
						TestFailureKind:       boundUTF8(failure.Kind, 64),
						TestFailureType:       boundUTF8(failure.Type, 1024),
					}))
				}
			}
			continue
		}

		message := boundedTestMessage(string(artifacts[i].log))
		if message == "" {
			message = boundedTestMessage(attempt.statusDetails)
		}
		if message == "" {
			message = fmt.Sprintf("test %s finished with status %s", attempt.key.targetLabel, finalStatus)
		}
		fingerprint, _ := error_tracking.TestFallbackFingerprint(attempt.key.targetLabel, finalStatus.String(), message)
		occurrences = append(occurrences, e.newTestOccurrence(invocationID, attempt, &schema.ErrorOccurrence{
			Fingerprint:           fingerprint,
			ErrorType:             "test/" + finalStatus.String(),
			Message:               message,
			FingerprintVersion:    error_tracking.TestFallbackFingerprintVersion,
			FingerprintSource:     "test_result_fallback",
			FingerprintConfidence: "low",
		}))
	}
	return occurrences
}

// Some runners (notably rules_go) report a generic failed parent testcase in
// addition to its failed subtests. Keep a parent with its own diagnostic, but
// suppress a generic aggregate so one underlying assertion is not presented as
// both a parent issue and a child issue.
func leafFailedTestCases(cases []junit.TestCase) []junit.TestCase {
	result := make([]junit.TestCase, 0, len(cases))
	for i, candidate := range cases {
		hasFailedChild := false
		for j, other := range cases {
			if i == j || candidate.SuiteName != other.SuiteName || candidate.ClassName != other.ClassName {
				continue
			}
			if candidate.Name != "" && strings.HasPrefix(other.Name, candidate.Name+"/") {
				hasFailedChild = true
				break
			}
		}
		if hasFailedChild && isGenericAggregateFailure(candidate.Failures) {
			continue
		}
		result = append(result, candidate)
	}
	return result
}

func isGenericAggregateFailure(failures []junit.Failure) bool {
	if len(failures) == 0 {
		return false
	}
	for _, failure := range failures {
		if !isGenericAggregateText(failure.Message) || !isGenericAggregateText(failure.Body) {
			return false
		}
		failureType := strings.Trim(strings.ToLower(strings.TrimSpace(failure.Type)), ".:")
		if failureType != "" && failureType != "failure" && failureType != "error" {
			return false
		}
	}
	return true
}

func isGenericAggregateText(value string) bool {
	value = strings.Trim(strings.ToLower(strings.TrimSpace(value)), ".:")
	switch value {
	case "", "failed", "failure", "aggregate failed", "aggregate failure", "test failed", "test failure":
		return true
	default:
		return false
	}
}

func (e *EventChannel) newTestOccurrence(invocationID string, attempt *testAttempt, occurrence *schema.ErrorOccurrence) *schema.ErrorOccurrence {
	occurrence.EventTimeUsec = error_tracking.ClampEventTimeUsec(attempt.eventTimeUsec)
	occurrence.InvocationID = invocationID
	occurrence.Attempt = e.attempt
	occurrence.SequenceNumber = attempt.sequenceNumber
	occurrence.TargetLabel = boundUTF8(attempt.key.targetLabel, 1024)
	occurrence.ExitCode = attempt.exitCode
	occurrence.TestRun = attempt.run
	occurrence.TestShard = attempt.shard
	occurrence.TestAttempt = attempt.attempt
	occurrence.TestCachedLocally = attempt.cachedLocally
	occurrence.TestCachedRemotely = attempt.cachedRemotely
	occurrence.TestStrategy = boundUTF8(attempt.strategy, 128)
	return occurrence
}

func (e *EventChannel) readTestArtifact(ctx context.Context, cacheURL *url.URL, file *build_event_stream.File, maxBytes int64, truncate bool, aggregateBytes *atomic.Int64) ([]byte, error) {
	if file == nil {
		return nil, status.NotFoundError("test artifact not present")
	}
	limit := maxBytes
	if !truncate {
		limit++
	}
	w := &boundedArtifactWriter{maxBytes: limit, aggregateBytes: aggregateBytes}
	if contents := file.GetContents(); len(contents) > 0 {
		if _, err := w.Write(contents); err != nil && !truncate {
			return nil, err
		}
	} else {
		u, err := url.Parse(file.GetUri())
		if err != nil || u.Scheme != "bytestream" || cacheURL == nil || cacheURL.Host == "" || u.Host != cacheURL.Host || e.env.GetPooledByteStreamClient() == nil {
			return nil, status.InvalidArgumentError("invalid test artifact URI")
		}
		if err := e.env.GetPooledByteStreamClient().StreamBytestreamFileChunk(ctx, u, 0, limit, w); err != nil && !(truncate && len(w.buf) > 0) {
			return nil, err
		}
	}
	if !truncate && int64(len(w.buf)) > maxBytes {
		return nil, status.ResourceExhaustedError("test artifact exceeds size limit")
	}
	return w.buf, nil
}

type boundedArtifactWriter struct {
	buf            []byte
	maxBytes       int64
	aggregateBytes *atomic.Int64
}

func (w *boundedArtifactWriter) Write(p []byte) (int, error) {
	remaining := w.maxBytes - int64(len(w.buf))
	if remaining <= 0 {
		return 0, status.ResourceExhaustedError("test artifact exceeds size limit")
	}
	n := int64(len(p))
	if n > remaining {
		n = remaining
	}
	for {
		used := w.aggregateBytes.Load()
		available := int64(maxTestArtifactBytes) - used
		if available <= 0 {
			return 0, status.ResourceExhaustedError("test artifacts exceed aggregate size limit")
		}
		if n > available {
			n = available
		}
		if w.aggregateBytes.CompareAndSwap(used, used+n) {
			break
		}
	}
	w.buf = append(w.buf, p[:n]...)
	if int64(len(p)) > n {
		return int(n), status.ResourceExhaustedError("test artifact exceeds size limit")
	}
	return int(n), nil
}

func boundedTestMessage(parts ...string) string {
	nonBlank := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			nonBlank = append(nonBlank, part)
		}
	}
	return boundUTF8(sanitizeErrorOutput(strings.Join(nonBlank, "\n")), error_tracking.MaxMessageBytes)
}

func boundUTF8(value string, maxBytes int) string {
	value = strings.ToValidUTF8(value, "\uFFFD")
	if len(value) <= maxBytes {
		return value
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	return value[:end]
}

func (e *EventChannel) addErrorOccurrence(occurrence *schema.ErrorOccurrence, file *build_event_stream.File) {
	if len(e.errorOccurrences) < error_tracking.MaxRawOccurrencesPerInvocation {
		e.errorOccurrences = append(e.errorOccurrences, occurrence)
		e.errorOccurrenceFingerprintCounts[occurrence.Fingerprint]++
		if file = e.retainErrorOutputFile(file); file != nil {
			e.errorOutputFiles[occurrence] = file
		}
		return
	}

	// Once the raw candidate budget is full, replace an older duplicate so a
	// repeated cascade cannot hide a later distinct root. URI-backed diagnostics
	// have only a provisional fingerprint here, so replacing the oldest duplicate
	// even when the incoming provisional fingerprint matches keeps recent output
	// candidates available for enrichment at finalization.
	replaceIndex := -1
	for i, existing := range e.errorOccurrences {
		if e.errorOccurrenceFingerprintCounts[existing.Fingerprint] > 1 {
			replaceIndex = i
			break
		}
	}
	// Preserve terminal failures even in the pathological case where the raw
	// budget already contains only unique candidates.
	if replaceIndex == -1 && isTerminalErrorOccurrence(occurrence) {
		for i, existing := range e.errorOccurrences {
			if !isTerminalErrorOccurrence(existing) {
				replaceIndex = i
				break
			}
		}
	}
	if replaceIndex == -1 {
		return
	}
	replaced := e.errorOccurrences[replaceIndex]
	e.testArtifactBytes = max(0, e.testArtifactBytes-retainedArtifactBytes(e.errorOutputFiles[replaced]))
	delete(e.errorOutputFiles, replaced)
	e.errorOccurrenceFingerprintCounts[replaced.Fingerprint]--
	e.errorOccurrences[replaceIndex] = occurrence
	e.errorOccurrenceFingerprintCounts[occurrence.Fingerprint]++
	if file = e.retainErrorOutputFile(file); file != nil {
		e.errorOutputFiles[occurrence] = file
	}
}

func (e *EventChannel) retainErrorOutputFile(file *build_event_stream.File) *build_event_stream.File {
	if file == nil {
		return nil
	}
	remainingBytes := maxTestArtifactBytes - e.testArtifactBytes
	retained := cloneQueuedTestArtifact(file, maxTestLogBytes, &remainingBytes)
	e.testArtifactBytes = maxTestArtifactBytes - remainingBytes
	if retained.GetUri() == "" && len(retained.GetContents()) == 0 {
		return nil
	}
	return retained
}

func retainedArtifactBytes(file *build_event_stream.File) int {
	if file == nil {
		return 0
	}
	return len(file.GetName()) + len(file.GetUri()) + len(file.GetContents())
}

func isTerminalErrorOccurrence(occurrence *schema.ErrorOccurrence) bool {
	return strings.HasPrefix(occurrence.ErrorType, "build/") || strings.HasPrefix(occurrence.ErrorType, "aborted/")
}

func sanitizeErrorOutput(output string) string {
	output = strings.ToValidUTF8(output, "\uFFFD")
	output = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			return -1
		}
		return r
	}, output)
	return redact.RedactText(output)
}

func (e *EventChannel) enrichErrorOccurrences(ctx context.Context) {
	if len(e.errorOutputFiles) == 0 || e.env.GetPooledByteStreamClient() == nil {
		return
	}
	cacheURL, err := url.Parse(cache_api_url.String())
	if err != nil || cacheURL.Host == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	for occurrence, file := range e.errorOutputFiles {
		group.Go(func() error {
			u, err := url.Parse(file.GetUri())
			if err != nil || u.Scheme != "bytestream" || u.Host != cacheURL.Host {
				return nil
			}
			var output bytes.Buffer
			if err := e.env.GetPooledByteStreamClient().StreamBytestreamFileChunk(groupCtx, u, 0, error_tracking.MaxMessageBytes, &output); err != nil {
				return nil
			}
			error_tracking.EnrichOccurrence(occurrence, sanitizeErrorOutput(output.String()))
			return nil
		})
	}
	_ = group.Wait()
}

func shouldFlushImmediately(bazelBuildEvent *build_event_stream.BuildEvent) bool {
	// Workspace status event: Most of the command line options and workspace info
	// has come through by then, so we have a good amount of info to show the user
	// about the in-progress build
	//
	// Child invocations configured event: If a child invocation starts, flush
	// the event stream so we can link to the child invocation in the UI
	return isWorkspaceStatusEvent(bazelBuildEvent) || isChildInvocationsConfiguredEvent(bazelBuildEvent)
}

const apiFacetsExpiration = 1 * time.Hour

func (e *EventChannel) flushAPIFacets(iid string) error {
	if e.collector == nil || e.env.GetAPIService() == nil || !e.env.GetAPIService().CacheEnabled() {
		return nil
	}

	userInfo, err := e.env.GetAuthenticator().AuthenticatedUser(e.ctx)
	if userInfo == nil || err != nil {
		return nil
	}

	for label, target := range e.apiTargetMap.Targets {
		b, err := proto.Marshal(target)
		if err != nil {
			return err
		}
		key := api_common.TargetLabelKey(userInfo.GetGroupID(), iid, label)
		if err := e.collector.Set(e.ctx, key, string(b), apiFacetsExpiration); err != nil {
			return err
		}
	}
	return nil
}

func (e *EventChannel) collectAPIFacets(iid string, event *build_event_stream.BuildEvent) error {
	if e.collector == nil || e.env.GetAPIService() == nil || !e.env.GetAPIService().CacheEnabled() {
		return nil
	}

	userInfo, err := e.env.GetAuthenticator().AuthenticatedUser(e.ctx)
	if userInfo == nil || err != nil {
		return nil
	}

	e.apiTargetMap.ProcessEvent(iid, event)

	action := &apipb.Action{
		Id: &apipb.Action_Id{
			InvocationId: iid,
		},
	}
	action = api_common.FillActionFromBuildEvent(event, action)
	if action != nil {
		action = api_common.FillActionOutputFilesFromBuildEvent(event, action)
	} else {
		// early exit if this isn't an action event.
		return nil
	}
	b, err := proto.Marshal(action)
	if err != nil {
		return err
	}
	key := api_common.ActionLabelKey(userInfo.GetGroupID(), iid, action.GetTargetLabel())
	if err := e.collector.ListAppend(e.ctx, key, string(b)); err != nil {
		return err
	}
	if err := e.collector.Expire(e.ctx, key, apiFacetsExpiration); err != nil {
		return err
	}
	return nil
}

func (e *EventChannel) writeBuildMetadata(ctx context.Context, invocationID string) error {
	db := e.env.GetInvocationDB()
	invocationProto := e.beValues.Invocation()
	if *enableChunkedEventLogs {
		if e.logWriter == nil {
			if err := e.InitializeLogWriter(invocationID); err != nil {
				return err
			}
		}
		invocationProto.LastChunkId = e.logWriter.GetLastChunkId(ctx)
	}
	ti, err := e.tableInvocationFromProto(invocationProto, "" /*=blobID*/)
	if err != nil {
		return err
	}
	ti.Attempt = e.attempt
	updated, err := db.UpdateInvocation(ctx, ti)
	if err != nil {
		return err
	}
	if !updated {
		e.isVoid = true
		return status.CanceledErrorf("Attempt %d of invocation %s pre-empted by more recent attempt, no build metadata written.", e.attempt, invocationID)
	}
	e.lastDBUpdateTime = e.env.GetClock().Now()
	return nil
}

func (e *EventChannel) GetNumDroppedEvents() uint64 {
	return e.numDroppedEventsBeforeProcessing
}

func (e *EventChannel) GetInitialSequenceNumber() int64 {
	return e.initialSequenceNumber
}

func extractOptions(event *build_event_stream.BuildEvent) (string, error) {
	switch p := event.GetPayload().(type) {
	case *build_event_stream.BuildEvent_Started:
		return p.Started.GetOptionsDescription(), nil
	case *build_event_stream.BuildEvent_OptionsParsed:
		return strings.Join(p.OptionsParsed.GetCmdLine(), " "), nil
	}
	return "", nil
}

func getNumActionsFromOptions(event *build_event_stream.BuildEvent) int {
	options, err := extractOptions(event)
	if err != nil {
		log.Warningf("Could not extract options for ui_actions_shown, defaulting to %d: %d", defaultActionsShown, err)
		return defaultActionsShown
	}
	optionsList, err := shlex.Split(options)
	if err != nil {
		log.Warningf("Could not shlex split options '%s' for ui_actions_shown, defaulting to %d: %v", options, defaultActionsShown, err)
		return defaultActionsShown
	}
	actionsShownValues := getOptionValues(optionsList, "ui_actions_shown")
	cursesValues := getOptionValues(optionsList, "curses")
	if len(cursesValues) > 0 {
		curses := cursesValues[len(cursesValues)-1]
		if curses == "no" {
			return 0
		} else if curses != "yes" && curses != "auto" {
			log.Warningf("Unrecognized argument to curses, assuming auto: %v", curses)
		}
	}
	if len(actionsShownValues) > 0 {
		n, err := strconv.Atoi(actionsShownValues[len(actionsShownValues)-1])
		if err != nil {
			log.Warningf("Invalid argument to ui_actions_shown, defaulting to %d: %v", defaultActionsShown, err)
		} else if n < 1 {
			return 1
		} else {
			return n
		}
	}
	return defaultActionsShown
}

func getOptionValues(options []string, optionName string) []string {
	values := []string{}
	flag := "--" + optionName
	for _, option := range options {
		if option == "--" {
			break
		}
		if after, found := strings.CutPrefix(option, flag+"="); found {
			values = append(values, after)
		}
	}
	return values
}

type invocationEventCB func(*inpb.InvocationEvent) error

func streamRawInvocationEvents(env environment.Env, ctx context.Context, streamID string, callback invocationEventCB) error {
	eventAllocator := func() proto.Message { return &inpb.InvocationEvent{} }
	pr := protofile.NewBufferedProtoReader(env.GetBlobstore(), streamID, eventAllocator)
	for {
		event, err := pr.ReadProto(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := callback(event.(*inpb.InvocationEvent)); err != nil {
			return err
		}
	}
	return nil
}

// LookupInvocation looks up the invocation, including all events. Prefer to use
// LookupInvocationWithCallback whenever possible, which avoids buffering events
// in memory.
func LookupInvocation(env environment.Env, ctx context.Context, iid string) (*inpb.Invocation, error) {
	var events []*inpb.InvocationEvent
	inv, err := LookupInvocationWithCallback(ctx, env, iid, func(event *inpb.InvocationEvent) error {
		// Certain buggy rulesets will mark intermediate output files as
		// important-outputs. This can result in very large BES streams which
		// use a ton of memory and are not displayable by the browser. If we
		// detect a large number of events coming through, begin dropping non-
		// important events so that this invocation can be displayed.
		if len(events) >= *buildEventFilterStartThreshold && !accumulator.IsImportantEvent(event.GetBuildEvent()) {
			return nil
		}
		events = append(events, event)
		return nil
	})
	if err != nil {
		return nil, err
	}
	inv.Event = events
	return inv, nil
}

// LookupInvocationWithCallback looks up an invocation but uses a callback for
// events instead of buffering events into the events list.
//
// TODO: switch to using this API wherever possible.
func LookupInvocationWithCallback(ctx context.Context, env environment.Env, iid string, cb invocationEventCB) (*inpb.Invocation, error) {
	ti, err := env.GetInvocationDB().LookupInvocation(ctx, iid)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("invocation not found")
		}
		return nil, err
	}

	// If this is an incomplete invocation, attempt to fill cache stats
	// from counters rather than trying to read them from invocation b/c
	// they won't be set yet.
	if ti.InvocationStatus == int64(inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS) {
		if cacheStats := hit_tracker.CollectCacheStats(ctx, env, iid); cacheStats != nil {
			fillInvocationFromCacheStats(cacheStats, ti)
		}
	}

	invocation := invocationdb.TableInvocationToProto(ti)

	var scoreCard *capb.ScoreCard
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		// When detailed stats are enabled, the scorecard is not inlined in the
		// invocation.
		if !hit_tracker.DetailedStatsEnabled() {
			// The cache ScoreCard is not stored in the table invocation, so we do this lookup
			// after converting the table invocation to a proto invocation.
			if ti.InvocationStatus == int64(inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS) {
				scoreCard = hit_tracker.ScoreCard(ctx, env, iid)
			} else {
				sc, err := scorecard.Read(ctx, env, iid, ti.Attempt)
				if err != nil {
					log.Warningf("Failed to read scorecard for invocation %s: %s", iid, err)
				} else {
					scoreCard = sc
				}
			}
		}
		return nil
	})

	eg.Go(func() error {
		return FetchAllInvocationEventsWithCallback(ctx, env, invocation, ti.RedactionFlags, cb)
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	invocation.ScoreCard = scoreCard
	return invocation, nil
}

func FetchAllInvocationEventsWithCallback(ctx context.Context, env environment.Env, inv *inpb.Invocation, invRedactionFlags int32, cb invocationEventCB) error {
	var screenWriter *terminal.ScreenWriter
	if !inv.GetHasChunkedEventLogs() {
		var err error
		screenWriter, err = terminal.NewScreenWriter(0, 0)
		if err != nil {
			return err
		}
	}
	var redactor *redact.StreamingRedactor
	if invRedactionFlags&redact.RedactionFlagStandardRedactions != redact.RedactionFlagStandardRedactions {
		// only redact if we hadn't redacted enough, only parse again if we redact
		redactor = redact.NewStreamingRedactor()
	}
	beValues := accumulator.NewBEValues(inv)
	structuredCommandLines := []*command_line.CommandLine{}
	streamID := GetStreamIdFromInvocationIdAndAttempt(inv.GetInvocationId(), inv.GetAttempt())
	err := streamRawInvocationEvents(env, ctx, streamID, func(event *inpb.InvocationEvent) error {
		if redactor != nil {
			if err := redactor.RedactAPIKeysWithSlowRegexp(ctx, event.GetBuildEvent()); err != nil {
				return err
			}
			if err := redactor.RedactMetadata(event.GetBuildEvent()); err != nil {
				return err
			}
			if err := beValues.AddEvent(event.GetBuildEvent()); err != nil {
				return err
			}
		}

		switch p := event.GetBuildEvent().GetPayload().(type) {
		case *build_event_stream.BuildEvent_Started:
			// Drop child pattern expanded events since this list can be
			// very long and we don't render these currently.
			event.BuildEvent.Children = nil
		case *build_event_stream.BuildEvent_Expanded:
			if len(event.GetBuildEvent().GetId().GetPattern().GetPattern()) > 0 {
				pattern, truncated := TruncateStringSlice(event.GetBuildEvent().GetId().GetPattern().GetPattern(), maxPatternLengthBytes)
				inv.PatternsTruncated = truncated
				event.BuildEvent.GetId().GetPattern().Pattern = pattern
			}
			// Don't return child TargetConfigured events to the UI; the UI
			// only cares about the actual TargetConfigured event payloads.
			event.BuildEvent.Children = nil
			// UI doesn't render TestSuiteExpansions yet (though we probably
			// should at some point?) So don't return these either.
			p.Expanded.TestSuiteExpansions = nil
		case *build_event_stream.BuildEvent_Progress:
			if screenWriter != nil {
				screenWriter.Write([]byte(p.Progress.GetStderr()))
				screenWriter.Write([]byte(p.Progress.GetStdout()))
			}
			// Don't serve progress event contents to the UI since they are too
			// large. Instead, logs are available either via the
			// console_buffer field or the separate logs RPC.
			p.Progress.Stderr = ""
			p.Progress.Stdout = ""
		case *build_event_stream.BuildEvent_StructuredCommandLine:
			structuredCommandLines = append(structuredCommandLines, p.StructuredCommandLine)
		}

		if err := cb(event); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	// TODO: Can we remove this StructuredCommandLine field? These are
	// already available in the events list.
	inv.StructuredCommandLine = structuredCommandLines
	if screenWriter != nil {
		inv.ConsoleBuffer = screenWriter.Render()
	}
	return nil
}

func (e *EventChannel) tableInvocationFromProto(p *inpb.Invocation, blobID string) (*tables.Invocation, error) {
	uuid, err := uuid.StringToBytes(p.GetInvocationId())
	if err != nil {
		return nil, err
	}

	i := &tables.Invocation{}
	i.InvocationID = p.GetInvocationId() // Required.
	i.ErrorTrackingIncarnation = e.invocationIncarnation
	i.InvocationUUID = uuid
	i.Success = p.GetSuccess()
	i.User = p.GetUser()
	i.DurationUsec = p.GetDurationUsec()
	i.Host = p.GetHost()
	i.RepoURL = p.GetRepoUrl()
	if norm, err := gitutil.NormalizeRepoURL(p.GetRepoUrl()); err == nil {
		i.RepoURL = norm.String()
	}
	i.BranchName = p.GetBranchName()
	i.CommitSHA = p.GetCommitSha()
	i.Role = p.GetRole()
	i.Command = p.GetCommand()
	if p.Pattern != nil {
		i.Pattern = invocation_format.ShortFormatPatterns(p.GetPattern())
	}
	i.ActionCount = p.GetActionCount()
	i.BlobID = blobID
	i.InvocationStatus = int64(p.GetInvocationStatus())
	i.LastChunkId = p.GetLastChunkId()
	i.RedactionFlags = redact.RedactionFlagStandardRedactions
	i.Attempt = p.GetAttempt()
	i.BazelExitCode = p.GetBazelExitCode()
	tags, err := invocation_format.JoinTags(p.GetTags())
	if err != nil {
		return nil, err
	}
	i.Tags = tags
	i.ParentRunID = p.GetParentRunId()
	i.RunID = p.GetRunId()

	userGroupPerms, err := perms.ForAuthenticatedGroup(e.ctx, e.env)
	if err != nil {
		return nil, err
	} else {
		i.Perms = userGroupPerms.Perms
	}
	if p.GetReadPermission() == inpb.InvocationPermission_PUBLIC {
		i.Perms |= perms.OTHERS_READ
	}
	i.DownloadOutputsOption = int64(p.GetDownloadOutputsOption())
	i.RemoteExecutionEnabled = p.GetRemoteExecutionEnabled()
	i.UploadLocalResultsEnabled = p.GetUploadLocalResultsEnabled()
	return i, nil
}

func GetStreamIdFromInvocationIdAndAttempt(iid string, attempt uint64) string {
	if attempt == 0 {
		// This invocation predates the attempt-tracking functionality, so its
		// streamId does not contain the attempt number.
		return iid
	}
	return iid + "/" + strconv.FormatUint(attempt, 10)
}

func toStoredInvocation(inv *tables.Invocation) *sipb.StoredInvocation {
	return &sipb.StoredInvocation{
		InvocationId:             inv.InvocationID,
		User:                     inv.User,
		Host:                     inv.Host,
		Pattern:                  inv.Pattern,
		Role:                     inv.Role,
		BranchName:               inv.BranchName,
		CommitSha:                inv.CommitSHA,
		RepoUrl:                  inv.RepoURL,
		Command:                  inv.Command,
		InvocationStatus:         inv.InvocationStatus,
		Success:                  inv.Success,
		Tags:                     inv.Tags,
		ErrorTrackingIncarnation: inv.ErrorTrackingIncarnation,
	}
}

func incrementInvocationUsage(ctx context.Context, ut interfaces.UsageTracker) {
	labels, olapLabels, err := usageutil.LabelsForUsageRecording(ctx, usageutil.ServerName())
	if err != nil {
		log.CtxWarningf(ctx, "Failed to compute invocation usage labels: %s", err)
		return
	}
	if err := ut.Increment(ctx, labels, &tables.UsageCounts{Invocations: 1}); err != nil {
		log.CtxWarningf(ctx, "Failed to increment invocation usage: %s", err)
	}
	// TODO: add 'tool' label? (bazel, buildbuddy_ci_runner, other)
	olapCounts := map[sku.SKU]int64{sku.BuildEventsBESCount: 1}
	if err := ut.IncrementOLAP(ctx, olapLabels, olapCounts); err != nil {
		log.CtxWarningf(ctx, "Failed to increment OLAP build events count usage: %s", err)
	}
}

// TruncateStringSlice truncates the given string slice so that when the strings
// are joined with a space (" "), the total byte length of the resulting string
// does not exceed the given character limit.
func TruncateStringSlice(strs []string, charLimit int) (truncatedList []string, truncated bool) {
	length := 0
	for i, s := range strs {
		if i > 0 {
			// When rendered in the UI, each arg except the first will be
			// preceded by a space. Count this towards the char limit.
			length += 1
		}
		if length+len(s) > charLimit {
			return strs[:i], true
		}
		length += len(s)
	}
	return strs, false
}
