package build_event_handler

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_status_reporter"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/target_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/scorecard"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/protofile"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/google/shlex"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	api_common "github.com/buildbuddy-io/buildbuddy/server/api/common"
	api_config "github.com/buildbuddy-io/buildbuddy/server/api/config"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	gstatus "google.golang.org/grpc/status"
)

const (
	defaultChunkFileSizeBytes = 1000 * 100 // 100KB

	// How many workers to spin up for writing cache stats to the DB.
	numStatsRecorderWorkers = 8

	// How many workers to spin up for looking up invocations before webhooks are
	// notified.
	numWebhookInvocationLookupWorkers = 8
	// How many workers to spin up for notifying webhooks.
	numWebhookNotifyWorkers = 16

	// How long to wait before giving up on webhook requests.
	webhookNotifyTimeout = 1 * time.Minute

	// Default number of actions shown by bazel
	defaultActionsShown = 8

	// Exit code in Finished event indicating that the build was interrupted (i.e. killed by user).
	InterruptedExitCode = 8
)

var (
	chunkFileSizeBytes                = flag.Int("storage.chunk_file_size_bytes", 3_000_000 /* 3 MB */, "How many bytes to buffer in memory before flushing a chunk of build protocol data to disk.")
	enableChunkedEventLogs            = flag.Bool("storage.enable_chunked_event_logs", false, "If true, Event logs will be stored separately from the invocation proto in chunks.")
	requireInvocationEventParseOnRead = flag.Bool("app.require_invocation_event_parse_on_read", false, "If true, invocation responses will be filled from database values and then by parsing the events on read.")

	cacheStatsFinalizationDelay = flag.Duration(
		"cache_stats_finalization_delay", 500*time.Millisecond,
		"The time allowed for all metrics collectors across all apps to flush their "+
			"local cache stats to the backing storage, before finalizing stats in the DB.")
)

type BuildEventHandler struct {
	env           environment.Env
	statsRecorder *statsRecorder
	openChannels  *sync.WaitGroup
}

func NewBuildEventHandler(env environment.Env) *BuildEventHandler {
	openChannels := &sync.WaitGroup{}
	onStatsRecorded := make(chan *invocationJWT, 4096)
	statsRecorder := newStatsRecorder(env, openChannels, onStatsRecorded)
	webhookNotifier := newWebhookNotifier(env, onStatsRecorded)

	statsRecorder.Start()
	webhookNotifier.Start()
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		statsRecorder.Stop()
		webhookNotifier.Stop()
		return nil
	})

	return &BuildEventHandler{
		env:           env,
		statsRecorder: statsRecorder,
		openChannels:  openChannels,
	}
}

func (b *BuildEventHandler) OpenChannel(ctx context.Context, iid string) interfaces.BuildEventChannel {
	buildEventAccumulator := accumulator.NewBEValues(iid)

	b.openChannels.Add(1)
	onClose := func() {
		b.openChannels.Done()
	}

	return &EventChannel{
		env:            b.env,
		statsRecorder:  b.statsRecorder,
		ctx:            ctx,
		pw:             nil,
		beValues:       buildEventAccumulator,
		redactor:       redact.NewStreamingRedactor(b.env),
		statusReporter: build_status_reporter.NewBuildStatusReporter(b.env, buildEventAccumulator),
		targetTracker:  target_tracker.NewTargetTracker(b.env, buildEventAccumulator),
		collector:      b.env.GetMetricsCollector(),
		apiTargetMap:   make(api_common.TargetMap),

		hasReceivedEventWithOptions: false,
		hasReceivedStartedEvent:     false,
		unprocessedStartingEvents:   make(map[string]struct{}),
		bufferedEvents:              make([]*inpb.InvocationEvent, 0),
		logWriter:                   nil,
		onClose:                     onClose,
		attempt:                     1,
	}
}

// invocationJWT represents an invocation ID as well as the JWT granting access
// to it. It should only be used for background tasks that need access to the
// JWT after the build event stream is already closed.
type invocationJWT struct {
	id      string
	jwt     string
	attempt uint64
}

// recordStatsTask contains the info needed to record the stats for an
// invocation. These tasks are enqueued to statsRecorder and executed in the
// background.
type recordStatsTask struct {
	*invocationJWT
	// createdAt is the time at which this task was created.
	createdAt time.Time
	// files contains a mapping of file digests to file name metadata for files
	// referenced in the BEP.
	files map[string]*build_event_stream.File
}

// statsRecorder listens for finalized invocations and copies cache stats from
// the metrics collector to the DB.
type statsRecorder struct {
	env          environment.Env
	openChannels *sync.WaitGroup
	// onStatsRecorded is a channel for this statsRecorder to notify after
	// recording stats for each invocation. Invocations sent on this channel are
	// considered "finalized".
	onStatsRecorded chan<- *invocationJWT
	eg              errgroup.Group

	mu      sync.Mutex // protects(tasks, stopped)
	tasks   chan *recordStatsTask
	stopped bool
}

func newStatsRecorder(env environment.Env, openChannels *sync.WaitGroup, onStatsRecorded chan<- *invocationJWT) *statsRecorder {
	return &statsRecorder{
		env:             env,
		openChannels:    openChannels,
		onStatsRecorded: onStatsRecorded,
		tasks:           make(chan *recordStatsTask, 4096),
	}
}

// Enqueue enqueues a task for the given invocation's stats to be recorded
// once they are available.
func (r *statsRecorder) Enqueue(ctx context.Context, invocation *inpb.Invocation) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		alert.UnexpectedEvent(
			"stats_recorder_finalize_after_shutdown",
			"Invocation %q was marked finalized after the stats recorder was shut down.",
			invocation.GetInvocationId())
		return
	}
	jwt := ""
	if auth := r.env.GetAuthenticator(); auth != nil {
		jwt = auth.TrustedJWTFromAuthContext(ctx)
	}
	req := &recordStatsTask{
		invocationJWT: &invocationJWT{
			id:      invocation.GetInvocationId(),
			attempt: invocation.Attempt,
			jwt:     jwt,
		},
		createdAt: time.Now(),
		files:     scorecard.ExtractFiles(invocation),
	}
	select {
	case r.tasks <- req:
		break
	default:
		alert.UnexpectedEvent(
			"stats_recorder_channel_buffer_full",
			"Failed to write cache stats: stats recorder task buffer is full")
	}
}

func (r *statsRecorder) Start() {
	ctx := r.env.GetServerContext()
	for i := 0; i < numStatsRecorderWorkers; i++ {
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

func (r *statsRecorder) handleTask(ctx context.Context, task *recordStatsTask) {
	start := time.Now()
	defer func() {
		metrics.StatsRecorderDuration.Observe(float64(time.Since(start).Microseconds()))
	}()

	// Apply the finalization delay relative to when the invocation was marked
	// finalized, rather than relative to now. Otherwise each worker would be
	// unnecessarily throttled.
	time.Sleep(time.Until(task.createdAt.Add(*cacheStatsFinalizationDelay)))
	ti := &tables.Invocation{InvocationID: task.invocationJWT.id, Attempt: task.invocationJWT.attempt}
	if stats := hit_tracker.CollectCacheStats(ctx, r.env, task.invocationJWT.id); stats != nil {
		fillInvocationFromCacheStats(stats, ti)
	}
	if sc := hit_tracker.ScoreCard(ctx, r.env, task.invocationJWT.id); sc != nil {
		scorecard.FillBESMetadata(sc, task.files)
		if err := scorecard.Write(ctx, r.env, task.invocationJWT.id, task.invocationJWT.attempt, sc); err != nil {
			log.Errorf("Error writing scorecard blob: %s", err)
		}
	}

	updated, err := r.env.GetInvocationDB().UpdateInvocation(ctx, ti)
	if err != nil {
		log.Errorf("Failed to write cache stats for invocation: %s", err)
	}
	// Cleanup regardless of whether the stats are flushed successfully to
	// the DB (since we won't retry the flush and we don't need these stats
	// for any other purpose).
	hit_tracker.CleanupCacheStats(ctx, r.env, task.invocationJWT.id)
	if !updated {
		log.Warningf("Attempt %d of invocation %s pre-empted by more recent attempt, no cache stats flushed.", task.invocationJWT.attempt, task.invocationJWT.id)
		// Don't notify the webhook; the more recent attempt should trigger
		// the notification when it is finalized.
		return
	}

	// Once cache stats are populated, notify the onStatsRecorded channel in
	// a non-blocking fashion.
	select {
	case r.onStatsRecorded <- task.invocationJWT:
		break
	default:
		alert.UnexpectedEvent(
			"webhook_channel_buffer_full",
			"Failed to notify webhook: channel buffer is full")
	}
}

func (r *statsRecorder) Stop() {
	// Wait for all EventHandler channels to be closed to ensure there will be no
	// more calls to Enqueue.
	// TODO(bduffany): This has a race condition where the server can be shutdown
	// just after the stream request is accepted by the server but before calling
	// openChannels.Add(1). Can fix this by explicitly waiting for the gRPC server
	// shutdown to finish, which ensures all streaming requests have terminated.
	r.openChannels.Wait()

	r.mu.Lock()
	r.stopped = true
	close(r.tasks)
	r.mu.Unlock()

	if err := r.eg.Wait(); err != nil {
		log.Error(err.Error())
	}

	close(r.onStatsRecorded)
}

type notifyWebhookTask struct {
	// hook is the webhook to notify of a completed invocation.
	hook interfaces.Webhook
	// invocationJWT contains the invocation ID and JWT for the invocation.
	*invocationJWT
	// invocation is the complete invocation looked up from the invocationJWT.
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
	ij := t.invocationJWT
	if auth := env.GetAuthenticator(); auth != nil {
		ctx = auth.AuthContextFromTrustedJWT(ctx, ij.jwt)
	}
	return t.hook.NotifyComplete(ctx, t.invocation)
}

// webhookNotifier listens for invocations to be finalized (including stats)
// and notifies webhooks.
type webhookNotifier struct {
	env environment.Env
	// invocations is a channel of finalized invocations. On each invocation
	// sent to this channel, we notify all configured webhooks.
	invocations <-chan *invocationJWT

	tasks       chan *notifyWebhookTask
	lookupGroup errgroup.Group
	notifyGroup errgroup.Group
}

func newWebhookNotifier(env environment.Env, invocations <-chan *invocationJWT) *webhookNotifier {
	return &webhookNotifier{
		env:         env,
		invocations: invocations,
		tasks:       make(chan *notifyWebhookTask, 4096),
	}
}

func (w *webhookNotifier) Start() {
	ctx := w.env.GetServerContext()

	w.lookupGroup = errgroup.Group{}
	for i := 0; i < numWebhookInvocationLookupWorkers; i++ {
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
	for i := 0; i < numWebhookNotifyWorkers; i++ {
		metrics.WebhookNotifyWorkers.Inc()
		w.notifyGroup.Go(func() error {
			defer metrics.WebhookNotifyWorkers.Dec()
			for task := range w.tasks {
				if err := notifyWithTimeout(ctx, w.env, task); err != nil {
					log.Warningf("Failed to notify webhook for invocation %s: %s", task.invocation.GetInvocationId(), err)
				}
			}
			return nil
		})
	}
}

func (w *webhookNotifier) lookupAndCreateTask(ctx context.Context, ij *invocationJWT) error {
	start := time.Now()
	defer func() {
		metrics.WebhookInvocationLookupDuration.Observe(float64(time.Since(start).Microseconds()))
	}()

	invocation, err := w.lookupInvocation(ctx, ij)
	if err != nil {
		return err
	}

	// Don't call webhooks for disconnected invocations.
	if invocation.GetInvocationStatus() == inpb.Invocation_DISCONNECTED_INVOCATION_STATUS {
		return nil
	}

	for _, hook := range w.env.GetWebhooks() {
		w.tasks <- &notifyWebhookTask{
			hook:          hook,
			invocationJWT: ij,
			invocation:    invocation,
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

func (w *webhookNotifier) lookupInvocation(ctx context.Context, ij *invocationJWT) (*inpb.Invocation, error) {
	if auth := w.env.GetAuthenticator(); auth != nil {
		ctx = auth.AuthContextFromTrustedJWT(ctx, ij.jwt)
	}
	return LookupInvocation(w.env, ctx, ij.id)
}

func isFinalEvent(obe *pepb.OrderedBuildEvent) bool {
	switch obe.Event.Event.(type) {
	case *bepb.BuildEvent_ComponentStreamFinished:
		return true
	}
	return false
}

func (e *EventChannel) isFirstStartedEvent(bazelBuildEvent *build_event_stream.BuildEvent) bool {
	if e.hasReceivedStartedEvent {
		return false
	}
	_, ok := bazelBuildEvent.Payload.(*build_event_stream.BuildEvent_Started)
	return ok
}

func (e *EventChannel) isFirstEventWithOptions(bazelBuildEvent *build_event_stream.BuildEvent) bool {
	switch p := bazelBuildEvent.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		return p.Started.OptionsDescription != "" && !e.hasReceivedEventWithOptions
	case *build_event_stream.BuildEvent_OptionsParsed:
		return !e.hasReceivedEventWithOptions
	}
	return false
}

func isWorkspaceStatusEvent(bazelBuildEvent *build_event_stream.BuildEvent) bool {
	switch bazelBuildEvent.Payload.(type) {
	case *build_event_stream.BuildEvent_WorkspaceStatus:
		return true
	}
	return false
}

func readBazelEvent(obe *pepb.OrderedBuildEvent, out *build_event_stream.BuildEvent) error {
	switch buildEvent := obe.Event.Event.(type) {
	case *bepb.BuildEvent_BazelEvent:
		return buildEvent.BazelEvent.UnmarshalTo(out)
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
	apiTargetMap   api_common.TargetMap

	startedEvent                     *build_event_stream.BuildEvent_Started
	bufferedEvents                   []*inpb.InvocationEvent
	unprocessedStartingEvents        map[string]struct{}
	numDroppedEventsBeforeProcessing uint64
	hasReceivedEventWithOptions      bool
	hasReceivedStartedEvent          bool
	logWriter                        *eventlog.EventLogWriter
	onClose                          func()
	attempt                          uint64

	// isVoid determines whether all EventChannel operations are NOPs. This is set
	// when we're retrying an invocation that is already complete, or is
	// incomplete but was created too far in the past.
	isVoid bool
}

func (e *EventChannel) fillInvocationFromEvents(ctx context.Context, streamID string, invocation *inpb.Invocation) error {
	pr := protofile.NewBufferedProtoReader(e.env.GetBlobstore(), streamID)
	var screenWriter *terminal.ScreenWriter
	if !invocation.HasChunkedEventLogs {
		screenWriter = terminal.NewScreenWriter()
	}
	parser := event_parser.NewStreamingEventParser(screenWriter)
	parser.FillInvocation(invocation)
	for {
		event := &inpb.InvocationEvent{}
		err := pr.ReadProto(ctx, event)
		if err == nil {
			parser.ParseEvent(event)
		} else if err == io.EOF {
			break
		} else {
			log.Warningf("Error reading proto from log: %s", err)
			return err
		}
	}
	parser.FillInvocation(invocation)
	return nil
}

func (e *EventChannel) writeCompletedBlob(ctx context.Context, blobID string, invocation *inpb.Invocation) error {
	protoBytes, err := proto.Marshal(invocation)
	if err != nil {
		return err
	}
	_, err = e.env.GetBlobstore().WriteBlob(ctx, blobID, protoBytes)
	return err
}

func (e *EventChannel) Close() {
	e.onClose()
}

func (e *EventChannel) FinalizeInvocation(iid string) error {
	if e.isVoid {
		return nil
	}

	ctx, cancel := background.ExtendContextForFinalization(e.ctx, 10*time.Second)
	defer cancel()

	invocationStatus := inpb.Invocation_DISCONNECTED_INVOCATION_STATUS
	if e.beValues.BuildFinished() {
		invocationStatus = inpb.Invocation_COMPLETE_INVOCATION_STATUS
	}

	invocation := &inpb.Invocation{
		InvocationId:        iid,
		InvocationStatus:    invocationStatus,
		Attempt:             e.attempt,
		HasChunkedEventLogs: e.logWriter != nil,
		BazelExitCode:       e.beValues.BuildExitCode(),
	}

	if e.pw != nil {
		if err := e.pw.Flush(ctx); err != nil {
			return err
		}
	}

	err := e.fillInvocationFromEvents(
		ctx,
		GetStreamIdFromInvocationIdAndAttempt(iid, e.attempt),
		invocation,
	)
	if err != nil {
		return err
	}
	if e.logWriter != nil {
		if err := e.logWriter.Close(ctx); err != nil {
			return err
		}
		invocation.LastChunkId = e.logWriter.GetLastChunkId(ctx)
	}

	ti, err := tableInvocationFromProto(invocation, iid)
	if err != nil {
		return err
	}
	recordInvocationMetrics(ti)
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
	if invocationStatus == inpb.Invocation_DISCONNECTED_INVOCATION_STATUS {
		log.Warningf("Reporting disconnected status for invocation %s.", iid)
		e.statusReporter.ReportDisconnect(ctx)
	}

	e.statsRecorder.Enqueue(ctx, invocation)
	return nil
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
}

func invocationStatusLabel(ti *tables.Invocation) string {
	if ti.InvocationStatus == int64(inpb.Invocation_COMPLETE_INVOCATION_STATUS) {
		if ti.Success {
			return "success"
		}
		return "failure"
	}
	if ti.InvocationStatus == int64(inpb.Invocation_DISCONNECTED_INVOCATION_STATUS) {
		return "disconnected"
	}
	return "unknown"
}

func recordInvocationMetrics(ti *tables.Invocation) {
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
}

func md5Int64(text string) int64 {
	hash := md5.Sum([]byte(text))
	return int64(binary.BigEndian.Uint64(hash[:8]))
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

	seqNo := event.OrderedBuildEvent.SequenceNumber
	streamID := event.OrderedBuildEvent.StreamId
	iid := streamID.InvocationId

	if isFinalEvent(event.OrderedBuildEvent) {
		return nil
	}

	var bazelBuildEvent build_event_stream.BuildEvent
	if err := readBazelEvent(event.OrderedBuildEvent, &bazelBuildEvent); err != nil {
		log.Warningf("error reading bazel event: %s", err)
		return err
	}

	invocationEvent := &inpb.InvocationEvent{
		EventTime:      event.OrderedBuildEvent.Event.EventTime,
		BuildEvent:     &bazelBuildEvent,
		SequenceNumber: event.OrderedBuildEvent.SequenceNumber,
	}

	// Bazel sends an Interrupted exit code in the finished event if the user cancelled the build.
	// Use that signal to cancel any actions that are currently in the remote execution system.
	if f, ok := bazelBuildEvent.Payload.(*build_event_stream.BuildEvent_Finished); ok {
		if f.Finished.GetExitCode().GetCode() == InterruptedExitCode && e.env.GetRemoteExecutionService() != nil {
			if err := e.env.GetRemoteExecutionService().Cancel(e.ctx, iid); err != nil {
				log.Warningf("Could not cancel executions for invocation %q: %s", iid, err)
			}
		}
	}
	if seqNo == 1 {
		log.Debugf("First event! sequence: %d invocation_id: %s, project_id: %s, notification_keywords: %s", seqNo, iid, event.ProjectId, event.NotificationKeywords)
	}

	if e.isFirstStartedEvent(&bazelBuildEvent) {
		e.hasReceivedStartedEvent = true
		e.unprocessedStartingEvents[bazelBuildEvent.Id.String()] = struct{}{}
		for _, child := range bazelBuildEvent.Children {
			switch child.Id.(type) {
			case *build_event_stream.BuildEventId_OptionsParsed:
				e.unprocessedStartingEvents[child.String()] = struct{}{}
			case *build_event_stream.BuildEventId_WorkspaceStatus:
				e.unprocessedStartingEvents[child.String()] = struct{}{}
			case *build_event_stream.BuildEventId_BuildMetadata:
				e.unprocessedStartingEvents[child.String()] = struct{}{}
			case *build_event_stream.BuildEventId_StructuredCommandLine:
				e.unprocessedStartingEvents[child.String()] = struct{}{}
			case *build_event_stream.BuildEventId_UnstructuredCommandLine:
				e.unprocessedStartingEvents[child.String()] = struct{}{}
			}
		}
	}
	// If this is the first event with options, keep track of the project ID and save any notification keywords.
	if e.isFirstEventWithOptions(&bazelBuildEvent) {
		e.hasReceivedEventWithOptions = true
		log.Debugf("Received options! sequence: %d invocation_id: %s", seqNo, iid)

		if auth := e.env.GetAuthenticator(); auth != nil {
			options, err := extractOptions(&bazelBuildEvent)
			if err != nil {
				return err
			}
			apiKey, err := auth.ParseAPIKeyFromString(options)
			if err != nil {
				return err
			}
			if apiKey != "" {
				e.ctx = auth.AuthContextFromAPIKey(e.ctx, apiKey)
				authError := e.ctx.Value(interfaces.AuthContextUserErrorKey)
				if authError != nil {
					if err, ok := authError.(error); ok {
						return err
					}
					return status.UnknownError(fmt.Sprintf("%v", authError))
				}
			}
		}

		invocationUUID, err := uuid.StringToBytes(iid)
		if err != nil {
			return err
		}
		ti := &tables.Invocation{
			InvocationID:     iid,
			InvocationUUID:   invocationUUID,
			InvocationStatus: int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS),
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
			log.Warningf("Voiding EventChannel for invocation %s: invocation already exists and is either completed or was last updated over 4 hours ago, so may not be retried.", iid)
			e.isVoid = true
			return nil
		}
		e.attempt = ti.Attempt
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
			numLinesToRetain := getNumActionsFromOptions(&bazelBuildEvent)
			if numLinesToRetain != 0 {
				// the number of lines curses can overwrite is 3 + the ui_actions shown:
				// 1 for the progress tracker, 1 for each action, and 2 blank lines.
				// 0 indicates that curses is not being used.
				numLinesToRetain += 3
			}
			e.logWriter = eventlog.NewEventLogWriter(
				e.ctx,
				e.env.GetBlobstore(),
				e.env.GetKeyValStore(),
				eventlog.GetEventLogPathFromInvocationIdAndAttempt(iid, e.attempt),
				numLinesToRetain,
			)
		}
		// Since this is the first event with options and we just parsed the API key,
		// now is a good time to record invocation usage for the group. Check that
		// this is the first attempt of this invocation, to guarantee that we
		// don't increment the usage on invocation retries.
		if ut := e.env.GetUsageTracker(); ut != nil && ti.Attempt == 1 {
			if err := ut.Increment(e.ctx, &tables.UsageCounts{Invocations: 1}); err != nil {
				log.Warningf("Failed to record invocation usage: %s", err)
			}
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

func (e *EventChannel) processSingleEvent(event *inpb.InvocationEvent, iid string) error {
	if err := e.redactor.RedactAPIKey(e.ctx, event.BuildEvent); err != nil {
		return err
	}
	e.redactor.RedactMetadata(event.BuildEvent)
	e.beValues.AddEvent(event.BuildEvent) // in-memory structure to hold common values we want from the event.

	switch p := event.BuildEvent.Payload.(type) {
	case *build_event_stream.BuildEvent_Progress:
		if e.logWriter != nil {
			e.logWriter.Write(e.ctx, []byte(p.Progress.Stderr))
			e.logWriter.Write(e.ctx, []byte(p.Progress.Stdout))
			// Don't store the log in the protostream if we're
			// writing it separately to blobstore
			p.Progress.Stderr = ""
			p.Progress.Stdout = ""
		}
	}

	e.targetTracker.TrackTargetsForEvent(e.ctx, event.BuildEvent)
	e.statusReporter.ReportStatusForEvent(e.ctx, event.BuildEvent)

	if err := e.collectAPIFacets(iid, event.BuildEvent); err != nil {
		log.Warningf("Error collecting API facets: %s", err)
	}

	// For everything else, just save the event to our buffer and keep on chugging.
	if e.pw != nil {
		if err := e.pw.WriteProtoToStream(e.ctx, event); err != nil {
			return err
		}

		// Small optimization: Flush the event stream after the workspace status event. Most of the
		// command line options and workspace info has come through by then, so we have
		// something to show the user. Flushing the proto file here allows that when the
		// client fetches status for the incomplete build. Also flush if we haven't in over a minute.
		if isWorkspaceStatusEvent(event.BuildEvent) || e.pw.TimeSinceLastWrite().Minutes() > 1 {
			if err := e.pw.Flush(e.ctx); err != nil {
				return err
			}
		}
	}
	if *requireInvocationEventParseOnRead {
		if isWorkspaceStatusEvent(event.BuildEvent) {
			if err := e.writeBuildMetadata(e.ctx, iid); err != nil {
				return err
			}
		}
	} else if len(e.unprocessedStartingEvents) > 0 {
		if _, ok := e.unprocessedStartingEvents[event.BuildEvent.Id.String()]; ok {
			delete(e.unprocessedStartingEvents, event.BuildEvent.Id.String())
			if len(e.unprocessedStartingEvents) == 0 {
				// When we have processed all starting events, update the invocation in
				// the DB so that it can be searched by its commit SHA, user name, etc.
				// even while the invocation is still in progress.
				if err := e.writeBuildMetadata(e.ctx, iid); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

const apiFacetsExpiration = 1 * time.Hour

func (e *EventChannel) flushAPIFacets(iid string) error {
	auth := e.env.GetAuthenticator()
	if e.collector == nil || !api_config.CacheEnabled() || auth == nil {
		return nil
	}

	userInfo, err := auth.AuthenticatedUser(e.ctx)
	if userInfo == nil || err != nil {
		return nil
	}

	for label, target := range e.apiTargetMap {
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
	auth := e.env.GetAuthenticator()
	if e.collector == nil || !api_config.CacheEnabled() || auth == nil {
		return nil
	}

	userInfo, err := auth.AuthenticatedUser(e.ctx)
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
	ti := &tables.Invocation{
		InvocationID: invocationID,
	}
	invocationProto := TableInvocationToProto(ti)
	err := e.fillInvocationFromEvents(
		ctx,
		GetStreamIdFromInvocationIdAndAttempt(invocationID, e.attempt),
		invocationProto,
	)
	if err != nil {
		return err
	}
	if e.logWriter != nil {
		invocationProto.LastChunkId = e.logWriter.GetLastChunkId(ctx)
	}
	ti, err = tableInvocationFromProto(invocationProto, ti.BlobID)
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
	return nil
}

func (e *EventChannel) GetNumDroppedEvents() uint64 {
	return e.numDroppedEventsBeforeProcessing
}

func extractOptions(event *build_event_stream.BuildEvent) (string, error) {
	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		return p.Started.OptionsDescription, nil
	case *build_event_stream.BuildEvent_OptionsParsed:
		return strings.Join(p.OptionsParsed.CmdLine, " "), nil
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
		if strings.HasPrefix(option, flag+"=") {
			values = append(values, strings.TrimPrefix(option, flag+"="))
		}
	}
	return values
}

func LookupInvocation(env environment.Env, ctx context.Context, iid string) (*inpb.Invocation, error) {
	ti, err := env.GetInvocationDB().LookupInvocation(ctx, iid)
	if err != nil {
		return nil, err
	}

	// If this is an incomplete invocation, attempt to fill cache stats
	// from counters rather than trying to read them from invocation b/c
	// they won't be set yet.
	if ti.InvocationStatus == int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS) {
		if cacheStats := hit_tracker.CollectCacheStats(ctx, env, iid); cacheStats != nil {
			fillInvocationFromCacheStats(cacheStats, ti)
		}
	}

	invocationMu := sync.Mutex{}
	invocation := TableInvocationToProto(ti)
	streamID := GetStreamIdFromInvocationIdAndAttempt(iid, ti.Attempt)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var scoreCard *capb.ScoreCard
		// When detailed stats are enabled, the scorecard is not inlined in the
		// invocation.
		if !hit_tracker.DetailedStatsEnabled() {
			// The cache ScoreCard is not stored in the table invocation, so we do this lookup
			// after converting the table invocation to a proto invocation.
			if ti.InvocationStatus == int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS) {
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
		if scoreCard != nil {
			invocationMu.Lock()
			invocation.ScoreCard = scoreCard
			invocationMu.Unlock()
		}
		return nil
	})

	eg.Go(func() error {
		var screenWriter *terminal.ScreenWriter
		if !invocation.HasChunkedEventLogs {
			screenWriter = terminal.NewScreenWriter()
		}
		var redactor *redact.StreamingRedactor
		if ti.RedactionFlags&redact.RedactionFlagStandardRedactions != redact.RedactionFlagStandardRedactions {
			// only redact if we hadn't redacted enough, only parse again if we redact
			redactor = redact.NewStreamingRedactor(env)
		}
		var parser *event_parser.StreamingEventParser
		if *requireInvocationEventParseOnRead || redactor != nil {
			parser = event_parser.NewStreamingEventParser(screenWriter)
		}
		events := []*inpb.InvocationEvent{}
		structuredCommandLines := []*command_line.CommandLine{}
		pr := protofile.NewBufferedProtoReader(env.GetBlobstore(), streamID)
		for {
			event := &inpb.InvocationEvent{}
			err := pr.ReadProto(ctx, event)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Warningf("Error reading proto from log: %s", err)
				return err
			}
			if parser != nil {
				if redactor != nil {
					if err := redactor.RedactAPIKeysWithSlowRegexp(ctx, event.BuildEvent); err != nil {
						return err
					}
					redactor.RedactMetadata(event.BuildEvent)
				}
				parser.ParseEvent(event)
			} else {
				events = append(events, event)
				switch p := event.BuildEvent.Payload.(type) {
				case *build_event_stream.BuildEvent_Progress:
					if screenWriter != nil {
						screenWriter.Write([]byte(p.Progress.Stderr))
						screenWriter.Write([]byte(p.Progress.Stdout))
					}
					// Now that we've updated our screenwriter, zero out
					// progress output in the event so they don't eat up
					// memory.
					p.Progress.Stderr = ""
					p.Progress.Stdout = ""
				case *build_event_stream.BuildEvent_StructuredCommandLine:
					structuredCommandLines = append(structuredCommandLines, p.StructuredCommandLine)
				}
			}
		}
		invocationMu.Lock()
		if parser != nil {
			parser.FillInvocation(invocation)
		} else {
			invocation.Event = events
			invocation.StructuredCommandLine = structuredCommandLines
			if screenWriter != nil {
				invocation.ConsoleBuffer = string(screenWriter.RenderAsANSI())
			}
		}
		invocationMu.Unlock()
		return nil
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return invocation, nil
}

func tableInvocationFromProto(p *inpb.Invocation, blobID string) (*tables.Invocation, error) {
	uuid, err := uuid.StringToBytes(p.InvocationId)
	if err != nil {
		return nil, err
	}

	i := &tables.Invocation{}
	i.InvocationID = p.InvocationId // Required.
	i.InvocationUUID = uuid
	i.Success = p.Success
	i.User = p.User
	i.DurationUsec = p.DurationUsec
	i.Host = p.Host
	i.RepoURL = p.RepoUrl
	if norm, err := gitutil.NormalizeRepoURL(p.RepoUrl); err == nil {
		i.RepoURL = norm.String()
	}
	i.BranchName = p.BranchName
	i.CommitSHA = p.CommitSha
	i.Role = p.Role
	i.Command = p.Command
	if p.Pattern != nil {
		i.Pattern = invocation_format.ShortFormatPatterns(p.Pattern)
	}
	i.ActionCount = p.ActionCount
	i.BlobID = blobID
	i.InvocationStatus = int64(p.InvocationStatus)
	if p.ReadPermission == inpb.InvocationPermission_PUBLIC {
		i.Perms = perms.OTHERS_READ
	}
	i.LastChunkId = p.LastChunkId
	i.RedactionFlags = redact.RedactionFlagStandardRedactions
	i.Attempt = p.Attempt
	i.BazelExitCode = p.BazelExitCode
	return i, nil
}

func TableInvocationToProto(i *tables.Invocation) *inpb.Invocation {
	out := &inpb.Invocation{}
	out.InvocationId = i.InvocationID // Required.
	out.Success = i.Success
	out.User = i.User
	out.DurationUsec = i.DurationUsec
	out.Host = i.Host
	out.RepoUrl = i.RepoURL
	out.BranchName = i.BranchName
	out.CommitSha = i.CommitSHA
	out.Role = i.Role
	out.Command = i.Command
	if i.Pattern != "" {
		out.Pattern = strings.Split(i.Pattern, ", ")
	}
	out.ActionCount = i.ActionCount
	// BlobID is not present in output client proto.
	out.InvocationStatus = inpb.Invocation_InvocationStatus(i.InvocationStatus)
	out.CreatedAtUsec = i.Model.CreatedAtUsec
	out.UpdatedAtUsec = i.Model.UpdatedAtUsec
	if i.Perms&perms.OTHERS_READ > 0 {
		out.ReadPermission = inpb.InvocationPermission_PUBLIC
	} else {
		out.ReadPermission = inpb.InvocationPermission_GROUP
	}
	out.CreatedWithCapabilities = capabilities.FromInt(i.CreatedWithCapabilities)
	out.Acl = perms.ToACLProto(&uidpb.UserId{Id: i.UserID}, i.GroupID, i.Perms)
	out.CacheStats = &capb.CacheStats{
		ActionCacheHits:                   i.ActionCacheHits,
		ActionCacheMisses:                 i.ActionCacheMisses,
		ActionCacheUploads:                i.ActionCacheUploads,
		CasCacheHits:                      i.CasCacheHits,
		CasCacheMisses:                    i.CasCacheMisses,
		CasCacheUploads:                   i.CasCacheUploads,
		TotalDownloadSizeBytes:            i.TotalDownloadSizeBytes,
		TotalDownloadTransferredSizeBytes: i.TotalDownloadTransferredSizeBytes,
		TotalUploadSizeBytes:              i.TotalUploadSizeBytes,
		TotalUploadTransferredSizeBytes:   i.TotalUploadTransferredSizeBytes,
		TotalDownloadUsec:                 i.TotalDownloadUsec,
		TotalUploadUsec:                   i.TotalUploadUsec,
		TotalCachedActionExecUsec:         i.TotalCachedActionExecUsec,
		DownloadThroughputBytesPerSecond:  i.DownloadThroughputBytesPerSecond,
		UploadThroughputBytesPerSecond:    i.UploadThroughputBytesPerSecond,
	}
	out.LastChunkId = i.LastChunkId
	if i.LastChunkId != "" {
		out.HasChunkedEventLogs = true
	}
	out.Attempt = i.Attempt
	out.BazelExitCode = i.BazelExitCode
	return out
}

func GetStreamIdFromInvocationIdAndAttempt(iid string, attempt uint64) string {
	if attempt == 0 {
		// This invocation predates the attempt-tracking functionality, so its
		// streamId does not contain the attempt number.
		return iid
	}
	return iid + "/" + strconv.FormatUint(attempt, 10)
}
