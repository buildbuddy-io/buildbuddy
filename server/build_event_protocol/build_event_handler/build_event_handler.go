package build_event_handler

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
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
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/protofile"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	gstatus "google.golang.org/grpc/status"
)

const (
	defaultChunkFileSizeBytes = 1000 * 100 // 100KB

	// The time allowed for the cache hit trackers across all app instances to
	// flush their stats from their local buffer to the backing storage (Redis).
	// We wait this long before we write the invocation's cache stats to the DB.
	cacheStatsFinalizationDelay = 500 * time.Millisecond

	// How many workers to spin up for writing cache stats to the DB.
	numStatsRecorderWorkers = 8

	// How many workers to spin up for notifying webhooks.
	numWebhookNotifierWorkers = 16

	// How long to wait before giving up on webhook requests.
	webhookNotifyTimeout = 1 * time.Minute
)

var (
	enableOptimizedRedaction = flag.Bool("enable_optimized_redaction", false, "Enables more efficient API key redaction.")
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
	chunkFileSizeBytes := b.env.GetConfigurator().GetStorageChunkFileSizeBytes()
	if chunkFileSizeBytes == 0 {
		chunkFileSizeBytes = defaultChunkFileSizeBytes
	}
	buildEventAccumulator := accumulator.NewBEValues(iid)

	b.openChannels.Add(1)
	onClose := func() {
		b.openChannels.Done()
	}

	return &EventChannel{
		env:                     b.env,
		statsRecorder:           b.statsRecorder,
		ctx:                     ctx,
		pw:                      protofile.NewBufferedProtoWriter(b.env.GetBlobstore(), iid, chunkFileSizeBytes),
		beValues:                buildEventAccumulator,
		redactor:                redact.NewStreamingRedactor(b.env),
		statusReporter:          build_status_reporter.NewBuildStatusReporter(b.env, buildEventAccumulator),
		targetTracker:           target_tracker.NewTargetTracker(b.env, buildEventAccumulator),
		hasReceivedStartedEvent: false,
		eventsBeforeStarted:     make([]*inpb.InvocationEvent, 0),
		logWriter:               nil,
		onClose:                 onClose,
	}
}

// invocationJWT represents an invocation ID as well as the JWT granting access
// to it. It should only be used for background tasks that need access to the
// JWT after the build event stream is already closed.
type invocationJWT struct {
	id  string
	jwt string
}

// recordStatsTask contains the info needed to record the stats for an
// invocation. These tasks are enqueued to statsRecorder and executed in the
// background.
type recordStatsTask struct {
	*invocationJWT
	// createdAt is the time at which this task was created.
	createdAt time.Time
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
			id:  invocation.GetInvocationId(),
			jwt: jwt,
		},
		createdAt: time.Now(),
	}
	select {
	case r.tasks <- req:
		break
	default:
		log.Error("Failed to write cache stats: stats recorder task buffer is full")
	}
}

func scoreCardBlobName(invocationID string) string {
	blobFileName := invocationID + "-scorecard.pb"
	return filepath.Join(invocationID, blobFileName)
}

func writeScoreCard(ctx context.Context, env environment.Env, invocationID string, scoreCard *capb.ScoreCard) error {
	scoreCardBuf, err := proto.Marshal(scoreCard)
	if err != nil {
		return err
	}
	blobStore := env.GetBlobstore()
	_, err = blobStore.WriteBlob(ctx, scoreCardBlobName(invocationID), scoreCardBuf)
	return err
}

func readScoreCard(ctx context.Context, env environment.Env, invocationID string) (*capb.ScoreCard, error) {
	blobStore := env.GetBlobstore()
	buf, err := blobStore.ReadBlob(ctx, scoreCardBlobName(invocationID))
	if err != nil {
		return nil, err
	}
	sc := &capb.ScoreCard{}
	if err := proto.Unmarshal(buf, sc); err != nil {
		return nil, err
	}
	return sc, nil
}

func (r *statsRecorder) Start() {
	r.eg = errgroup.Group{}
	ctx := context.Background()
	for i := 0; i < numStatsRecorderWorkers; i++ {
		r.eg.Go(func() error {
			for task := range r.tasks {
				// Apply the finalization delay relative to when the invocation was marked
				// finalized, rather than relative to now. Otherwise each worker would be
				// unnecessarily throttled.
				time.Sleep(time.Until(task.createdAt.Add(cacheStatsFinalizationDelay)))
				ti := &tables.Invocation{InvocationID: task.invocationJWT.id}
				if stats := hit_tracker.CollectCacheStats(ctx, r.env, task.invocationJWT.id); stats != nil {
					fillInvocationFromCacheStats(stats, ti)
				}
				if scoreCard := hit_tracker.ScoreCard(ctx, r.env, task.invocationJWT.id); scoreCard != nil {
					if err := writeScoreCard(ctx, r.env, task.invocationJWT.id, scoreCard); err != nil {
						log.Errorf("Error writing scorecard blob: %s", err)
					}
				}

				if _, err := r.env.GetInvocationDB().InsertOrUpdateInvocation(ctx, ti); err != nil {
					log.Errorf("Failed to write cache stats for invocation: %s", err)
				}
				// Cleanup regardless of whether the stats are flushed successfully to
				// the DB (since we won't retry the flush and we don't need these stats
				// for any other purpose).
				hit_tracker.CleanupCacheStats(ctx, r.env, task.invocationJWT.id)

				// Once cache stats are populated, notify the onStatsRecorded channel in
				// a non-blocking fashion.
				select {
				case r.onStatsRecorded <- task.invocationJWT:
					break
				default:
					log.Warningf("Failed to notify stats recorder listeners: channel buffer is full")
				}
			}
			return nil
		})
	}
}

func (r *statsRecorder) Stop() {
	// Wait for all EventHandler channels to be closed to ensure there will be
	// no more calls to Enqueue.
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
	// doneSendingTasks receives a signal after the invocations channel is closed
	// and all buffered invocations in the channel have been processed, meaning
	// no more tasks will be sent on the tasks channel.
	doneSendingTasks chan struct{}

	tasks chan *notifyWebhookTask
	eg    errgroup.Group
}

func newWebhookNotifier(env environment.Env, invocations <-chan *invocationJWT) *webhookNotifier {
	return &webhookNotifier{
		env:              env,
		invocations:      invocations,
		doneSendingTasks: make(chan struct{}),
		tasks:            make(chan *notifyWebhookTask, 4096),
	}
}

func (w *webhookNotifier) Start() {
	w.eg = errgroup.Group{}
	ctx := context.Background()

	w.eg.Go(func() error {
		// Listen for invocations that have been finalized and start a notify
		// webhook task for each webhook.
		for ij := range w.invocations {
			invocation, err := w.lookupInvocation(ctx, ij)
			if err != nil {
				log.Warningf("Failed to lookup invocation before notifying webhook: %s", err)
				continue
			}

			// Don't call webhooks for disconnected invocations.
			if invocation.GetInvocationStatus() == inpb.Invocation_DISCONNECTED_INVOCATION_STATUS {
				continue
			}

			for _, hook := range w.env.GetWebhooks() {
				w.tasks <- &notifyWebhookTask{
					hook:          hook,
					invocationJWT: ij,
					invocation:    invocation,
				}
			}
		}
		w.doneSendingTasks <- struct{}{}
		return nil
	})

	for i := 0; i < numWebhookNotifierWorkers; i++ {
		w.eg.Go(func() error {
			for task := range w.tasks {
				if err := notifyWithTimeout(ctx, w.env, task); err != nil {
					log.Warningf("Failed to notify webhook for invocation %s: %s", task.invocation.GetInvocationId(), err)
				}
			}
			return nil
		})
	}
}

func (w *webhookNotifier) Stop() {
	// Make sure we are done sending tasks on the task channel before we close it.
	<-w.doneSendingTasks
	close(w.tasks)

	if err := w.eg.Wait(); err != nil {
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

func isStartedEvent(bazelBuildEvent *build_event_stream.BuildEvent) bool {
	switch bazelBuildEvent.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		return true
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
		return ptypes.UnmarshalAny(buildEvent.BazelEvent, out)
	}
	return fmt.Errorf("Not a bazel event %s", obe)
}

type EventChannel struct {
	ctx                     context.Context
	env                     environment.Env
	pw                      *protofile.BufferedProtoWriter
	beValues                *accumulator.BEValues
	redactor                *redact.StreamingRedactor
	statusReporter          *build_status_reporter.BuildStatusReporter
	targetTracker           *target_tracker.TargetTracker
	statsRecorder           *statsRecorder
	eventsBeforeStarted     []*inpb.InvocationEvent
	hasReceivedStartedEvent bool
	logWriter               *eventlog.EventLogWriter
	onClose                 func()
	// isVoid determines whether all EventChannel operations are NOPs. This is set
	// when we're retrying an invocation that is already complete, or is
	// incomplete but was created too far in the past.
	isVoid bool
}

func (e *EventChannel) fillInvocationFromEvents(ctx context.Context, iid string, invocation *inpb.Invocation) error {
	pr := protofile.NewBufferedProtoReader(e.env.GetBlobstore(), iid)
	parser := event_parser.NewStreamingEventParser()
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

	status := inpb.Invocation_DISCONNECTED_INVOCATION_STATUS
	if e.beValues.BuildFinished() {
		status = inpb.Invocation_COMPLETE_INVOCATION_STATUS
	}

	invocation := &inpb.Invocation{
		InvocationId:     iid,
		InvocationStatus: status,
	}

	if status == inpb.Invocation_DISCONNECTED_INVOCATION_STATUS {
		log.Warningf("Reporting disconnected status for invocation %s.", iid)
		e.statusReporter.ReportDisconnect(ctx)
	}

	if err := e.pw.Flush(ctx); err != nil {
		return err
	}

	err := e.fillInvocationFromEvents(ctx, iid, invocation)
	if err != nil {
		return err
	}
	if e.logWriter != nil {
		if err := e.logWriter.Close(ctx); err != nil {
			return err
		}
		invocation.LastChunkId = e.logWriter.GetLastChunkId(ctx)
	}

	ti := tableInvocationFromProto(invocation, iid)
	recordInvocationMetrics(ti)
	if _, err := e.env.GetInvocationDB().InsertOrUpdateInvocation(ctx, ti); err != nil {
		return err
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

	// If this is the first event, keep track of the project ID and save any notification keywords.
	if isStartedEvent(&bazelBuildEvent) {
		isFirstStartedEvent := !e.hasReceivedStartedEvent
		e.hasReceivedStartedEvent = true
		log.Debugf("Started event! sequence: %d invocation_id: %s, project_id: %s, notification_keywords: %s", seqNo, iid, event.ProjectId, event.NotificationKeywords)

		if auth := e.env.GetAuthenticator(); auth != nil {
			options, err := extractOptionsFromStartedBuildEvent(&bazelBuildEvent)
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

		if isFirstStartedEvent {
			inv, err := e.env.GetInvocationDB().LookupInvocation(e.ctx, iid)
			if err == nil {
				// We are retrying a previous invocation.
				if inv.InvocationStatus == int64(inpb.Invocation_COMPLETE_INVOCATION_STATUS) {
					// The invocation is neither disconnected nor in-progress, it is not
					// valid to retry.
					log.Warningf("Voiding EventChannel for invocation %s: invocation already exists and is completed.", iid)
					e.isVoid = true
					return nil
				} else if time.UnixMicro(inv.UpdatedAtUsec).Before(time.Now().Add(time.Hour * -4)) {
					// The invocation was last updated over 4 hours ago; it is not valid
					// to retry.
					log.Warningf("Voiding EventChannel for invocation %s: invocation already exists and was last updated over 4 hours ago, so may not be retried.", iid)
					e.isVoid = true
					return nil
				}
				if err := protofile.DeleteExistingChunks(e.ctx, e.env.GetBlobstore(), iid); err != nil {
					return status.WrapErrorf(err, "Failed to delete existing build event protos when retrying invocation %s", iid)
				}
				if err := chunkstore.New(e.env.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).DeleteBlob(e.ctx, eventlog.GetEventLogPathFromInvocationId(iid)); err != nil {
					return status.WrapErrorf(err, "Failed to delete existing event log when retrying invocation %s", iid)
				}
			} else if !db.IsRecordNotFound(err) {
				// RecordNotFound means this invocation has never existed, which is not
				// an error. All other errors are real errors.
				return err
			}
			if e.env.GetConfigurator().GetStorageEnableChunkedEventLogs() {
				e.logWriter = eventlog.NewEventLogWriter(e.ctx, e.env.GetBlobstore(), e.env.GetKeyValStore(), iid)
			}
		}

		ti := &tables.Invocation{
			InvocationID:     iid,
			InvocationPK:     md5Int64(iid),
			InvocationStatus: int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS),
			RedactionFlags:   redact.RedactionFlagStandardRedactions,
		}

		if e.logWriter != nil {
			ti.LastChunkId = e.logWriter.GetLastChunkId(e.ctx)
		}
		created, err := e.env.GetInvocationDB().InsertOrUpdateInvocation(e.ctx, ti)
		if err != nil {
			return err
		}

		// Since this is the Started event and we just parsed the API key, now is
		// a good time to record invocation usage for the group. Sanity check that
		// we just inserted a new row into the DB, to guarantee that we don't
		// increment the usage on invocation retries.
		if ut := e.env.GetUsageTracker(); ut != nil && created {
			if err := ut.Increment(e.ctx, &tables.UsageCounts{Invocations: 1}); err != nil {
				log.Warningf("Failed to record invocation usage: %s", err)
			}
		}

	} else if !e.hasReceivedStartedEvent {
		e.eventsBeforeStarted = append(e.eventsBeforeStarted, invocationEvent)
		if len(e.eventsBeforeStarted) > 10 {
			log.Warningf("We got over 10 build events before the started event for invocation %s, dropping %+v", iid, e.eventsBeforeStarted[0])
			e.eventsBeforeStarted = e.eventsBeforeStarted[1:]
		}
		return nil
	}

	// Process buffered events.
	for _, event := range e.eventsBeforeStarted {
		if err := e.processSingleEvent(event, iid); err != nil {
			return err
		}
	}
	e.eventsBeforeStarted = nil

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
			// For now, write logs to both chunks and the invocation proto
			// p.Progress.Stderr = ""
			// p.Progress.Stdout = ""
		}
	}

	if e.env.GetConfigurator().EnableTargetTracking() {
		e.targetTracker.TrackTargetsForEvent(e.ctx, event.BuildEvent)
	}
	e.statusReporter.ReportStatusForEvent(e.ctx, event.BuildEvent)

	// For everything else, just save the event to our buffer and keep on chugging.
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
	// When we get the workspace status event, update the invocation in the DB
	// so that it can be searched by its commit SHA, user name, etc. even
	// while the invocation is still in progress.
	if isWorkspaceStatusEvent(event.BuildEvent) {
		if err := e.writeBuildMetadata(e.ctx, iid); err != nil {
			return err
		}
	}

	return nil
}

func (e *EventChannel) writeBuildMetadata(ctx context.Context, invocationID string) error {
	db := e.env.GetInvocationDB()
	ti := &tables.Invocation{
		InvocationID: invocationID,
	}
	invocationProto := TableInvocationToProto(ti)
	err := e.fillInvocationFromEvents(ctx, invocationID, invocationProto)
	if err != nil {
		return err
	}
	if e.logWriter != nil {
		invocationProto.LastChunkId = e.logWriter.GetLastChunkId(ctx)
	}
	ti = tableInvocationFromProto(invocationProto, ti.BlobID)
	if _, err := db.InsertOrUpdateInvocation(ctx, ti); err != nil {
		return err
	}
	return nil
}

func extractOptionsFromStartedBuildEvent(event *build_event_stream.BuildEvent) (string, error) {
	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		return p.Started.OptionsDescription, nil
	}
	return "", nil
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

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var scoreCard *capb.ScoreCard
		// The cache ScoreCard is not stored in the table invocation, so we do this lookup
		// after converting the table invocation to a proto invocation.
		if ti.InvocationStatus == int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS) {
			scoreCard = hit_tracker.ScoreCard(ctx, env, iid)
		} else {
			if sc, err := readScoreCard(ctx, env, iid); err == nil {
				scoreCard = sc
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
		redactor := redact.NewStreamingRedactor(env)
		parser := event_parser.NewStreamingEventParser()
		pr := protofile.NewBufferedProtoReader(env.GetBlobstore(), iid)
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
			if ti.RedactionFlags&redact.RedactionFlagStandardRedactions == 0 {
				if err := redactor.RedactAPIKeysWithSlowRegexp(ctx, event.BuildEvent); err != nil {
					return err
				}
				redactor.RedactMetadata(event.BuildEvent)
			}
			parser.ParseEvent(event)
		}
		invocationMu.Lock()
		parser.FillInvocation(invocation)
		invocationMu.Unlock()
		return nil
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return invocation, nil
}

func tableInvocationFromProto(p *inpb.Invocation, blobID string) *tables.Invocation {
	i := &tables.Invocation{}
	i.InvocationID = p.InvocationId // Required.
	i.InvocationPK = md5Int64(p.InvocationId)
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
	return i
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
	out.Acl = perms.ToACLProto(&uidpb.UserId{Id: i.UserID}, i.GroupID, i.Perms)
	out.CacheStats = &capb.CacheStats{
		ActionCacheHits:                  i.ActionCacheHits,
		ActionCacheMisses:                i.ActionCacheMisses,
		ActionCacheUploads:               i.ActionCacheUploads,
		CasCacheHits:                     i.CasCacheHits,
		CasCacheMisses:                   i.CasCacheMisses,
		CasCacheUploads:                  i.CasCacheUploads,
		TotalDownloadSizeBytes:           i.TotalDownloadSizeBytes,
		TotalUploadSizeBytes:             i.TotalUploadSizeBytes,
		TotalDownloadUsec:                i.TotalDownloadUsec,
		TotalUploadUsec:                  i.TotalUploadUsec,
		TotalCachedActionExecUsec:        i.TotalCachedActionExecUsec,
		DownloadThroughputBytesPerSecond: i.DownloadThroughputBytesPerSecond,
		UploadThroughputBytesPerSecond:   i.UploadThroughputBytesPerSecond,
	}
	out.LastChunkId = i.LastChunkId
	if i.LastChunkId != "" {
		out.HasChunkedEventLogs = true
	}
	return out
}
