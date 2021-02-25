package build_event_handler

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_status_reporter"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/target_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/protofile"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/prometheus/client_golang/prometheus"

	gstatus "google.golang.org/grpc/status"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

const (
	defaultChunkFileSizeBytes = 1000 * 100 // 100KB
)

type BuildEventHandler struct {
	env environment.Env
}

func NewBuildEventHandler(env environment.Env) *BuildEventHandler {
	return &BuildEventHandler{
		env: env,
	}
}

func (b *BuildEventHandler) OpenChannel(ctx context.Context, iid string) interfaces.BuildEventChannel {
	chunkFileSizeBytes := b.env.GetConfigurator().GetStorageChunkFileSizeBytes()
	if chunkFileSizeBytes == 0 {
		chunkFileSizeBytes = defaultChunkFileSizeBytes
	}
	buildEventAccumulator := accumulator.NewBEValues(iid)
	return &EventChannel{
		env:            b.env,
		ctx:            ctx,
		pw:             protofile.NewBufferedProtoWriter(b.env.GetBlobstore(), iid, chunkFileSizeBytes),
		beValues:       buildEventAccumulator,
		statusReporter: build_status_reporter.NewBuildStatusReporter(b.env, buildEventAccumulator),
		targetTracker:  target_tracker.NewTargetTracker(b.env, buildEventAccumulator),
	}
}

func isFinalEvent(obe *pepb.OrderedBuildEvent) bool {
	switch obe.Event.Event.(type) {
	case *bepb.BuildEvent_ComponentStreamFinished:
		return true
	}
	return false
}

func isWorkspaceStatusEvent(bazelBuildEvent build_event_stream.BuildEvent) bool {
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
	ctx            context.Context
	env            environment.Env
	pw             *protofile.BufferedProtoWriter
	beValues       *accumulator.BEValues
	statusReporter *build_status_reporter.BuildStatusReporter
	targetTracker  *target_tracker.TargetTracker
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
			log.Printf("returning some other error: %s", err)
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

func (e *EventChannel) MarkInvocationDisconnected(ctx context.Context, iid string) error {
	e.statusReporter.ReportDisconnect(ctx)

	if err := e.pw.Flush(ctx); err != nil {
		return err
	}
	invocation := &inpb.Invocation{
		InvocationId:     iid,
		InvocationStatus: inpb.Invocation_DISCONNECTED_INVOCATION_STATUS,
	}

	err := e.fillInvocationFromEvents(ctx, iid, invocation)
	if err != nil {
		return err
	}

	ti := tableInvocationFromProto(invocation, iid)
	return e.env.GetInvocationDB().InsertOrUpdateInvocation(ctx, ti)
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

func (e *EventChannel) FinalizeInvocation(iid string) error {
	if err := e.pw.Flush(e.ctx); err != nil {
		return err
	}
	invocation := &inpb.Invocation{
		InvocationId:     iid,
		InvocationStatus: inpb.Invocation_COMPLETE_INVOCATION_STATUS,
	}
	err := e.fillInvocationFromEvents(e.ctx, iid, invocation)
	if err != nil {
		return err
	}

	ti := tableInvocationFromProto(invocation, iid)
	if cacheStats := hit_tracker.CollectCacheStats(e.ctx, e.env, iid); cacheStats != nil {
		fillInvocationFromCacheStats(cacheStats, ti)
	}
	recordInvocationMetrics(ti)
	if err := e.env.GetInvocationDB().InsertOrUpdateInvocation(e.ctx, ti); err != nil {
		return err
	}

	// Notify our webhooks, if we have any.
	for _, hook := range e.env.GetWebhooks() {
		go func() {
			// We use context background here because the request context will
			// be closed soon and we don't want to block while calling webhooks.
			if err := hook.NotifyComplete(context.Background(), invocation); err != nil {
				log.Printf("Error calling webhook: %s", err)
			}
		}()
	}
	if searcher := e.env.GetInvocationSearchService(); searcher != nil {
		go func() {
			if err := searcher.IndexInvocation(context.Background(), invocation); err != nil {
				log.Printf("Error indexing invocation: %s", err)
			}
		}()
	}
	return nil
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
	seqNo := event.OrderedBuildEvent.SequenceNumber
	streamID := event.OrderedBuildEvent.StreamId
	iid := streamID.InvocationId

	if isFinalEvent(event.OrderedBuildEvent) {
		return nil
	}

	var bazelBuildEvent build_event_stream.BuildEvent
	if err := readBazelEvent(event.OrderedBuildEvent, &bazelBuildEvent); err != nil {
		log.Printf("error reading bazel event: %s", err)
		return err
	}

	e.beValues.AddEvent(&bazelBuildEvent) // in-memory structure to hold common values we want from the event.

	// If this is the first event, keep track of the project ID and save any notification keywords.
	if seqNo == 1 {
		log.Printf("First event! invocation_id: %s, project_id: %s, notification_keywords: %s", iid, event.ProjectId, event.NotificationKeywords)
		ti := &tables.Invocation{
			InvocationID:     iid,
			InvocationPK:     md5Int64(iid),
			InvocationStatus: int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS),
		}

		if auth := e.env.GetAuthenticator(); auth != nil {
			options, err := extractOptionsFromStartedBuildEvent(bazelBuildEvent)
			if err != nil {
				return err
			}
			if apiKey := auth.ParseAPIKeyFromString(options); apiKey != "" {
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

		if err := e.env.GetInvocationDB().InsertOrUpdateInvocation(e.ctx, ti); err != nil {
			return err
		}
	}

	if e.env.GetConfigurator().EnableTargetTracking() {
		e.targetTracker.TrackTargetsForEvent(e.ctx, &bazelBuildEvent)
	}
	e.statusReporter.ReportStatusForEvent(e.ctx, &bazelBuildEvent)

	// For everything else, just save the event to our buffer and keep on chugging.
	err := e.pw.WriteProtoToStream(e.ctx, &inpb.InvocationEvent{
		EventTime:      event.OrderedBuildEvent.Event.EventTime,
		BuildEvent:     &bazelBuildEvent,
		SequenceNumber: event.OrderedBuildEvent.SequenceNumber,
	})
	if err != nil {
		return err
	}

	// Small optimization: Flush the event stream after the workspace status event. Most of the
	// command line options and workspace info has come through by then, so we have
	// something to show the user. Flushing the proto file here allows that when the
	// client fetches status for the incomplete build. Also flush if we haven't in over a minute.
	if isWorkspaceStatusEvent(bazelBuildEvent) || e.pw.TimeSinceLastWrite().Minutes() > 1 {
		if err := e.pw.Flush(e.ctx); err != nil {
			return err
		}
	}
	// When we get the workspace status event, update the invocation in the DB
	// so that it can be searched by its commit SHA, user name, etc. even
	// while the invocation is still in progress.
	if isWorkspaceStatusEvent(bazelBuildEvent) {
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
	ti = tableInvocationFromProto(invocationProto, ti.BlobID)
	if err := db.InsertOrUpdateInvocation(ctx, ti); err != nil {
		return err
	}
	return nil
}

func extractOptionsFromStartedBuildEvent(event build_event_stream.BuildEvent) (string, error) {
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

	invocation := TableInvocationToProto(ti)

	parser := event_parser.NewStreamingEventParser()
	pr := protofile.NewBufferedProtoReader(env.GetBlobstore(), iid)
	for {
		event := &inpb.InvocationEvent{}
		err := pr.ReadProto(ctx, event)
		if err == nil {
			parser.ParseEvent(event)
		} else if err == io.EOF {
			break
		} else {
			log.Printf("returning some other error: %s", err)
			return nil, err
		}
	}
	parser.FillInvocation(invocation)
	return invocation, nil
}

// TODO(siggisim): pull this out somewhere central
func truncatedJoin(list []string, maxItems int) string {
	length := len(list)
	if length > maxItems {
		return fmt.Sprintf("%s and %d more", strings.Join(list[0:maxItems], ", "), length-maxItems)
	}
	return strings.Join(list, ", ")
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
	i.CommitSHA = p.CommitSha
	i.Role = p.Role
	i.Command = p.Command
	if p.Pattern != nil {
		i.Pattern = truncatedJoin(p.Pattern, 3)
	}
	i.ActionCount = p.ActionCount
	i.BlobID = blobID
	i.InvocationStatus = int64(p.InvocationStatus)
	if p.ReadPermission == inpb.InvocationPermission_PUBLIC {
		i.Perms = perms.OTHERS_READ
	}
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
	return out
}
