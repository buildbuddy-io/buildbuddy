package build_event_handler

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_status_reporter"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/protofile"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"

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
	env            environment.Env
	pw             *protofile.BufferedProtoWriter
	statusReporter *build_status_reporter.BuildStatusReporter
}

func (e *EventChannel) readAllTempBlobs(ctx context.Context, blobID string) ([]*inpb.InvocationEvent, error) {
	events := make([]*inpb.InvocationEvent, 0)
	pr := protofile.NewBufferedProtoReader(e.env.GetBlobstore(), blobID)
	for {
		event := &inpb.InvocationEvent{}
		err := pr.ReadProto(ctx, event)
		if err == nil {
			events = append(events, event)
		} else if err == io.EOF {
			break
		} else {
			log.Printf("returning some other error: %s", err)
			return nil, err
		}
	}
	return events, nil
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

	events, err := e.readAllTempBlobs(ctx, iid)
	if err != nil {
		return err
	}
	event_parser.FillInvocationFromEvents(events, invocation)

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
	ti.TotalCachedActionExecUsec = cacheStats.GetTotalCachedActionExecUsec()
}

func (e *EventChannel) FinalizeInvocation(ctx context.Context, iid string) error {
	if err := e.pw.Flush(ctx); err != nil {
		return err
	}
	invocation := &inpb.Invocation{
		InvocationId:     iid,
		InvocationStatus: inpb.Invocation_COMPLETE_INVOCATION_STATUS,
	}
	events, err := e.readAllTempBlobs(ctx, iid)
	if err != nil {
		return err
	}
	event_parser.FillInvocationFromEvents(events, invocation)

	ti := tableInvocationFromProto(invocation, iid)
	if cacheStats := hit_tracker.CollectCacheStats(ctx, e.env, iid); cacheStats != nil {
		fillInvocationFromCacheStats(cacheStats, ti)
	}
	if err := e.env.GetInvocationDB().InsertOrUpdateInvocation(ctx, ti); err != nil {
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

func (e *EventChannel) HandleEvent(ctx context.Context, event *pepb.PublishBuildToolEventStreamRequest) error {
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

	// If this is the first event, keep track of the project ID and save any notification keywords.
	if seqNo == 1 {
		log.Printf("First event! project_id: %s, notification_keywords: %s", event.ProjectId, event.NotificationKeywords)
		ti := &tables.Invocation{
			InvocationID:     iid,
			InvocationStatus: int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS),
		}

		if auth := e.env.GetAuthenticator(); auth != nil {
			options, err := extractOptionsFromBuildEvent(bazelBuildEvent)
			if err != nil {
				return err
			}
			if apiKey := auth.ParseAPIKeyFromString(options); apiKey != "" {
				ctx = auth.AuthContextFromAPIKey(ctx, apiKey)
			}
		}

		if err := e.env.GetInvocationDB().InsertOrUpdateInvocation(ctx, ti); err != nil {
			return err
		}
	}

	e.statusReporter.ReportStatusForEvent(ctx, &bazelBuildEvent)

	// For everything else, just save the event to our buffer and keep on chugging.
	err := e.pw.WriteProtoToStream(ctx, &inpb.InvocationEvent{
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
		return e.pw.Flush(ctx)
	}
	return nil
}

func extractOptionsFromBuildEvent(event build_event_stream.BuildEvent) (string, error) {
	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		return p.Started.OptionsDescription, nil
	}
	return "", fmt.Errorf("First build event was not a Started event, it was %+v", event.Payload)
}

func OpenChannel(env environment.Env, ctx context.Context, iid string) *EventChannel {
	chunkFileSizeBytes := env.GetConfigurator().GetStorageChunkFileSizeBytes()
	if chunkFileSizeBytes == 0 {
		chunkFileSizeBytes = defaultChunkFileSizeBytes
	}
	return &EventChannel{
		env:            env,
		pw:             protofile.NewBufferedProtoWriter(env.GetBlobstore(), iid, chunkFileSizeBytes),
		statusReporter: build_status_reporter.NewBuildStatusReporter(env, iid),
	}
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
	pr := protofile.NewBufferedProtoReader(env.GetBlobstore(), iid)
	buildEvents := make([]*inpb.InvocationEvent, 0)
	for {
		event := &inpb.InvocationEvent{}
		err := pr.ReadProto(ctx, event)
		if err == nil {
			buildEvents = append(buildEvents, event)
		} else if err == io.EOF {
			break
		} else {
			return nil, err
		}
	}
	event_parser.FillInvocationFromEvents(buildEvents, invocation)
	return invocation, nil
}

func tableInvocationFromProto(p *inpb.Invocation, blobID string) *tables.Invocation {
	i := &tables.Invocation{}
	i.InvocationID = p.InvocationId // Required.
	i.Success = p.Success
	i.User = p.User
	i.DurationUsec = p.DurationUsec
	i.Host = p.Host
	i.RepoURL = p.RepoUrl
	i.CommitSHA = p.CommitSha
	i.Role = p.Role
	i.Command = p.Command
	if p.Pattern != nil {
		i.Pattern = strings.Join(p.Pattern, ", ")
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
		ActionCacheHits:           i.ActionCacheHits,
		ActionCacheMisses:         i.ActionCacheMisses,
		ActionCacheUploads:        i.ActionCacheUploads,
		CasCacheHits:              i.CasCacheHits,
		CasCacheMisses:            i.CasCacheMisses,
		CasCacheUploads:           i.CasCacheUploads,
		TotalDownloadSizeBytes:    i.TotalDownloadSizeBytes,
		TotalUploadSizeBytes:      i.TotalUploadSizeBytes,
		TotalDownloadUsec:         i.TotalDownloadUsec,
		TotalUploadUsec:           i.TotalUploadUsec,
		TotalCachedActionExecUsec: i.TotalCachedActionExecUsec,
	}
	return out
}
