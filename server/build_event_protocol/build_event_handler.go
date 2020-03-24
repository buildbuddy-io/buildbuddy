package build_event_handler

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/protofile"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"

	"proto/build_event_stream"
	bepb "proto/build_events"
	inpb "proto/invocation"
	pepb "proto/publish_build_event"
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

func readBazelEvent(obe *pepb.OrderedBuildEvent, out *build_event_stream.BuildEvent) error {
	switch buildEvent := obe.Event.Event.(type) {
	case *bepb.BuildEvent_BazelEvent:
		return ptypes.UnmarshalAny(buildEvent.BazelEvent, out)
	}
	return fmt.Errorf("Not a bazel event %s", obe)
}

type EventChannel struct {
	env environment.Env
	pw  *protofile.BufferedProtoWriter
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

func (e *EventChannel) finalizeInvocation(ctx context.Context, iid string) error {
	invocation := &inpb.Invocation{
		InvocationId:     iid,
		InvocationStatus: inpb.Invocation_COMPLETE_INVOCATION_STATUS,
	}
	events, err := e.readAllTempBlobs(ctx, iid)
	if err != nil {
		return err
	}
	event_parser.FillInvocationFromEvents(events, invocation)

	ti := &tables.Invocation{}
	ti.FromProtoAndBlobID(invocation, iid)
	if err := e.env.GetDatabase().InsertOrUpdateInvocation(ctx, ti); err != nil {
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
	if e.env.GetSearcher() != nil {
		go func() {
			if err := e.env.GetSearcher().IndexInvocation(context.Background(), invocation); err != nil {
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

	// If this is the last event, write the buffer and complete the invocation record.
	if isFinalEvent(event.OrderedBuildEvent) {
		if err := e.pw.Flush(ctx); err != nil {
			return err
		}
		return e.finalizeInvocation(ctx, iid)
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
		if err := e.env.GetDatabase().InsertOrUpdateInvocation(ctx, ti); err != nil {
			return err
		}
	}

	// For everything else, just save the event to our buffer and keep on chugging.
	err := e.pw.WriteProtoToStream(ctx, &inpb.InvocationEvent{
		EventTime:      event.OrderedBuildEvent.Event.EventTime,
		BuildEvent:     &bazelBuildEvent,
		SequenceNumber: event.OrderedBuildEvent.SequenceNumber,
	})
	if err != nil {
		return err
	}

	// Small optimization: Flush the event stream after ~15 events or so. Most of the
	// command line options and workspace info has come through by then, so we have
	// something to show the user. Flushing the proto file here allows that when the
	// client fetches status for the incomplete build.
	if seqNo == 15 {
		return e.pw.Flush(ctx)
	}
	return nil
}

func OpenChannel(env environment.Env, ctx context.Context, iid string) *EventChannel {
	chunkFileSizeBytes := env.GetConfigurator().GetStorageChunkFileSizeBytes()
	if chunkFileSizeBytes == 0 {
		chunkFileSizeBytes = defaultChunkFileSizeBytes
	}
	return &EventChannel{
		env: env,
		pw:  protofile.NewBufferedProtoWriter(env.GetBlobstore(), iid, chunkFileSizeBytes),
	}
}

func LookupInvocation(env environment.Env, ctx context.Context, iid string) (*inpb.Invocation, error) {
	ti, err := env.GetDatabase().LookupInvocation(ctx, iid)
	if err != nil {
		return nil, err
	}
	invocation := ti.ToProto()
	pr := protofile.NewBufferedProtoReader(env.GetBlobstore(), iid)
	for {
		event := &inpb.InvocationEvent{}
		err := pr.ReadProto(ctx, event)
		if err == nil {
			invocation.Event = append(invocation.Event, event)
		} else if err == io.EOF {
			break
		} else {
			return nil, err
		}
	}
	event_parser.FillInvocationFromEvents(invocation.Event, invocation)
	return invocation, nil
}
