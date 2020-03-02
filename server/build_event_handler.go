package build_event_handler

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/tryflame/buildbuddy/server/config"
	"github.com/tryflame/buildbuddy/server/event_parser"
	"github.com/tryflame/buildbuddy/server/interfaces"
	"github.com/tryflame/buildbuddy/server/protofile"
	"github.com/tryflame/buildbuddy/server/tables"

	bpb "proto"
	"proto/build_event_stream"
	inpb "proto/invocation"
)

const (
	defaultChunkFileSizeBytes = 1000 * 100 // 100KB
)

type BuildEventHandler struct {
	bs interfaces.Blobstore
	db interfaces.Database
	c  *config.Configurator
}

func NewBuildEventHandler(bs interfaces.Blobstore, c *config.Configurator, db interfaces.Database) *BuildEventHandler {
	return &BuildEventHandler{
		bs: bs,
		c:  c,
		db: db,
	}
}

func isFinalEvent(obe *bpb.OrderedBuildEvent) bool {
	switch obe.Event.Event.(type) {
	case *bpb.BuildEvent_ComponentStreamFinished:
		return true
	}
	return false
}

func readBazelEvent(obe *bpb.OrderedBuildEvent, out *build_event_stream.BuildEvent) error {
	switch buildEvent := obe.Event.Event.(type) {
	case *bpb.BuildEvent_BazelEvent:
		return ptypes.UnmarshalAny(buildEvent.BazelEvent, out)
	}
	return fmt.Errorf("Not a bazel event %s", obe)
}

type EventChannel struct {
	bs interfaces.Blobstore
	db interfaces.Database
	pw *protofile.BufferedProtoWriter
}

func (e *EventChannel) readAllTempBlobs(ctx context.Context, blobID string) ([]*inpb.InvocationEvent, error) {
	events := make([]*inpb.InvocationEvent, 0)
	pr := protofile.NewBufferedProtoReader(e.bs, blobID)
	for {
		event := &inpb.InvocationEvent{}
		err := pr.ReadProto(ctx, event)
		if err == nil {
			events = append(events, event)
		} else if err == io.EOF {
			break
		} else {
			log.Printf("returning some other errror!")
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
	return e.bs.WriteBlob(ctx, blobID, protoBytes)
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

	// TODO(tylerw): We can probably omit this and just rely entirely on chunk reader.
	completedBlobID := iid + "-completed.pb"
	if err := e.writeCompletedBlob(ctx, completedBlobID, invocation); err != nil {
		return err
	}

	ti := &tables.Invocation{}
	ti.FromProtoAndBlobID(invocation, completedBlobID)
	return e.db.InsertOrUpdateInvocation(ctx, ti)
}

func (e *EventChannel) HandleEvent(ctx context.Context, event *bpb.PublishBuildToolEventStreamRequest) error {
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
		if err := e.db.InsertOrUpdateInvocation(ctx, ti); err != nil {
			return err
		}
	}

	// For everything else, just save the event to our buffer and keep on chugging.
	return e.pw.WriteProtoToStream(ctx, &inpb.InvocationEvent{
		EventTime:      event.OrderedBuildEvent.Event.EventTime,
		BuildEvent:     &bazelBuildEvent,
		SequenceNumber: event.OrderedBuildEvent.SequenceNumber,
	})
}

func (h *BuildEventHandler) OpenChannel(ctx context.Context, iid string) *EventChannel {
	chunkFileSizeBytes := h.c.GetStorageChunkFileSizeBytes()
	if chunkFileSizeBytes == 0 {
		chunkFileSizeBytes = defaultChunkFileSizeBytes
	}
	return &EventChannel{
		bs: h.bs,
		db: h.db,
		pw: protofile.NewBufferedProtoWriter(h.bs, iid, chunkFileSizeBytes),
	}
}

func (h *BuildEventHandler) LookupInvocation(ctx context.Context, iid string) (*inpb.Invocation, error) {
	ti, err := h.db.LookupInvocation(ctx, iid)
	if err != nil {
		return nil, err
	}
	invocation := ti.ToProto()

	pr := protofile.NewBufferedProtoReader(h.bs, iid)
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

	// Trick the frontend into showing partial results :)
	//invocation.InvocationStatus = inpb.Invocation_COMPLETE_INVOCATION_STATUS
	return invocation, nil
}
