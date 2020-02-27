package build_event_handler

import (
	"context"
	"fmt"
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/gorm"
	"github.com/tryflame/buildbuddy/server/blobstore"
	"github.com/tryflame/buildbuddy/server/database"
	"github.com/tryflame/buildbuddy/server/event_parser"
	"github.com/tryflame/buildbuddy/server/tables"

	bpb "proto"
	"proto/build_event_stream"
	inpb "proto/invocation"
)

const (
	eventBufferStartingCapacity = 20
)

type BuildEventHandler struct {
	bs blobstore.Blobstore
	db *database.Database
}

func NewBuildEventHandler(bs blobstore.Blobstore, db *database.Database) *BuildEventHandler {
	return &BuildEventHandler{
		bs: bs,
		db: db,
	}
}

func (e *EventChannel) writeToBlobstore(ctx context.Context, blobID string, invocation *inpb.Invocation) error {
	protoBytes, err := proto.Marshal(invocation)
	if err != nil {
		return err
	}
	return e.bs.WriteBlob(ctx, blobID, protoBytes)
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

func (e *EventChannel) insertOrUpdateInvocation(ctx context.Context, iid string) error {
	return e.db.GormDB.Transaction(func(tx *gorm.DB) error {
		existingRow := tx.Raw(`SELECT i.invocation_id FROM Invocations as i WHERE i.invocation_id = ?`, iid)
		err := existingRow.Scan(&struct{}{}).Error
		if err == gorm.ErrRecordNotFound {
			i := &tables.Invocation{
				InvocationID:     iid,
				InvocationStatus: int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS),
			}
			return tx.Create(i).Error
		}
		if err == nil {
			return tx.Exec("UPDATE Invocations SET invocation_status = ? WHERE invocation_id = ?",
				int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS), iid).Error
		}
		return err
	})
}

func (e *EventChannel) finalizeInvocation(ctx context.Context, iid string) error {
	invocation := &inpb.Invocation{
		InvocationId: iid,
	}
	event_parser.FillInvocationFromEvents(e.eventBuffer, invocation)
	return e.db.GormDB.Transaction(func(tx *gorm.DB) error {
		i := &tables.Invocation{}
		// This is pretty gnarly. Let's remove the ORM here asap.
		tx.Where("invocation_id = ?", iid).First(&i)
		i.FromProto(invocation)
		i.BlobID = iid
		i.InvocationStatus = int64(inpb.Invocation_COMPLETE_INVOCATION_STATUS)
		if err := tx.Save(i).Error; err != nil {
			return err
		}
		// Write the blob inside the transaction. All or nothing.
		return e.writeToBlobstore(ctx, iid, invocation)
	})
}

type EventChannel struct {
	bs blobstore.Blobstore
	db *database.Database

	eventBuffer []*inpb.InvocationEvent
}

func (e *EventChannel) HandleEvent(ctx context.Context, event *bpb.PublishBuildToolEventStreamRequest) error {
	seqNo := event.OrderedBuildEvent.SequenceNumber
	streamID := event.OrderedBuildEvent.StreamId
	iid := streamID.InvocationId

	// If this is the last event, write the buffer and complete the invocation record.
	if isFinalEvent(event.OrderedBuildEvent) {
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
		if err := e.insertOrUpdateInvocation(ctx, iid); err != nil {
			return err
		}
	}

	// For everything else, just save the event to our buffer and keep on chugging.
	e.eventBuffer = append(e.eventBuffer, &inpb.InvocationEvent{
		EventTime:  event.OrderedBuildEvent.Event.EventTime,
		BuildEvent: &bazelBuildEvent,
	})

	return nil
}

func (h *BuildEventHandler) OpenChannel(ctx context.Context, iid string) *EventChannel {
	return &EventChannel{
		bs:          h.bs,
		db:          h.db,
		eventBuffer: make([]*inpb.InvocationEvent, 0, eventBufferStartingCapacity),
	}
}

func (h *BuildEventHandler) LookupInvocation(ctx context.Context, invocationID string) (*inpb.Invocation, error) {
	protoBytes, err := h.bs.ReadBlob(ctx, invocationID)
	if err != nil {
		return nil, err
	}

	invocation := new(inpb.Invocation)
	if err := proto.Unmarshal(protoBytes, invocation); err != nil {
		return nil, err
	}
	return invocation, nil
}
