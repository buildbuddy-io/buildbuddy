package build_event_handler

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/jinzhu/gorm"
	"github.com/tryflame/buildbuddy/server/blobstore"
	"github.com/tryflame/buildbuddy/server/database"
	"github.com/tryflame/buildbuddy/server/event_parser"
	"github.com/tryflame/buildbuddy/server/tables"
	"proto/build_event_stream"

	inpb "proto/invocation"
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

func (h *BuildEventHandler) writeToBlobstore(ctx context.Context, blobID string, invocation *inpb.Invocation) error {
	protoBytes, err := proto.Marshal(invocation)
	if err != nil {
		return err
	}
	return h.bs.WriteBlob(ctx, blobID, protoBytes)
}

func (h *BuildEventHandler) HandleEvents(ctx context.Context, invocationID string, buildEvents []*build_event_stream.BuildEvent) error {
	invocation := &inpb.Invocation{
		InvocationId: invocationID,
		BuildEvent:   buildEvents,
	}
	event_parser.FillInvocationFromEvents(buildEvents, invocation)
	return h.db.GormDB.Transaction(func(tx *gorm.DB) error {
		i := &tables.Invocation{}
		i.FromProto(invocation)
		i.BlobID = invocationID
		if err := tx.Create(i).Error; err != nil {
			return err
		}

		// Write the blob inside the transaction. All or nothing.
		return h.writeToBlobstore(ctx, invocationID, invocation)
	})
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
