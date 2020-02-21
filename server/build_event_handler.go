package build_event_handler

import (
	"bytes"
	"io"
	"io/ioutil"

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

func (h *BuildEventHandler) writeToBlobstore(invocation *inpb.Invocation) error {
	protoBytes, err := proto.Marshal(invocation)
	if err != nil {
		return err
	}
	blob, err := h.bs.GetBlob(invocation.InvocationId)
	if err != nil {
		return err
	}
	protoBytesBuf := bytes.NewBuffer(protoBytes)
	if _, err := io.Copy(blob, protoBytesBuf); err != nil {
		return err
	}
	return nil
}

func (h *BuildEventHandler) HandleEvents(invocationID string, buildEvents []*build_event_stream.BuildEvent) error {
	invocation := &inpb.Invocation{
		InvocationId: invocationID,
		BuildEvent:   buildEvents,
	}
	event_parser.FillInvocationFromEvents(buildEvents, invocation)
	return h.db.GormDB.Transaction(func(tx *gorm.DB) error {
		i := &tables.Invocation{}
		i.FromProto(invocation)
		if err := tx.Create(i).Error; err != nil {
			return err
		}

		// Write the blob inside the transaction. All or nothing.
		return h.writeToBlobstore(invocation)
	})
}

func (h *BuildEventHandler) LookupInvocation(invocationID string) (*inpb.Invocation, error) {
	blob, err := h.bs.GetBlob(invocationID)
	if err != nil {
		return nil, err
	}

	protoBytes, err := ioutil.ReadAll(blob)
	if err != nil {
		return nil, err
	}

	invocation := new(inpb.Invocation)
	if err := proto.Unmarshal(protoBytes, invocation); err != nil {
		return nil, err
	}
	return invocation, nil
}
