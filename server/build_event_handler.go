package build_event_handler

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/golang/protobuf/proto"

	"github.com/tryflame/buildbuddy/server/blobstore"
	"proto/build_event_stream"

	ipb "proto/invocation"
)

type BuildEventHandler struct {
	bs blobstore.Blobstore
}

func NewBuildEventHandler(bs blobstore.Blobstore) *BuildEventHandler {
	return &BuildEventHandler{
		bs: bs,
	}
}

func (b *BuildEventHandler) HandleEvents(invocationID string, buildEvents []*build_event_stream.BuildEvent) error {
	invocation := &ipb.Invocation{
		InvocationId: invocationID,
		BuildEvent:   buildEvents,
	}

	protoBytes, err := proto.Marshal(invocation)
	if err != nil {
		return err
	}
	protoBytesBuf := bytes.NewBuffer(protoBytes)

	blob, err := b.bs.GetBlob(invocationID)
	if err != nil {
		return err
	}
	io.Copy(blob, protoBytesBuf)

	return nil
}

func (b *BuildEventHandler) LookupInvocation(invocationID string) (*ipb.Invocation, error) {
	blob, err := b.bs.GetBlob(invocationID)
	if err != nil {
		return nil, err
	}

	protoBytes, err := ioutil.ReadAll(blob)
	if err != nil {
		return nil, err
	}

	invocation := new(ipb.Invocation)
	if err := proto.Unmarshal(protoBytes, invocation); err != nil {
		return nil, err
	}
	return invocation, nil
}
