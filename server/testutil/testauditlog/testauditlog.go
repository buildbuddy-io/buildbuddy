package testauditlog

import (
	"context"
	"testing"

	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type FakeEntry struct {
	Resource *alpb.ResourceID
	Action   alpb.Action
	Request  proto.Message
}

type FakeAuditLog struct {
	entries []*FakeEntry

	t *testing.T
	// Map of FooState protos to their corresponding fields in ResourceState proto.
	payloadTypes map[protoreflect.MessageDescriptor]protoreflect.FieldDescriptor
}

func New(t *testing.T) *FakeAuditLog {
	payloadTypes := make(map[protoreflect.MessageDescriptor]protoreflect.FieldDescriptor)
	pfs := (&alpb.Entry_ResourceRequest{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < pfs.Len(); i++ {
		pf := pfs.Get(i)
		payloadTypes[pf.Message()] = pf
	}
	return &FakeAuditLog{t: t, payloadTypes: payloadTypes}
}

func (f *FakeAuditLog) Log(ctx context.Context, resource *alpb.ResourceID, action alpb.Action, req proto.Message) {
	_, ok := f.payloadTypes[req.ProtoReflect().Descriptor()]
	if !ok {
		require.FailNowf(f.t, "request type missing from Entry ResourceRequest proto", "missing type: %s", req.ProtoReflect().Descriptor().FullName())
		return
	}
	f.entries = append(f.entries, &FakeEntry{
		Resource: resource,
		Action:   action,
		Request:  req,
	})
}

func (f *FakeAuditLog) GetLogs(ctx context.Context, req *alpb.GetAuditLogsRequest) (*alpb.GetAuditLogsResponse, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (f *FakeAuditLog) GetAllEntries() []*FakeEntry {
	return f.entries
}

func (f *FakeAuditLog) Reset() {
	f.entries = nil
}
