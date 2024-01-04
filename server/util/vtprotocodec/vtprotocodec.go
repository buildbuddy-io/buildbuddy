package vtprotocodec

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"

	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto" // for default proto registration purposes
)

const Name = "proto"

// vtprotoCodec represents a codec able to encode and decode vt enabled
// proto messages.
type vtprotoCodec struct{}

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

func (vtprotoCodec) Marshal(v any) ([]byte, error) {
	vv, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("failed to marshal, message is %T, want proto.Message", v)
	}
	return proto.Marshal(vv)
}

func (vtprotoCodec) Unmarshal(data []byte, v any) error {
	vv, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
	}
	return proto.Unmarshal(data, vv)
}

func (vtprotoCodec) Name() string {
	return Name
}

// RegisterCodec registers the vtprotoCodec to encode/decode proto messages with
// all gRPC clients and servers.
func Register() {
	encoding.RegisterCodec(vtprotoCodec{})
}
