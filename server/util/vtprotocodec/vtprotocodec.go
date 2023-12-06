package vtprotocodec

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"

	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto" // for default proto registration purposes
)

const Name = "proto"

// vtprotoCodec represents a codec able to encode and decode vt enabled
// proto messages.
type vtprotoCodec struct{}

func (vtprotoCodec) Marshal(v any) ([]byte, error) {
	return proto.Marshal(v)
}

func (vtprotoCodec) Unmarshal(data []byte, v any) error {
	return proto.Unmarshal(data, v)
}

func (vtprotoCodec) Name() string {
	return Name
}

// RegisterCodec registers the vtprotoCodec to encode/decode proto messages with
// all gRPC clients and servers.
func Register() {
	encoding.RegisterCodec(vtprotoCodec{})
}
