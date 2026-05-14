package vtprotocodec

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	_ "google.golang.org/grpc/encoding/proto" // for default proto registration purposes
)

const Name = "proto"

// CodecV2 implements encoding.CodecV2 and uses vtproto and default buffer pool
// to encode/decode proto messages. The implementation is heavily inspired by
// https://github.com/planetscale/vtprotobuf/pull/138
// and https://github.com/vitessio/vitess/pull/16790.
type CodecV2 struct {
	fallback encoding.CodecV2
}

func (CodecV2) Name() string {
	return Name
}

func (c *CodecV2) Marshal(v any) (mem.BufferSlice, error) {
	m, ok := v.(proto.VTProtoMessage)
	if !ok {
		return c.fallback.Marshal(v)
	}
	size := m.SizeVT()
	if mem.IsBelowBufferPoolingThreshold(size) {
		buf := make([]byte, size)
		n, err := m.MarshalToSizedBufferVT(buf)
		if err != nil {
			return nil, err
		}
		return mem.BufferSlice{mem.SliceBuffer(buf[:n])}, nil
	}
	pool := mem.DefaultBufferPool()
	buf := pool.Get(size)
	n, err := m.MarshalToSizedBufferVT(*buf)
	if err != nil {
		pool.Put(buf)
		return nil, err
	}
	*buf = (*buf)[:n]
	return mem.BufferSlice{mem.NewBuffer(buf, pool)}, nil
}

func (c *CodecV2) Unmarshal(data mem.BufferSlice, v any) error {
	m, ok := v.(proto.VTProtoMessage)
	if !ok {
		return c.fallback.Unmarshal(data, v)
	}
	buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
	defer buf.Free()
	return m.UnmarshalVT(buf.ReadOnlyData())
}

// RegisterCodec registers the vtprotoCodec to encode/decode proto messages with
// all gRPC clients and servers.
func Register() {
	encoding.RegisterCodecV2(&CodecV2{
		// the default codecv2 implemented in @org_golang_google_grpc//encoding/proto.
		fallback: encoding.GetCodecV2("proto"),
	})
}
