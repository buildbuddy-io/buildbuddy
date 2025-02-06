package vtprotocodec

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	_ "google.golang.org/grpc/encoding/proto" // for default proto registration purposes
)

const Name = "proto"

// vtprotoCodecV2 implements encoding.CodecV2 and uses vtproto and default buffer pool
// to encode/decode proto messages. The implementation is heavily inspired by
// https://github.com/planetscale/vtprotobuf/pull/138
// and https://github.com/vitessio/vitess/pull/16790.
type vtprotoCodecV2 struct {
	fallback encoding.CodecV2
}

func (vtprotoCodecV2) Name() string {
	return Name
}

func (c *vtprotoCodecV2) Marshal(v any) (mem.BufferSlice, error) {
	m, ok := v.(proto.VtprotoMessage)
	if !ok {
		return c.fallback.Marshal(v)
	}

	size := m.SizeVT()
	if mem.IsBelowBufferPoolingThreshold(size) {
		buf := make([]byte, 0, size)
		if _, err := m.MarshalToSizedBufferVT(buf[:size]); err != nil {
			return nil, err
		}
		return mem.BufferSlice{mem.SliceBuffer(buf)}, nil
	}

	pool := mem.DefaultBufferPool()
	buf := pool.Get(size)
	if _, err := m.MarshalToSizedBufferVT((*buf)[:size]); err != nil {
		pool.Put(buf)
		return nil, err
	}
	return mem.BufferSlice{mem.NewBuffer(buf, pool)}, nil
}

func (c *vtprotoCodecV2) Unmarshal(data mem.BufferSlice, v any) error {
	m, ok := v.(proto.VtprotoMessage)
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
	encoding.RegisterCodecV2(&vtprotoCodecV2{
		fallback: encoding.GetCodecV2("proto"),
	})
}
