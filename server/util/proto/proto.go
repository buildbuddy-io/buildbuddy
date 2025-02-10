package proto

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/vtprotocodec"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	gproto "google.golang.org/protobuf/proto"
)

var Size = gproto.Size
var Merge = gproto.Merge
var Equal = gproto.Equal
var MarshalOld = gproto.Marshal

var String = gproto.String
var Float32 = gproto.Float32
var Float64 = gproto.Float64
var Uint64 = gproto.Uint64
var Int32 = gproto.Int32
var Int64 = gproto.Int64
var Bool = gproto.Bool

type Message = gproto.Message
type MarshalOptions = gproto.MarshalOptions

func Marshal(v Message) ([]byte, error) {
	bs, err := encoding.GetCodecV2(vtprotocodec.Name).Marshal(v)
	if err != nil {
		return nil, err
	}
	// Returns the underlying bytes buffer for READ-ONLY purposes.
	// It's undefined behavior to modify the returned slice.
	// Caller should make a copy if they need to modify the slice directly.
	return bs.MaterializeToBuffer(mem.DefaultBufferPool()).ReadOnlyData(), nil
}

func MarshalVT(v Message) ([]byte, error) {
	vt, ok := v.(vtprotocodec.VTProtoMessage)
	if ok {
		return vt.MarshalVT()
	}
	return MarshalOld(v)
}

func Unmarshal(b []byte, v Message) error {
	return encoding.
		GetCodecV2(vtprotocodec.Name).
		Unmarshal(mem.BufferSlice{mem.NewBuffer(&b, mem.DefaultBufferPool())}, v)
}

func UnmarshalVT(b []byte, v Message) error {
	vt, ok := v.(vtprotocodec.VTProtoMessage)
	if ok {
		return vt.UnmarshalVT(b)
	}

	return gproto.Unmarshal(b, v)
}

func Clone(v Message) Message {
	if vt, ok := v.(vtprotocodec.VTProtoMessage); ok {
		return vt.CloneMessageVT()
	}
	return gproto.Clone(v)
}
