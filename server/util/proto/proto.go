package proto

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	gproto "google.golang.org/protobuf/proto"

	"github.com/buildbuddy-io/buildbuddy/server/util/vtprotocodec"
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
	// TODO(sluongng): For some reason, this test
	//   bazel test --config=remote-minimal //enterprise/server/raft/replica:replica_test --test_filter=TestBatchTransaction
	// failed without the following code block. Investigate why.
	//
	// if vt, ok := v.(vtprotocodec.VTProtoMessage); ok {
	// 	return vt.MarshalVT()
	// }
	bs, err := encoding.GetCodecV2(vtprotocodec.Name).Marshal(v)
	return bs.Materialize(), err
}

func Unmarshal(b []byte, v Message) error {
	return encoding.
		GetCodecV2(vtprotocodec.Name).
		Unmarshal(mem.BufferSlice{mem.SliceBuffer(b)}, v)
}

func Clone(v Message) Message {
	if vt, ok := v.(vtprotocodec.VTProtoMessage); ok {
		return vt.CloneMessageVT()
	}
	return gproto.Clone(v)
}
