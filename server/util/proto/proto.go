package proto

import (
	gproto "google.golang.org/protobuf/proto"
)

var Size = gproto.Size
var Merge = gproto.Merge
var Equal = gproto.Equal
var MarshalOld = gproto.Marshal

var String = gproto.String
var Float64 = gproto.Float64
var Uint64 = gproto.Uint64
var Int32 = gproto.Int32
var Bool = gproto.Bool

type Message = gproto.Message
type MarshalOptions = gproto.MarshalOptions

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
	CloneMessageVT() Message
}

func Marshal(v Message) ([]byte, error) {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.MarshalVT()
	}
	return MarshalOld(v)
}

func Unmarshal(b []byte, v Message) error {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.UnmarshalVT(b)
	}

	return gproto.Unmarshal(b, v)
}

func Clone(v Message) Message {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.CloneMessageVT()
	}
	return gproto.Clone(v)
}
