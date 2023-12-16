package proto

import (
	"fmt"

	gproto "google.golang.org/protobuf/proto"
)

var Clone = gproto.Clone
var Size = gproto.Size
var MarshalOld = gproto.Marshal
var UnmarshalOld = gproto.Unmarshal

type Message = gproto.Message

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

func Marshal(v interface{}) ([]byte, error) {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.MarshalVT()
	}

	msg, ok := v.(Message)
	if !ok {
		return nil, fmt.Errorf("failed to marshal, message is %T, want proto.Message", v)
	}
	return MarshalOld(msg)
}

func Unmarshal(b []byte, v interface{}) error {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.UnmarshalVT(b)
	}

	msg, ok := v.(Message)
	if !ok {
		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
	}
	return UnmarshalOld(b, msg)
}
