package rbuilder

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/golang/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

// TODO(tylerw): find a more elegant way of dealing with these kinds
// of errors.
func MustMarshal(m proto.Message) []byte {
	buf, err := proto.Marshal(m)
	if err != nil {
		log.Errorf("Error marshaling proto: %s", err)
		return nil
	}
	return buf
}

func MustUnmarshal(rsp interface{}, m proto.Message) {
	buf, ok := rsp.([]byte)
	if !ok {
		log.Errorf("Could not coerce value to []byte.")
		return
	}
	if err := proto.Unmarshal(buf, m); err != nil {
		log.Errorf("Error unmarshaling proto: %s", err)
	}
}

func DirectWriteRequestBuf(kv *rfpb.KV) []byte {
	return MustMarshal(&rfpb.RequestUnion{
		Value: &rfpb.RequestUnion_DirectWrite{
			DirectWrite: &rfpb.DirectWriteRequest{
				Kv: kv,
			},
		},
	})
}

func DirectReadRequestBuf(key []byte) []byte {
	return MustMarshal(&rfpb.RequestUnion{
		Value: &rfpb.RequestUnion_DirectRead{
			DirectRead: &rfpb.DirectReadRequest{
				Key: key,
			},
		},
	})
}

func DirectReadResponse(val interface{}) *rfpb.KV {
	rsp := &rfpb.ResponseUnion{}
	MustUnmarshal(val, rsp)
	return rsp.GetDirectRead().GetKv()
}

func FileWriteRequestBuf(fileRecord *rfpb.FileRecord) []byte {
	return MustMarshal(&rfpb.RequestUnion{
		Value: &rfpb.RequestUnion_FileWrite{
			FileWrite: &rfpb.FileWriteRequest{
				FileRecord: fileRecord,
			},
		},
	})
}
