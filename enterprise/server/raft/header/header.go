package header

import (
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

func New(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor, mode rfpb.Header_ConsistencyMode) *rfpb.Header {
	return &rfpb.Header{
		Replica:         replica,
		RangeId:         rd.GetRangeId(),
		Generation:      rd.GetGeneration(),
		ConsistencyMode: mode,
	}
}

func NewWithoutRangeInfo(replica *rfpb.ReplicaDescriptor, mode rfpb.Header_ConsistencyMode) *rfpb.Header {
	return &rfpb.Header{
		Replica:         replica,
		ConsistencyMode: mode,
	}
}
