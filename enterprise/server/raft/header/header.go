package header

import (
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

func New(rd *rfpb.RangeDescriptor, replicaIdx int, mode rfpb.Header_ConsistencyMode) *rfpb.Header {
	return &rfpb.Header{
		Replica:         rd.GetReplicas()[replicaIdx],
		RangeId:         rd.GetRangeId(),
		Generation:      rd.GetGeneration(),
		ConsistencyMode: mode,
	}
}

func NewWithoutRangeInfo(rd *rfpb.RangeDescriptor, replicaIdx int, mode rfpb.Header_ConsistencyMode) *rfpb.Header {
	return &rfpb.Header{
		Replica:         rd.GetReplicas()[replicaIdx],
		ConsistencyMode: mode,
	}
}
