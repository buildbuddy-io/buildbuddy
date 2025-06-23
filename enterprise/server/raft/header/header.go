package header

import (
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type MakeFunc func(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor) *rfpb.Header

func MakeLinearizableWithRangeValidation(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor) *rfpb.Header {
	return New(rd, replica, rfpb.Header_LINEARIZABLE)
}

func MakeLinearizableWithoutRangeValidation(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor) *rfpb.Header {
	return NewWithoutRangeInfo(replica, rfpb.Header_LINEARIZABLE)
}

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
