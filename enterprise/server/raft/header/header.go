package header

import (
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type MakeFunc func(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor) *rfpb.Header

// MakeLinearizableWithRangeValidation is a MakeFunc that could be passed to various sender functions.
// The returned header has range validation and linearizable consistency mode on.
func MakeLinearizableWithRangeValidation(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor) *rfpb.Header {
	return New(rd, replica, rfpb.Header_LINEARIZABLE)
}

// MakeLinearizableWithRangeValidation is a MakeFunc that could be passed to various sender functions.
// The returned header has lease validation and linearizable consistency mode on, but skipping the generation check.
func MakeLinearizableWithLeaseValidationOnly(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor) *rfpb.Header {
	header := New(rd, replica, rfpb.Header_LINEARIZABLE)
	header.SkipGenerationCheck = true
	return header
}

// MakeLinearizableWithoutRangeValidation is a MakeFunc that could be passed to various sender functions.
// The returned header has range validation off and linearizable consistency mode on.
func MakeLinearizableWithoutRangeValidation(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor) *rfpb.Header {
	return NewWithoutRangeInfo(replica, rfpb.Header_LINEARIZABLE)
}

// New returns a header that turns on range validation.
func New(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor, mode rfpb.Header_ConsistencyMode) *rfpb.Header {
	return &rfpb.Header{
		Replica:         replica,
		RangeId:         rd.GetRangeId(),
		Generation:      rd.GetGeneration(),
		ConsistencyMode: mode,
	}
}

// NewWithoutRangeInfo returns a header with range validation turned off
func NewWithoutRangeInfo(replica *rfpb.ReplicaDescriptor, mode rfpb.Header_ConsistencyMode) *rfpb.Header {
	return &rfpb.Header{
		Replica:         replica,
		ConsistencyMode: mode,
	}
}
