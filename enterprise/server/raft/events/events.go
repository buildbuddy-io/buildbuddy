package events

import (
	"fmt"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type EventType int

const (
	// Value will be the range's RangeUsage.
	EventRangeUsageUpdated EventType = iota
	// Value will be the lease-acquired range's RangeDescriptor.
	EventRangeLeaseAcquired
	// Value will be the lease-dropped range's RangeDescriptor.
	EventRangeLeaseDropped
)

type Event interface {
	EventType() EventType
	String() string
}

func (t EventType) String() string {
	switch t {
	case EventRangeUsageUpdated:
		return "range-usage-updated"
	case EventRangeLeaseAcquired:
		return "range-lease-acquired"
	case EventRangeLeaseDropped:
		return "range-lease-dropped"
	default:
		return fmt.Sprintf("unknown event type: %d", t)
	}
}

type RangeEvent struct {
	Type EventType
}

func (r RangeEvent) EventType() EventType {
	return r.Type
}

func (r RangeEvent) String() string {
	switch r.Type {
	case EventRangeLeaseAcquired, EventRangeLeaseDropped:
		return r.Type.String()
	default:
		return fmt.Sprintf("unknown event type: %s", r.Type)
	}
}

type RangeUsageEvent struct {
	Type            EventType
	RangeDescriptor *rfpb.RangeDescriptor
	ReplicaUsage    *rfpb.ReplicaUsage
}

func (u RangeUsageEvent) EventType() EventType {
	return u.Type
}

func (u RangeUsageEvent) String() string {
	switch u.Type {
	case EventRangeUsageUpdated:
		return fmt.Sprintf("range-usage-updated for range ID %d", u.RangeDescriptor.GetRangeId())
	default:
		return fmt.Sprintf("unknown event type: %s", u.Type)
	}
}
