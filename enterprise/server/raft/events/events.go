package events

import (
	"fmt"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type EventType int

const (
	// Value will be the added range's RangeDescriptor.
	EventRangeAdded EventType = iota
	// Value will be the removed range's RangeDescriptor.
	EventRangeRemoved
	// Value will be the range's RangeUsage.
	EventRangeUsageUpdated
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
	case EventRangeAdded:
		return "range-added"
	case EventRangeRemoved:
		return "range-removed"
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
	Type            EventType
	RangeDescriptor *rfpb.RangeDescriptor
}

func (r RangeEvent) EventType() EventType {
	return r.Type
}

func (r RangeEvent) String() string {
	switch r.Type {
	case EventRangeAdded:
		return "range-added"
	case EventRangeRemoved:
		return "range-removed"
	case EventRangeLeaseAcquired:
		return "range-lease-acquired"
	case EventRangeLeaseDropped:
		return "range-lease-dropped"
	default:
		return fmt.Sprintf("unknown event type: %d", r.Type)
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
		return fmt.Sprintf("unknown event type: %d", u.Type)
	}
}
