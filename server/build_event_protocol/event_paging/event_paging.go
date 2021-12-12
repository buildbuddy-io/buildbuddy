package event_paging

import (
	"fmt"
	"strings"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

// Truncate limits the number of events per type to the given value. It
// populates the event_paging field in the invocation so that clients know
// whether there are more events available for each paging type.
func Truncate(in *inpb.Invocation, limitPerType int32) {
	out := []*inpb.InvocationEvent{}
	count := map[key]int32{}
	truncCount := map[key]int32{}
	for _, event := range in.Event {
		if event.BuildEvent == nil || event.BuildEvent.Payload == nil {
			out = append(out, event)
			continue
		}
		k := keyOf(event)
		count[k]++
		if count[k] > limitPerType {
			fmt.Printf("Truncating %+v\n", k)
			truncCount[k]++
			continue
		}
		out = append(out, event)
	}
	in.Event = out
	for k, count := range truncCount {
		in.EventPaging = append(in.EventPaging, &inpb.Invocation_EventPaging{
			PageType:       k.ToProto(),
			RemainingCount: count,
		})
	}
}

// key is a representation of the PageType proto that may be used as a map key.
type key struct {
	PayloadType string
	Status      int32
}

func keyOf(event *inpb.InvocationEvent) key {
	return key{
		PayloadType: strings.TrimPrefix(fmt.Sprintf("%T", event.BuildEvent.Payload), "*build_event_stream.BuildEvent_"),
		Status:      eventStatus(event),
	}
}

func (k *key) ToProto() *inpb.InvocationEvent_PageType {
	return &inpb.InvocationEvent_PageType{
		PayloadType: k.PayloadType,
		Status:      k.Status,
	}
}

func eventStatus(event *inpb.InvocationEvent) int32 {
	switch p := event.BuildEvent.Payload.(type) {
	case *bespb.BuildEvent_Completed:
		if p.Completed.Success {
			return 1
		}
		return 0
	default:
		return -1
	}
}
