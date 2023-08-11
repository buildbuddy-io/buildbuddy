package event_index

import (
	"sort"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"

	cmnpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	api_common "github.com/buildbuddy-io/buildbuddy/server/api/common"
)

const (
	// Only index up to this many non-important events as a safeguard against
	// excessive memory / CPU consumption.
	maxEventCount = 2_000_000
)

// Index holds a few data structures to make it easier to aggregate data from
// raw BES events and organize them into pages.
type Index struct {
	AllTargetLabels            []string
	BuildTargetByLabel         map[string]*trpb.Target
	TestTargetByLabel          map[string]*trpb.Target
	TargetCompleteEventByLabel map[string]*bespb.BuildEvent
	TestResultEventsByLabel    map[string][]*bespb.BuildEvent
	TargetsByStatus            map[cmnpb.Status][]*trpb.Target
	NamedSetOfFilesByID        map[string]*bespb.NamedSetOfFiles
	ActionEvents               []*bespb.BuildEvent
	// Events which aren't indexed by target and are instead returned in the
	// top-level invocation proto.
	TopLevelEvents []*inpb.InvocationEvent

	eventCount      int
	rootCauseLabels map[string]bool
}

func New() *Index {
	return &Index{
		BuildTargetByLabel:         map[string]*trpb.Target{},
		TestTargetByLabel:          map[string]*trpb.Target{},
		TargetCompleteEventByLabel: map[string]*bespb.BuildEvent{},
		TargetsByStatus:            map[cmnpb.Status][]*trpb.Target{},
		TestResultEventsByLabel:    map[string][]*bespb.BuildEvent{},
		NamedSetOfFilesByID:        map[string]*bespb.NamedSetOfFiles{},
		rootCauseLabels:            map[string]bool{},
	}
}

// Add adds a single event to the index.
// Don't forget to call Finalize once all events are added.
func (idx *Index) Add(event *inpb.InvocationEvent) {
	if idx.eventCount >= maxEventCount && !accumulator.IsImportantEvent(event.GetBuildEvent()) {
		return
	}
	idx.eventCount++

	switch p := event.GetBuildEvent().GetPayload().(type) {
	case *bespb.BuildEvent_NamedSetOfFiles:
		nsid := event.GetBuildEvent().GetId().GetNamedSet().GetId()
		idx.NamedSetOfFilesByID[nsid] = p.NamedSetOfFiles
	case *bespb.BuildEvent_Configured:
		label := event.GetBuildEvent().GetId().GetTargetConfigured().GetLabel()
		idx.AllTargetLabels = append(idx.AllTargetLabels, label)
		idx.BuildTargetByLabel[label] = &trpb.Target{
			Metadata: &trpb.TargetMetadata{Label: label},
			Status:   cmnpb.Status_BUILDING,
		}
	case *bespb.BuildEvent_Completed:
		label := event.GetBuildEvent().GetId().GetTargetCompleted().GetLabel()
		// TODO: when transitions are used, this will only record a single
		// Completed event per label, even if the same label was built for
		// multiple configurations. Figure out how to deal with
		// multi-configuration builds here.
		idx.TargetCompleteEventByLabel[label] = event.GetBuildEvent()
		target := idx.BuildTargetByLabel[label]
		if target == nil {
			return
		}
		target.Status = cmnpb.Status_BUILT
		// Check for "root cause" labels.
		completed := event.GetBuildEvent().GetCompleted()
		if !completed.GetSuccess() {
			for _, c := range event.GetBuildEvent().GetChildren() {
				if label := c.GetActionCompleted().GetLabel(); label != "" {
					// TODO: is `label` guaranteed to be in BuildTargetByLabel
					// at this stage? If so, can set root_cause directly here
					/// instead of building a map.
					idx.rootCauseLabels[label] = true
				}
			}
		}
	case *bespb.BuildEvent_TestSummary:
		label := event.GetBuildEvent().GetId().GetTestSummary().GetLabel()
		summary := p.TestSummary
		idx.TestTargetByLabel[label] = &trpb.Target{
			Metadata:    &trpb.TargetMetadata{Label: label},
			Status:      api_common.TestStatusToStatus(summary.GetOverallStatus()),
			Timing:      api_common.TestTimingFromSummary(summary),
			TestSummary: summary,
		}
	case *bespb.BuildEvent_TestResult:
		label := event.GetBuildEvent().GetId().GetTestResult().GetLabel()
		idx.TestResultEventsByLabel[label] = append(idx.TestResultEventsByLabel[label], event.GetBuildEvent())
		if idx.TestTargetByLabel[label] == nil {
			idx.TestTargetByLabel[label] = &trpb.Target{
				Metadata: &trpb.TargetMetadata{Label: label},
				// There may be multiple TestResults per test label (sharding,
				// attempts, etc.), so mark as TESTING until we get the
				// TestSummary event.
				Status: cmnpb.Status_TESTING,
			}
		}
	case *bespb.BuildEvent_Action:
		idx.ActionEvents = append(idx.ActionEvents, event.GetBuildEvent())
	case *bespb.BuildEvent_Progress:
		// Drop progress events
		return
	default:
		idx.TopLevelEvents = append(idx.TopLevelEvents, event)
	}
}

func (idx *Index) Finalize() {
	// Sort target labels.
	sort.Strings(idx.AllTargetLabels)
	// Apply rootCauseLabels to targets, and discard.
	for label := range idx.rootCauseLabels {
		if t := idx.BuildTargetByLabel[label]; t != nil {
			t.RootCause = true
		}
	}
	idx.rootCauseLabels = nil
	// Build TargetsByStatus index now that we know all the statuses.
	for _, t := range idx.BuildTargetByLabel {
		idx.TargetsByStatus[t.Status] = append(idx.TargetsByStatus[t.Status], t)
	}
	for _, t := range idx.TestTargetByLabel {
		idx.TargetsByStatus[t.Status] = append(idx.TargetsByStatus[t.Status], t)
	}
	// Sort TargetsByStatus list values.
	for _, targets := range idx.TargetsByStatus {
		sort.Slice(targets, func(i, j int) bool {
			return targets[i].GetMetadata().GetLabel() < targets[j].GetMetadata().GetLabel()
		})
	}
}
