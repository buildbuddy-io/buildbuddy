package testusage

import (
	"context"
	"maps"
	"reflect"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
)

// Tracker is a usage tracker for use in tests.
// Use [NewTracker] to get an instance.
type Tracker struct {
	increments     []increment
	olapIncrements []olapIncrement
}

func NewTracker() *Tracker {
	return &Tracker{}
}

type increment struct {
	GroupID string
	Labels  tables.UsageLabels
	Counts  tables.UsageCounts
}

type olapIncrement struct {
	GroupID string
	Labels  sku.Labels
	SKU     sku.SKU
	Count   int64
}

func (ut *Tracker) Increment(ctx context.Context, labels *tables.UsageLabels, counts *tables.UsageCounts) error {
	c, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		// Don't track unauth'd or anonymous requests.
		return nil
	}

	ut.increments = append(ut.increments, increment{c.GroupID, *labels, *counts})
	return nil
}

func (ut *Tracker) IncrementOLAP(ctx context.Context, labels map[sku.LabelName]sku.LabelValue, sku sku.SKU, count int64) error {
	c, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		// Don't track unauth'd or anonymous requests.
		return nil
	}
	// Ignore zero counts.
	if count == 0 {
		return nil
	}
	ut.olapIncrements = append(ut.olapIncrements, olapIncrement{c.GroupID, labels, sku, count})
	return nil
}

// Reset clears all recorded increments.
func (ut *Tracker) Reset() {
	ut.increments = nil
	ut.olapIncrements = nil
}

type Total struct {
	GroupID string
	Labels  tables.UsageLabels
	Counts  tables.UsageCounts
}

func (ut *Tracker) Totals() []Total {
	totals := map[string]*Total{}
	for _, inc := range ut.increments {
		key := usageutil.EncodeCollection(&usageutil.Collection{
			GroupID: inc.GroupID,
			Origin:  inc.Labels.Origin,
			Client:  inc.Labels.Client,
			Server:  inc.Labels.Server,
		})
		if _, ok := totals[key]; !ok {
			totals[key] = &Total{
				GroupID: inc.GroupID,
				Labels:  inc.Labels,
				Counts:  tables.UsageCounts{},
			}
		}
		totals[key].Counts = addCounts(totals[key].Counts, inc.Counts)
	}
	keys := make([]string, 0, len(totals))
	keys = slices.AppendSeq(keys, maps.Keys(totals))
	slices.Sort(keys)
	out := make([]Total, 0, len(totals))
	for _, key := range keys {
		out = append(out, *totals[key])
	}
	return out
}

type OLAPTotal struct {
	GroupID string
	Labels  sku.Labels
	Counts  map[sku.SKU]int64
}

// OLAPTotals returns all the recorded OLAP totals. The elements are returned in
// a deterministic order to avoid flaky assertions, but tests can use
// [assert.ElementsMatch] for assertions to avoid having to worry about the
// order at all.
func (ut *Tracker) OLAPTotals() []OLAPTotal {
	totals := map[string]OLAPTotal{}
	for _, inc := range ut.olapIncrements {
		if inc.Count == 0 {
			continue
		}
		key := usageutil.EncodeOLAPCollection(&usageutil.OLAPCollection{
			GroupID: inc.GroupID,
			Labels:  inc.Labels,
		})
		if _, ok := totals[key]; !ok {
			totals[key] = OLAPTotal{
				GroupID: inc.GroupID,
				Labels:  inc.Labels,
				Counts:  make(map[sku.SKU]int64, 1),
			}
		}
		totals[key].Counts[inc.SKU] += inc.Count
	}
	keys := make([]string, 0, len(totals))
	keys = slices.AppendSeq(keys, maps.Keys(totals))
	slices.Sort(keys)
	out := make([]OLAPTotal, 0, len(totals))
	for _, key := range keys {
		out = append(out, totals[key])
	}
	return out
}

func addCounts(a, b tables.UsageCounts) tables.UsageCounts {
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	c := tables.UsageCounts{}
	vc := reflect.ValueOf(&c).Elem()
	for i := 0; i < va.NumField(); i++ {
		vc.Field(i).SetInt(va.Field(i).Int() + vb.Field(i).Int())
	}
	return c
}
