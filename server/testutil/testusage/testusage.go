package testusage

import (
	"context"
	"maps"
	"reflect"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
)

// Tracker is a usage tracker for use in tests.
// Use [NewTracker] to get an instance.
type Tracker struct {
	increments []increment
	jwtParser  interfaces.JWTParser ``
}

func NewTracker(env environment.Env) *Tracker {
	return &Tracker{jwtParser: env.GetJWTParser()}
}

type increment struct {
	GroupID string
	Labels  tables.UsageLabels
	Counts  tables.UsageCounts
}

func (ut *Tracker) Increment(ctx context.Context, labels *tables.UsageLabels, counts *tables.UsageCounts) error {
	c, err := claims.ClaimsFromContext(ctx, ut.jwtParser)
	if err != nil {
		// Don't track unauth'd or anonymous requests.
		return nil
	}

	ut.increments = append(ut.increments, increment{c.GetGroupID(), *labels, *counts})
	return nil
}

// Reset clears all recorded increments.
func (ut *Tracker) Reset() {
	ut.increments = nil
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

func addCounts(a, b tables.UsageCounts) tables.UsageCounts {
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	c := tables.UsageCounts{}
	vc := reflect.ValueOf(&c).Elem()
	for i := 0; i < va.NumField(); i++ {
		vc.Field(i).SetInt(va.Field(i).Int() + vb.Field(i).Int())
	}
	return c
}
