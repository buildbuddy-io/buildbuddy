package main

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/trafficstats/providerips"
)

func TestReportOverlaps(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ranges []providerips.IPRange
		want   int
	}{
		{
			name: "disjoint",
			ranges: []providerips.IPRange{
				{Provider: "A", CIDR: "10.0.0.0/8"},
				{Provider: "B", CIDR: "11.0.0.0/8"},
				{Provider: "B", CIDR: "11.1.0.0/16"}, // Same provider nesting is fine.
			},
			want: 0,
		},
		{
			name: "nested_in_other_provider",
			ranges: []providerips.IPRange{
				{Provider: "B", CIDR: "10.1.2.0/24"},
				{Provider: "A", CIDR: "10.0.0.0/8"},
			},
			want: 1,
		},
		{
			name: "identical_prefix_two_providers",
			ranges: []providerips.IPRange{
				{Provider: "A", CIDR: "10.0.0.0/16"},
				{Provider: "B", CIDR: "10.0.0.0/16"},
			},
			want: 1,
		},
		{
			name: "enclosing_prefix_not_the_nearest",
			ranges: []providerips.IPRange{
				{Provider: "A", CIDR: "10.0.0.0/8"},
				{Provider: "A", CIDR: "10.0.0.0/24"}, // Sibling of the next entry, not an ancestor.
				{Provider: "B", CIDR: "10.1.0.0/24"}, // Inside A's /8 only.
			},
			want: 1,
		},
		{
			name: "allowed_pair",
			ranges: []providerips.IPRange{
				{Provider: "Azure", CIDR: "4.172.0.0/15"},
				{Provider: "GitHub", CIDR: "4.172.0.0/15"},
				{Provider: "GitHub", CIDR: "3.41.210.0/24"},
				{Provider: "AWS", CIDR: "3.41.128.0/17"},
			},
			want: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := reportOverlaps(tc.ranges); got != tc.want {
				t.Errorf("reportOverlaps() = %d, want %d", got, tc.want)
			}
		})
	}
}
