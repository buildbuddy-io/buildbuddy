package peerset_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/peerset"
	"github.com/stretchr/testify/assert"
)

type wantPeerHandoff struct {
	peer    string
	handoff string
}

func contains(needle string, haystack []string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

func TestGetNextPeer(t *testing.T) {
	tests := []struct {
		p           *peerset.PeerSet
		peersToFail []string
		expected    []wantPeerHandoff
	}{
		{
			peerset.New([]string{"a"}, []string{"b", "c"}),
			[]string{"a"},
			[]wantPeerHandoff{
				{"a", ""},
				{"b", "a"},
				{"", ""},
			},
		},
		{
			peerset.New([]string{"a", "b", "c"}, []string{}),
			[]string{"b"},
			[]wantPeerHandoff{
				{"a", ""},
				{"b", ""},
				{"c", ""},
				{"", ""},
			},
		},
		{
			peerset.New([]string{"a", "b", "c"}, []string{"d", "e", "f", "g"}),
			[]string{"a", "c"},
			[]wantPeerHandoff{
				{"a", ""},
				{"b", ""},
				{"c", ""},
				{"d", "a"},
				{"e", "c"},
				{"", ""},
			},
		},
		{
			peerset.New([]string{"a", "b", "c"}, []string{"d", "e", "f", "g"}),
			[]string{"c", "d"},
			[]wantPeerHandoff{
				{"a", ""},
				{"b", ""},
				{"c", ""},
				{"d", "c"},
				{"e", "c"},
				{"", ""},
			},
		},
		{
			peerset.New([]string{"a", "b", "c"}, []string{"d", "e", "f", "g"}),
			[]string{"b", "c", "d", "e"},
			[]wantPeerHandoff{
				{"a", ""},
				{"b", ""},
				{"c", ""},
				{"d", "b"},
				{"e", "b"},
				{"f", "b"},
				{"g", "c"},
				{"", ""},
			},
		},
	}

	for _, test := range tests {
		for _, want := range test.expected {
			peer, handoff := test.p.GetNextPeerAndHandoff()
			if contains(peer, test.peersToFail) {
				test.p.MarkPeerAsFailed(peer)
			}
			assert.Equal(t, want.peer, peer)
			assert.Equal(t, want.handoff, handoff)
		}
	}
}

func TestGetBackfillTargets(t *testing.T) {
	tests := []struct {
		p                     *peerset.PeerSet
		peersToFail           []string
		expectedPeers         []string
		expectedBackfillHosts []string
	}{
		{
			peerset.New([]string{"a"}, []string{"b", "c"}),
			[]string{"a"},
			[]string{"a", "b", ""},
			[]string{},
		},
		{
			peerset.New([]string{"a", "b", "c"}, []string{}),
			[]string{"b"},
			[]string{"a", "b", "c", ""},
			[]string{"a"},
		},
		{
			peerset.New([]string{"a", "b", "c"}, []string{"d", "e", "f", "g"}),
			[]string{"a", "b"},
			[]string{"a", "b", "c", "d", "e", ""},
			[]string{"c"},
		},
		{
			peerset.New([]string{"a", "b", "c"}, []string{"d", "e", "f"}),
			[]string{},
			[]string{"a", "b", "c", ""},
			[]string{"a", "b"},
		},
	}

	for _, test := range tests {
		for i := 0; i < len(test.expectedPeers); i++ {
			peer := test.p.GetNextPeer()
			assert.Equal(t, test.expectedPeers[i], peer)
			if contains(peer, test.peersToFail) {
				test.p.MarkPeerAsFailed(peer)
			}
		}
		_, backfillHosts := test.p.GetBackfillTargets()
		assert.Equal(t, test.expectedBackfillHosts, backfillHosts)
	}
}

func TestGetBackfillTargetsWithBlockBackfills(t *testing.T) {
	// PreferredPeers contains a mix of peers that should be readable
	// (a, b, c, d) but b and d are blocked from backfill (e.g. non-canonical
	// same-zone peers that may hold a read-through cached copy).
	for _, tc := range []struct {
		name           string
		preferred      []string
		fallback       []string
		blockBackfills []string
		consume        int // number of peers to consume via GetNextPeer before computing backfill
		wantSource     string
		wantTargets    []string
	}{
		{
			name:           "hit on canonical primary skips blocked targets",
			preferred:      []string{"a", "b", "c", "d"},
			blockBackfills: []string{"b", "d"},
			consume:        3, // hit on "c"
			wantSource:     "c",
			wantTargets:    []string{"a"},
		},
		{
			name:           "hit on blocked peer still skips blocked earlier peers",
			preferred:      []string{"a", "b", "c", "d"},
			blockBackfills: []string{"b", "d"},
			consume:        2, // hit on "b" (blocked)
			wantSource:     "b",
			wantTargets:    []string{"a"},
		},
		{
			name:           "nil block list = current behavior (no filter)",
			preferred:      []string{"a", "b", "c", "d"},
			blockBackfills: nil,
			consume:        3,
			wantSource:     "c",
			wantTargets:    []string{"a", "b"},
		},
		{
			name:           "block list covering all preferred peers leaves no targets",
			preferred:      []string{"a", "b", "c", "d"},
			blockBackfills: []string{"a", "b"},
			consume:        3,
			wantSource:     "c",
			wantTargets:    []string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := peerset.New(tc.preferred, tc.fallback)
			p.BlockBackfills = tc.blockBackfills
			for i := 0; i < tc.consume; i++ {
				p.GetNextPeer()
			}
			source, targets := p.GetBackfillTargets()
			assert.Equal(t, tc.wantSource, source)
			assert.Equal(t, tc.wantTargets, targets)
		})
	}
}
