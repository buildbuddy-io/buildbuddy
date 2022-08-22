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

func TestNewRead(t *testing.T) {
	localhost := "a"

	tests := []struct {
		preferred []string
		fallback  []string
	}{
		{
			[]string{localhost},
			[]string{"b", "c"},
		},
		{
			[]string{"b", "c", localhost},
			[]string{},
		},
		{
			[]string{"b", localhost, "c"},
			[]string{"d", "e", "f", "g"},
		},
	}

	for _, test := range tests {
		p := peerset.NewRead("a", test.preferred, test.fallback)
		i := 0
		for peer, handoff := p.GetNextPeerAndHandoff(); peer != ""; peer, handoff = p.GetNextPeerAndHandoff() {
			// Test that hinted handoffs only refer to preferred nodes.
			if handoff != "" {
				assert.Contains(t, test.preferred, handoff)
			}
			// Test that if localhost was a peer, it was returned first.
			if i == 0 && contains(localhost, test.preferred) {
				assert.Equal(t, localhost, peer)
			}
			// Test that the peer came from preferred or fallback.
			assert.Contains(t, append(test.preferred, test.fallback...), peer)
			i += 1
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
