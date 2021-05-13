package peerset

import (
	"math/rand"
)

type PeerSet struct {
	PreferredPeers []string
	FallbackPeers  []string
	FailedPeers    []string
	i              int // next peer index
}

func New(preferredPeers, fallbackPeers []string) *PeerSet {
	return &PeerSet{
		i:              0,
		PreferredPeers: preferredPeers,
		FallbackPeers:  fallbackPeers,
		FailedPeers:    nil,
	}
}

func NewRead(localhost string, preferredPeers, fallbackPeers []string) *PeerSet {
	// we'll reorder preferredPeers into this new slice.
	reordered := make([]string, 0, len(preferredPeers))

	// If localhost is present in the preferredPeers list, move it to the
	// front. (Skip a network lookup if we can)
	for i := len(preferredPeers) - 1; i >= 0; i-- {
		p := preferredPeers[i]
		if p == localhost {
			preferredPeers = append(preferredPeers, preferredPeers[i+1:]...)
			reordered = append(reordered, localhost)
			break
		}
	}

	// Shuffle the rest of the preferredPeers, to try to distribute
	// load more evenly and avoid "hot-keys" becoming a problem.
	rand.Shuffle(len(preferredPeers), func(i, j int) {
		preferredPeers[i], preferredPeers[j] = preferredPeers[j], preferredPeers[i]
	})
	reordered = append(reordered, preferredPeers...)

	return New(reordered, fallbackPeers)
}

func (p *PeerSet) MarkPeerAsFailed(failedPeer string) {
	for _, peer := range p.PreferredPeers {
		if peer == failedPeer {
			p.FailedPeers = append(p.FailedPeers, failedPeer)
			return
		}
	}
}

// GetNextPeerAndHandoff returns the next available peer and a handoff peer
// that can be specified as the "hinted handoff peer". When all peers have been
// exhausted, the empty string will be returned.
func (p *PeerSet) GetNextPeerAndHandoff() (string, string) {
	// A function we can defer to increment our peer counter.
	increment := func() {
		p.i += 1
	}

	i := p.i
	numPreferred := len(p.PreferredPeers)
	if i < numPreferred {
		// Return preferred peers if they haven't yet been exhausted.
		// There are no hinted handoffs to return yet.
		defer increment()
		return p.PreferredPeers[i], ""
	}
	i -= numPreferred

	if i < len(p.FallbackPeers) && i < len(p.FailedPeers) {
		defer increment()
		return p.FallbackPeers[i], p.FailedPeers[i]
	}
	return "", ""
}

// GetNextPeer is as convenience method for GetNextPeerAndHandoff that discards
// the handoff when it's not needed.
func (p *PeerSet) GetNextPeer() string {
	peer, _ := p.GetNextPeerAndHandoff()
	return peer
}

// GetBackfillTargets returns the last used peer (which we assume was the one
// that successfully returned a value) and a list of the skipped peers before
// it, if they were preferred peers.
//
// N.B. Backfill is NOT based on errors encountered. It assumes that the last
// peer a caller read from GetNextPeer was the one containing the item, and all
// previous peers lacked it. Therefore, the last peer is returned as a source
// and all previous peers are returned as destinations for the backfill.
// Secondary hosts can never be backfilled, but they can be used as sources for
// a backfill.
func (p *PeerSet) GetBackfillTargets() (string, []string) {
	lastUsedIndex := p.i - 1
	if lastUsedIndex < len(p.PreferredPeers) {
		return p.PreferredPeers[lastUsedIndex], p.PreferredPeers[:lastUsedIndex]
	}
	lastUsedIndex -= len(p.PreferredPeers)
	if lastUsedIndex < len(p.FallbackPeers) {
		return p.FallbackPeers[lastUsedIndex], p.PreferredPeers
	}
	return "", []string{}
}
