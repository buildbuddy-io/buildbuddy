package peerset

import (
	"math/rand"
)

const maxFailedFallbackPeers = 3

type PeerSet struct {
	PreferredPeers      []string
	FallbackPeers       []string
	FailedPeers         []string
	FailedFallbackPeers []string
	i                   int // next peer index
}

func New(preferredPeers, fallbackPeers []string) *PeerSet {
	return &PeerSet{
		i:                   0,
		PreferredPeers:      preferredPeers,
		FallbackPeers:       fallbackPeers,
		FailedPeers:         nil,
		FailedFallbackPeers: nil,
	}
}

func NewRead(localhost string, preferredPeers, fallbackPeers []string) *PeerSet {
	rest := make([]string, 0, len(preferredPeers))
	first := make([]string, 0, 1)
	for _, p := range preferredPeers {
		if p == localhost {
			first = append(first, p)
		} else {
			rest = append(rest, p)
		}
	}
	rand.Shuffle(len(rest), func(i, j int) {
		rest[i], rest[j] = rest[j], rest[i]
	})
	return New(append(first, rest...), fallbackPeers)
}

func (p *PeerSet) MarkPeerAsFailed(failedPeer string) {
	for _, peer := range p.PreferredPeers {
		if peer == failedPeer {
			p.FailedPeers = append(p.FailedPeers, failedPeer)
			return
		}
	}
	p.FailedFallbackPeers = append(p.FailedFallbackPeers, failedPeer)
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

	// Give up if we have already tried enough fallback peers.
	if len(p.FailedFallbackPeers) >= maxFailedFallbackPeers {
		return "", ""
	}

	// Once we're out of preferred peers, start going through the fallback
	// peers.
	i -= numPreferred

	fallbackIdx := i
	// If a fallback peer is marked as failed after this function returns, it
	// will be added to the list of failed fallback peers. We use this fact to
	// prevent failedIdx from advancing to the next failed peer until we find a
	// good fallback peer.
	failedIdx := i - len(p.FailedFallbackPeers)

	if fallbackIdx < len(p.FallbackPeers) && failedIdx < len(p.FailedPeers) {
		defer increment()
		return p.FallbackPeers[fallbackIdx], p.FailedPeers[failedIdx]
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
// N.B. Backfill assumes that the last peer a caller read from GetNextPeer
// contained the item, and all previous peers, which did not fail, lacked it.
// Therefore, the last peer is returned as a source and all previous peers are
// returned as destinations for the backfill.
//
// Secondary hosts can never be backfilled, but they can be used as sources for
// a backfill.
func (p *PeerSet) GetBackfillTargets() (string, []string) {
	lastUsedIndex := p.i - 1

	source := ""
	targets := make([]string, 0)

	if lastUsedIndex < len(p.PreferredPeers) {
		source, targets = p.PreferredPeers[lastUsedIndex], p.PreferredPeers[:lastUsedIndex]
	} else {
		lastUsedIndex -= len(p.PreferredPeers)
		if lastUsedIndex < len(p.FallbackPeers) {
			source, targets = p.FallbackPeers[lastUsedIndex], p.PreferredPeers
		}
	}
	// Ensure no failed peers are returned.
	for _, f := range p.FailedPeers {
		if f == source {
			return "", []string{}
		}
	}

	filteredTargets := make([]string, 0, len(targets))
	for _, t := range targets {
		isFailed := false
		for _, f := range p.FailedPeers {
			if t == f {
				isFailed = true
				break
			}
		}
		if !isFailed {
			filteredTargets = append(filteredTargets, t)
		}
	}
	return source, filteredTargets
}
