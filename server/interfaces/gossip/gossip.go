// Package gossip defines the gossip-related service interfaces used by
// the BuildBuddy server. It is a small leaf package so that consumers of
// the umbrella //server/interfaces package don't have to drag
// hashicorp/serf + memberlist (and their transitive msgpack, go-metrics,
// immutable-radix, etc. dependencies) into their build closures.
package gossip

import (
	"context"

	"github.com/hashicorp/serf/serf"
)

// Listener receives gossip events from the GossipService.
type Listener interface {
	OnEvent(eventType serf.EventType, event serf.Event)
}

// Service is the gossip/membership manager exposed to the rest of the server.
type Service interface {
	ListenAddr() string
	JoinList() []string
	AddListener(listener Listener)
	LocalMember() serf.Member
	Members() []serf.Member
	SetTags(tags map[string]string) error
	SendUserEvent(name string, payload []byte, coalesce bool) error
	Query(name string, payload []byte, params *serf.QueryParam) (*serf.QueryResponse, error)
	Statusz(ctx context.Context) string
	Leave() error
	Shutdown() error
}
