package gossip

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/network"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

// A Broker listens for serf events.
type Listener interface {
	OnEvent(eventType serf.EventType, event serf.Event)
}

// A GossipManager will listen (on `advertiseAddress`), connect to `seeds`,
// and gossip any information provided via the broker interface. To leave
// gracefully, clients should call GossipManager.Leave() followed by
// GossipManager.Shutdown().
type GossipManager struct {
	cancelFunc    context.CancelFunc
	serfInstance  *serf.Serf
	serfEventChan chan serf.Event

	ListenAddr string
	Join       []string

	mu        sync.Mutex
	listeners []Listener
	tags      map[string]string
}

func (gm *GossipManager) processEvents() {
	for {
		select {
		case event := <-gm.serfEventChan:
			gm.mu.Lock()
			listeners := gm.listeners
			gm.mu.Unlock()

			for _, listener := range listeners {
				listener.OnEvent(event.EventType(), event)
			}
		}
	}
}

func (gm *GossipManager) AddListener(listener Listener) {
	if listener == nil {
		log.Error("listener cannot be nil")
		return
	}
	// The listener may be added after the gossip manager has already been
	// started, so notify it of any already connected nodes.
	existingMembersEvent := serf.MemberEvent{
		Type:    serf.EventMemberUpdate,
		Members: gm.serfInstance.Members(),
	}
	listener.OnEvent(existingMembersEvent.Type, existingMembersEvent)
	gm.mu.Lock()
	gm.listeners = append(gm.listeners, listener)
	gm.mu.Unlock()
}

func (gm *GossipManager) LocalMember() serf.Member {
	return gm.serfInstance.LocalMember()
}
func (gm *GossipManager) Members() []serf.Member {
	return gm.serfInstance.Members()
}
func (gm *GossipManager) Leave() error {
	return gm.serfInstance.Leave()
}
func (gm *GossipManager) Shutdown() error {
	gm.cancelFunc()
	return gm.serfInstance.Shutdown()
}
func (gm *GossipManager) SetTags(tags map[string]string) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	for tagName, tagValue := range tags {
		if tagValue == "" {
			delete(gm.tags, tagName)
		} else {
			gm.tags[tagName] = tagValue
		}
	}
	return gm.serfInstance.SetTags(gm.tags)
}

func (gm *GossipManager) getTags() map[string]string {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	rmap := make(map[string]string, len(gm.tags))
	for tagName, tagValue := range gm.tags {
		rmap[tagName] = tagValue
	}
	return rmap
}

func (gm *GossipManager) SendUserEvent(name string, payload []byte, coalesce bool) error {
	return gm.serfInstance.UserEvent(name, payload, coalesce)
}

func (gm *GossipManager) Query(name string, payload []byte, params *serf.QueryParam) (*serf.QueryResponse, error) {
	return gm.serfInstance.Query(name, payload, params)
}

func formatMember(m serf.Member) string {
	return fmt.Sprintf("Name: %s Addr: %s:%d Status: %+v", m.Name, m.Addr.String(), m.Port, m.Status)
}

func (gm *GossipManager) Statusz(ctx context.Context) string {
	buf := "<pre>"
	thisNode := gm.LocalMember()
	buf += fmt.Sprintf("Node: %+v\n", formatMember(thisNode))

	buf += "Tags:\n"
	tagStrings := make([]string, len(gm.getTags()))
	for tagKey, tagValue := range gm.getTags() {
		tagStrings = append(tagStrings, fmt.Sprintf("\t%q => %q\n", tagKey, tagValue))
	}
	sort.Strings(tagStrings)
	for _, tagString := range tagStrings {
		buf += tagString
	}

	buf += "Peers:\n"
	peers := gm.Members()
	sort.Slice(peers, func(i, j int) bool { return peers[i].Name < peers[j].Name })
	for _, peerMember := range peers {
		if peerMember.Name == thisNode.Name {
			continue
		}
		buf += fmt.Sprintf("\t%s\n", formatMember(peerMember))
	}

	buf += "Stats:\n"
	var statStrings []string
	for k, v := range gm.serfInstance.Stats() {
		statStrings = append(statStrings, fmt.Sprintf("\t%s: %s\n", k, v))
	}
	sort.Strings(statStrings)
	for _, statString := range statStrings {
		buf += statString
	}
	buf += "</pre>"
	return buf
}

// Adapt our log writer into one that is compatible with
// serf.
type logWriter struct {
	log.Logger
}

func (lw *logWriter) Write(d []byte) (int, error) {
	s := strings.TrimSuffix(string(d), "\n")
	// Gossip logs are very verbose and there is
	// very little useful info in DEBUG/INFO level logs.
	if strings.Contains(s, "[DEBUG]") {
		log.Debug(s)
	} else if strings.Contains(s, "[INFO]") {
		log.Info(s)
	} else {
		log.Warning(s)
	}

	return len(d), nil
}

func NewGossipManager(name, listenAddress string, join []string) (*GossipManager, error) {
	log.Printf("Starting GossipManager on %q", listenAddress)

	subLog := log.NamedSubLogger(fmt.Sprintf("GossipManager(%s)", name))

	bindAddr, bindPort, err := network.ParseAddress(listenAddress)
	if err != nil {
		return nil, err
	}
	memberlistConfig := memberlist.DefaultLANConfig()
	memberlistConfig.BindAddr = bindAddr
	memberlistConfig.BindPort = bindPort
	memberlistConfig.LogOutput = &logWriter{subLog}

	serfConfig := serf.DefaultConfig()
	serfConfig.NodeName = name
	serfConfig.MemberlistConfig = memberlistConfig
	serfConfig.LogOutput = &logWriter{subLog}
	// this is the maximum value that serf supports.
	serfConfig.UserEventSizeLimit = 9 * 1024
	serfConfig.BroadcastTimeout = time.Second

	serfConfig.CoalescePeriod = 10 * time.Second
	serfConfig.QuiescentPeriod = time.Second

	serfConfig.UserCoalescePeriod = 10 * time.Second
	serfConfig.UserQuiescentPeriod = time.Second

	ctx, cancel := context.WithCancel(context.TODO())

	// spoiler: gossip girl was actually a:
	gossipMan := &GossipManager{
		cancelFunc:    cancel,
		ListenAddr:    listenAddress,
		Join:          join,
		serfEventChan: make(chan serf.Event, 16),
		mu:            sync.Mutex{},
		listeners:     make([]Listener, 0),
		tags:          make(map[string]string, 0),
	}
	serfConfig.EventCh = gossipMan.serfEventChan
	go gossipMan.processEvents()

	serfInstance, err := serf.Create(serfConfig)
	if err != nil {
		return nil, err
	}

	otherNodes := make([]string, 0, len(join))
	for _, node := range join {
		if node != listenAddress {
			otherNodes = append(otherNodes, node)
		}
	}
	if len(otherNodes) > 0 {
		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			retrier := retry.New(ctx, &retry.Options{
				InitialBackoff: 10 * time.Second,
				MaxBackoff:     180 * time.Second,
				Multiplier:     2,
			})
			once := sync.Once{}
			for retrier.Next() {
				log.Debugf("I am %q, attempting to join %+v", listenAddress, otherNodes)
				_, err := serfInstance.Join(otherNodes, false)
				once.Do(wg.Done)
				if err == nil {
					return
				}
				log.Debugf("Join failed: %s", err)
			}
			log.Warningf("Gossip: %q failed to join other nodes: %+v", listenAddress, otherNodes)
		}()
		wg.Wait()
	}
	gossipMan.serfInstance = serfInstance
	statusz.AddSection("gossip_manager", "Serf Gossip Network", gossipMan)
	return gossipMan, nil
}
