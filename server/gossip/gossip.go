package gossip

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/network"

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

func (gm *GossipManager) SendUserEvent(name string, payload []byte, coalesce bool) error {
	return gm.serfInstance.UserEvent(name, payload, coalesce)
}

func (gm *GossipManager) Query(name string, payload []byte, params *serf.QueryParam) (*serf.QueryResponse, error) {
	return gm.serfInstance.Query(name, payload, params)
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
		//		log.Debug(s)
	} else if strings.Contains(s, "[INFO]") {
		//		log.Info(s)
	} else {
		log.Warning(s)
	}

	return len(d), nil
}

func NewGossipManager(listenAddress string, join []string) (*GossipManager, error) {
	log.Printf("Starting GossipManager on %q", listenAddress)

	subLog := log.NamedSubLogger(fmt.Sprintf("GossipManager(%s)", listenAddress))

	bindAddr, bindPort, err := network.ParseAddress(listenAddress)
	if err != nil {
		return nil, err
	}
	memberlistConfig := memberlist.DefaultLANConfig()
	memberlistConfig.BindAddr = bindAddr
	memberlistConfig.BindPort = bindPort
	memberlistConfig.LogOutput = &logWriter{subLog}

	serfConfig := serf.DefaultConfig()
	serfConfig.NodeName = listenAddress
	serfConfig.MemberlistConfig = memberlistConfig
	serfConfig.LogOutput = &logWriter{subLog}
	// this is the maximum value that serf supports.
	serfConfig.UserEventSizeLimit = 9 * 1024
	serfConfig.BroadcastTimeout = time.Second

	// spoiler: gossip girl was actually a:
	gossipMan := &GossipManager{
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
		log.Debugf("I am %q, attempting to join %+v", listenAddress, otherNodes)
		_, err := serfInstance.Join(otherNodes, false)
		if err != nil {
			log.Debugf("Join failed: %s", err)
		}
	}
	gossipMan.serfInstance = serfInstance
	return gossipMan, nil
}
