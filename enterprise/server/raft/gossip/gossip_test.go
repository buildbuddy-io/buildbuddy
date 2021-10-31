package gossip_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
)

type testBroker struct {
	memberEvent func(updateType serf.EventType, member *serf.Member)
	sendTagFn   func(tagName, tagValue string) error
}

func (b *testBroker) MemberEvent(updateType serf.EventType, member *serf.Member) {
	if b.memberEvent != nil {
		b.memberEvent(updateType, member)
	}
}

func (b *testBroker) RegisterTagProviderFn(setTag func(tagName, tagValue string) error) {
	b.sendTagFn = setTag
}

func (b *testBroker) SetTag(tagName, tagValue string) error {
	return b.sendTagFn(tagName, tagValue)
}

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", app.FreePort(t))
}

func newGossipManager(t *testing.T, addr string, seeds []string, broker gossip.Broker) *gossip.GossipManager {
	node, err := gossip.NewGossipManager(addr, seeds)
	require.Nil(t, err)
	require.NotNil(t, node)
	node.AddBroker(broker)
	return node
}

func TestDiscovery(t *testing.T) {
	node1Addr := localAddr(t)
	node1 := newGossipManager(t, node1Addr, nil /*=seeds*/, &testBroker{})
	defer node1.Shutdown()

	sawNode1 := make(chan struct{})
	memberEventCB := func(updateType serf.EventType, member *serf.Member) {
		if updateType == serf.EventMemberJoin {
			if fmt.Sprintf("%s:%d", member.Addr, member.Port) == node1Addr {
				close(sawNode1)
			}
		}
	}

	node2 := newGossipManager(t, localAddr(t), []string{node1Addr}, &testBroker{memberEvent: memberEventCB})
	defer node2.Shutdown()

	select {
	case <-time.After(10 * time.Second):
		t.Fatalf("Timed out waiting for nodes to discover each other via gossip")
	case <-sawNode1:
		break
	}
}

func TestSendTag(t *testing.T) {
	node1Addr := localAddr(t)
	broker1 := &testBroker{}
	node1 := newGossipManager(t, node1Addr, nil /*=seeds*/, broker1)
	defer node1.Shutdown()

	sawTag := make(chan struct{})
	memberEventCB := func(updateType serf.EventType, member *serf.Member) {
		tagVal, ok := member.Tags["testTagName"]
		if ok && tagVal == "testTagValue" {
			close(sawTag)
		}
	}

	node2 := newGossipManager(t, localAddr(t), []string{node1Addr}, &testBroker{memberEvent: memberEventCB})
	defer node2.Shutdown()

	err := broker1.SetTag("testTagName", "testTagValue")
	require.Nil(t, err)

	select {
	case <-time.After(10 * time.Second):
		t.Fatalf("Timed out waiting for tags to be received")
	case <-sawTag:
		break
	}
}
