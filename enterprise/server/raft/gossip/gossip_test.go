package gossip_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
)

type testBroker struct {
	memberEvent     func(updateType serf.EventType, member *serf.Member)
	consumeMetadata func(name string, payload []byte)
	sendMetadataCB  func(name string, payload []byte) error
}

func (b *testBroker) MemberEvent(updateType serf.EventType, member *serf.Member) {
	if b.memberEvent != nil {
		b.memberEvent(updateType, member)
	}
}

func (b *testBroker) RegisterMetadataProvider(sendMetadataCB func(name string, payload []byte) error) {
	b.sendMetadataCB = sendMetadataCB
}
func (b *testBroker) SendMetadata(name string, payload []byte) error {
	return b.sendMetadataCB(name, payload)
}
func (b *testBroker) ConsumeMetadata(name string, payload []byte) {
	if b.consumeMetadata != nil {
		b.consumeMetadata(name, payload)
	}
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

func TestMetadata(t *testing.T) {
	node1Addr := localAddr(t)
	node1Broker := &testBroker{}
	node1 := newGossipManager(t, node1Addr, nil /*=seeds*/, node1Broker)
	defer node1.Shutdown()

	var wg sync.WaitGroup
	mu := sync.Mutex{}
	sentMetadata := make(map[string][]byte, 0)
	consumeMetadataCB := func(name string, gotPayload []byte) {
		mu.Lock()
		defer mu.Unlock()
		sentPayload, ok := sentMetadata[name]
		require.True(t, ok)
		require.Equal(t, sentPayload, gotPayload)
		delete(sentMetadata, name)
		wg.Done()
	}

	node2Broker := &testBroker{
		consumeMetadata: consumeMetadataCB,
	}
	node2 := newGossipManager(t, localAddr(t), []string{node1Addr}, node2Broker)
	defer node2.Shutdown()

	// send a bunch of metadata and wait for it to be received, timeout
	// after 10 seconds.
	for i := 0; i < 10; i++ {
		d, buf := testdigest.NewRandomDigestBuf(t, 2<<i)
		mu.Lock()
		sentMetadata[d.GetHash()] = buf
		mu.Unlock()

		err := node1Broker.SendMetadata(d.GetHash(), buf)
		require.Nil(t, err)
		wg.Add(1)
	}

	doneChannel := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneChannel)
	}()

	select {
	case <-time.After(10 * time.Second):
		t.Fatalf("Not all metadata was received. Missing: %d", len(sentMetadata))
	case <-doneChannel:
		break
	}
}
