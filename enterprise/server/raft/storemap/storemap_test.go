package storemap

import (
	"context"
	"encoding/base64"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestStoreStatusStateMachine(t *testing.T) {
	clock := clockwork.NewFakeClock()
	mockGossipManager := &mockGossipManager{
		members: make(map[string]serf.Member),
	}

	sm := create(mockGossipManager, clock, log.NamedSubLogger("test"))

	nhid := "node-1"
	usage := &rfpb.StoreUsage{
		Node: &rfpb.NodeDescriptor{
			Nhid: nhid,
		},
	}
	buf, err := proto.Marshal(usage)
	require.NoError(t, err)

	storeTags := make(map[string]string, 1)
	storeTags[constants.StoreUsageTag] = base64.StdEncoding.EncodeToString(buf)

	repls := []*rfpb.ReplicaDescriptor{
		{Nhid: proto.String(nhid)},
	}

	// Node joins and is alive -> suspect
	mockGossipManager.setMemberTags(nhid, storeTags)
	mockGossipManager.setMemberStatus(nhid, serf.StatusAlive)

	sm.updateStoreDetail(nhid, usage, serf.StatusAlive)
	res := sm.DivideByStatus(repls)
	require.Len(t, res.SuspectReplicas, 1, "SuspectReplicas")
	require.Len(t, res.LiveReplicas, 0, "LiveReplicas")
	require.Len(t, res.DeadReplicas, 0, "DeadReplicas")

	// 30 seconds pass, node still alive -> available
	clock.Advance(31 * time.Second)
	res = sm.DivideByStatus(repls)
	require.Len(t, res.SuspectReplicas, 0, "SuspectReplicas")
	require.Len(t, res.LiveReplicas, 1, "LiveReplicas")
	require.Len(t, res.DeadReplicas, 0, "DeadReplicas")

	// Node goes down -> unknown
	log.Infof("set to leaving")
	mockGossipManager.setMemberStatus(nhid, serf.StatusLeaving)
	sm.updateStoreDetail(nhid, usage, serf.StatusLeaving)
	res = sm.DivideByStatus(repls)
	require.Len(t, res.SuspectReplicas, 0, "SuspectReplicas")
	require.Len(t, res.LiveReplicas, 0, "LiveReplicas")
	require.Len(t, res.DeadReplicas, 0, "DeadReplicas")

	// 5 minutes pass, node still down -> dead
	clock.Advance(6 * time.Minute)
	res = sm.DivideByStatus(repls)
	require.Len(t, res.SuspectReplicas, 0, "SuspectReplicas")
	require.Len(t, res.LiveReplicas, 0, "LiveReplicas")
	require.Len(t, res.DeadReplicas, 1, "DeadReplicas")

	// Node come back again -> suspect
	mockGossipManager.setMemberStatus(nhid, serf.StatusAlive)
	sm.updateStoreDetail(nhid, usage, serf.StatusAlive)

	res = sm.DivideByStatus(repls)
	require.Len(t, res.SuspectReplicas, 1, "SuspectReplicas")
	require.Len(t, res.LiveReplicas, 0, "LiveReplicas")
	require.Len(t, res.DeadReplicas, 0, "DeadReplicas")
}

func TestMissingUsageTag(t *testing.T) {
	clock := clockwork.NewFakeClock()
	mockGossipManager := &mockGossipManager{
		members: make(map[string]serf.Member),
	}

	sm := create(mockGossipManager, clock, log.NamedSubLogger("test"))

	nhid := "node-1"

	repls := []*rfpb.ReplicaDescriptor{
		{Nhid: proto.String(nhid)},
	}

	// Node joins and is alive, but no usage tag -> unknown
	mockGossipManager.setMemberStatus(nhid, serf.StatusAlive)

	sm.updateStoreDetail(nhid, nil, serf.StatusAlive)
	res := sm.DivideByStatus(repls)
	require.Len(t, res.SuspectReplicas, 0, "SuspectReplicas")
	require.Len(t, res.LiveReplicas, 0, "LiveReplicas")
	require.Len(t, res.DeadReplicas, 0, "DeadReplicas")

	// 5 minutes later, still no usage tag -> dead
	clock.Advance(6 * time.Minute)
	res = sm.DivideByStatus(repls)
	require.Len(t, res.SuspectReplicas, 0, "SuspectReplicas")
	require.Len(t, res.LiveReplicas, 0, "LiveReplicas")
	require.Len(t, res.DeadReplicas, 1, "DeadReplicas")

	// Set usage tag -> suspect
	usage := &rfpb.StoreUsage{
		Node: &rfpb.NodeDescriptor{
			Nhid: nhid,
		},
	}
	buf, err := proto.Marshal(usage)
	require.NoError(t, err)

	storeTags := make(map[string]string, 1)
	storeTags[constants.StoreUsageTag] = base64.StdEncoding.EncodeToString(buf)

	mockGossipManager.setMemberTags(nhid, storeTags)
	res = sm.DivideByStatus(repls)
	require.Len(t, res.SuspectReplicas, 1, "SuspectReplicas")
	require.Len(t, res.LiveReplicas, 0, "LiveReplicas")
	require.Len(t, res.DeadReplicas, 0, "DeadReplicas")
}

type mockGossipManager struct {
	mu       sync.Mutex
	members  map[string]serf.Member
	listener interfaces.GossipListener

	t *testing.T
}

func (m *mockGossipManager) Members() []serf.Member {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Collect(maps.Values(m.members))
}

func (m *mockGossipManager) setMemberStatus(nhid string, status serf.MemberStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	member := m.members[nhid]
	member.Status = status
	m.members[nhid] = member
}

func (m *mockGossipManager) setMemberTags(nhid string, tags map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	member := m.members[nhid]
	member.Tags = tags
	m.members[nhid] = member
}

func (m *mockGossipManager) AddListener(l interfaces.GossipListener) {
	m.listener = l
}

func (m *mockGossipManager) ListenAddr() string {
	return ""
}

func (m *mockGossipManager) JoinList() []string {
	return nil
}

func (m *mockGossipManager) LocalMember() serf.Member {
	return serf.Member{}
}

func (m *mockGossipManager) SetTags(tags map[string]string) error {
	return nil
}

func (m *mockGossipManager) SendUserEvent(name string, payload []byte, coalesce bool) error {
	return nil
}
func (m *mockGossipManager) Query(name string, payload []byte, params *serf.QueryParam) (*serf.QueryResponse, error) {
	return nil, nil
}
func (m *mockGossipManager) Statusz(ctx context.Context) string {
	return ""
}
func (m *mockGossipManager) Leave() error {
	return nil
}
func (m *mockGossipManager) Shutdown() error {
	return nil
}
