package store

import (
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

type Store struct {
	rootDir string
	fileDir string

	nodeHost      *dragonboat.NodeHost
	gossipManager *gossip.GossipManager
	sender        *sender.Sender
	apiClient     *client.APIClient
}

func New(rootDir, fileDir string, nodeHost *dragonboat.NodeHost, gossipManager *gossip.GossipManager, sender *sender.Sender, apiClient *client.APIClient) *Store {
	return &Store{
		rootDir:       rootDir,
		fileDir:       fileDir,
		nodeHost:      nodeHost,
		gossipManager: gossipManager,
		sender:        sender,
		apiClient:     apiClient,
	}
}

func containsMetaRange(rd *rfpb.RangeDescriptor) bool {
	r := rangemap.Range{Left: rd.Left, Right: rd.Right}
	return r.Contains([]byte{constants.MinByte}) && r.Contains([]byte{constants.MaxByte - 1})
}

// We need to implement the RangeTracker interface so that stores opened and
// closed on this node will notify us when their range appears and disappears.
// We'll use this information to drive the range tags we broadcast.
func (s *Store) AddRange(rd *rfpb.RangeDescriptor) {
	log.Printf("AddRange: %+v", rd)
	if containsMetaRange(rd) {
		// If we own the metarange, use gossip to notify other nodes
		// of that fact.
		buf, err := proto.Marshal(rd)
		if err != nil {
			log.Errorf("Error marshaling metarange descriptor: %s", err)
			return
		}
		s.gossipManager.SetTag(constants.MetaRangeTag, string(buf))
	}
}

func (s *Store) RemoveRange(rd *rfpb.RangeDescriptor) {
	log.Printf("RemoveRange: %+v", rd)
}

func (s *Store) ReplicaFactoryFn(clusterID, nodeID uint64) dbsm.IOnDiskStateMachine {
	return replica.New(s.rootDir, s.fileDir, clusterID, nodeID, s, s.sender, s.apiClient)
}
