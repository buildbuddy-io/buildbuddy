package store_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbConfig "github.com/lni/dragonboat/v3/config"
)

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
}

func newGossipManager(t testing.TB, nodeAddr string, seeds []string) *gossip.GossipManager {
	node, err := gossip.NewGossipManager("name-"+nodeAddr, nodeAddr, seeds)
	require.NoError(t, err)
	t.Cleanup(func() {
		node.Shutdown()
	})
	return node
}

type storeFactory struct {
	rootDir     string
	fileDir     string
	gossipAddrs []string
	reg         registry.NodeRegistry
}

func newStoreFactory(t *testing.T) *storeFactory {
	rootDir := testfs.MakeTempDir(t)
	fileDir := filepath.Join(rootDir, "files")
	if err := disk.EnsureDirectoryExists(fileDir); err != nil {
		t.Fatal(err)
	}
	return &storeFactory{
		rootDir: rootDir,
		fileDir: fileDir,
		reg:     registry.NewStaticNodeRegistry(1, nil),
	}
}

type nodeRegistryFactory func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error)

func (nrf nodeRegistryFactory) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	return nrf(nhid, streamConnections, v)
}

type TestingStore struct {
	*store.Store
	NodeHost  *dragonboat.NodeHost
	APIClient *client.APIClient
	Sender    *sender.Sender

	RootDir     string
	RaftAddress string
	GRPCAddress string
}

func (ts *TestingStore) NewReplica(clusterID, nodeID uint64) *replica.Replica {
	return replica.New(ts.RootDir, clusterID, nodeID, ts.Store, []disk.Partition{})
}

func (sf *storeFactory) NewStore(t *testing.T) (*TestingStore, *dragonboat.NodeHost) {
	nodeAddr := localAddr(t)
	gm := newGossipManager(t, nodeAddr, sf.gossipAddrs)
	sf.gossipAddrs = append(sf.gossipAddrs, nodeAddr)

	ts := &TestingStore{
		RaftAddress: localAddr(t),
		GRPCAddress: localAddr(t),
		RootDir:     filepath.Join(sf.rootDir, fmt.Sprintf("store-%d", len(sf.gossipAddrs))),
	}
	require.Nil(t, disk.EnsureDirectoryExists(ts.RootDir))

	reg := sf.reg
	nrf := nodeRegistryFactory(func(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
		return reg, nil
	})

	raftListener := listener.DefaultListener()
	nhc := dbConfig.NodeHostConfig{
		WALDir:         filepath.Join(ts.RootDir, "wal"),
		NodeHostDir:    filepath.Join(ts.RootDir, "nodehost"),
		RTTMillisecond: 1,
		RaftAddress:    ts.RaftAddress,
		Expert: dbConfig.ExpertConfig{
			NodeRegistryFactory: nrf,
		},
		AddressByNodeHostID: false,
		RaftEventListener:   raftListener,
		SystemEventListener: raftListener,
	}
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("error creating NodeHost: %s", err)
	}
	ts.NodeHost = nodeHost

	te := testenv.GetTestEnv(t)
	apiClient := client.NewAPIClient(te, nodeHost.ID())
	ts.APIClient = apiClient

	rc := rangecache.New()
	gm.AddListener(rc)
	ts.Sender = sender.New(rc, reg, apiClient)
	reg.AddNode(nodeHost.ID(), ts.RaftAddress, ts.GRPCAddress)
	s, err := store.New(ts.RootDir, nodeHost, gm, ts.Sender, reg, apiClient, []disk.Partition{})
	require.NoError(t, err)
	require.NotNil(t, s)
	s.Start(ts.GRPCAddress)
	ts.Store = s
	return ts, nodeHost
}

func TestAddGetRemoveRange(t *testing.T) {
	sf := newStoreFactory(t)
	s1, _ := sf.NewStore(t)
	r1 := s1.NewReplica(1, 1)

	rd := &rfpb.RangeDescriptor{
		Left:    []byte("a"),
		Right:   []byte("z"),
		RangeId: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}
	s1.AddRange(rd, r1)

	gotRd := s1.GetRange(1)
	require.Equal(t, rd, gotRd)

	s1.RemoveRange(rd, r1)
	gotRd = s1.GetRange(1)
	require.Nil(t, gotRd)
}

func TestStartCluster(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartClusterRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)
}

func TestGetClusterMembership(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartClusterRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	replicas, err := s1.GetClusterMembership(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 3, len(replicas))
}

func TestAddNodeToCluster(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	s4, nh4 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartClusterRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	rd := s1.GetRange(1)
	_, err = s1.AddClusterNode(ctx, &rfpb.AddClusterNodeRequest{
		Range: rd,
		Node: &rfpb.NodeDescriptor{
			Nhid:        nh4.ID(),
			RaftAddress: s4.RaftAddress,
			GrpcAddress: s4.GRPCAddress,
		},
	})
	require.NoError(t, err)

	replicas, err := s1.GetClusterMembership(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 4, len(replicas))
}

func TestRemoveNodeFromCluster(t *testing.T) {
	t.Skip()
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	s4, nh4 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartClusterRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
		nh4.ID(): s4.GRPCAddress,
	})
	require.NoError(t, err)

	rd := s1.GetRange(1)
	_, err = s1.RemoveClusterNode(ctx, &rfpb.RemoveClusterNodeRequest{
		Range:  rd,
		NodeId: 4,
	})
	require.NoError(t, err)

	replicas, err := s1.GetClusterMembership(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 3, len(replicas))
}

func writeRecord(ctx context.Context, t *testing.T, ts *TestingStore, groupID string, sizeBytes int64) *rfpb.FileRecord {
	d, buf := testdigest.NewRandomDigestBuf(t, sizeBytes)
	fr := &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:   resource.CacheType_CAS,
			PartitionId: groupID,
		},
		Digest: d,
	}

	fs := filestore.New(filestore.Opts{
		IsolateByGroupIDs:           true,
		PrioritizeHashInMetadataKey: true,
	})
	fileMetadataKey := metadataKey(t, fr)

	_, err := ts.APIClient.Get(ctx, ts.GRPCAddress)
	require.NoError(t, err)

	writeCloserMetadata := fs.InlineWriter(ctx, d.GetSizeBytes())
	bytesWritten, err := writeCloserMetadata.Write(buf)
	require.NoError(t, err)

	now := time.Now()
	md := &rfpb.FileMetadata{
		FileRecord:      fr,
		StorageMetadata: writeCloserMetadata.Metadata(),
		StoredSizeBytes: int64(bytesWritten),
		LastModifyUsec:  now.UnixMicro(),
		LastAccessUsec:  now.UnixMicro(),
	}
	protoBytes, err := proto.Marshal(md)
	require.NoError(t, err)

	writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   fileMetadataKey,
			Value: protoBytes,
		},
	}).ToProto()
	require.NoError(t, err)
	writeRsp, err := ts.Sender.SyncPropose(ctx, fileMetadataKey, writeReq)
	require.NoError(t, err)
	err = rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
	require.NoError(t, err)

	return fr
}

func metadataKey(t *testing.T, fr *rfpb.FileRecord) []byte {
	fs := filestore.New(filestore.Opts{
		IsolateByGroupIDs:           true,
		PrioritizeHashInMetadataKey: true,
	})
	fk, err := fs.FileMetadataKey(fr)
	require.NoError(t, err)
	return fk
}

func readRecord(ctx context.Context, t *testing.T, ts *TestingStore, fr *rfpb.FileRecord) {
	fk := metadataKey(t, fr)

	err := ts.Sender.Run(ctx, fk, func(c rfspb.ApiClient, h *rfpb.Header) error {
		rc, err := client.RemoteReader(ctx, c, &rfpb.ReadRequest{
			Header:     h,
			FileRecord: fr,
		})
		if err != nil {
			return err
		}
		d := testdigest.ReadDigestAndClose(t, rc)
		require.True(t, proto.Equal(d, fr.GetDigest()))
		return nil
	})
	require.NoError(t, err)
}

func writeNRecords(ctx context.Context, t *testing.T, store *TestingStore, n int) []*rfpb.FileRecord {
	var groupID string
	out := make([]*rfpb.FileRecord, 0, n)
	for i := 0; i < n; i++ {
		if i%10 == 0 {
			g, err := random.RandomString(16)
			require.NoError(t, err)
			groupID = strings.ToLower(g)
		}
		out = append(out, writeRecord(ctx, t, store, groupID, 1000))
	}
	return out
}

func TestSplitMetaRange(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartClusterRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	rd := s1.GetRange(1)

	// Attempting to Split an empty range will always fail. So write a
	// a small number of records before trying to Split.
	writeNRecords(ctx, t, s1, 10)

	// Attempting to Split the metarange should fail.
	_, err = s1.SplitCluster(ctx, &rfpb.SplitClusterRequest{
		Range: rd,
	})
	require.Error(t, err)
}

func headerFromRangeDescriptor(rd *rfpb.RangeDescriptor) *rfpb.Header {
	return &rfpb.Header{RangeId: rd.GetRangeId(), Generation: rd.GetGeneration()}
}

func getStoreWithRangeLease(t testing.TB, stores []*TestingStore, header *rfpb.Header) *TestingStore {
	for _, s := range stores {
		if _, err := s.LeasedRange(header); err == nil {
			return s
		}
	}
	t.Fatalf("No store found holding rangelease for header %+v", header)
	return nil
}

func TestSplitNonMetaRange(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3}
	err := bringup.SendStartClusterRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	// Attempting to Split an empty range will always fail. So write a
	// a small number of records before trying to Split.
	written := writeNRecords(ctx, t, s1, 50)

	rd := s1.GetRange(2)
	header := headerFromRangeDescriptor(rd)
	s := getStoreWithRangeLease(t, stores, header)
	_, err = s.SplitCluster(ctx, &rfpb.SplitClusterRequest{
		Header: header,
		Range:  rd,
	})
	require.NoError(t, err)

	// Expect that a new cluster was added with clusterID = 4
	// having 3 replicas.
	replicas, err := s1.GetClusterMembership(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, 3, len(replicas))

	// Check that all files are still found.
	for _, fr := range written {
		readRecord(ctx, t, s3, fr)
	}

	// // Write some more records to the new right range.
	written = append(written, writeNRecords(ctx, t, s1, 50)...)

	rd = s1.GetRange(4)
	header = headerFromRangeDescriptor(rd)
	s = getStoreWithRangeLease(t, stores, header)
	_, err = s.SplitCluster(ctx, &rfpb.SplitClusterRequest{
		Header: header,
		Range:  rd,
	})
	require.NoError(t, err)

	// Expect that a new cluster was added with clusterID = 4
	// having 3 replicas.
	replicas, err = s1.GetClusterMembership(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, 3, len(replicas))

	// Check that all files are found.
	for _, fr := range written {
		readRecord(ctx, t, s3, fr)
	}
}

func TestListCluster(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()

	err := bringup.SendStartClusterRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	list, err := s1.ListCluster(ctx, &rfpb.ListClusterRequest{})
	require.NoError(t, err)
	require.Equal(t, 2, len(list.GetRangeReplicas()))
}

func bytesToUint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

func TestPostFactoSplit(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	s4, nh4 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*TestingStore{s1, s2, s3, s4}
	err := bringup.SendStartClusterRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	// Attempting to Split an empty range will always fail. So write a
	// a small number of records before trying to Split.
	written := writeNRecords(ctx, t, s1, 50)

	rd := s1.GetRange(2)
	header := headerFromRangeDescriptor(rd)
	s := getStoreWithRangeLease(t, stores, header)
	splitResponse, err := s.SplitCluster(ctx, &rfpb.SplitClusterRequest{
		Header: header,
		Range:  rd,
	})
	require.NoError(t, err)

	// Expect that a new cluster was added with 3 replicas.
	replicas, err := s1.GetClusterMembership(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, 3, len(replicas))

	// Check that all files are found.
	for _, fr := range written {
		readRecord(ctx, t, s3, fr)
	}

	// Now bring up a new replica in the original cluster.
	_, err = s3.AddClusterNode(ctx, &rfpb.AddClusterNodeRequest{
		Range: s1.GetRange(2),
		Node: &rfpb.NodeDescriptor{
			Nhid:        nh4.ID(),
			RaftAddress: s4.RaftAddress,
			GrpcAddress: s4.GRPCAddress,
		},
	})
	require.NoError(t, err)

	r1, err := s1.GetReplica(2)
	require.NoError(t, err)
	r1DB, err := r1.TestingDB()
	require.NoError(t, err)

	lastIndexBytes, closer, err := r1DB.Get([]byte(constants.LastAppliedIndexKey))
	require.NoError(t, err)
	lastAppliedIndex := bytesToUint64(lastIndexBytes)
	closer.Close()
	r1DB.Close()

	var r4 *replica.Replica
	for {
		r4, err = s4.GetReplica(2)
		if err == nil {
			break
		}
		if !status.IsOutOfRangeError(err) {
			require.FailNowf(t, "unexpected error", "unexpected error %s", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for raft replication to finish bringing the new node up to date.
	waitStart := time.Now()
	for {
		r4DB, err := r4.TestingDB()
		if err == nil {
			indexBytes, closer, err := r4DB.Get([]byte(constants.LastAppliedIndexKey))
			require.NoError(t, err)
			newReplicaIndex := bytesToUint64(indexBytes)
			closer.Close()
			r4DB.Close()

			if newReplicaIndex >= lastAppliedIndex {
				log.Infof("Replica caught up in %s", time.Since(waitStart))
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Now verify that all keys that should be on the new node are present.
	for _, fr := range written {
		fmk := metadataKey(t, fr)
		if bytes.Compare(fmk, splitResponse.GetLeft().GetRight()) >= 0 {
			continue
		}
		rd := s4.GetRange(2)
		rc, err := r4.Reader(ctx, &rfpb.Header{
			RangeId:    rd.GetRangeId(),
			Generation: rd.GetGeneration(),
		}, fr, 0, 0)
		require.NoError(t, err)
		d := testdigest.ReadDigestAndClose(t, rc)
		require.True(t, proto.Equal(d, fr.GetDigest()))
	}
}

func TestManySplits(t *testing.T) {
	sf := newStoreFactory(t)
	s1, nh1 := sf.NewStore(t)
	s2, nh2 := sf.NewStore(t)
	s3, nh3 := sf.NewStore(t)
	ctx := context.Background()
	stores := []*TestingStore{s1, s2, s3}

	err := bringup.SendStartClusterRequests(ctx, s1.NodeHost, s1.APIClient, map[string]string{
		nh1.ID(): s1.GRPCAddress,
		nh2.ID(): s2.GRPCAddress,
		nh3.ID(): s3.GRPCAddress,
	})
	require.NoError(t, err)

	var written []*rfpb.FileRecord
	for i := 0; i < 4; i++ {
		written = append(written, writeNRecords(ctx, t, stores[0], 100)...)

		var clusters []uint64
		var seen = make(map[uint64]struct{})
		list, err := s1.ListCluster(ctx, &rfpb.ListClusterRequest{})
		require.NoError(t, err)

		for _, rangeReplica := range list.GetRangeReplicas() {
			for _, replica := range rangeReplica.GetRange().GetReplicas() {
				clusterID := replica.GetClusterId()
				if _, ok := seen[clusterID]; !ok {
					clusters = append(clusters, clusterID)
					seen[clusterID] = struct{}{}
				}
			}
		}

		for _, clusterID := range clusters {
			if clusterID == 1 {
				continue
			}
			rd := s1.GetRange(clusterID)
			header := headerFromRangeDescriptor(rd)
			s := getStoreWithRangeLease(t, stores, header)
			_, err = s.SplitCluster(ctx, &rfpb.SplitClusterRequest{
				Header: header,
				Range:  rd,
			})
			require.NoError(t, err)
			require.NoError(t, err)

			// Expect that a new cluster was added with the new
			// clusterID and 3 replicas.
			replicas, err := s.GetClusterMembership(ctx, clusterID)
			require.NoError(t, err)
			require.Equal(t, 3, len(replicas))
		}

		// Check that all files are found.
		for _, fr := range written {
			readRecord(ctx, t, s1, fr)
		}
	}
}
