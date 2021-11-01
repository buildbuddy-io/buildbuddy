package sender

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
)

type Sender struct {
	// Keeps track of which raft nodes are responsible for which data.
	rangeCache *rangecache.RangeCache

	// Keeps track of which raft nodes live on which machines.
	nodeRegistry *registry.DynamicNodeRegistry

	// Keeps track of connections to other machines.
	apiClient *client.APIClient
}

func New(rangeCache *rangecache.RangeCache, nodeRegistry *registry.DynamicNodeRegistry, apiClient *client.APIClient) *Sender {
	return &Sender{
		rangeCache:   rangeCache,
		nodeRegistry: nodeRegistry,
		apiClient:    apiClient,
	}
}

func (s *Sender) connectionForReplicaDesciptor(ctx context.Context, rd *rfpb.ReplicaDescriptor) (rfspb.ApiClient, error) {
	addr, _, err := s.nodeRegistry.ResolveGRPC(rd.GetClusterId(), rd.GetNodeId())
	if err != nil {
		return nil, err
	}
	return s.apiClient.Get(ctx, addr)
}

func (s *Sender) fetchRangeDescriptorFromMetaRange(ctx context.Context, key []byte) (*rfpb.RangeDescriptor, error) {
	rangeDescriptor := s.rangeCache.Get(constants.MetaRangePrefix)
	if rangeDescriptor == nil {
		return nil, status.FailedPreconditionError("RangeCache did not have meta range. This should not happen")
	}

	for _, replica := range rangeDescriptor.GetReplicas() {
		client, err := s.connectionForReplicaDesciptor(ctx, replica)
		if err != nil {
			log.Errorf("Error getting api conn: %s", err)
			continue
		}
		batchReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
			Left: keys.RangeMetaKey(key),
		}).ToProto()
		if err != nil {
			return nil, err
		}
		rsp, err := client.SyncRead(ctx, &rfpb.SyncReadRequest{
			Replica: replica,
			Batch:   batchReq,
		})
		if err != nil {
			continue
		}
		scanRsp, err := rbuilder.NewBatchResponse(rsp).ScanResponse(0)
		if err != nil {
			log.Errorf("Error reading scan response: %s", err)
			continue
		}
		for _, kv := range scanRsp.GetKvs() {
			rd := &rfpb.RangeDescriptor{}
			if err := proto.Unmarshal(kv.GetValue(), rd); err != nil {
				log.Errorf("scan returned unparsable kv: %s", err)
				continue
			}
			if err := s.rangeCache.UpdateRange(rd); err != nil {
				log.Errorf("error updating rangecache: %s", err)
			}
		}
	}
	rd := s.rangeCache.Get(key)
	if rd != nil {
		return rd, nil
	}

	return nil, status.UnavailableErrorf("Error finding range descriptor for %q", key)
}

func (s *Sender) getOrCreateRangeDescriptor(ctx context.Context, key []byte) (*rfpb.RangeDescriptor, error) {
	rangeDescriptor := s.rangeCache.Get(key)
	// TODO(tylerw): loop until available or context timeout?
	if rangeDescriptor == nil {
		rd, err := s.fetchRangeDescriptorFromMetaRange(ctx, key)
		if err != nil {
			return nil, err
		}
		if err := s.rangeCache.UpdateRange(rd); err != nil {
			return nil, err
		}
		rangeDescriptor = rd
	}
	return rangeDescriptor, nil
}

func (s *Sender) GetAllNodes(ctx context.Context, key []byte) ([]string, error) {
	rangeDescriptor, err := s.getOrCreateRangeDescriptor(ctx, key)
	if err != nil {
		return nil, err
	}

	allNodes := make([]string, 0, len(rangeDescriptor.GetReplicas()))
	for _, replica := range rangeDescriptor.GetReplicas() {
		addr, _, err := s.nodeRegistry.ResolveGRPC(replica.GetClusterId(), replica.GetNodeId())
		if err != nil {
			log.Errorf("registry error resolving %+v: %s", replica, err)
			continue
		}
		allNodes = append(allNodes, addr)
	}
	return allNodes, nil
}

func (s *Sender) Run(ctx context.Context, key []byte, fn func(c rfspb.ApiClient, rd *rfpb.ReplicaDescriptor) error) error {
	rangeDescriptor, err := s.getOrCreateRangeDescriptor(ctx, key)
	if err != nil {
		return err
	}

	var lastErr error
	for _, replica := range rangeDescriptor.GetReplicas() {
		addr, _, err := s.nodeRegistry.ResolveGRPC(replica.GetClusterId(), replica.GetNodeId())
		if err != nil {
			lastErr = err
			continue
		}
		client, err := s.apiClient.Get(ctx, addr)
		if err != nil {
			lastErr = err
			continue
		}
		lastErr = fn(client, replica)
		if lastErr != nil {
			if status.IsOutOfRangeError(lastErr) {
				continue
			}
		}
		return lastErr
	}
	return status.UnavailableErrorf("Run failed. (%d attempts): %s", len(rangeDescriptor.GetReplicas()), lastErr)
}
