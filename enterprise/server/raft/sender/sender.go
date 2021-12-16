package sender

import (
	"context"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
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

	// Map of connections for eache rangeID and mutex to protec.
	mu             sync.RWMutex
	cachedReplicas map[uint64]*rfpb.ReplicaDescriptor
}

func New(rangeCache *rangecache.RangeCache, nodeRegistry *registry.DynamicNodeRegistry, apiClient *client.APIClient) *Sender {
	return &Sender{
		rangeCache:     rangeCache,
		nodeRegistry:   nodeRegistry,
		apiClient:      apiClient,
		cachedReplicas: make(map[uint64]*rfpb.ReplicaDescriptor),
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
		header := &rfpb.Header{
			Replica: replica,
			RangeId: rangeDescriptor.GetRangeId(),
		}
		rsp, err := client.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: header,
			Batch:  batchReq,
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

func (s *Sender) lookupRangeDescriptor(ctx context.Context, key []byte) (*rfpb.RangeDescriptor, error) {
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
	rangeDescriptor, err := s.lookupRangeDescriptor(ctx, key)
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

func (s *Sender) orderReplicas(rd *rfpb.RangeDescriptor) []*rfpb.ReplicaDescriptor {
	s.mu.RLock()
	cachedReplica, ok := s.cachedReplicas[rd.GetRangeId()]
	s.mu.RUnlock()

	if !ok {
		return rd.GetReplicas()
	}

	replicas := make([]*rfpb.ReplicaDescriptor, 0, len(rd.GetReplicas()))
	replicas = append(replicas, cachedReplica)

	for _, replica := range rd.GetReplicas() {
		if replica.GetClusterId() == cachedReplica.GetClusterId() &&
			replica.GetNodeId() == cachedReplica.GetNodeId() {
			continue
		}
		replicas = append(replicas, replica)
	}
	return replicas
}

func (s *Sender) Run(ctx context.Context, key []byte, fn func(c rfspb.ApiClient, h *rfpb.Header) error) error {
	retrier := retry.DefaultWithContext(ctx)
	var lastErr error
	for retrier.Next() {
		rangeDescriptor, err := s.lookupRangeDescriptor(ctx, key)
		if err != nil {
			lastErr = err
			continue
		}
		replicas := s.orderReplicas(rangeDescriptor)
		for _, replica := range replicas {
			addr, _, err := s.nodeRegistry.ResolveGRPC(replica.GetClusterId(), replica.GetNodeId())
			if err != nil {
				// If we can't resolve the node, probably best
				// to back off and wait for it.
				log.Printf("ResolveGRPC returned err: %s, breaking", err)
				//return err
				lastErr = err
				break
			}
			client, err := s.apiClient.Get(ctx, addr)
			if err != nil {
				log.Printf("apiClient.Get returned err: %s, breaking", err)
				//return err
				lastErr = err
				break
			}

			header := &rfpb.Header{
				Replica: replica,
				RangeId: rangeDescriptor.GetRangeId(),
			}
			lastErr = fn(client, header)
			if lastErr != nil {
				if status.IsOutOfRangeError(lastErr) || status.IsUnavailableError(lastErr) {
					log.Printf("sender.Run got outOfRange or Unavailable on %+v, continuing to next replica", replica)
					continue
				}
			} else {
				// TODO(tylerw): Make this lock free?
				s.mu.Lock()
				if _, ok := s.cachedReplicas[rangeDescriptor.GetRangeId()]; !ok {
					s.cachedReplicas[rangeDescriptor.GetRangeId()] = replica
				}
				s.mu.Unlock()
			}
			return lastErr
		}
	}
	return lastErr
}

func (s *Sender) SyncPropose(ctx context.Context, key []byte, batchCmd *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	var rsp *rfpb.SyncProposeResponse
	err := s.Run(ctx, key, func(c rfspb.ApiClient, h *rfpb.Header) error {
		r, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
			Header: h,
			Batch:  batchCmd,
		})
		if err != nil {
			return err
		}
		rsp = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rsp.GetBatch(), nil
}
