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
	nodeRegistry registry.NodeRegistry

	// Keeps track of connections to other machines.
	apiClient *client.APIClient
}

func New(rangeCache *rangecache.RangeCache, nodeRegistry registry.NodeRegistry, apiClient *client.APIClient) *Sender {
	return &Sender{
		rangeCache:   rangeCache,
		nodeRegistry: nodeRegistry,
		apiClient:    apiClient,
	}
}

func makeHeader(rangeDescriptor *rfpb.RangeDescriptor, replicaIdx int) *rfpb.Header {
	return &rfpb.Header{
		Replica:    rangeDescriptor.GetReplicas()[replicaIdx],
		RangeId:    rangeDescriptor.GetRangeId(),
		Generation: rangeDescriptor.GetGeneration(),
	}
}

func (s *Sender) ConnectionForReplicaDesciptor(ctx context.Context, rd *rfpb.ReplicaDescriptor) (rfspb.ApiClient, error) {
	addr, _, err := s.nodeRegistry.ResolveGRPC(rd.GetClusterId(), rd.GetNodeId())
	if err != nil {
		return nil, err
	}
	return s.apiClient.Get(ctx, addr)
}

func (s *Sender) fetchRangeDescriptorFromMetaRange(ctx context.Context, key []byte) (*rfpb.RangeDescriptor, error) {
	metaRangeDescriptor := s.rangeCache.Get(constants.MetaRangePrefix)
	if metaRangeDescriptor == nil {
		return nil, status.FailedPreconditionError("RangeCache did not have meta range. This should not happen")
	}

	for i, replica := range metaRangeDescriptor.GetReplicas() {
		client, err := s.ConnectionForReplicaDesciptor(ctx, replica)
		if err != nil {
			log.Errorf("Error getting api conn: %s", err)
			continue
		}
		batchReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
			Left:     keys.RangeMetaKey(key),
			Right:    constants.SystemPrefix,
			ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
		}).ToProto()
		if err != nil {
			return nil, err
		}
		header := makeHeader(metaRangeDescriptor, i)
		rsp, err := client.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: header,
			Batch:  batchReq,
		})
		if err != nil {
			if status.IsOutOfRangeError(err) || status.IsUnavailableError(err) {
				log.Printf("Unable to get rd from replica: %+v: %s", replica, err)
				continue
			}
			return nil, err
		}
		scanRsp, err := rbuilder.NewBatchResponseFromProto(rsp.GetBatch()).ScanResponse(0)
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
			return rd, nil
		}
	}

	return nil, status.UnavailableErrorf("Error finding range descriptor for %q", key)
}

func (s *Sender) LookupRangeDescriptor(ctx context.Context, key []byte) (*rfpb.RangeDescriptor, error) {
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
		log.Debugf("Updated rangeCache with rd: %+v", rd)
		rangeDescriptor = rd
	}
	return rangeDescriptor, nil
}

func (s *Sender) GetAllNodes(ctx context.Context, key []byte) ([]*client.PeerHeader, error) {
	rangeDescriptor, err := s.LookupRangeDescriptor(ctx, key)
	if err != nil {
		return nil, err
	}

	allNodes := make([]*client.PeerHeader, 0, len(rangeDescriptor.GetReplicas()))
	for i, replica := range rangeDescriptor.GetReplicas() {
		addr, _, err := s.nodeRegistry.ResolveGRPC(replica.GetClusterId(), replica.GetNodeId())
		if err != nil {
			log.Errorf("registry error resolving %+v: %s", replica, err)
			continue
		}

		allNodes = append(allNodes, &client.PeerHeader{
			Header:   makeHeader(rangeDescriptor, i),
			GRPCAddr: addr,
		})
	}
	return allNodes, nil
}

func (s *Sender) Run(ctx context.Context, key []byte, fn func(c rfspb.ApiClient, h *rfpb.Header) error) error {
	retrier := retry.DefaultWithContext(ctx)
	var lastErr error
	for retrier.Next() {
		rangeDescriptor, err := s.LookupRangeDescriptor(ctx, key)
		if err != nil {
			lastErr = err
			log.Printf("sender.Run error getting rd for %q: %s", key, err)
			continue
		}
		for i, replica := range rangeDescriptor.GetReplicas() {
			addr, _, err := s.nodeRegistry.ResolveGRPC(replica.GetClusterId(), replica.GetNodeId())
			if err != nil {
				lastErr = err
				break
			}
			client, err := s.apiClient.Get(ctx, addr)
			if err != nil {
				lastErr = err
				break
			}
			header := makeHeader(rangeDescriptor, i)
			lastErr = fn(client, header)
			if lastErr != nil {
				if status.IsOutOfRangeError(lastErr) || status.IsUnavailableError(lastErr) {
					log.Debugf("sender.Run got outOfRange or Unavailable on %+v, continuing to next replica: %s", replica, lastErr)
					continue
				}
			} else {
				if i != 0 {
					// If a replica served a request, mark it as the preferred
					// replica in the rangecache.
					s.rangeCache.SetPreferredReplica(replica, rangeDescriptor)
				}
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

func (s *Sender) Increment(ctx context.Context, key []byte, n uint64) (uint64, error) {
	batch, err := rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   key,
		Delta: n,
	}).ToProto()
	if err != nil {
		return 0, err
	}
	rsp, err := s.SyncPropose(ctx, key, batch)
	if err != nil {
		return 0, err
	}
	batchResp := rbuilder.NewBatchResponseFromProto(rsp)
	incResponse, err := batchResp.IncrementResponse(0)
	if err != nil {
		return 0, err
	}
	return incResponse.GetValue(), nil
}
