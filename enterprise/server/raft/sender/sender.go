package sender

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
)

type ISender interface {
	SyncPropose(ctx context.Context, key []byte, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error)
	SyncRead(ctx context.Context, key []byte, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error)
}

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

func (s *Sender) connectionForReplicaDescriptor(ctx context.Context, rd *rfpb.ReplicaDescriptor) (rfspb.ApiClient, error) {
	addr, _, err := s.nodeRegistry.ResolveGRPC(rd.GetClusterId(), rd.GetNodeId())
	if err != nil {
		return nil, err
	}
	return s.apiClient.Get(ctx, addr)
}

func lookupRangeDescriptor(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, key []byte) (*rfpb.RangeDescriptor, error) {
	batchReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Left:     keys.RangeMetaKey(key),
		Right:    constants.SystemPrefix,
		ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
	}).ToProto()
	if err != nil {
		return nil, err
	}
	rsp, err := c.SyncRead(ctx, &rfpb.SyncReadRequest{
		Header: h,
		Batch:  batchReq,
	})
	if err != nil {
		return nil, err
	}
	scanRsp, err := rbuilder.NewBatchResponseFromProto(rsp.GetBatch()).ScanResponse(0)
	if err != nil {
		log.Errorf("Error reading scan response: %s", err)
		return nil, err
	}

	if len(scanRsp.GetKvs()) == 0 {
		log.Errorf("scan response had 0 kvs")
		return nil, status.FailedPreconditionError("scan response had 0 kvs")
	}
	for _, kv := range scanRsp.GetKvs() {
		rd := &rfpb.RangeDescriptor{}
		if err := proto.Unmarshal(kv.GetValue(), rd); err != nil {
			log.Errorf("scan returned unparsable kv: %s", err)
			continue
		}
		return rd, nil
	}
	return nil, status.UnavailableErrorf("Error finding range descriptor for %q", key)
}

func (s *Sender) fetchRangeDescriptorFromMetaRange(ctx context.Context, key []byte) (*rfpb.RangeDescriptor, error) {
	retrier := retry.DefaultWithContext(ctx)

	var rangeDescriptor *rfpb.RangeDescriptor
	fn := func(c rfspb.ApiClient, h *rfpb.Header) error {
		rd, err := lookupRangeDescriptor(ctx, c, h, key)
		if err != nil {
			return err
		}
		rangeDescriptor = rd
		return nil
	}
	for retrier.Next() {
		metaRangeDescriptor := s.rangeCache.Get(constants.MetaRangePrefix)
		if metaRangeDescriptor == nil {
			log.Warningf("RangeCache did not have meta range yet (key %q)", key)
			continue
		}
		_, err := s.tryReplicas(ctx, metaRangeDescriptor, fn)
		if err == nil {
			return rangeDescriptor, nil
		}
		if !status.IsOutOfRangeError(err) {
			return nil, err
		}
	}
	return nil, status.UnavailableErrorf("Error finding range descriptor for %q", key)
}

func (s *Sender) LookupRangeDescriptor(ctx context.Context, key []byte, skipCache bool) (*rfpb.RangeDescriptor, error) {
	rangeDescriptor := s.rangeCache.Get(key)
	if rangeDescriptor == nil || skipCache {
		rd, err := s.fetchRangeDescriptorFromMetaRange(ctx, key)
		if err != nil {
			return nil, err
		}
		rangeDescriptor = rd
	}
	return rangeDescriptor, nil
}

func (s *Sender) GetAllNodes(ctx context.Context, key []byte) ([]*client.PeerHeader, error) {
	rangeDescriptor, err := s.LookupRangeDescriptor(ctx, key, false /*skipCache*/)
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

type runFunc func(c rfspb.ApiClient, h *rfpb.Header) error

func (s *Sender) tryReplicas(ctx context.Context, rd *rfpb.RangeDescriptor, fn runFunc) (int, error) {
	for i, replica := range rd.GetReplicas() {
		client, err := s.connectionForReplicaDescriptor(ctx, replica)
		if err != nil {
			return 0, err
		}
		header := makeHeader(rd, i)
		err = fn(client, header)
		if err != nil {
			if status.IsOutOfRangeError(err) {
				m := status.Message(err)
				switch {
				// range not found, no replicas are likely to have it; bail.
				case strings.HasPrefix(m, constants.RangeNotFoundMsg), strings.HasPrefix(m, constants.RangeNotCurrentMsg):
					log.Debugf("out of range: %s (skipping rangecache)", m)
					return 0, err
				case strings.HasPrefix(m, constants.RangeNotLeasedMsg), strings.HasPrefix(m, constants.RangeLeaseInvalidMsg):
					log.Debugf("out of range: %s (skipping replica %d)", m, replica.GetNodeId())
					continue
				default:
					return 0, err
				}
			}
			return 0, err
		}
		return i, nil
	}
	return 0, status.OutOfRangeErrorf("No replicas available in range: %d", rd.GetRangeId())
}

func (s *Sender) Run(ctx context.Context, key []byte, fn runFunc) error {
	retrier := retry.DefaultWithContext(ctx)
	skipRangeCache := false
	for retrier.Next() {
		rangeDescriptor, err := s.LookupRangeDescriptor(ctx, key, skipRangeCache)
		if err != nil {
			log.Warningf("sender.Run error getting rd for %q: %s, %s, %+v", key, err, s.rangeCache.String(), s.rangeCache.Get(key))
			continue
		}
		i, err := s.tryReplicas(ctx, rangeDescriptor, fn)
		if err == nil {
			replica := rangeDescriptor.GetReplicas()[i]
			if err := s.rangeCache.UpdateRange(rangeDescriptor); err == nil {
				s.rangeCache.SetPreferredReplica(replica, rangeDescriptor)
			} else {
				log.Errorf("Error updating rangecache: %s", err)
			}
			return nil
		}
		skipRangeCache = true
		if !status.IsOutOfRangeError(err) {
			return err
		}
	}
	return status.UnavailableError("sender.Run retries exceeded")
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

func (s *Sender) SyncRead(ctx context.Context, key []byte, batchCmd *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	var rsp *rfpb.SyncReadResponse
	err := s.Run(ctx, key, func(c rfspb.ApiClient, h *rfpb.Header) error {
		r, err := c.SyncRead(ctx, &rfpb.SyncReadRequest{
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

func (s *Sender) DirectRead(ctx context.Context, key []byte) ([]byte, error) {
	batch, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: key,
	}).ToProto()
	if err != nil {
		return nil, err
	}
	rsp, err := s.SyncRead(ctx, key, batch)
	if err != nil {
		return nil, err
	}
	batchResp := rbuilder.NewBatchResponseFromProto(rsp)
	readResponse, err := batchResp.DirectReadResponse(0)
	if err != nil {
		return nil, err
	}
	return readResponse.GetKv().GetValue(), nil
}
