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

func (s *Sender) grpcAddrForReplicaDescriptor(rd *rfpb.ReplicaDescriptor) (string, error) {
	addr, _, err := s.nodeRegistry.ResolveGRPC(rd.GetClusterId(), rd.GetNodeId())
	if err != nil {
		return "", err
	}
	return addr, nil
}

func (s *Sender) connectionForReplicaDescriptor(ctx context.Context, rd *rfpb.ReplicaDescriptor) (rfspb.ApiClient, error) {
	addr, err := s.grpcAddrForReplicaDescriptor(rd)
	if err != nil {
		return nil, err
	}
	return s.apiClient.Get(ctx, addr)
}

func lookupRangeDescriptor(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, key []byte) (*rfpb.RangeDescriptor, error) {
	batchReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Left:     keys.RangeMetaKey(key),
		Right:    constants.SystemPrefix,
		ScanType: rfpb.ScanRequest_SEEKGT_SCAN_TYPE,
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

// Run looks up the replicas that are responsible for the given key and executes
// fn for each replica until the function succeeds or returns an unretriable
// error.
//
// The range ownership information is retrieved from the range cache, so it can
// be stale. If the remote peer indicates that the client is using a stale
// range, this function will look up the range information again bypassing the
// cache and try the fn again with the new replica information. If the
// fn succeeds with the new replicas, the range cache will be updated with
// the new ownership information.
func (s *Sender) Run(ctx context.Context, key []byte, fn runFunc) error {
	retrier := retry.DefaultWithContext(ctx)
	skipRangeCache := false
	var lastError error
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
		lastError = err
	}
	return status.UnavailableErrorf("sender.Run retries exceeded for key: %q err: %s", key, lastError)
}

type runAllFunc func(peerHeaders []*client.PeerHeader) error

// RunAll is similar to Run but instead of trying replicas one at a time,
// fn will be passed information about all the replicas for performing
// operations that span multiple replicas.
//
// If any of the replicas indicate that range information is stale, the whole
// operation will be retried using fresh range information.
//
// Run should generally be used instead unless there's a specialized reason that
// all replicas need to be consulted.
func (s *Sender) RunAll(ctx context.Context, key []byte, fn runAllFunc) error {
	retrier := retry.DefaultWithContext(ctx)
	skipRangeCache := false
	var lastError error
	for retrier.Next() {
		rd, err := s.LookupRangeDescriptor(ctx, key, skipRangeCache)
		if err != nil {
			log.Warningf("sender.RunAll error getting rd for %q: %s, %s, %+v", key, err, s.rangeCache.String(), s.rangeCache.Get(key))
			continue
		}

		var clients []*client.PeerHeader
		for i, replica := range rd.GetReplicas() {
			addr, err := s.grpcAddrForReplicaDescriptor(replica)
			if err != nil {
				return err
			}
			c, err := s.apiClient.Get(ctx, addr)
			h := makeHeader(rd, i)
			clients = append(clients, &client.PeerHeader{Header: h, GRPCAddr: addr, GRPClient: c})
		}

		err = fn(clients)
		if err == nil {
			if err := s.rangeCache.UpdateRange(rd); err != nil {
				log.Errorf("Error updating rangecache: %s", err)
			}
			return nil
		}
		skipRangeCache = true
		if !status.IsOutOfRangeError(err) {
			return err
		}
		lastError = err
	}
	return status.UnavailableErrorf("sender.RunAll retries exceeded for key: %q err: %s", key, lastError)
}

// KeyMeta contains a key with arbitrary data attached.
type KeyMeta struct {
	Key  []byte
	Meta interface{}
}

type rangeKeys struct {
	keys []*KeyMeta
	rd   *rfpb.RangeDescriptor
}

type runMultiKeyFunc func(c rfspb.ApiClient, h *rfpb.Header, keys []*KeyMeta) (interface{}, error)

func (s *Sender) partitionKeysByRange(ctx context.Context, keys []*KeyMeta, skipRangeCache bool) (map[uint64]*rangeKeys, error) {
	keysByRange := make(map[uint64]*rangeKeys)
	for _, keyMeta := range keys {
		rd, err := s.LookupRangeDescriptor(ctx, keyMeta.Key, skipRangeCache)
		if err != nil {
			return nil, err
		}
		if v, ok := keysByRange[rd.GetRangeId()]; ok {
			// Uncommon, but means range information changed while we were
			// partitioning.
			if !proto.Equal(v.rd, rd) {
				return s.partitionKeysByRange(ctx, keys, skipRangeCache)
			}
		} else {
			keysByRange[rd.GetRangeId()] = &rangeKeys{rd: rd}
		}
		keysByRange[rd.GetRangeId()].keys = append(keysByRange[rd.GetRangeId()].keys, keyMeta)
	}
	return keysByRange, nil
}

// RunMultiKey is similar to Run but works on multiple keys to support batch
// operations.
//
// This method takes care of partitioning the keys into different
// ranges and passing appropriate replica information to the user provided
// function.
//
// RunMultiKey returns a combined slice of the values returned from successful
// fn calls.
func (s *Sender) RunMultiKey(ctx context.Context, keys []*KeyMeta, fn runMultiKeyFunc) ([]interface{}, error) {
	retrier := retry.DefaultWithContext(ctx)
	skipRangeCache := false
	var rsps []interface{}
	remainingKeys := keys
	var lastError error
	for retrier.Next() {
		keysByRange, err := s.partitionKeysByRange(ctx, remainingKeys, skipRangeCache)
		if err != nil {
			return nil, err
		}

		// We'll repopulate remaining keys below.
		remainingKeys = nil

		for _, rk := range keysByRange {
			var rangeRsp interface{}
			_, err = s.tryReplicas(ctx, rk.rd, func(c rfspb.ApiClient, h *rfpb.Header) error {
				rsp, err := fn(c, h, rk.keys)
				if err != nil {
					return err
				}
				rangeRsp = rsp
				return nil
			})
			if err != nil {
				if !status.IsOutOfRangeError(err) {
					return nil, err
				}
				skipRangeCache = true
				// No luck for the keys in the range on this try, let's try them
				// on the next attempt.
				remainingKeys = append(remainingKeys, rk.keys...)
			} else {
				rsps = append(rsps, rangeRsp)
			}
			lastError = err
		}

		if len(remainingKeys) == 0 {
			return rsps, nil
		}
	}
	return nil, status.UnavailableErrorf("sender.RunMultiKey retries exceeded, err: %s", lastError)
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
