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
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	gstatus "google.golang.org/grpc/status"
)

type ISender interface {
	SyncPropose(ctx context.Context, key []byte, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error)
	SyncRead(ctx context.Context, key []byte, batch *rfpb.BatchCmdRequest, mods ...Option) (*rfpb.BatchCmdResponse, error)
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

func makeHeader(rangeDescriptor *rfpb.RangeDescriptor, replicaIdx int, mode rfpb.Header_ConsistencyMode) *rfpb.Header {
	return &rfpb.Header{
		Replica:         rangeDescriptor.GetReplicas()[replicaIdx],
		RangeId:         rangeDescriptor.GetRangeId(),
		Generation:      rangeDescriptor.GetGeneration(),
		ConsistencyMode: mode,
	}
}

func (s *Sender) grpcAddrForReplicaDescriptor(rd *rfpb.ReplicaDescriptor) (string, error) {
	addr, _, err := s.nodeRegistry.ResolveGRPC(rd.GetShardId(), rd.GetReplicaId())
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
		Start:    keys.RangeMetaKey(key),
		End:      constants.SystemPrefix,
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

func (s *Sender) GetMetaRangeDescriptor() *rfpb.RangeDescriptor {
	return s.rangeCache.Get(constants.MetaRangePrefix)
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
		_, err := s.tryReplicas(ctx, metaRangeDescriptor, fn, rfpb.Header_LINEARIZABLE)
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
	if len(key) == 0 {
		return nil, status.FailedPreconditionError("A non-nil key is required")
	}
	rangeDescriptor := s.rangeCache.Get(key)
	if rangeDescriptor == nil || skipCache {
		rd, err := s.fetchRangeDescriptorFromMetaRange(ctx, key)
		if err != nil {
			return nil, err
		}
		s.rangeCache.UpdateRange(rd)
		rangeDescriptor = rd
	}
	r := rangemap.Range{Start: rangeDescriptor.GetStart(), End: rangeDescriptor.GetEnd()}
	if !r.Contains(key) {
		log.Fatalf("Found range %+v that doesn't contain key: %q", rangeDescriptor, string(key))
	}
	return rangeDescriptor, nil
}

func (s *Sender) UpdateRange(rangeDescriptor *rfpb.RangeDescriptor) error {
	return s.rangeCache.UpdateRange(rangeDescriptor)
}

type runFunc func(c rfspb.ApiClient, h *rfpb.Header) error

func (s *Sender) tryReplicas(ctx context.Context, rd *rfpb.RangeDescriptor, fn runFunc, mode rfpb.Header_ConsistencyMode) (int, error) {
	for i, replica := range rd.GetReplicas() {
		client, err := s.connectionForReplicaDescriptor(ctx, replica)
		if err != nil {
			if status.IsUnavailableError(err) {
				log.Debugf("tryReplicas: replica %+v is unavailable: %s", replica, err)
				// try a different replica if the current replica is not available.
				continue
			}
			return 0, err
		}
		header := makeHeader(rd, i, mode)
		err = fn(client, header)
		if err == nil {
			return i, nil
		}
		if status.IsOutOfRangeError(err) {
			m := status.Message(err)
			switch {
			// range not found, no replicas are likely to have it; bail.
			case strings.HasPrefix(m, constants.RangeNotFoundMsg), strings.HasPrefix(m, constants.RangeNotCurrentMsg):
				log.Debugf("out of range: %s (skipping rangecache)", m)
				return 0, err
			case strings.HasPrefix(m, constants.RangeNotLeasedMsg), strings.HasPrefix(m, constants.RangeLeaseInvalidMsg):
				log.Debugf("out of range: %s (skipping replica %+v)", m, replica)
				continue
			default:
				break
			}
		}
		if status.IsUnavailableError(err) {
			log.Debugf("replica %+v is unavailable: %s", replica, err)
			// try a different replica if the current replica is not available.
			continue
		}
		return 0, err
	}
	return 0, status.OutOfRangeErrorf("No replicas available in range: %d", rd.GetRangeId())
}

type Options struct {
	ConsistencyMode rfpb.Header_ConsistencyMode
}

func defaultOptions() *Options {
	return &Options{
		ConsistencyMode: rfpb.Header_LINEARIZABLE,
	}
}

type Option func(*Options)

func WithConsistencyMode(mode rfpb.Header_ConsistencyMode) Option {
	return func(o *Options) {
		o.ConsistencyMode = mode
	}
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
func (s *Sender) Run(ctx context.Context, key []byte, fn runFunc, mods ...Option) error {
	opts := defaultOptions()
	for _, mod := range mods {
		mod(opts)
	}

	retrier := retry.DefaultWithContext(ctx)
	skipRangeCache := false
	var lastError error
	for retrier.Next() {
		rangeDescriptor, err := s.LookupRangeDescriptor(ctx, key, skipRangeCache)
		if err != nil {
			log.Warningf("sender.Run error getting rd for %q: %s, %s, %+v", key, err, s.rangeCache.String(), s.rangeCache.Get(key))
			continue
		}
		i, err := s.tryReplicas(ctx, rangeDescriptor, fn, opts.ConsistencyMode)
		if err == nil {
			if i != 0 {
				replica := rangeDescriptor.GetReplicas()[i]
				s.rangeCache.SetPreferredReplica(replica, rangeDescriptor)
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
func (s *Sender) RunMultiKey(ctx context.Context, keys []*KeyMeta, fn runMultiKeyFunc, mods ...Option) ([]interface{}, error) {
	opts := defaultOptions()
	for _, mod := range mods {
		mod(opts)
	}

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

		var errs []error
		for _, rk := range keysByRange {
			var rangeRsp interface{}
			_, err = s.tryReplicas(ctx, rk.rd, func(c rfspb.ApiClient, h *rfpb.Header) error {
				rsp, err := fn(c, h, rk.keys)
				if err != nil {
					return err
				}
				rangeRsp = rsp
				return nil
			}, opts.ConsistencyMode)
			if err != nil {
				if !status.IsOutOfRangeError(err) {
					return nil, err
				}
				errs = append(errs, err)
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
	customHeader := batchCmd.Header
	err := s.Run(ctx, key, func(c rfspb.ApiClient, h *rfpb.Header) error {
		if customHeader == nil {
			batchCmd.Header = h
		}
		r, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
			Header: h,
			Batch:  batchCmd,
		})
		if err != nil {
			return err
		}
		if err := gstatus.FromProto(r.GetBatch().GetStatus()).Err(); err != nil {
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

func (s *Sender) SyncRead(ctx context.Context, key []byte, batchCmd *rfpb.BatchCmdRequest, mods ...Option) (*rfpb.BatchCmdResponse, error) {
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
	}, mods...)
	if err != nil {
		return nil, err
	}
	return rsp.GetBatch(), nil
}

func (s *Sender) RunTxn(ctx context.Context, txn *rbuilder.TxnBuilder) error {
	// TODO(tylerw): make this durable if the coordinator restarts by writing
	// the TxnRequest proto to durable storage with an enum state-field and
	// building a statemachine that will run them to completion even after
	// restart.
	txnProto, err := txn.ToProto()
	if err != nil {
		return err
	}

	// Check that each statement addresses a different shard. If two statements
	// address a single shard, they should be combined, otherwise finalization
	// will fail when attempted twice.
	shardStatementMap := make(map[uint64]int64)
	for i, statement := range txnProto.GetStatements() {
		shardID := statement.GetReplica().GetShardId()
		existing, ok := shardStatementMap[shardID]
		if ok {
			return status.FailedPreconditionErrorf("Statements %d and %d both address shard %d. Only one batch per shard is allowed", i, existing, shardID)
		}
	}

	var prepareError error
	prepared := make([]*rfpb.TxnRequest_Statement, 0)
	for i, statement := range txnProto.GetStatements() {
		batch := statement.GetRawBatch()
		batch.TransactionId = txnProto.GetTransactionId()

		// Prepare each statement.
		c, err := s.connectionForReplicaDescriptor(ctx, statement.GetReplica())
		if err != nil {
			return err
		}
		syncRsp, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
			Header: &rfpb.Header{
				Replica: statement.GetReplica(),
			},
			Batch: batch,
		})
		if err != nil {
			log.Errorf("Error preparing txn statement %d: %s", i, err)
			prepareError = err
			break
		}
		rsp := rbuilder.NewBatchResponseFromProto(syncRsp.GetBatch())
		if err := rsp.AnyError(); err != nil {
			log.Errorf("Error preparing txn statement %d: %s", i, err)
			prepareError = err
			break
		}
		prepared = append(prepared, statement)
	}

	// Determine whether to ROLLBACK or COMMIT based on whether or not all
	// statements in the transaction were successfully prepared.
	operation := rfpb.FinalizeOperation_ROLLBACK
	if len(prepared) == len(txnProto.GetStatements()) {
		operation = rfpb.FinalizeOperation_COMMIT
	}

	for _, statement := range prepared {
		// Finalize each statement.
		batch := rbuilder.NewBatchBuilder().SetTransactionID(txnProto.GetTransactionId())
		batch.SetFinalizeOperation(operation)

		batchProto, err := batch.ToProto()
		if err != nil {
			return err
		}

		// Prepare each statement.
		c, err := s.connectionForReplicaDescriptor(ctx, statement.GetReplica())
		if err != nil {
			return err
		}
		syncRsp, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
			Header: &rfpb.Header{
				Replica: statement.GetReplica(),
			},
			Batch: batchProto,
		})
		if err != nil {
			return err
		}
		rsp := rbuilder.NewBatchResponseFromProto(syncRsp.GetBatch())
		if err := rsp.AnyError(); err != nil {
			return err
		}
	}

	if prepareError != nil {
		return prepareError
	}
	return nil
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
