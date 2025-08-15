package sender

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/header"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"go.opentelemetry.io/otel/attribute"

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

	// Keeps track of connections to other machines.
	apiClient *client.APIClient
}

func New(rangeCache *rangecache.RangeCache, apiClient *client.APIClient) *Sender {
	return &Sender{
		rangeCache: rangeCache,
		apiClient:  apiClient,
	}
}

func scanKVs(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, scanReq *rfpb.ScanRequest) ([]*rfpb.KV, error) {
	batchReq, err := rbuilder.NewBatchBuilder().Add(scanReq).ToProto()
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
		log.CtxErrorf(ctx, "Error reading scan response: %s", err)
		return nil, err
	}
	return scanRsp.GetKvs(), nil
}

func scanRangeDescriptors(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, scanReq *rfpb.ScanRequest) ([]*rfpb.RangeDescriptor, error) {
	kvs, err := scanKVs(ctx, c, h, scanReq)
	if err != nil {
		return nil, err
	}

	res := make([]*rfpb.RangeDescriptor, 0, len(kvs))
	for _, kv := range kvs {
		rd := &rfpb.RangeDescriptor{}
		if err := proto.Unmarshal(kv.GetValue(), rd); err != nil {
			return nil, status.InternalErrorf("scan returned unparsable kv (key=%q): %s", kv.GetKey(), err)
		}
		res = append(res, rd)
	}
	return res, nil
}

func scanPartitionDescriptors(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, scanReq *rfpb.ScanRequest) ([]*rfpb.PartitionDescriptor, error) {
	kvs, err := scanKVs(ctx, c, h, scanReq)
	if err != nil {
		return nil, err
	}

	res := make([]*rfpb.PartitionDescriptor, 0, len(kvs))
	for _, kv := range kvs {
		pd := &rfpb.PartitionDescriptor{}
		if err := proto.Unmarshal(kv.GetValue(), pd); err != nil {
			return nil, status.InternalErrorf("scan returned unparsable kv (key=%q): %s", kv.GetKey(), err)
		}
		res = append(res, pd)
	}
	return res, nil
}

func lookupRangeDescriptor(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, key []byte) (*rfpb.RangeDescriptor, error) {
	req := &rfpb.ScanRequest{
		Start:    keys.RangeMetaKey(key),
		End:      constants.SystemPrefix,
		ScanType: rfpb.ScanRequest_SEEKGT_SCAN_TYPE,
		Limit:    1,
	}
	res, err := scanRangeDescriptors(ctx, c, h, req)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, status.NotFoundErrorf("range descriptor not found for key %q", key)
	}
	return res[0], nil
}

func fetchRanges(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, rangeIDs []uint64) (*rfpb.FetchRangesResponse, error) {
	batchReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.FetchRangesRequest{
		RangeIds: rangeIDs,
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
	return rbuilder.NewBatchResponseFromProto(rsp.GetBatch()).FetchRangesResponse(0)
}

func (s *Sender) GetMetaRangeDescriptor() *rfpb.RangeDescriptor {
	return s.rangeCache.Get(constants.MetaRangePrefix)
}

func (s *Sender) fetchRangeDescriptorFromMetaRange(ctx context.Context, key []byte) (*rfpb.RangeDescriptor, error) {
	var rangeDescriptor *rfpb.RangeDescriptor
	fn := func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header) error {
		rd, err := lookupRangeDescriptor(ctx, c, h, key)
		if err != nil {
			return err
		}
		rangeDescriptor = rd
		return nil
	}
	if err := s.runOnMetaRange(ctx, fn); err != nil {
		return nil, err
	}
	return rangeDescriptor, nil
}

// LookupRangeDescriptorsForPartition looks up range descriptors associated with
// a partition and the number of results is capped by the given limit.
func (s *Sender) LookupRangeDescriptorsForPartition(ctx context.Context, partitionID string, limit int64) ([]*rfpb.RangeDescriptor, error) {
	res := make([]*rfpb.RangeDescriptor, 0, limit)
	fn := func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header) error {
		partitionPrefix := []byte(filestore.PartitionDirectoryPrefix + partitionID + "/")
		req := &rfpb.ScanRequest{
			Start:    keys.RangeMetaKey(partitionPrefix),
			End:      keys.RangeMetaKey(keys.MakeKey(partitionPrefix, keys.MaxByte)),
			ScanType: rfpb.ScanRequest_SEEKGT_SCAN_TYPE,
			Limit:    limit,
		}
		ranges, err := scanRangeDescriptors(ctx, c, h, req)
		if err != nil {
			return err
		}
		res = ranges
		return nil
	}
	if err := s.runOnMetaRange(ctx, fn); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Sender) LookupRangeDescriptor(ctx context.Context, key []byte, skipCache bool) (returnedRD *rfpb.RangeDescriptor, returnedErr error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer func() {
		replica_ids := []int64{}
		for _, repl := range returnedRD.GetReplicas() {
			replica_ids = append(replica_ids, int64(repl.GetReplicaId()))
		}
		replicaIDAttr := attribute.Int64Slice("replicas", replica_ids)
		genAttr := attribute.Int("gen", int(returnedRD.GetGeneration()))
		spn.SetAttributes(replicaIDAttr, genAttr)
		tracing.RecordErrorToSpan(spn, returnedErr)
		spn.End()
	}()
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
		log.CtxFatalf(ctx, "Found range %+v that doesn't contain key: %q", rangeDescriptor, string(key))
	}
	return rangeDescriptor, nil
}

func (s *Sender) runOnMetaRange(ctx context.Context, fn runFunc) error {
	retrier := retry.DefaultWithContext(ctx)
	var lastError error
	for retrier.Next() {
		metaRangeDescriptor := s.GetMetaRangeDescriptor()
		if metaRangeDescriptor == nil {
			log.CtxWarning(ctx, "RangeCache did not have meta range yet")
			continue
		}
		_, err := s.tryReplicas(ctx, metaRangeDescriptor, fn, rfpb.Header_LINEARIZABLE)
		if err == nil {
			return nil
		}
		if !status.IsOutOfRangeError(err) {
			return err
		}
		lastError = err
	}
	return status.UnavailableErrorf("sender.runOnMetaRange exceeded retry, lastError: %s", lastError)
}

func (s *Sender) LookupRangeDescriptorsByIDs(ctx context.Context, rangeIDs []uint64) ([]*rfpb.RangeDescriptor, error) {
	if len(rangeIDs) == 0 {
		return nil, nil
	}
	var ranges []*rfpb.RangeDescriptor
	fn := func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header) error {
		rsp, err := fetchRanges(ctx, c, h, rangeIDs)
		if err != nil {
			return err
		}
		ranges = rsp.GetRanges()
		return nil
	}
	if err := s.runOnMetaRange(ctx, fn); err != nil {
		return nil, err
	}
	return ranges, nil
}

func (s *Sender) UpdateRange(rangeDescriptor *rfpb.RangeDescriptor) error {
	return s.rangeCache.UpdateRange(rangeDescriptor)
}

type runFunc func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header) error

func (s *Sender) tryReplicas(ctx context.Context, rd *rfpb.RangeDescriptor, fn runFunc, mode rfpb.Header_ConsistencyMode) (int, error) {
	return s.TryReplicas(ctx, rd, fn, func(rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor) *rfpb.Header {
		return header.New(rd, replica, mode)
	})
}

// tryReplica tries the fn on the replica and returns whether to try a different replica
func (s *Sender) tryReplica(ctx context.Context, rd *rfpb.RangeDescriptor, replica *rfpb.ReplicaDescriptor, fn runFunc, makeHeaderFn header.MakeFunc) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		// continue for loop
	}
	client, err := s.apiClient.GetForReplica(ctx, replica)
	if err != nil {
		if status.IsUnavailableError(err) {
			return true, status.WrapErrorf(err, "skipping c%dn%d: unavailable when getting client", replica.GetRangeId(), replica.GetReplicaId())
		}
		return false, err
	}
	header := makeHeaderFn(rd, replica)

	fnCtx, spn := tracing.StartSpan(ctx)
	spn.SetName("TryReplicas: fn")
	rangeIDAttr := attribute.Int64("range_id", int64(replica.GetRangeId()))
	replicaIDAttr := attribute.Int64("replica_id", int64(replica.GetReplicaId()))
	spn.SetAttributes(rangeIDAttr, replicaIDAttr)

	err = fn(fnCtx, client, header)
	tracing.RecordErrorToSpan(spn, err)
	spn.End()
	if err == nil {
		return false, nil
	}
	if status.IsOutOfRangeError(err) {
		switch m := status.Message(err); {
		// range not found, no replicas are likely to have it; bail.
		case strings.HasPrefix(m, constants.RangeNotCurrentMsg):
			return false, status.OutOfRangeErrorf("failed to TryReplicas on c%dn%d: no replicas are likely to have it: %s", replica.GetRangeId(), replica.GetReplicaId(), m)
		case strings.HasPrefix(m, constants.RangeNotLeasedMsg), strings.HasPrefix(m, constants.RangeLeaseInvalidMsg), strings.HasPrefix(m, constants.RangeNotFoundMsg):
			return true, status.WrapErrorf(err, "skipping c%dn%d: out of range", replica.GetRangeId(), replica.GetReplicaId())
		default:
			break
		}
	}
	if status.IsUnavailableError(err) {
		// try a different replica if the current replica is not available.
		return true, status.WrapErrorf(err, "skipping c%dn%d: unavailable when running fn", replica.GetRangeId(), replica.GetReplicaId())
	}
	return false, err
}

func (s *Sender) TryReplicas(ctx context.Context, rd *rfpb.RangeDescriptor, fn runFunc, makeHeaderFn header.MakeFunc) (replicaIdx int, returnedErr error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	attr := attribute.Int64("range_id", int64(rd.GetRangeId()))
	spn.SetAttributes(attr)
	defer spn.End()

	logs := []string{}
	defer func() {
		tracing.RecordErrorToSpan(spn, returnedErr)
		if returnedErr != nil {
			if len(logs) > 0 {
				log.CtxDebugf(ctx, "failed to TryReplicas: %s. Detailed logs: %s", returnedErr, strings.Join(logs, "\n"))
			}
		}
		replicaIdxAttr := attribute.Int64("replica_idx", int64(replicaIdx))
		spn.SetAttributes(replicaIdxAttr)
	}()

	for i, replica := range rd.GetReplicas() {
		shouldContinue, err := s.tryReplica(ctx, rd, replica, fn, makeHeaderFn)
		if shouldContinue {
			if err != nil {
				logs = append(logs, err.Error())
			}
			continue
		}

		if err == nil {
			return i, nil
		}

		return 0, err
	}

	for _, replica := range rd.GetStaging() {
		shouldContinue, err := s.tryReplica(ctx, rd, replica, fn, makeHeaderFn)
		if shouldContinue {
			if err != nil {
				logs = append(logs, err.Error())
			}
			continue
		}

		// even if fn succeeds on the replica, we want to return 0 to signal to
		// the caller not to set the preferred replica
		return 0, err
	}
	return 0, status.OutOfRangeErrorf("No replicas available in range %d", rd.GetRangeId())
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

func isConflictKeyError(err error) bool {
	return status.IsUnavailableError(err) && strings.Contains(status.Message(err), constants.ConflictKeyMsg)
}

// run looks up the replicas that are responsible for the given key and executes
// fn for each replica until the function succeeds or returns an unretriable
// error.
//
// The range ownership information is retrieved from the range cache, so it can
// be stale. If the remote peer indicates that the client is using a stale
// range, this function will look up the range information again bypassing the
// cache and try the fn again with the new replica information. If the
// fn succeeds with the new replicas, the range cache will be updated with
// the new ownership information.
func (s *Sender) run(ctx context.Context, key []byte, fn runFunc, mods ...Option) (returnedErr error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer func() {
		tracing.RecordErrorToSpan(spn, returnedErr)
		spn.End()
	}()
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
			log.CtxWarningf(ctx, "sender.run error getting rd for %q: %s, %+v", key, err, s.rangeCache.Get(key))
			continue
		}
		i, err := s.tryReplicas(ctx, rangeDescriptor, fn, opts.ConsistencyMode)
		if err == nil {
			if i != 0 {
				replica := rangeDescriptor.GetReplicas()[i]
				s.rangeCache.SetPreferredReplica(ctx, replica, rangeDescriptor)
			}
			return nil
		}
		skipRangeCache = true
		if !status.IsOutOfRangeError(err) && !isConflictKeyError(err) {
			return err
		}
		lastError = err
	}
	return status.UnavailableErrorf("sender.run retries exceeded for key: %q err: %s", key, lastError)
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
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
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

		for _, rk := range keysByRange {
			var rangeRsp interface{}
			i, err := s.tryReplicas(ctx, rk.rd, func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header) error {
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
				skipRangeCache = true
				// No luck for the keys in the range on this try, let's try them
				// on the next attempt.
				remainingKeys = append(remainingKeys, rk.keys...)
			} else {
				if i != 0 {
					replica := rk.rd.GetReplicas()[i]
					s.rangeCache.SetPreferredReplica(ctx, replica, rk.rd)
				}
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
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
	var rsp *rfpb.SyncProposeResponse
	customHeader := batchCmd.Header
	err := s.run(ctx, key, func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header) error {
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

// SyncProposeWithRangeDescriptor calls SyncPropose on different replicas
// specified in the given range descriptor, until one of the replica succeeds.
func (s *Sender) SyncProposeWithRangeDescriptor(ctx context.Context, rd *rfpb.RangeDescriptor, batchCmd *rfpb.BatchCmdRequest, makeHeaderFn header.MakeFunc) (*rfpb.SyncProposeResponse, error) {
	var syncRsp *rfpb.SyncProposeResponse
	runFn := func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header) error {
		r, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
			Header: h,
			Batch:  batchCmd,
		})
		if err != nil {
			return err
		}
		syncRsp = r
		return nil
	}
	retrier := retry.DefaultWithContext(ctx)
	var lastError error
	for retrier.Next() {
		_, err := s.TryReplicas(ctx, rd, runFn, makeHeaderFn)
		if !status.IsOutOfRangeError(err) && !isConflictKeyError(err) {
			return syncRsp, err
		}
		lastError = err
	}
	return syncRsp, status.UnavailableErrorf("SyncProposeWithRangeDescriptor retries exceeded for rd: %+v err: %s", rd, lastError)
}

func (s *Sender) SyncRead(ctx context.Context, key []byte, batchCmd *rfpb.BatchCmdRequest, mods ...Option) (*rfpb.BatchCmdResponse, error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
	var rsp *rfpb.SyncReadResponse
	err := s.run(ctx, key, func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header) error {
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

func (s *Sender) FetchPartitionDescriptors(ctx context.Context) ([]*rfpb.PartitionDescriptor, error) {
	var res []*rfpb.PartitionDescriptor
	start, end := keys.Range(constants.PartitionPrefix)
	fn := func(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header) error {
		req := &rfpb.ScanRequest{
			Start:    start,
			End:      end,
			ScanType: rfpb.ScanRequest_SEEKGE_SCAN_TYPE,
		}
		partitions, err := scanPartitionDescriptors(ctx, c, h, req)
		if err != nil {
			return err
		}
		res = partitions
		return nil
	}
	if err := s.runOnMetaRange(ctx, fn); err != nil {
		return nil, err
	}
	return res, nil
}
