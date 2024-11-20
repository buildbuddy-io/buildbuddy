package sender

import (
	"context"
	"fmt"
	"strings"

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

func lookupRangeDescriptor(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, key []byte) (*rfpb.RangeDescriptor, error) {
	batchReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    keys.RangeMetaKey(key),
		End:      constants.SystemPrefix,
		ScanType: rfpb.ScanRequest_SEEKGT_SCAN_TYPE,
		Limit:    1,
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

func lookupActiveReplicas(ctx context.Context, c rfspb.ApiClient, h *rfpb.Header, replicas []*rfpb.ReplicaDescriptor) ([]*rfpb.ReplicaDescriptor, error) {
	replicaKey := func(r *rfpb.ReplicaDescriptor) string {
		return fmt.Sprintf("%d-%d", r.GetRangeId(), r.GetReplicaId())
	}
	candidateReplicas := make(map[string]*rfpb.ReplicaDescriptor, len(replicas))
	for _, r := range replicas {
		candidateReplicas[replicaKey(r)] = r
	}

	batchReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.ScanRequest{
		Start:    constants.MetaRangePrefix,
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

	matchedReplicas := make([]*rfpb.ReplicaDescriptor, 0, len(replicas))
	rd := &rfpb.RangeDescriptor{}
	for _, kv := range scanRsp.GetKvs() {
		if err := proto.Unmarshal(kv.GetValue(), rd); err != nil {
			log.Errorf("scan returned unparsable kv: %s", err)
			continue
		}
		for _, r2 := range rd.GetReplicas() {
			if r, present := candidateReplicas[replicaKey(r2)]; present {
				matchedReplicas = append(matchedReplicas, r)
				// delete this candidate so it cannot possibly
				// show up more than once in matchedReplicas.
				delete(candidateReplicas, replicaKey(r2))
			}
		}
	}
	return matchedReplicas, nil
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
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
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

func (s *Sender) LookupActiveReplicas(ctx context.Context, candidates []*rfpb.ReplicaDescriptor) ([]*rfpb.ReplicaDescriptor, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	retrier := retry.DefaultWithContext(ctx)

	var activeReplicas []*rfpb.ReplicaDescriptor
	fn := func(c rfspb.ApiClient, h *rfpb.Header) error {
		replicas, err := lookupActiveReplicas(ctx, c, h, candidates)
		if err != nil {
			return err
		}
		activeReplicas = replicas
		return nil
	}
	for retrier.Next() {
		metaRangeDescriptor := s.rangeCache.Get(constants.MetaRangePrefix)
		if metaRangeDescriptor == nil {
			log.Warning("RangeCache did not have meta range yet")
			continue
		}
		_, err := s.tryReplicas(ctx, metaRangeDescriptor, fn, rfpb.Header_LINEARIZABLE)
		if err == nil {
			return activeReplicas, nil
		}
		if !status.IsOutOfRangeError(err) {
			return nil, err
		}
	}
	return nil, status.UnavailableError("Error finding active replicas")
}

func (s *Sender) UpdateRange(rangeDescriptor *rfpb.RangeDescriptor) error {
	return s.rangeCache.UpdateRange(rangeDescriptor)
}

type runFunc func(c rfspb.ApiClient, h *rfpb.Header) error
type makeHeaderFunc func(rd *rfpb.RangeDescriptor, replicaIdx int) *rfpb.Header

func (s *Sender) tryReplicas(ctx context.Context, rd *rfpb.RangeDescriptor, fn runFunc, mode rfpb.Header_ConsistencyMode) (int, error) {
	return s.TryReplicas(ctx, rd, fn, func(rd *rfpb.RangeDescriptor, replicaIdx int) *rfpb.Header {
		return header.New(rd, replicaIdx, mode)
	})
}

func (s *Sender) TryReplicas(ctx context.Context, rd *rfpb.RangeDescriptor, fn runFunc, makeHeaderFn makeHeaderFunc) (int, error) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
	for i, replica := range rd.GetReplicas() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			// continue for loop
		}
		client, err := s.apiClient.GetForReplica(ctx, replica)
		if err != nil {
			if status.IsUnavailableError(err) {
				log.Debugf("TryReplicas: replica %+v is unavailable: %s", replica, err)
				// try a different replica if the current replica is not available.
				continue
			}
			return 0, err
		}
		header := makeHeaderFn(rd, i)
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
func (s *Sender) run(ctx context.Context, key []byte, fn runFunc, mods ...Option) error {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
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
			log.Warningf("sender.run error getting rd for %q: %s, %+v", key, err, s.rangeCache.Get(key))
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
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
	var rsp *rfpb.SyncProposeResponse
	customHeader := batchCmd.Header
	err := s.run(ctx, key, func(c rfspb.ApiClient, h *rfpb.Header) error {
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
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
	var rsp *rfpb.SyncReadResponse
	err := s.run(ctx, key, func(c rfspb.ApiClient, h *rfpb.Header) error {
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
