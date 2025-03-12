package rangelease

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const (
	// Acquire the lease for this long.
	defaultLeaseDuration = 9 * time.Second

	// Renew the lease this many seconds *before* expiry.
	defaultGracePeriod = 4 * time.Second
)

func ContainsMetaRange(rd *rfpb.RangeDescriptor) bool {
	r := rangemap.Range{Start: rd.Start, End: rd.End}
	return r.Contains(constants.MetaRangePrefix) && r.Contains([]byte{constants.UnsplittableMaxByte - 1})
}

type Lease struct {
	nodeHost client.NodeHost
	log      log.Logger
	replica  *replica.Replica
	liveness *nodeliveness.Liveness
	session  *client.Session

	leaseDuration time.Duration
	gracePeriod   time.Duration

	rangeDescriptor *rfpb.RangeDescriptor
	mu              sync.RWMutex
	leaseRecord     *rfpb.RangeLeaseRecord

	timeUntilLeaseRenewal time.Duration
	stopped               bool
	quitLease             chan struct{}
}

func New(nodeHost client.NodeHost, session *client.Session, log log.Logger, liveness *nodeliveness.Liveness, rd *rfpb.RangeDescriptor, r *replica.Replica) *Lease {
	return &Lease{
		nodeHost:              nodeHost,
		log:                   log,
		replica:               r,
		liveness:              liveness,
		session:               session,
		leaseDuration:         defaultLeaseDuration,
		gracePeriod:           defaultGracePeriod,
		rangeDescriptor:       rd,
		mu:                    sync.RWMutex{},
		leaseRecord:           &rfpb.RangeLeaseRecord{},
		timeUntilLeaseRenewal: time.Duration(math.MaxInt64),
		stopped:               true,
	}
}

func (l *Lease) WithTimeouts(leaseDuration, gracePeriod time.Duration) *Lease {
	l.leaseDuration = leaseDuration
	l.gracePeriod = gracePeriod
	return l
}

func (l *Lease) Lease(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	attr := attribute.Int64("range_id", int64(l.rangeDescriptor.GetRangeId()))
	span.SetAttributes(attr)
	defer span.End()
	_, err := l.renewLeaseUntilValid(ctx)
	return err
}

func (l *Lease) Release(ctx context.Context) error {
	err := l.dropLease(ctx)
	return err
}

func (l *Lease) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	// close the background lease-renewal thread.
	if !l.stopped {
		l.stopped = true
		close(l.quitLease)
	}
}

func (l *Lease) dropLease(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	l.Stop()

	l.mu.Lock()
	defer l.mu.Unlock()
	// clear existing lease if it's valid.
	valid := l.verifyLease(ctx, l.leaseRecord) == nil
	if !valid {
		return nil
	}
	return l.clearLeaseValue(ctx)
}

func (l *Lease) GetRangeDescriptor() *rfpb.RangeDescriptor {
	return l.rangeDescriptor
}

func (l *Lease) verifyLease(ctx context.Context, rl *rfpb.RangeLeaseRecord) (returnedErr error) {
	ctx, span := tracing.StartSpan(ctx)
	defer func() {
		if returnedErr != nil {
			span.RecordError(returnedErr)
			span.SetStatus(codes.Error, returnedErr.Error())
		}
		span.End()
	}()
	if rl == nil {
		return status.FailedPreconditionError("Invalid rangeLease: nil")
	}

	replicaRL := l.replica.GetRangeLease()
	if !proto.Equal(replicaRL, rl) {
		return status.FailedPreconditionErrorf("rangeLease %v does not match replica %v", rl, replicaRL)
	}

	// This is a node epoch based lease, so check node and epoch.
	if nl := rl.GetNodeLiveness(); nl != nil {
		if err := l.liveness.BlockingValidateNodeLiveness(ctx, nl); err != nil {
			return status.InternalErrorf("failed to validate node liveness: %s", err)
		}
		return nil
	}

	// This is a time based lease, so check expiration time.
	expireAt := time.Unix(0, rl.GetReplicaExpiration().GetExpiration())
	if time.Now().After(expireAt) {
		return status.FailedPreconditionErrorf("Invalid rangeLease: expired at %s", expireAt)
	}
	return nil
}

func (l *Lease) sendCasRequest(ctx context.Context, expectedValue, newVal []byte) (returnedKV *rfpb.KV, returnedErr error) {
	ctx, span := tracing.StartSpan(ctx)
	defer func() {
		if returnedErr != nil {
			span.RecordError(returnedErr)
			span.SetStatus(codes.Error, returnedErr.Error())
		}
		span.End()
	}()
	leaseKey := constants.LocalRangeLeaseKey
	casRequest, err := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   leaseKey,
			Value: newVal,
		},
		ExpectedValue: expectedValue,
	}).ToProto()
	if err != nil {
		return nil, err
	}
	rsp, err := l.session.SyncProposeLocal(ctx, l.nodeHost, l.replica.RangeID(), casRequest)
	if err != nil {
		return nil, err
	}
	casResponse, err := rbuilder.NewBatchResponseFromProto(rsp).CASResponse(0)
	if casResponse != nil {
		return casResponse.GetKv(), err
	}
	return nil, err
}

func (l *Lease) clearLeaseValue(ctx context.Context) error {
	l.leaseRecord = nil
	return nil
}

func (l *Lease) assembleLeaseRequest(ctx context.Context) (returnedRecord *rfpb.RangeLeaseRecord, returnedErr error) {
	ctx, span := tracing.StartSpan(ctx)
	defer func() {
		if returnedErr != nil {
			span.RecordError(returnedErr)
			span.SetStatus(codes.Error, returnedErr.Error())
		}
		span.End()
	}()

	// To prevent circular dependencies:
	//    (metarange -> range lease -> node liveness -> metarange)
	// any range that includes the metarange will be leased with a
	// time-based lease, rather than a node epoch based one.
	leaseRecord := &rfpb.RangeLeaseRecord{}
	if ContainsMetaRange(l.rangeDescriptor) {
		leaseRecord.Value = &rfpb.RangeLeaseRecord_ReplicaExpiration_{
			ReplicaExpiration: &rfpb.RangeLeaseRecord_ReplicaExpiration{
				Nhid:       []byte(l.nodeHost.ID()),
				Expiration: time.Now().Add(l.leaseDuration).UnixNano(),
			},
		}
	} else {
		nl, err := l.liveness.BlockingGetCurrentNodeLiveness(ctx)
		if err != nil {
			return nil, err
		}
		leaseRecord.Value = &rfpb.RangeLeaseRecord_NodeLiveness_{
			NodeLiveness: nl,
		}
	}
	return leaseRecord, nil
}

func (l *Lease) renewLease(ctx context.Context) (returnedErr error) {
	ctx, span := tracing.StartSpan(ctx)
	defer func() {
		if returnedErr != nil {
			span.RecordError(returnedErr)
			span.SetStatus(codes.Error, returnedErr.Error())
		}
		span.End()
	}()
	var expectedValue []byte
	if l.leaseRecord != nil {
		buf, err := proto.Marshal(l.leaseRecord)
		if err != nil {
			return err
		}
		expectedValue = buf
	}

	leaseRequest, err := l.assembleLeaseRequest(ctx)
	if err != nil {
		return err
	}
	newVal, err := proto.Marshal(leaseRequest)
	if err != nil {
		return err
	}

	if bytes.Equal(newVal, expectedValue) {
		var replicaRLValue []byte
		replicaRL := l.replica.GetRangeLease()
		if replicaRL != nil {
			replicaRLValue, err = proto.Marshal(replicaRL)
			if err != nil {
				return err
			}
		}
		if bytes.Equal(replicaRLValue, expectedValue) {
			// For node-epoch based leases, forcing renewal is kind of non-
			// sensical. Rather than prevent this at a higher level, we
			// detect the case where we are trying to set the lease to the
			// already set value, and short-circuit renewal.
			return nil
		}
	}

	kv, err := l.sendCasRequest(ctx, expectedValue, newVal)
	if err == nil {
		// This means we set the lease succesfully.
		l.leaseRecord = leaseRequest
		span.AddEvent(fmt.Sprintf("sendCasRequest succeeded, setting l.leaseRecord=%v", leaseRequest))
	} else if status.IsFailedPreconditionError(err) && strings.Contains(err.Error(), constants.CASErrorMessage) {
		// This means another lease was active -- we should save it, so that
		// we can correctly set the expected value with our next CAS request,
		// and witness its epoch so that our next set request has a higher one.
		activeLease := &rfpb.RangeLeaseRecord{}
		err := proto.Unmarshal(kv.GetValue(), activeLease)
		if err != nil {
			return err
		}
		l.leaseRecord = activeLease
		span.AddEvent(fmt.Sprintf("another lease is active, setting l.leaseRecord=%v", activeLease))
	} else {
		return err
	}

	// If the lease is now set and has an expiration date in the future,
	// set time until lease renewal. This will not happen for node liveness
	// epoch based range leases.
	if l.leaseRecord != nil && l.leaseRecord.GetReplicaExpiration().GetExpiration() != 0 {
		expiration := time.Unix(0, l.leaseRecord.GetReplicaExpiration().GetExpiration())
		timeUntilExpiry := time.Until(expiration)
		l.timeUntilLeaseRenewal = timeUntilExpiry - l.gracePeriod
	}
	return nil
}

func (l *Lease) renewLeaseUntilValid(ctx context.Context) (returnedRecord *rfpb.RangeLeaseRecord, returnedErr error) {
	ctx, span := tracing.StartSpan(ctx)
	defer func() {
		if returnedErr != nil {
			span.RecordError(returnedErr)
			span.SetStatus(codes.Error, returnedErr.Error())
		}
		span.End()
	}()
	l.mu.Lock()
	defer l.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default: // continue with for loop
		}
		if err := l.renewLease(ctx); err != nil {
			return nil, status.InternalErrorf("failed to renew lease: %s", err)
		}
		if err := l.verifyLease(ctx, l.leaseRecord); err == nil {
			break
		}
	}

	// We just renewed the lease. If there isn't already a background
	// thread running to keep it renewed, start one now.
	if l.stopped {
		l.stopped = false
		l.quitLease = make(chan struct{})
		if l.leaseRecord.GetReplicaExpiration().GetExpiration() != 0 {
			// Only start the renew-goroutine for time-based
			// leases which need periodic renewal.
			go l.keepLeaseAlive(l.quitLease)
		}
	}
	return l.leaseRecord, nil
}

func (l *Lease) keepLeaseAlive(quit chan struct{}) {
	for {
		l.mu.RLock()
		timeUntilRenewal := l.timeUntilLeaseRenewal
		l.mu.RUnlock()

		select {
		case <-quit:
			return
		case <-time.After(timeUntilRenewal):
			ctx, spn := tracing.StartSpan(context.Background())
			_, err := l.renewLeaseUntilValid(ctx)
			if err != nil {
				log.Errorf("failed to ensure valid lease for c%dn%d: %s", l.replica.RangeID(), l.replica.ReplicaID(), err)
			}
			spn.End()
		}
	}
}

func (l *Lease) Valid(ctx context.Context) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.verifyLease(ctx, l.leaseRecord); err == nil {
		return true
	}
	return false
}

func (l *Lease) string(ctx context.Context, rd *rfpb.RangeDescriptor, lr *rfpb.RangeLeaseRecord) string {
	// Don't lock here (to avoid recursive locking).
	leaseName := fmt.Sprintf("RangeLease(%d) [%q, %q)", rd.GetRangeId(), rd.GetStart(), rd.GetEnd())
	err := l.verifyLease(ctx, lr)
	if err != nil {
		return fmt.Sprintf("%s invalid (%s)", leaseName, err)
	}
	if nl := lr.GetNodeLiveness(); nl != nil {
		return fmt.Sprintf("%s [node epoch: %d]", leaseName, nl.GetEpoch())
	}
	lifetime := time.Until(time.Unix(0, lr.GetReplicaExpiration().GetExpiration()))
	return fmt.Sprintf("%s [expires in: %s]", leaseName, lifetime)
}

func (l *Lease) Desc(ctx context.Context) string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.string(ctx, l.rangeDescriptor, l.leaseRecord)
}
