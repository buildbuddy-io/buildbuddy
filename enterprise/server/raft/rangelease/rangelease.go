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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

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
	return r.Contains(keys.MinByte) && r.Contains([]byte{constants.UnsplittableMaxByte - 1})
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
	_, err := l.ensureValidLease(ctx, false)
	return err
}

func (l *Lease) Release(ctx context.Context) error {
	err := l.dropLease(ctx)
	return err
}

func (l *Lease) dropLease(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// close the background lease-renewal thread.
	if !l.stopped {
		l.stopped = true
		close(l.quitLease)
	}

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

func (l *Lease) verifyLease(ctx context.Context, rl *rfpb.RangeLeaseRecord) error {
	if rl == nil {
		return status.FailedPreconditionError("Invalid rangeLease: nil")
	}

	if !proto.Equal(l.replica.GetRangeLease(), rl) {
		return status.FailedPreconditionError("rangeLease does not match replica")
	}

	// This is a node epoch based lease, so check node and epoch.
	if nl := rl.GetNodeLiveness(); nl != nil {
		return l.liveness.BlockingValidateNodeLiveness(ctx, nl)
	}

	// This is a time based lease, so check expiration time.
	expireAt := time.Unix(0, rl.GetReplicaExpiration().GetExpiration())
	if time.Now().After(expireAt) {
		return status.FailedPreconditionErrorf("Invalid rangeLease: expired at %s", expireAt)
	}
	return nil
}

func (l *Lease) sendCasRequest(ctx context.Context, expectedValue, newVal []byte) (*rfpb.KV, error) {
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

func (l *Lease) assembleLeaseRequest(ctx context.Context) (*rfpb.RangeLeaseRecord, error) {
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

func (l *Lease) renewLease(ctx context.Context) error {
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

func (l *Lease) ensureValidLease(ctx context.Context, forceRenewal bool) (*rfpb.RangeLeaseRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	alreadyValid := false
	if err := l.verifyLease(ctx, l.leaseRecord); err == nil {
		alreadyValid = true
	}

	if alreadyValid && !forceRenewal {
		return l.leaseRecord, nil
	}

	renewed := false
	for !renewed {
		if err := l.renewLease(ctx); err != nil {
			return nil, err
		}
		if err := l.verifyLease(ctx, l.leaseRecord); err == nil {
			renewed = true
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
		l.mu.Lock()
		timeUntilRenewal := l.timeUntilLeaseRenewal
		l.mu.Unlock()

		select {
		case <-quit:
			return
		case <-time.After(timeUntilRenewal):
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := l.ensureValidLease(ctx, true /*forceRenewal*/)
			cancel()
			if err != nil {
				log.Errorf("failed to ensure valid lease for c%dn%d: %s", l.replica.RangeID(), l.replica.ReplicaID(), err)
			}
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
