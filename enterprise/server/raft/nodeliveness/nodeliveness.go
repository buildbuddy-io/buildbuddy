package nodeliveness

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const (
	// Acquire the lease for this long.
	defaultLeaseDuration = 9 * time.Second

	// Renew the lease this many seconds *before* expiry.
	defaultGracePeriod = 4 * time.Second
)

type Liveness struct {
	localSender   client.LocalSender
	nodeID        []byte
	clock         *serf.LamportClock
	leaseDuration time.Duration
	gracePeriod   time.Duration

	mu                    sync.RWMutex
	lastLivenessRecord    *rfpb.NodeLivenessRecord
	quitLease             chan struct{}
	timeUntilLeaseRenewal time.Duration
}

func New(nodeID string, localSender client.LocalSender) *Liveness {
	return &Liveness{
		nodeID:                []byte(nodeID),
		localSender:           localSender,
		clock:                 &serf.LamportClock{},
		leaseDuration:         defaultLeaseDuration,
		gracePeriod:           defaultGracePeriod,
		mu:                    sync.RWMutex{},
		lastLivenessRecord:    &rfpb.NodeLivenessRecord{},
		quitLease:             make(chan struct{}),
		timeUntilLeaseRenewal: 0 * time.Second,
	}
}

func (h *Liveness) WithTimeouts(leaseDuration, gracePeriod time.Duration) *Liveness {
	h.leaseDuration = leaseDuration
	h.gracePeriod = gracePeriod
	return h
}

func (h *Liveness) Valid() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if err := h.verifyLease(h.lastLivenessRecord); err == nil {
		return true
	}
	return false
}

func (h *Liveness) Lease() error {
	h.quitLease = make(chan struct{})
	_, err := h.ensureValidLease(false)
	if err == nil {
		go h.keepLeaseAlive()
	}
	return err
}

func (h *Liveness) Release() error {
	close(h.quitLease)
	// clear existing lease if it's valid.
	h.mu.RLock()
	valid := h.verifyLease(h.lastLivenessRecord) == nil
	h.mu.RUnlock()

	if valid {
		h.mu.Lock()
		defer h.mu.Unlock()
		if err := h.clearLease(); err != nil {
			return err
		}
	}
	return nil
}

func (h *Liveness) BlockingGetCurrentNodeLiveness() (*rfpb.RangeLeaseRecord_NodeLiveness, error) {
	l, err := h.ensureValidLease(false /*=renew*/)
	if err != nil {
		return nil, err
	}
	return &rfpb.RangeLeaseRecord_NodeLiveness{
		NodeId: h.nodeID,
		Epoch:  l.GetEpoch(),
	}, nil
}

func (h *Liveness) BlockingValidateNodeLiveness(nl *rfpb.RangeLeaseRecord_NodeLiveness) error {
	if bytes.Compare(nl.GetNodeId(), h.nodeID) != 0 {
		return status.FailedPreconditionErrorf("Invalid rangeLease: nodeID mismatch")
	}
	l, err := h.ensureValidLease(false /*=renew*/)
	if err != nil {
		return err
	}
	if l.GetEpoch() != nl.GetEpoch() {
		return status.FailedPreconditionErrorf("Invalid rangeLease: epoch %d is no longer valid (current epoch: %d)", nl.GetEpoch(), l.GetEpoch())
	}
	return nil
}

func (h *Liveness) verifyLease(l *rfpb.NodeLivenessRecord) error {
	if serf.LamportTime(l.GetEpoch()) != h.clock.Time() {
		return status.FailedPreconditionErrorf("LeaseInvalid: lease epoch %d != current epoch: %d", l.GetEpoch(), h.clock.Time())
	}

	now := time.Now()
	expireTime := time.Unix(0, l.GetExpiration())
	if now.After(expireTime) {
		return status.FailedPreconditionErrorf("LeaseInvalid: expired at %s (current time: %s)", expireTime, now)
	}

	return nil
}

func (h *Liveness) ensureValidLease(forceRenewal bool) (*rfpb.NodeLivenessRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !forceRenewal {
		if err := h.verifyLease(h.lastLivenessRecord); err == nil {
			return h.lastLivenessRecord, nil
		}
	}

	renewed := false
	for !renewed {
		if err := h.renewLease(); err != nil {
			return nil, err
		}
		if err := h.verifyLease(h.lastLivenessRecord); err == nil {
			renewed = true
		}
	}
	return h.lastLivenessRecord, nil
}

func (h *Liveness) sendCasRequest(ctx context.Context, expectedValue, newVal []byte) (*rfpb.KV, error) {
	leaseKey := keys.MakeKey(constants.SystemPrefix, h.nodeID)
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
	rsp, err := h.localSender.SyncProposeLocal(ctx, 1, casRequest)
	if err != nil {
		// This indicates a communication error proposing the message.
		return nil, err
	}
	casResponse, err := rbuilder.NewBatchResponseFromProto(rsp).CASResponse(0)
	if casResponse != nil {
		return casResponse.GetKv(), err
	}
	return nil, err
}

func (h *Liveness) clearLease() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var expectedValue []byte
	if h.lastLivenessRecord != nil {
		buf, err := proto.Marshal(h.lastLivenessRecord)
		if err != nil {
			return err
		}
		expectedValue = buf
	}

	leaseRequest := &rfpb.NodeLivenessRecord{
		Epoch:      int64(h.clock.Time()),
		Expiration: 1,
	}
	newVal, err := proto.Marshal(leaseRequest)
	if err != nil {
		return err
	}

	_, err = h.sendCasRequest(ctx, expectedValue, newVal)
	if err == nil {
		h.lastLivenessRecord = nil
	}
	return err
}

func (h *Liveness) renewLease() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var expectedValue []byte
	if h.lastLivenessRecord != nil {
		buf, err := proto.Marshal(h.lastLivenessRecord)
		if err != nil {
			return err
		}
		expectedValue = buf
	}

	leaseRequest := &rfpb.NodeLivenessRecord{
		Epoch:      int64(h.clock.Time()),
		Expiration: time.Now().Add(h.leaseDuration).UnixNano(),
	}
	newVal, err := proto.Marshal(leaseRequest)
	if err != nil {
		return err
	}

	kv, err := h.sendCasRequest(ctx, expectedValue, newVal)
	if err == nil {
		// This means we set the lease succesfully.
		h.lastLivenessRecord = leaseRequest
		expiration := time.Unix(0, h.lastLivenessRecord.GetExpiration())
		timeUntilExpiry := expiration.Sub(time.Now())
		h.timeUntilLeaseRenewal = timeUntilExpiry - h.gracePeriod
	} else if status.IsFailedPreconditionError(err) && strings.Contains(err.Error(), constants.CASErrorMessage) {
		// This means another lease was active -- we should save it, so that
		// we can correctly set the expected value with our next CAS request,
		// and witness its epoch so that our next set request has a higher one.
		if h.lastLivenessRecord == nil {
			h.lastLivenessRecord = &rfpb.NodeLivenessRecord{}
		}
		err := proto.Unmarshal(kv.GetValue(), h.lastLivenessRecord)
		if err != nil {
			return err
		}
		h.clock.Witness(serf.LamportTime(h.lastLivenessRecord.GetEpoch()))
	} else {
		return err
	}
	return nil
}

func (h *Liveness) keepLeaseAlive() {
	for {
		select {
		case <-h.quitLease:
			// TODO(tylerw): attempt to drop lease gracefully.
			return
		case <-time.After(h.timeUntilLeaseRenewal):
			h.ensureValidLease(true /*=forceRenewal*/)
		}
	}
}

func (h *Liveness) String() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	err := h.verifyLease(h.lastLivenessRecord)
	if err != nil {
		return fmt.Sprintf("Liveness(%q): invalid (%s)", string(h.nodeID), err)
	}
	l := h.lastLivenessRecord
	lifetime := time.Unix(0, l.GetExpiration()).Sub(time.Now())
	return fmt.Sprintf("Liveness(%q): [epoch: %d, expires in %s]", string(h.nodeID), l.GetEpoch(), lifetime)
}
