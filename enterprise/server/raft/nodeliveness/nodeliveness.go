package nodeliveness

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hashicorp/serf/serf"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const (
	// Acquire the lease for this long.
	defaultLeaseDuration = 9 * time.Second

	// Renew the lease this many seconds *before* expiry.
	defaultGracePeriod = 4 * time.Second
)

type Liveness struct {
	sender        sender.ISender
	nhid          []byte
	clock         *serf.LamportClock
	leaseDuration time.Duration
	gracePeriod   time.Duration

	mu                    sync.RWMutex
	lastLivenessRecord    *rfpb.NodeLivenessRecord
	stopped               bool
	quitLease             chan struct{}
	timeUntilLeaseRenewal time.Duration

	livenessListeners []chan<- *rfpb.NodeLivenessRecord
}

func New(nodehostID string, sender sender.ISender) *Liveness {
	return &Liveness{
		nhid:                  []byte(nodehostID),
		sender:                sender,
		clock:                 &serf.LamportClock{},
		leaseDuration:         defaultLeaseDuration,
		gracePeriod:           defaultGracePeriod,
		mu:                    sync.RWMutex{},
		lastLivenessRecord:    &rfpb.NodeLivenessRecord{},
		stopped:               true,
		quitLease:             make(chan struct{}),
		timeUntilLeaseRenewal: 0 * time.Second,
		livenessListeners:     make([]chan<- *rfpb.NodeLivenessRecord, 0),
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
	_, err := h.ensureValidLease(false)
	return err
}

func (h *Liveness) Release() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// close the background lease-renewal thread.
	if !h.stopped {
		h.stopped = true
		close(h.quitLease)
	}

	// clear existing lease if it's valid.
	valid := h.verifyLease(h.lastLivenessRecord) == nil
	if !valid {
		return nil
	}

	if h.verifyLease(h.lastLivenessRecord) == nil {
		return h.clearLease()
	}
	return nil
}

// Returns a channel and associated close function. rfpb.NodeLivenessUpdates
// will be published on the channel when the liveness record changes.
// Listeners *must not block* or they risk dropping updates.
func (h *Liveness) AddListener() <-chan *rfpb.NodeLivenessRecord {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch := make(chan *rfpb.NodeLivenessRecord, 5)
	h.livenessListeners = append(h.livenessListeners, ch)
	if err := h.verifyLease(h.lastLivenessRecord); err == nil {
		ch <- h.lastLivenessRecord
	}
	return ch
}

func (h *Liveness) BlockingGetCurrentNodeLiveness() (*rfpb.RangeLeaseRecord_NodeLiveness, error) {
	l, err := h.ensureValidLease(false /*=renew*/)
	if err != nil {
		return nil, err
	}
	return &rfpb.RangeLeaseRecord_NodeLiveness{
		Nhid:  h.nhid,
		Epoch: l.GetEpoch(),
	}, nil
}

func (h *Liveness) BlockingValidateNodeLiveness(nl *rfpb.RangeLeaseRecord_NodeLiveness) error {
	if !bytes.Equal(nl.GetNhid(), h.nhid) {
		return status.FailedPreconditionErrorf("Invalid rangeLease: replicaID mismatch")
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

func (h *Liveness) setLastLivenessRecord(nlr *rfpb.NodeLivenessRecord) {
	epochChanged := nlr.GetEpoch()-h.lastLivenessRecord.GetEpoch() != 0

	// Set the member var.
	h.lastLivenessRecord = nlr

	if !epochChanged {
		return
	}

	// Update any listeners.
	for _, ch := range h.livenessListeners {
		ch <- nlr
	}
}

func (h *Liveness) ensureValidLease(forceRenewal bool) (*rfpb.NodeLivenessRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	alreadyValid := false
	if err := h.verifyLease(h.lastLivenessRecord); err == nil {
		alreadyValid = true
	}

	if alreadyValid && !forceRenewal {
		return h.lastLivenessRecord, nil
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

	if !alreadyValid {
		log.Debugf("Acquired %s", h.string(h.nhid, h.lastLivenessRecord))
	}

	// We just renewed the lease. If there isn't already a background
	// thread running to keep it renewed, start one now.
	if h.stopped {
		h.stopped = false
		h.quitLease = make(chan struct{})
		go h.keepLeaseAlive(h.quitLease)
	}
	return h.lastLivenessRecord, nil
}

func (h *Liveness) sendCasRequest(ctx context.Context, expectedValue, newVal []byte) (*rfpb.KV, error) {
	leaseKey := keys.MakeKey(constants.SystemPrefix, h.nhid)
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
	rsp, err := h.sender.SyncPropose(ctx, leaseKey, casRequest)
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

	_, err = h.sendCasRequest(context.TODO(), expectedValue, newVal)
	if err == nil {
		h.setLastLivenessRecord(nil)
	}
	return err
}

func (h *Liveness) renewLease() error {
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

	kv, err := h.sendCasRequest(context.TODO(), expectedValue, newVal)
	if err == nil {
		// This means we set the lease succesfully.
		h.setLastLivenessRecord(leaseRequest)
		expiration := time.Unix(0, h.lastLivenessRecord.GetExpiration())
		timeUntilExpiry := expiration.Sub(time.Now())
		h.timeUntilLeaseRenewal = timeUntilExpiry - h.gracePeriod
	} else if status.IsFailedPreconditionError(err) && strings.Contains(err.Error(), constants.CASErrorMessage) {
		// This means another lease was active -- we should save it, so that
		// we can correctly set the expected value with our next CAS request,
		// and witness its epoch so that our next set request has a higher one.
		if h.lastLivenessRecord == nil {
			h.setLastLivenessRecord(&rfpb.NodeLivenessRecord{})
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

func (h *Liveness) keepLeaseAlive(quitLease chan struct{}) {
	for {
		h.mu.Lock()
		ttr := h.timeUntilLeaseRenewal
		h.mu.Unlock()

		select {
		case <-quitLease:
			// TODO(tylerw): attempt to drop lease gracefully.
			return
		case <-time.After(ttr):
			h.ensureValidLease(true /*=forceRenewal*/)
		}
	}
}

func (h *Liveness) string(replicaID []byte, llr *rfpb.NodeLivenessRecord) string {
	// Don't lock here (to avoid recursive locking).
	err := h.verifyLease(llr)
	if err != nil {
		return fmt.Sprintf("Liveness(%q): invalid (%s)", string(replicaID), err)
	}
	lifetime := time.Unix(0, llr.GetExpiration()).Sub(time.Now())
	return fmt.Sprintf("Liveness(%q): [epoch: %d, expires in %s]", string(replicaID), llr.GetEpoch(), lifetime)
}

func (h *Liveness) String() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.string(h.nhid, h.lastLivenessRecord)
}
