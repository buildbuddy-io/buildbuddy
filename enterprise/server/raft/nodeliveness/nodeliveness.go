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
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/hashicorp/serf/serf"
	"golang.org/x/sync/errgroup"

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
	ctx                   context.Context
	cancelFn              context.CancelFunc
	timeUntilLeaseRenewal time.Duration

	livenessListeners []chan<- *rfpb.NodeLivenessRecord

	keepAliveOnce sync.Once

	egMu sync.Mutex
	eg   *errgroup.Group
}

func New(ctx context.Context, nodehostID string, sender sender.ISender) *Liveness {
	ctx, cancelFn := context.WithCancel(ctx)
	eg, gctx := errgroup.WithContext(ctx)
	return &Liveness{
		ctx:                   gctx,
		eg:                    eg,
		cancelFn:              cancelFn,
		nhid:                  []byte(nodehostID),
		sender:                sender,
		clock:                 &serf.LamportClock{},
		leaseDuration:         defaultLeaseDuration,
		gracePeriod:           defaultGracePeriod,
		mu:                    sync.RWMutex{},
		lastLivenessRecord:    &rfpb.NodeLivenessRecord{},
		timeUntilLeaseRenewal: 0 * time.Second,
		livenessListeners:     make([]chan<- *rfpb.NodeLivenessRecord, 0),
	}
}

func (h *Liveness) WithTimeouts(leaseDuration, gracePeriod time.Duration) *Liveness {
	h.leaseDuration = leaseDuration
	h.gracePeriod = gracePeriod
	return h
}

func (h *Liveness) Valid(ctx context.Context) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if err := h.verifyLease(ctx, h.lastLivenessRecord); err == nil {
		return true
	}
	return false
}

func (h *Liveness) Lease(ctx context.Context) error {
	_, err := h.ensureValidLease(ctx, false)
	return err
}

func (h *Liveness) Stop() error {
	log.Debugf("Liveness (nhid=%s) shutdown started", h.nhid)
	now := time.Now()
	defer func() {
		log.Debugf("Liveness (nhid=%s) shutdown finished in %s", h.nhid, time.Since(now))
	}()
	h.cancelFn()
	h.egMu.Lock()
	h.eg.Wait()
	h.egMu.Unlock()
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
	if err := h.verifyLease(h.ctx, h.lastLivenessRecord); err == nil {
		ch <- h.lastLivenessRecord
	}
	return ch
}

func (h *Liveness) BlockingGetCurrentNodeLiveness(ctx context.Context) (*rfpb.RangeLeaseRecord_NodeLiveness, error) {
	l, err := h.ensureValidLease(ctx, false /*=renew*/)
	if err != nil {
		return nil, err
	}
	return &rfpb.RangeLeaseRecord_NodeLiveness{
		Nhid:  h.nhid,
		Epoch: l.GetEpoch(),
	}, nil
}

func (h *Liveness) BlockingValidateNodeLiveness(ctx context.Context, nl *rfpb.RangeLeaseRecord_NodeLiveness) (returnedErr error) {
	ctx, span := tracing.StartSpan(ctx)
	defer func() {
		tracing.RecordErrorToSpan(span, returnedErr)
		span.End()
	}()
	if !bytes.Equal(nl.GetNhid(), h.nhid) {
		return status.FailedPreconditionErrorf("Invalid rangeLease: NHID mismatch, expected=%q, but RangeLeaseRecord has %q", h.nhid, nl.GetNhid())
	}
	l, err := h.ensureValidLease(ctx, false /*=renew*/)
	if err != nil {
		return err
	}
	if l.GetEpoch() != nl.GetEpoch() {
		return status.FailedPreconditionErrorf("Invalid rangeLease: epoch %d is no longer valid (current epoch: %d)", nl.GetEpoch(), l.GetEpoch())
	}
	return nil
}

func (h *Liveness) verifyLease(ctx context.Context, l *rfpb.NodeLivenessRecord) (retErr error) {
	_, span := tracing.StartSpan(ctx)
	defer func() {
		tracing.RecordErrorToSpan(span, retErr)
		span.End()
	}()
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

func (h *Liveness) ensureValidLease(ctx context.Context, forceRenewal bool) (returnedRecord *rfpb.NodeLivenessRecord, returnedErr error) {
	ctx, span := tracing.StartSpan(ctx)
	defer func() {
		tracing.RecordErrorToSpan(span, returnedErr)
		span.End()
	}()
	start := time.Now()
	h.mu.RLock()
	lastRecord := h.lastLivenessRecord.CloneVT()
	h.mu.RUnlock()

	alreadyValid := false
	if err := h.verifyLease(ctx, lastRecord); err == nil {
		alreadyValid = true
	}

	if alreadyValid && !forceRenewal {
		return lastRecord, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// continue with for loop
		}
		if err := h.renewLease(ctx); err != nil {
			return nil, status.InternalErrorf("failed to renew node liveness: %s", err)
		}
		if err := h.verifyLease(ctx, h.lastLivenessRecord); err == nil {
			break
		}
	}

	if !alreadyValid {
		dur := time.Since(start)
		log.Debugf("Acquired %s after %s", h.string(h.nhid, h.lastLivenessRecord), dur)
	}

	// We just renewed the lease. If there isn't already a background
	// thread running to keep it renewed, start one now.
	h.keepAliveOnce.Do(func() {
		h.egMu.Lock()
		defer h.egMu.Unlock()
		h.eg.Go(func() error {
			h.keepLeaseAlive()
			return nil
		})
	})
	return h.lastLivenessRecord, nil
}

func (h *Liveness) sendCasRequest(ctx context.Context, expectedValue, newVal []byte) (retKV *rfpb.KV, retErr error) {
	ctx, span := tracing.StartSpan(ctx)
	defer func() {
		tracing.RecordErrorToSpan(span, retErr)
		span.End()
	}()
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
	h.setLastLivenessRecord(nil)
	return nil
}

func (h *Liveness) renewLease(ctx context.Context) (returnedErr error) {
	ctx, span := tracing.StartSpan(ctx)
	defer func() {
		tracing.RecordErrorToSpan(span, returnedErr)
		span.End()
	}()
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
		h.setLastLivenessRecord(leaseRequest)
		expiration := time.Unix(0, h.lastLivenessRecord.GetExpiration())
		timeUntilExpiry := time.Until(expiration)
		h.timeUntilLeaseRenewal = timeUntilExpiry - h.gracePeriod
		span.AddEvent(fmt.Sprintf("sendCasRequest succeeded, setting last liveness record=%v", leaseRequest))
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
		span.AddEvent(fmt.Sprintf("another lease is active, lastLivenessRecord is %v", h.lastLivenessRecord))
	} else {
		return err
	}
	return nil
}

func (h *Liveness) keepLeaseAlive() {
	for {
		h.mu.Lock()
		ttr := h.timeUntilLeaseRenewal
		h.mu.Unlock()

		select {
		case <-h.ctx.Done():
			// TODO(tylerw): attempt to drop lease gracefully.
			return
		case <-time.After(ttr):
			h.ensureValidLease(h.ctx, true /*=forceRenewal*/)
		}
	}
}

func (h *Liveness) string(replicaID []byte, llr *rfpb.NodeLivenessRecord) string {
	// Don't lock here (to avoid recursive locking).
	err := h.verifyLease(h.ctx, llr)
	if err != nil {
		return fmt.Sprintf("Liveness(%q): invalid (%s)", string(replicaID), err)
	}
	lifetime := time.Until(time.Unix(0, llr.GetExpiration()))
	return fmt.Sprintf("Liveness(%q): [epoch: %d, expires in %s]", string(replicaID), llr.GetEpoch(), lifetime)
}

func (h *Liveness) String() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.string(h.nhid, h.lastLivenessRecord)
}
