package nodeliveness

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const (
	// Acquire the lease for this long.
	nodeLeaseDuration = 9 * time.Second

	// Renew the lease this many seconds *before* expiry.
	nodeLeaseRenewGracePeriod = 4 * time.Second
)

type Liveness struct {
	localSender client.LocalSender
	nodeID      []byte
	clock       *serf.LamportClock

	mu                    sync.RWMutex
	lastLivenessRecord    *rfpb.NodeLivenessRecord
	quitLease             chan struct{}
	timeUntilLeaseRenewal time.Duration
}

func NewLiveness(nodeID string, localSender client.LocalSender) *Liveness {
	return &Liveness{
		nodeID:                []byte(nodeID),
		localSender:           localSender,
		clock:                 &serf.LamportClock{},
		mu:                    sync.RWMutex{},
		lastLivenessRecord:    &rfpb.NodeLivenessRecord{},
		quitLease:             make(chan struct{}),
		timeUntilLeaseRenewal: 0 * time.Second,
	}
}

func (h *Liveness) Start() {
	h.quitLease = make(chan struct{})
	go h.keepLeaseAlive()
	//go h.printLease()
}

func (h *Liveness) Stop() {
	close(h.quitLease)
}

// TODO(tylerw): remove this later; it's just for debugging.
func (h *Liveness) printLease() {
	for {
		select {
		case <-time.After(time.Second):
			l, err := h.blockingGetValidLease()
			if err != nil {
				log.Printf("no liveness yet: %s", err)
				continue
			}
			now := time.Now()
			expireTime := time.Unix(0, l.GetExpiration())
			log.Printf("liveness record: [node: %s epoch: %d, expires in: %s]", string(h.nodeID), l.GetEpoch(), expireTime.Sub(now))
		}
	}
}

func (h *Liveness) BlockingValidateEpoch(epoch int64) (bool, error) {
	if l, err := h.blockingGetValidLease(); err == nil {
		return l.GetEpoch() == epoch, nil
	} else {
		return false, err
	}
}

func (h *Liveness) BlockingGetCurrentEpoch() (int64, error) {
	if l, err := h.blockingGetValidLease(); err == nil {
		return l.GetEpoch(), nil
	} else {
		return -1, err
	}
}

func (h *Liveness) BlockingGetCurrentNodeLiveness() (*rfpb.RangeLeaseRecord_NodeLiveness, error) {
	l, err := h.blockingGetValidLease()
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
	l, err := h.blockingGetValidLease()
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

func (h *Liveness) blockingGetValidLease() (*rfpb.NodeLivenessRecord, error) {
	h.mu.RLock()
	var l *rfpb.NodeLivenessRecord
	if err := h.verifyLease(h.lastLivenessRecord); err == nil {
		l = h.lastLivenessRecord
	}
	h.mu.RUnlock()
	if l != nil {
		return l, nil
	}

	l, err := h.ensureValidLease(false /*=forceRenewal*/)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (h *Liveness) ensureValidLease(forceRenewal bool) (*rfpb.NodeLivenessRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !forceRenewal {
		if err := h.verifyLease(h.lastLivenessRecord); err == nil {
			return h.lastLivenessRecord, nil
		}
	}

	if err := h.renewLease(); err == nil {
		if err := h.verifyLease(h.lastLivenessRecord); err == nil {
			return h.lastLivenessRecord, nil
		}
	}
	err := h.renewLease()
	if err == nil {
		err = h.verifyLease(h.lastLivenessRecord)
		if err == nil {
			return h.lastLivenessRecord, nil
		}
	}
	return nil, err
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
		Expiration: time.Now().Add(nodeLeaseDuration).UnixNano(),
	}
	buf, err := proto.Marshal(leaseRequest)
	if err != nil {
		return err
	}

	leaseKey := keys.MakeKey(constants.SystemPrefix, h.nodeID)
	casRequest, err := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   leaseKey,
			Value: buf,
		},
		ExpectedValue: expectedValue,
	}).ToProto()
	if err != nil {
		return err
	}

	rsp, err := h.localSender.SyncProposeLocal(ctx, 1, casRequest)

	if err != nil {
		// This indicates a communication error proposing the message.
		return err
	}

	casResponse, err := rbuilder.NewBatchResponseFromProto(rsp).CASResponse(0)
	if err == nil {
		// This means we set the lease succesfully.
		h.lastLivenessRecord = leaseRequest
		expiration := time.Unix(0, h.lastLivenessRecord.GetExpiration())
		timeUntilExpiry := expiration.Sub(time.Now())
		h.timeUntilLeaseRenewal = timeUntilExpiry - nodeLeaseRenewGracePeriod
	} else if status.IsFailedPreconditionError(err) && strings.Contains(err.Error(), constants.CASErrorMessage) {
		// This means another lease was active -- we should save it, so that
		// we can correctly set the expected value with our next CAS request,
		// and witness its epoch so that our next set request has a higher one.
		err := proto.Unmarshal(casResponse.GetKv().GetValue(), h.lastLivenessRecord)
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
