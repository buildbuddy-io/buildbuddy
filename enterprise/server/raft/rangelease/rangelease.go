package rangelease

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	// Acquire the lease for this long.
	rangeLeaseDuration = 10 * time.Second

	// Renew the lease this many seconds before expiry.
	rangeLeaseRenewGracePeriod = 2 * time.Second
)

type Lease struct {
	nodeHost     *dragonboat.NodeHost
	nodeLiveness *nodelease.Heartbeat

	rangeDescriptor *rfpb.RangeDescriptor
	mu              sync.RWMutex
	activeLease     *rfpb.RangeLeaseRecord
	lastRecord      *rfpb.RangeLeaseRecord

	timeUntilLeaseRenewal time.Duration
	quitLease             chan struct{}
}

func New(nodeHost *dragonboat.NodeHost, nodeLiveness *nodelease.Heartbeat, rd *rfpb.RangeDescriptor) *Lease {
	return &Lease{
		nodeHost:              nodeHost,
		nodeLiveness:          nodeLiveness,
		rangeDescriptor:       rd,
		mu:                    sync.RWMutex{},
		timeUntilLeaseRenewal: time.Duration(math.MaxInt64),
		quitLease:             make(chan struct{}),
	}
}

func containsMetaRange(rd *rfpb.RangeDescriptor) bool {
	r := rangemap.Range{Left: rd.Left, Right: rd.Right}
	return r.Contains([]byte{constants.MinByte}) && r.Contains([]byte{constants.UnsplittableMaxByte - 1})
}

func (l *Lease) assembleLeaseRequest() (*rfpb.RangeLeaseRecord, error) {
	// To prevent circular dependencies:
	//    (metarange -> range lease -> node liveness -> metarange)
	// any range that includes the metarange will be leased with a
	// time-based lease, rather than a node epoch based one.
	leaseRecord := &rfpb.RangeLeaseRecord{}
	if containsMetaRange(l.rangeDescriptor) {
		leaseRecord.Value = &rfpb.RangeLeaseRecord_Expiration{
			Expiration: time.Now().Add(rangeLeaseDuration).UnixNano(),
		}
	} else {
		nl, err := l.nodeLiveness.BlockingGetCurrentNodeLiveness()
		if err != nil {
			return nil, err
		}
		leaseRecord.Value = &rfpb.RangeLeaseRecord_NodeLiveness_{
			NodeLiveness: nl,
		}
	}
	return leaseRecord, nil
}

func (l *Lease) getClusterID() (uint64, error) {
	replicas := l.rangeDescriptor.GetReplicas()
	for _, replicaDescriptor := range replicas {
		return replicaDescriptor.GetClusterId(), nil
	}
	return 0, status.FailedPreconditionError("No replicas in range")
}

func (l *Lease) syncProposeLocal(ctx context.Context, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	clusterID, err := l.getClusterID()
	if err != nil {
		return nil, err
	}
	sesh := l.nodeHost.GetNoOPSession(clusterID)
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}
	var raftResponse dbsm.Result
	retrier := retry.DefaultWithContext(ctx)
	for retrier.Next() {
		raftResponse, err = l.nodeHost.SyncPropose(ctx, sesh, buf)
		if err != nil {
			if err == dragonboat.ErrClusterNotReady {
				log.Errorf("continuing, got cluster not ready err...")
				continue
			}
			log.Errorf("Got unretriable SyncPropose err: %s", err)
			return nil, err
		}
		break
	}
	batchResponse := &rfpb.BatchCmdResponse{}
	if err := proto.Unmarshal(raftResponse.Data, batchResponse); err != nil {
		return nil, err
	}
	return batchResponse, err
}

func (l *Lease) renew() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var expectedValue []byte
	if l.lastRecord != nil {
		buf, err := proto.Marshal(l.lastRecord)
		if err != nil {
			return err
		}
		expectedValue = buf
	}

	leaseRequest, err := l.assembleLeaseRequest()
	if err != nil {
		return err
	}
	buf, err := proto.Marshal(leaseRequest)
	if err != nil {
		return err
	}

	leaseKey := constants.LocalRangeLeaseKey
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

	log.Printf("Sending range lease request: %+v", leaseRequest)
	rsp, err := l.syncProposeLocal(ctx, casRequest)
	log.Printf("range lease response: %+v, err: %s", rsp, err)
	// TODO(tylerw): for the morning: why is this syncpropose never really working?

	if err != nil {
		log.Errorf("range lease request returned error: %s", err)
		return err
	}

	l.activeLease = nil
	casResponse, err := rbuilder.NewBatchResponseFromProto(rsp).CASResponse(0)
	log.Printf("casResponse: %+v, err: %s", casResponse, err)
	if err == nil {
		// This means we set the lease succesfully.
		l.lastRecord = leaseRequest
		l.activeLease = leaseRequest
	} else if status.IsFailedPreconditionError(err) && strings.Contains(err.Error(), constants.CASErrorMessage) {
		log.Errorf("another lease was active, saving it.")
		// This means another lease was active -- we should save it, so that
		// we can correctly set the expected value with our next CAS request,
		// and witness its epoch so that our next set request has a higher one.
		l.lastRecord = &rfpb.RangeLeaseRecord{}
		err := proto.Unmarshal(casResponse.GetKv().GetValue(), l.lastRecord)
		if err != nil {
			log.Errorf("error unmarshaling proto??? %s", err)
			return err
		}
		log.Printf("anoother lease was active, now lastRecord is %+v", l.lastRecord)
	} else {
		return err
	}

	if l.activeLease != nil && l.activeLease.GetExpiration() != 0 {
		expiration := time.Unix(0, l.activeLease.GetExpiration())
		timeUntilExpiry := expiration.Sub(time.Now())
		l.timeUntilLeaseRenewal = timeUntilExpiry - rangeLeaseRenewGracePeriod
	}
	return nil
}

func (l *Lease) blockingGetValidLease() (*rfpb.RangeLeaseRecord, error) {
	log.Printf("blockingGetValidLease called")
	l.mu.RLock()
	log.Printf("blockingGetValidLease got lock")
	var rl *rfpb.RangeLeaseRecord
	if err := l.verify(l.activeLease); err == nil {
		rl = l.activeLease
	}
	l.mu.RUnlock()
	log.Printf("blockingGetValidLease unlocked")
	if rl != nil {
		log.Printf("blockingGetValidLease returning rl")
		return rl, nil
	}

	rl, err := l.ensureValidLease()
	if err != nil {
		log.Printf("ensureValidLease err: %s", err)
		return nil, err
	}
	return rl, nil
}

func (l *Lease) verify(rl *rfpb.RangeLeaseRecord) error {
	if rl == nil {
		return status.FailedPreconditionErrorf("Invalid rangeLease: nil")
	}
	if nl := rl.GetNodeLiveness(); nl != nil {
		// This is a node epoch based lease, so check node and epoch.
		return l.nodeLiveness.BlockingValidateNodeLiveness(nl)
	}

	// This is a time  based lease, so check expiration time.
	expireAt := time.Unix(0, rl.GetExpiration())
	if time.Now().After(expireAt) {
		return status.FailedPreconditionErrorf("Invalid rangeLease: expired at %s", expireAt)
	}
	return nil
}

func (l *Lease) ensureValidLease() (*rfpb.RangeLeaseRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.verify(l.activeLease); err == nil {
		log.Printf("returning active lease: %+v", l.activeLease)
		return l.activeLease, nil
	}
	if err := l.renew(); err == nil {
		if err := l.verify(l.activeLease); err == nil {
			return l.activeLease, nil
		}
	}
	err := l.renew()
	if err == nil {
		err = l.verify(l.activeLease)
		if err == nil {
			return l.activeLease, nil
		}
	}
	return nil, err
}

func (l *Lease) printLease() {
	for {
		select {
		case <-time.After(time.Second):
			if l.activeLease != nil {
				log.Printf("Active range lease: %+v", l.activeLease)
			} else {
				log.Printf("No active range lease.")
			}
		}
	}
}

func (l *Lease) keepLeaseAlive() {
	for {
		select {
		case <-l.quitLease:
			// TODO(tylerw): attempt to drop lease gracefully.
			return
		case <-time.After(l.timeUntilLeaseRenewal):
			l.ensureValidLease()
		}
	}
}

func (l *Lease) Acquire() error {
	log.Printf("rangelease.Acquire() called")
	l.quitLease = make(chan struct{})
	_, err := l.blockingGetValidLease()
	if err != nil {
		log.Printf("blockingGetValidLease returned err: %s", err)
		return err
	}
	go l.keepLeaseAlive()
	go l.printLease()
	return nil
}

func (l *Lease) Release() {
	close(l.quitLease)
}
