package client

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockmap"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

var (
	sessionLifetime = flag.Duration("cache.raft.client_session_lifetime", 1*time.Hour, "The duration of a client session before it's reset")
)

// A default timeout that can be applied to raft requests that do not have one
// set.
const DefaultContextTimeout = 10 * time.Second

type NodeHost interface {
	ID() string
	GetNoOPSession(rangeID uint64) *client.Session
	SyncPropose(ctx context.Context, session *client.Session, cmd []byte) (dbsm.Result, error)
	SyncRead(ctx context.Context, rangeID uint64, query interface{}) (interface{}, error)
	ReadIndex(rangeID uint64, timeout time.Duration) (*dragonboat.RequestState, error)
	ReadLocalNode(rs *dragonboat.RequestState, query interface{}) (interface{}, error)
	StaleRead(rangeID uint64, query interface{}) (interface{}, error)
}

type IRegistry interface {
	// Lookup the grpc address given a replica's rangeID and replicaID
	ResolveGRPC(rangeID uint64, replicaID uint64) (string, string, error)
}

type APIClient struct {
	env      environment.Env
	log      log.Logger
	mu       sync.Mutex
	clients  map[string]*grpc_client.ClientConnPool
	registry IRegistry
}

func NewAPIClient(env environment.Env, name string, registry IRegistry) *APIClient {
	return &APIClient{
		env:      env,
		log:      log.NamedSubLogger(fmt.Sprintf("Coordinator(%s)", name)),
		clients:  make(map[string]*grpc_client.ClientConnPool),
		registry: registry,
	}
}

func (c *APIClient) getClient(ctx context.Context, peer string) (rfspb.ApiClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.clients[peer]; ok {
		conn, err := client.GetReadyConnection()
		if err != nil {
			return nil, status.UnavailableErrorf("no connections to peer %q are ready", peer)
		}
		return rfspb.NewApiClient(conn), nil
	}
	log.Debugf("Creating new client for peer: %q", peer)
	conn, err := grpc_client.DialSimple("grpc://" + peer)
	if err != nil {
		return nil, err
	}
	c.clients[peer] = conn
	return rfspb.NewApiClient(conn), nil
}

func (c *APIClient) Get(ctx context.Context, peer string) (rfspb.ApiClient, error) {
	return c.getClient(ctx, peer)
}

func (c *APIClient) GetForReplica(ctx context.Context, rd *rfpb.ReplicaDescriptor) (rfspb.ApiClient, error) {
	addr, _, err := c.registry.ResolveGRPC(rd.GetRangeId(), rd.GetReplicaId())
	if err != nil {
		return nil, err
	}
	return c.getClient(ctx, addr)
}

func singleOpTimeout(ctx context.Context) time.Duration {
	// This value should be approximately 10x the config.RTTMilliseconds,
	// but we want to include a little more time for the operation itself to
	// complete.
	const maxTimeout = time.Second
	if deadline, ok := ctx.Deadline(); ok {
		dur := time.Until(deadline)
		if dur <= 0 {
			return dur
		}
		if dur < maxTimeout {
			// ensure that the returned duration / constants.RTTMillisecond > 0.
			return dur + constants.RTTMillisecond
		}
	}
	return maxTimeout
}

type aggErr struct {
	errCount          map[string]int
	lastErr           error
	lastNonTimeoutErr error
}

func (e *aggErr) err() error {
	if e.lastErr == nil {
		return nil
	}

	if e.lastErr == dragonboat.ErrShardNotFound || e.lastErr == dragonboat.ErrRejected {
		return e.lastErr
	} else if status.IsOutOfRangeError(e.lastErr) {
		return e.lastErr
	} else if e.lastErr == e.lastNonTimeoutErr || e.lastNonTimeoutErr == nil {
		return fmt.Errorf("last error: %s, errors encountered: %+v", e.lastErr, e.errCount)
	} else {
		return fmt.Errorf("last error: %s, last non-timeout error: %s, errors encountered: %+v", e.lastErr, e.lastNonTimeoutErr, e.errCount)
	}
}

func (e *aggErr) Add(err error) {
	if err == nil {
		return
	}
	e.lastErr = err
	if !errors.Is(err, dragonboat.ErrTimeoutTooSmall) && err != context.Canceled && err != context.DeadlineExceeded {
		e.lastNonTimeoutErr = err
	}
	e.errCount[err.Error()] += 1
}

func RunNodehostFn(ctx context.Context, nhf func(ctx context.Context) error) error {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	defer spn.End()
	// Ensure that the outer context has a timeout set to limit the total
	// time we'll attempt to run an operation.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultContextTimeout)
		defer cancel()
	}
	aggregatedErr := &aggErr{errCount: make(map[string]int)}
	for {
		select {
		case <-ctx.Done():
			aggregatedErr.Add(ctx.Err())
			return aggregatedErr.err()
		default:
			// continue with for loop
		}

		timeout := singleOpTimeout(ctx)
		if timeout <= 0 {
			// The deadline has already passed.
			continue
		}
		opCtx, cancel := context.WithTimeout(ctx, timeout)
		err := nhf(opCtx)
		cancel()

		if err != nil {
			aggregatedErr.Add(err)
			if dragonboat.IsTempError(err) {
				continue
			}
			return aggregatedErr.err()
		}
		return nil
	}
}

type Session struct {
	id        string
	index     uint64
	createdAt time.Time

	clock     clockwork.Clock
	refreshAt time.Time
	mu        sync.Mutex

	locker lockmap.Locker
}

func (s *Session) ToProto() *rfpb.Session {
	return &rfpb.Session{
		Id:            []byte(s.id),
		Index:         s.index,
		CreatedAtUsec: s.createdAt.UnixMicro(),
	}
}

func NewSession() *Session {
	return NewSessionWithClock(clockwork.NewRealClock())
}

func NewSessionWithClock(clock clockwork.Clock) *Session {
	now := clock.Now()
	return &Session{
		id:        uuid.New(),
		index:     0,
		clock:     clock,
		createdAt: now,
		refreshAt: now.Add(*sessionLifetime),
		locker:    lockmap.New(),
	}
}

// maybeRefresh resets the id and index when the session expired.
func (s *Session) maybeRefresh() {
	now := s.clock.Now()
	if s.refreshAt.After(now) {
		return
	}

	s.id = uuid.New()
	s.index = 0
	s.createdAt = now
	s.refreshAt = now.Add(*sessionLifetime)
}

func (s *Session) SyncProposeLocal(ctx context.Context, nodehost NodeHost, rangeID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	_, spn := tracing.StartSpan(ctx) // nolint:SA4006
	spn.SetName("SyncProposeLocal: locker.Lock")
	attr := attribute.Int64("range_id", int64(rangeID))
	spn.SetAttributes(attr)
	// At most one SyncProposeLocal can be run for the same replica per session.
	start := s.clock.Now()
	unlockFn := s.locker.Lock(fmt.Sprintf("%d", rangeID))
	spn.End()
	defer func() {
		unlockFn()
		metrics.RaftRangeLockDurationMsec.With(prometheus.Labels{
			metrics.RaftSessionIDLabel: s.id,
			metrics.RaftRangeIDLabel:   strconv.Itoa(int(rangeID)),
		}).Observe(float64(s.clock.Since(start).Milliseconds()))
	}()

	_, spn = tracing.StartSpan(ctx) // nolint:SA4006
	spn.SetName("SyncProposeLocal: set session")
	s.mu.Lock()
	// Refreshes the session if necessary
	s.maybeRefresh()
	s.index++
	batch.Session = s.ToProto()
	s.mu.Unlock()
	spn.End()

	sesh := nodehost.GetNoOPSession(rangeID)

	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}
	var raftResponse dbsm.Result

	err = RunNodehostFn(ctx, func(ctx context.Context) error {
		ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
		spn.SetName("nodehost.SyncPropose")
		fnStart := s.clock.Now()
		defer func() {
			spn.End()
			metrics.RaftNodeHostMethodDurationUsec.With(prometheus.Labels{
				metrics.RaftNodeHostMethodLabel: "SyncPropose",
				metrics.RaftRangeIDLabel:        strconv.Itoa(int(rangeID)),
			}).Observe(float64(s.clock.Since(fnStart).Microseconds()))
		}()
		result, err := nodehost.SyncPropose(ctx, sesh, buf)
		if err != nil {
			return err
		}
		if result.Value == constants.EntryErrorValue {
			s := &statuspb.Status{}
			if err := proto.Unmarshal(result.Data, s); err != nil {
				return err
			}
			if err := gstatus.FromProto(s).Err(); err != nil {
				return err
			}
		}
		raftResponse = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	batchResponse := &rfpb.BatchCmdResponse{}
	if err := proto.Unmarshal(raftResponse.Data, batchResponse); err != nil {
		return nil, err
	}
	return batchResponse, err
}

func SyncReadLocal(ctx context.Context, nodehost NodeHost, rangeID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	if batch.Header == nil {
		return nil, status.FailedPreconditionError("Header must be set")
	}
	var raftResponseIface interface{}
	err = RunNodehostFn(ctx, func(ctx context.Context) error {
		switch batch.GetHeader().GetConsistencyMode() {
		case rfpb.Header_LINEARIZABLE:
			rs, err := nodehost.ReadIndex(rangeID, singleOpTimeout(ctx))
			if err != nil {
				return err
			}
			v := <-rs.ResultC()
			if v.Completed() {
				raftResponseIface, err = nodehost.ReadLocalNode(rs, buf)
				return err
			} else if v.Rejected() {
				return dragonboat.ErrRejected
			} else if v.Timeout() {
				return dragonboat.ErrTimeout
			} else if v.Terminated() {
				return dragonboat.ErrShardClosed
			} else if v.Dropped() {
				return dragonboat.ErrShardNotReady
			} else if v.Aborted() {
				return dragonboat.ErrAborted
			} else {
				return status.FailedPreconditionError("Failed to read result")
			}

		case rfpb.Header_STALE, rfpb.Header_RANGELEASE:
			raftResponseIface, err = nodehost.StaleRead(rangeID, buf)
			return err
		default:
			return status.UnknownError("Unknown consistency mode")
		}

	})
	if err != nil {
		return nil, err
	}
	buf, ok := raftResponseIface.([]byte)
	if !ok {
		return nil, status.FailedPreconditionError("SyncRead returned a non-[]byte response.")
	}

	batchResponse := &rfpb.BatchCmdResponse{}
	if err := proto.Unmarshal(buf, batchResponse); err != nil {
		return nil, err
	}
	return batchResponse, nil
}

func SyncReadLocalBatch(ctx context.Context, nodehost *dragonboat.NodeHost, rangeID uint64, builder *rbuilder.BatchBuilder) (*rbuilder.BatchResponse, error) {
	batch, err := builder.ToProto()
	if err != nil {
		return nil, err
	}
	rsp, err := SyncReadLocal(ctx, nodehost, rangeID, batch)
	if err != nil {
		return nil, err
	}
	return rbuilder.NewBatchResponseFromProto(rsp), nil
}

func SessionLifetime() time.Duration {
	return *sessionLifetime
}
