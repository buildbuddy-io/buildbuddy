package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

// A default timeout that can be applied to raft requests that do not have one
// set.
const DefaultContextTimeout = 10 * time.Second

type NodeHost interface {
	ID() string
	GetNoOPSession(shardID uint64) *client.Session
	SyncPropose(ctx context.Context, session *client.Session, cmd []byte) (dbsm.Result, error)
	SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
	ReadIndex(shardID uint64, timeout time.Duration) (*dragonboat.RequestState, error)
	ReadLocalNode(rs *dragonboat.RequestState, query interface{}) (interface{}, error)
	StaleRead(shardID uint64, query interface{}) (interface{}, error)
}

type APIClient struct {
	env     environment.Env
	log     log.Logger
	mu      sync.Mutex
	clients map[string]*grpc_client.ClientConnPool
}

func NewAPIClient(env environment.Env, name string) *APIClient {
	return &APIClient{
		env:     env,
		log:     log.NamedSubLogger(fmt.Sprintf("Coordinator(%s)", name)),
		clients: make(map[string]*grpc_client.ClientConnPool),
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

func RunNodehostFn(ctx context.Context, nhf func(ctx context.Context) error) error {
	// Ensure that the outer context has a timeout set to limit the total
	// time we'll attempt to run an operation.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultContextTimeout)
		defer cancel()
	}
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
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
			lastErr = err
			if dragonboat.IsTempError(err) {
				continue
			}
			return err
		}
		return nil
	}
}

func SyncProposeLocal(ctx context.Context, nodehost NodeHost, shardID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	sesh := nodehost.GetNoOPSession(shardID)
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}
	var raftResponse dbsm.Result
	err = RunNodehostFn(ctx, func(ctx context.Context) error {
		defer canary.Start("nodehost.SyncPropose", time.Second)()
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

func SyncReadLocal(ctx context.Context, nodehost NodeHost, shardID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
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
			rs, err := nodehost.ReadIndex(shardID, singleOpTimeout(ctx))
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
			raftResponseIface, err = nodehost.StaleRead(shardID, buf)
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

func SyncReadLocalBatch(ctx context.Context, nodehost *dragonboat.NodeHost, shardID uint64, builder *rbuilder.BatchBuilder) (*rbuilder.BatchResponse, error) {
	batch, err := builder.ToProto()
	if err != nil {
		return nil, err
	}
	rsp, err := SyncReadLocal(ctx, nodehost, shardID, batch)
	if err != nil {
		return nil, err
	}
	return rbuilder.NewBatchResponseFromProto(rsp), nil
}
