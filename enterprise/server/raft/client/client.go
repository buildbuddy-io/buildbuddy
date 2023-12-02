package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
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

type apiClientAndConn struct {
	rfspb.ApiClient
}

type APIClient struct {
	env     environment.Env
	log     log.Logger
	mu      sync.Mutex
	clients map[string]*apiClientAndConn
}

func NewAPIClient(env environment.Env, name string) *APIClient {
	return &APIClient{
		env:     env,
		log:     log.NamedSubLogger(fmt.Sprintf("Coordinator(%s)", name)),
		clients: make(map[string]*apiClientAndConn, 0),
	}
}

func (c *APIClient) getClient(ctx context.Context, peer string) (rfspb.ApiClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.clients[peer]; ok {
		return client, nil
	}
	conn, err := grpc_client.DialSimple("grpc://" + peer)
	if err != nil {
		return nil, err
	}
	client := rfspb.NewApiClient(conn)
	c.clients[peer] = &apiClientAndConn{ApiClient: client}
	return client, nil
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
		if dur := deadline.Sub(time.Now()); dur < maxTimeout {
			return dur
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
			break
		}

		opCtx, cancel := context.WithTimeout(ctx, singleOpTimeout(ctx))
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

type NodeHostSender struct {
	*dragonboat.NodeHost
}

func (nhs *NodeHostSender) SyncProposeLocal(ctx context.Context, shardID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	return SyncProposeLocal(ctx, nhs.NodeHost, shardID, batch)
}
func (nhs *NodeHostSender) SyncReadLocal(ctx context.Context, shardID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	return SyncReadLocal(ctx, nhs.NodeHost, shardID, batch)
}

func SyncProposeLocalBatch(ctx context.Context, nodehost *dragonboat.NodeHost, shardID uint64, builder *rbuilder.BatchBuilder) (*rbuilder.BatchResponse, error) {
	batch, err := builder.ToProto()
	if err != nil {
		return nil, err
	}
	rsp, err := SyncProposeLocal(ctx, nodehost, shardID, batch)
	if err != nil {
		return nil, err
	}
	return rbuilder.NewBatchResponseFromProto(rsp), nil
}

func SyncProposeLocalBatchNoRsp(ctx context.Context, nodehost *dragonboat.NodeHost, shardID uint64, builder *rbuilder.BatchBuilder) error {
	rspBatch, err := SyncProposeLocalBatch(ctx, nodehost, shardID, builder)
	if err != nil {
		return err
	}
	return rspBatch.AnyError()
}

func RunTxn(ctx context.Context, nodehost *dragonboat.NodeHost, txn *rbuilder.TxnBuilder) error {
	// TODO(tylerw): make this durable if the coordinator restarts by writing
	// the TxnRequest proto to durable storage with an enum state-field and
	// building a statemachine that will run them to completion even after
	// restart.
	txnProto, err := txn.ToProto()
	if err != nil {
		return err
	}

	var prepareError error
	prepared := make([]*rfpb.TxnRequest_Statement, 0)
	for _, statement := range txnProto.GetStatements() {
		batch := statement.GetRawBatch()
		batch.TransactionId = txnProto.GetTransactionId()

		// Prepare each statement.
		rspProto, err := SyncProposeLocal(ctx, nodehost, statement.GetShardId(), batch)
		if err != nil {
			log.Errorf("Error preparing txn statement: %s", err)
			prepareError = err
			break
		}
		rsp := rbuilder.NewBatchResponseFromProto(rspProto)
		if err := rsp.AnyError(); err != nil {
			log.Errorf("Error preparing txn statement: %s", err)
			prepareError = err
			break
		}
		prepared = append(prepared, statement)
	}

	// Determine whether to ROLLBACK or COMMIT based on whether or not all
	// statements in the transaction were successfully prepared.
	operation := rfpb.FinalizeOperation_ROLLBACK
	if len(prepared) == len(txnProto.GetStatements()) {
		operation = rfpb.FinalizeOperation_COMMIT
	}

	for _, statement := range prepared {
		// Finalize each statement.
		batch := rbuilder.NewBatchBuilder().SetTransactionID(txnProto.GetTransactionId())
		batch.SetFinalizeOperation(operation)
		if _, err := SyncProposeLocalBatch(ctx, nodehost, statement.GetShardId(), batch); err != nil {
			return err
		}
	}

	if prepareError != nil {
		return prepareError
	}
	return nil
}
