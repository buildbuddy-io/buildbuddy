package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
)

// If a request is received that will result in a nodehost.Sync{Propose/Read},
// but not deadline is set, defaultContextTimeout will be applied.
const DefaultContextTimeout = 60 * time.Second

type NodeHost interface {
	ID() string
	GetNoOPSession(shardID uint64) *client.Session
	SyncPropose(ctx context.Context, session *client.Session, cmd []byte) (dbsm.Result, error)
	SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
	ReadIndex(shardID uint64, timeout time.Duration) (*dragonboat.RequestState, error)
	ReadLocalNode(rs *dragonboat.RequestState, query interface{}) (interface{}, error)
}

type apiClientAndConn struct {
	rfspb.ApiClient
	conn *grpc.ClientConn
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
	c.clients[peer] = &apiClientAndConn{ApiClient: client, conn: conn}
	return client, nil
}

func (c *APIClient) Get(ctx context.Context, peer string) (rfspb.ApiClient, error) {
	return c.getClient(ctx, peer)
}

func RunNodehostFn(ctx context.Context, nhf func(ctx context.Context) error) error {
	if _, ok := ctx.Deadline(); !ok {
		c, cancel := context.WithTimeout(ctx, DefaultContextTimeout)
		defer cancel()
		ctx = c
	}
	retrier := retry.DefaultWithContext(ctx)
	for retrier.Next() {
		err := nhf(ctx)
		if err != nil {
			if dragonboat.IsTempError(err) {
				continue
			}
			return err
		}
		return nil
	}
	return status.DeadlineExceededErrorf("exceeded retry limit for node host function")
}

func SyncProposeLocal(ctx context.Context, nodehost NodeHost, shardID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	sesh := nodehost.GetNoOPSession(shardID)
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}
	var raftResponse dbsm.Result
	err = RunNodehostFn(ctx, func(ctx context.Context) error {
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

func getTimeout(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline.Sub(time.Now())
	}
	return DefaultContextTimeout
}

func SyncReadLocal(ctx context.Context, nodehost NodeHost, shardID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	var raftResponseIface interface{}
	err = RunNodehostFn(ctx, func(ctx context.Context) error {
		rs, err := nodehost.ReadIndex(shardID, getTimeout(ctx))
		if err != nil {
			return err
		}
		v := <-rs.ResultC()
		if !v.Completed() {
			return status.FailedPreconditionError("Failed to read node index")
		}
		raftResponseIface, err = nodehost.ReadLocalNode(rs, buf)
		return err
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
