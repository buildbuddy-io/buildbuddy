package client

import (
	"context"
	"fmt"
	"io"
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

func RemoteReader(ctx context.Context, client rfspb.ApiClient, req *rfpb.ReadRequest) (io.ReadCloser, error) {
	stream, err := client.Read(ctx, req)
	if err != nil {
		return nil, err
	}
	reader, writer := io.Pipe()

	// Bit annoying here -- the gRPC stream won't give us an error until
	// we've called Recv on it. But we don't want to return a reader that
	// we know will error on first read with NotFound -- we want to return
	// that error now. So we'll wait for our goroutine to call Recv once
	// and return any error it gets in the main thread.
	firstError := make(chan error)
	go func() {
		readOnce := false
		for {
			rsp, err := stream.Recv()
			if !readOnce {
				firstError <- err
				readOnce = true
			}
			if rsp != nil {
				writer.Write(rsp.Data)
			}
			if err == io.EOF {
				writer.Close()
				break
			}
			if err != nil {
				writer.CloseWithError(err)
				break
			}

		}
	}()
	err = <-firstError

	// If we get an EOF, and we're expecting one - don't return an error.
	digestSize := req.GetFileRecord().GetDigest().GetSizeBytes()
	offset := req.GetOffset()
	if err == io.EOF && offset == digestSize {
		return reader, nil
	}
	return reader, err
}

func (c *APIClient) RemoteReader(ctx context.Context, client rfspb.ApiClient, req *rfpb.ReadRequest) (io.ReadCloser, error) {
	return RemoteReader(ctx, client, req)
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

func SyncReadLocal(ctx context.Context, nodehost NodeHost, shardID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	var raftResponseIface interface{}
	err = RunNodehostFn(ctx, func(ctx context.Context) error {
		raftResponseIface, err = nodehost.SyncRead(ctx, shardID, buf)
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
