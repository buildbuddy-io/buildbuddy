package client

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

// If a request is received that will result in a nodehost.Sync{Propose/Read},
// but not deadline is set, defaultContextTimeout will be applied.
const defaultContextTimeout = 60 * time.Second

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
	log.Debugf("Creating new client for peer: %q", peer)
	conn, err := grpc_client.DialTarget("grpc://" + peer)
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

func (c *APIClient) RemoteReader(ctx context.Context, client rfspb.ApiClient, req *rfpb.ReadRequest) (io.ReadCloser, error) {
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
			if err == io.EOF {
				writer.Close()
				break
			}
			if err != nil {
				writer.CloseWithError(err)
				break
			}
			writer.Write(rsp.Data)
		}
	}()
	err = <-firstError

	// If we get an EOF, and we're expecting one - don't return an error.
	digestSize := req.GetFileRecord().GetDigest().GetSizeBytes()
	offset := req.GetOffset()
	if err == io.EOF && digestSize == offset {
		return reader, nil
	}
	return reader, err
}

type streamWriteCloser struct {
	stream        rfspb.Api_WriteClient
	fileRecord    *rfpb.FileRecord
	bytesUploaded int64
}

func (wc *streamWriteCloser) Write(data []byte) (int, error) {
	req := &rfpb.WriteRequest{
		FileRecord:  wc.fileRecord,
		Data:        data,
		FinishWrite: false,
	}
	err := wc.stream.Send(req)
	return len(data), err
}

func (wc *streamWriteCloser) Close() error {
	req := &rfpb.WriteRequest{
		FileRecord:  wc.fileRecord,
		FinishWrite: true,
	}
	if err := wc.stream.Send(req); err != nil {
		return err
	}
	_, err := wc.stream.CloseAndRecv()
	return err
}

func (c *APIClient) RemoteWriter(ctx context.Context, peer string, fileRecord *rfpb.FileRecord) (io.WriteCloser, error) {
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}
	stream, err := client.Write(ctx)
	if err != nil {
		return nil, err
	}
	wc := &streamWriteCloser{
		fileRecord:    fileRecord,
		bytesUploaded: 0,
		stream:        stream,
	}
	return wc, nil
}

type multiWriteCloser struct {
	ctx           context.Context
	fileRecord    *rfpb.FileRecord
	log           log.Logger
	closers       map[string]io.WriteCloser
	mu            sync.Mutex
	totalNumPeers int
}

func fileRecordLogString(f *rfpb.FileRecord) string {
	return fmt.Sprintf("%s/%s/%d", f.GetIsolation().GetCacheType(), f.GetDigest().GetHash(), f.GetDigest().GetSizeBytes())
}

func (mc *multiWriteCloser) Write(data []byte) (int, error) {
	eg, _ := errgroup.WithContext(mc.ctx)
	for _, wc := range mc.closers {
		wc := wc
		eg.Go(func() error {
			n, err := wc.Write(data)
			if err != nil {
				return err
			}
			if n != len(data) {
				return io.ErrShortWrite
			}
			return nil
		})
	}

	return len(data), eg.Wait()
}

func (mc *multiWriteCloser) Close() error {
	eg, _ := errgroup.WithContext(mc.ctx)
	for _, wc := range mc.closers {
		wc := wc
		eg.Go(func() error {
			if err := wc.Close(); err != nil {
				return err
			}
			return nil
		})
	}
	err := eg.Wait()
	if err == nil {
		peers := make([]string, len(mc.closers))
		for peer := range mc.closers {
			peers = append(peers, peer)
		}
		mc.log.Debugf("Writer(%q) successfully wrote to peers %s", fileRecordLogString(mc.fileRecord), peers)
	} else {
		log.Errorf("MWC is returning err: %s", err)
	}
	return err
}

func (c *APIClient) MultiWriter(ctx context.Context, peers []string, fileRecord *rfpb.FileRecord) (io.WriteCloser, error) {
	mwc := &multiWriteCloser{
		ctx:        ctx,
		log:        c.log,
		fileRecord: fileRecord,
		closers:    make(map[string]io.WriteCloser, 0),
	}
	for _, peer := range peers {
		rwc, err := c.RemoteWriter(ctx, peer, fileRecord)
		if err != nil {
			log.Debugf("Skipping write %q to peer %q because: %s", fileRecordLogString(fileRecord), peer, err)
			continue
		}
		mwc.closers[peer] = rwc
	}
	if len(mwc.closers) < int(math.Ceil(float64(len(peers))/2)) {
		openPeers := make([]string, len(mwc.closers))
		for peer := range mwc.closers {
			openPeers = append(openPeers, peer)
		}
		log.Debugf("Could not open enough remoteWriters for fileRecord %s. All peers: %s, opened: %s", fileRecordLogString(fileRecord), peers, openPeers)
		return nil, status.UnavailableErrorf("Not enough peers (%d) available.", len(mwc.closers))
	}
	return mwc, nil
}

func SyncProposeLocal(ctx context.Context, nodehost *dragonboat.NodeHost, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		c, cancel := context.WithTimeout(ctx, defaultContextTimeout)
		defer cancel()
		ctx = c
	}

	sesh := nodehost.GetNoOPSession(clusterID)
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}
	var raftResponse dbsm.Result
	retrier := retry.DefaultWithContext(ctx)
	for retrier.Next() {
		raftResponse, err = nodehost.SyncPropose(ctx, sesh, buf)
		if err != nil {
			if dragonboat.IsTempError(err) {
				continue
			}
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

func SyncReadLocal(ctx context.Context, nodehost *dragonboat.NodeHost, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		c, cancel := context.WithTimeout(ctx, defaultContextTimeout)
		defer cancel()
		ctx = c
	}

	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	var raftResponseIface interface{}
	retrier := retry.DefaultWithContext(ctx)
	for retrier.Next() {
		raftResponseIface, err = nodehost.SyncRead(ctx, clusterID, buf)
		if err != nil {
			if dragonboat.IsTempError(err) {
				continue
			}
			return nil, err
		}
		break
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

type LocalSender interface {
	SyncProposeLocal(ctx context.Context, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error)
	SyncReadLocal(ctx context.Context, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error)
}

type NodeHostSender struct {
	*dragonboat.NodeHost
}

func (nhs *NodeHostSender) SyncProposeLocal(ctx context.Context, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	return SyncProposeLocal(ctx, nhs.NodeHost, clusterID, batch)
}
func (nhs *NodeHostSender) SyncReadLocal(ctx context.Context, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	return SyncReadLocal(ctx, nhs.NodeHost, clusterID, batch)
}
