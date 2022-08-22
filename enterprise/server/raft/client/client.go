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
	"github.com/lni/dragonboat/v3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

// If a request is received that will result in a nodehost.Sync{Propose/Read},
// but not deadline is set, defaultContextTimeout will be applied.
const DefaultContextTimeout = 60 * time.Second

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

type streamWriteCloser struct {
	stream        rfspb.Api_WriteClient
	header        *rfpb.Header
	fileRecord    *rfpb.FileRecord
	bytesUploaded int64
}

func (wc *streamWriteCloser) Write(data []byte) (int, error) {
	req := &rfpb.WriteRequest{
		Header:      wc.header,
		FileRecord:  wc.fileRecord,
		Data:        data,
		FinishWrite: false,
	}
	err := wc.stream.Send(req)
	return len(data), err
}

func (wc *streamWriteCloser) Close() error {
	req := &rfpb.WriteRequest{
		Header:      wc.header,
		FileRecord:  wc.fileRecord,
		FinishWrite: true,
	}
	if err := wc.stream.Send(req); err != nil {
		return err
	}
	_, err := wc.stream.CloseAndRecv()
	return err
}

func RemoteWriter(ctx context.Context, client rfspb.ApiClient, header *rfpb.Header, fileRecord *rfpb.FileRecord) (io.WriteCloser, error) {
	stream, err := client.Write(ctx)
	if err != nil {
		return nil, err
	}
	wc := &streamWriteCloser{
		header:        header,
		fileRecord:    fileRecord,
		bytesUploaded: 0,
		stream:        stream,
	}
	return wc, nil
}

func RemoteSyncWriter(ctx context.Context, client rfspb.ApiClient, header *rfpb.Header, fileRecord *rfpb.FileRecord) (io.WriteCloser, error) {
	stream, err := client.SyncWriter(ctx)
	if err != nil {
		return nil, err
	}
	wc := &streamWriteCloser{
		header:        header,
		fileRecord:    fileRecord,
		bytesUploaded: 0,
		stream:        stream,
	}
	return wc, nil
}

func (c *APIClient) RemoteWriter(ctx context.Context, peerHeader *PeerHeader, fileRecord *rfpb.FileRecord) (io.WriteCloser, error) {
	client, err := c.getClient(ctx, peerHeader.GRPCAddr)
	if err != nil {
		return nil, err
	}
	return RemoteWriter(ctx, client, peerHeader.Header, fileRecord)
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

type PeerHeader struct {
	Header   *rfpb.Header
	GRPCAddr string
}

func (c *APIClient) MultiWriter(ctx context.Context, peerHeaders []*PeerHeader, fileRecord *rfpb.FileRecord) (io.WriteCloser, error) {
	mwc := &multiWriteCloser{
		ctx:        ctx,
		log:        c.log,
		fileRecord: fileRecord,
		closers:    make(map[string]io.WriteCloser, 0),
	}
	for _, peerHeader := range peerHeaders {
		peer := peerHeader.GRPCAddr
		rwc, err := c.RemoteWriter(ctx, peerHeader, fileRecord)
		if err != nil {
			log.Debugf("Skipping write %q to peer %q because: %s", fileRecordLogString(fileRecord), peer, err)
			continue
		}
		mwc.closers[peer] = rwc
	}
	if len(mwc.closers) < int(math.Ceil(float64(len(peerHeaders))/2)) {
		openPeers := make([]string, len(mwc.closers))
		for peer := range mwc.closers {
			openPeers = append(openPeers, peer)
		}
		allPeers := make([]string, len(peerHeaders))
		for _, peerHeader := range peerHeaders {
			allPeers = append(allPeers, peerHeader.GRPCAddr)
		}
		log.Debugf("Could not open enough remoteWriters for fileRecord %s. All peers: %s, opened: %s", fileRecordLogString(fileRecord), allPeers, openPeers)
		return nil, status.UnavailableErrorf("Not enough peers (%d) available.", len(mwc.closers))
	}
	return mwc, nil
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

func getRequestState(ctx context.Context, rs *dragonboat.RequestState) (dbsm.Result, error) {
	select {
	case r := <-rs.AppliedC():
		if r.Completed() {
			return r.GetResult(), nil
		} else if r.Rejected() {
			return dbsm.Result{}, dragonboat.ErrRejected
		} else if r.Timeout() {
			return dbsm.Result{}, dragonboat.ErrTimeout
		} else if r.Terminated() {
			return dbsm.Result{}, dragonboat.ErrClusterClosed
		} else if r.Dropped() {
			return dbsm.Result{}, dragonboat.ErrClusterNotReady
		} else {
			log.Errorf("unknown v code %v", r)
			return dbsm.Result{}, status.InternalErrorf("unknown v code %v", r)
		}
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return dbsm.Result{}, dragonboat.ErrCanceled
		} else if ctx.Err() == context.DeadlineExceeded {
			return dbsm.Result{}, dragonboat.ErrTimeout
		}
	}
	return dbsm.Result{}, status.InternalError("unreachable")
}

func SyncProposeLocal(ctx context.Context, nodehost *dragonboat.NodeHost, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	sesh := nodehost.GetNoOPSession(clusterID)
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}
	var raftResponse dbsm.Result
	err = RunNodehostFn(ctx, func(ctx context.Context) error {
		rs, err := nodehost.Propose(sesh, buf, time.Second)
		if err != nil {
			return err
		}
		result, err := getRequestState(ctx, rs)
		if err != nil {
			return err
		}
		rs.Release()
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

func SyncReadLocal(ctx context.Context, nodehost *dragonboat.NodeHost, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	var raftResponseIface interface{}
	err = RunNodehostFn(ctx, func(ctx context.Context) error {
		raftResponseIface, err = nodehost.SyncRead(ctx, clusterID, buf)
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

func (nhs *NodeHostSender) SyncProposeLocal(ctx context.Context, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	return SyncProposeLocal(ctx, nhs.NodeHost, clusterID, batch)
}
func (nhs *NodeHostSender) SyncReadLocal(ctx context.Context, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	return SyncReadLocal(ctx, nhs.NodeHost, clusterID, batch)
}
