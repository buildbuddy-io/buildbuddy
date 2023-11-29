package cacheproxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"

	dcpb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	// Keep under the limit of ~4MB (save 256KB).
	// (Match the readBufSizeBytes in byte_stream_server.go)
	readBufSizeBytes = (1024 * 1024 * 4) - (1024 * 256)
)

type dcClient struct {
	dcpb.DistributedCacheClient
	conn         *grpc.ClientConn
	wasEverReady bool
}

type CacheProxy struct {
	env                   environment.Env
	cache                 interfaces.Cache
	log                   log.Logger
	readBufPool           *bytebufferpool.VariableSizePool
	writeBufPool          sync.Pool
	mu                    *sync.Mutex
	server                *grpc.Server
	clients               map[string]*dcClient
	heartbeatCallback     func(ctx context.Context, peer string)
	hintedHandoffCallback func(ctx context.Context, peer string, r *rspb.ResourceName)
	listenAddr            string
	zone                  string
}

func NewCacheProxy(env environment.Env, c interfaces.Cache, listenAddr string) *CacheProxy {
	proxy := &CacheProxy{
		env:         env,
		cache:       c,
		log:         log.NamedSubLogger(fmt.Sprintf("CacheProxy(%s)", listenAddr)),
		readBufPool: bytebufferpool.VariableSize(readBufSizeBytes),
		writeBufPool: sync.Pool{
			New: func() any {
				return bufio.NewWriterSize(io.Discard, readBufSizeBytes)
			},
		},
		listenAddr: listenAddr,
		mu:         &sync.Mutex{},
		// server goes here
		clients: make(map[string]*dcClient, 0),
	}
	zone, err := resources.GetZone()
	if err != nil {
		log.Warningf("Error detecting zone: %s", err)
	} else {
		proxy.zone = zone
	}
	return proxy
}

func (c *CacheProxy) StartListening() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.server != nil {
		return status.FailedPreconditionError("The server is already running.")
	}

	lis, err := net.Listen("tcp", c.listenAddr)
	if err != nil {
		return err
	}
	grpcOptions := grpc_server.CommonGRPCServerOptions(c.env)
	grpcServer := grpc.NewServer(grpcOptions...)
	reflection.Register(grpcServer)
	dcpb.RegisterDistributedCacheServer(grpcServer, c)
	c.server = grpcServer

	go func() {
		log.Printf("Listening on %s", c.listenAddr)
		if err := c.server.Serve(lis); err != nil {
			log.Warningf("Error serving: %s", err)
		}
	}()
	return nil
}

func (c *CacheProxy) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.server == nil {
		return status.FailedPreconditionError("The server was already stopped.")
	}
	err := grpc_server.GRPCShutdown(ctx, c.server)
	c.server = nil
	return err
}

func (c *CacheProxy) SetHeartbeatCallbackFunc(fn func(ctx context.Context, peer string)) {
	c.heartbeatCallback = fn
}

func (c *CacheProxy) SetHintedHandoffCallbackFunc(fn func(ctx context.Context, peer string, r *rspb.ResourceName)) {
	c.hintedHandoffCallback = fn
}

func digestFromKey(k *dcpb.Key) *repb.Digest {
	return &repb.Digest{
		Hash:      k.GetKey(),
		SizeBytes: k.GetSizeBytes(),
	}
}

func digestToKey(d *repb.Digest) *dcpb.Key {
	return &dcpb.Key{
		Key:       d.GetHash(),
		SizeBytes: d.GetSizeBytes(),
	}
}

func (c *CacheProxy) getClient(ctx context.Context, peer string) (dcpb.DistributedCacheClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.clients[peer]; ok {
		// The gRPC client library causes RPCs to block while a connection stays in CONNECTING state, regardless of the
		// failfast setting. This can happen when a replica goes away due to a rollout (or any other reason).
		isReady := client.conn.GetState() == connectivity.Ready
		if client.wasEverReady && !isReady {
			client.conn.Connect()
			return nil, status.UnavailableErrorf("connection to peer %q is not ready", peer)
		}
		client.wasEverReady = client.wasEverReady || isReady
		return client, nil
	}
	log.Debugf("Creating new client for peer: %q", peer)
	conn, err := grpc_client.DialInternalWithoutPooling(c.env, "grpc://"+peer)
	if err != nil {
		return nil, err
	}
	client := dcpb.NewDistributedCacheClient(conn)
	c.clients[peer] = &dcClient{DistributedCacheClient: client, conn: conn}
	return client, nil
}

func (c *CacheProxy) prepareContext(ctx context.Context) context.Context {
	if c.zone != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, resources.ZoneHeader, c.zone)
	}
	return ctx
}

func (c *CacheProxy) readWriteContext(ctx context.Context) (context.Context, error) {
	ctx, err := prefix.AttachUserPrefixToContext(c.prepareContext(ctx), c.env)
	if err != nil {
		return ctx, err
	}
	return ctx, err
}

// Distributed cache requests now contain resource.ResourceName structs, replacing usage of the dcpb.Isolation and
// dcpb.Key structs. However during a rollout, the new resource fields may not be set, and may require fallback to the
// older deprecated fields
func getResource(r *rspb.ResourceName, isolation *dcpb.Isolation, digestKey *dcpb.Key) *rspb.ResourceName {
	if r != nil {
		return r
	}

	d := digestFromKey(digestKey)
	return &rspb.ResourceName{
		Digest:       d,
		CacheType:    isolation.GetCacheType(),
		InstanceName: isolation.GetRemoteInstanceName(),
		Compressor:   repb.Compressor_IDENTITY,
	}
}

func getResources(resources []*rspb.ResourceName, isolation *dcpb.Isolation, digestKeys []*dcpb.Key) []*rspb.ResourceName {
	if resources != nil {
		return resources
	}

	log.Warningf("cacheproxy: falling back to digest keys; this should not happen")
	instanceName := isolation.GetRemoteInstanceName()
	cacheType := isolation.GetCacheType()
	rns := make([]*rspb.ResourceName, 0)
	for _, k := range digestKeys {
		d := digestFromKey(k)
		rn := digest.NewResourceName(d, instanceName, cacheType, repb.DigestFunction_SHA256).ToProto()
		rns = append(rns, rn)
	}

	return rns
}

func (c *CacheProxy) FindMissing(ctx context.Context, req *dcpb.FindMissingRequest) (*dcpb.FindMissingResponse, error) {
	ctx, err := c.readWriteContext(ctx)
	if err != nil {
		return nil, err
	}

	r := getResources(req.GetResources(), req.GetIsolation(), req.GetKey())
	missing, err := c.cache.FindMissing(ctx, r)
	if err != nil {
		return nil, err
	}
	rsp := &dcpb.FindMissingResponse{}
	for _, d := range missing {
		rsp.Missing = append(rsp.Missing, digestToKey(d))
	}
	return rsp, nil
}

func (c *CacheProxy) Metadata(ctx context.Context, req *dcpb.MetadataRequest) (*dcpb.MetadataResponse, error) {
	ctx, err := c.readWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	r := getResource(req.GetResource(), req.GetIsolation(), req.GetKey())
	md, err := c.cache.Metadata(ctx, r)
	if err != nil {
		return nil, err
	}
	return &dcpb.MetadataResponse{
		StoredSizeBytes: md.StoredSizeBytes,
		DigestSizeBytes: md.DigestSizeBytes,
		LastModifyUsec:  md.LastModifyTimeUsec,
		LastAccessUsec:  md.LastAccessTimeUsec,
	}, nil
}

// ResourceIsolationString returns a compact representation of a resource's isolation that is suitable for logging.
func ResourceIsolationString(r *rspb.ResourceName) string {
	rep := filepath.Join(r.GetInstanceName(), digest.CacheTypeToPrefix(r.GetCacheType()), r.GetDigest().GetHash())
	if !strings.HasSuffix(rep, "/") {
		rep += "/"
	}
	return rep
}

func (c *CacheProxy) Delete(ctx context.Context, req *dcpb.DeleteRequest) (*dcpb.DeleteResponse, error) {
	ctx, err := c.readWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	r := getResource(req.GetResource(), req.GetIsolation(), req.GetKey())
	err = c.cache.Delete(ctx, r)
	if err != nil {
		return nil, err
	}
	return &dcpb.DeleteResponse{}, nil
}

func (c *CacheProxy) GetMulti(ctx context.Context, req *dcpb.GetMultiRequest) (*dcpb.GetMultiResponse, error) {
	ctx, err := c.readWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	r := getResources(req.GetResources(), req.GetIsolation(), req.GetKey())
	found, err := c.cache.GetMulti(ctx, r)
	if err != nil {
		return nil, err
	}
	rsp := &dcpb.GetMultiResponse{}
	for d, buf := range found {
		if len(buf) == 0 {
			rn := &rspb.ResourceName{
				Digest:       d,
				InstanceName: req.GetIsolation().GetRemoteInstanceName(),
				CacheType:    req.GetIsolation().GetCacheType(),
			}
			c.log.Warningf("returned a zero-length response for %s", ResourceIsolationString(rn))
		}
		rsp.KeyValue = append(rsp.KeyValue, &dcpb.KV{
			Key:   digestToKey(d),
			Value: buf,
		})
	}
	return rsp, nil
}

func (c *CacheProxy) Read(req *dcpb.ReadRequest, stream dcpb.DistributedCache_ReadServer) error {
	ctx, err := c.readWriteContext(stream.Context())
	if err != nil {
		return err
	}
	up, _ := prefix.UserPrefixFromContext(ctx)
	rn := getResource(req.GetResource(), req.GetIsolation(), req.GetKey())
	reader, err := c.cache.Reader(ctx, rn, req.GetOffset(), req.GetLimit())
	if err != nil {
		c.log.Debugf("Read(%q) failed (user prefix: %s), err: %s", ResourceIsolationString(rn), up, err)
		return err
	}
	defer reader.Close()

	bufSize := int64(readBufSizeBytes)
	resourceSize := rn.GetDigest().GetSizeBytes()
	if resourceSize > 0 && resourceSize < bufSize {
		bufSize = resourceSize
	}
	copyBuf := c.readBufPool.Get(bufSize)
	defer c.readBufPool.Put(copyBuf)

	for {
		n, err := ioutil.ReadTryFillBuffer(reader, copyBuf)
		if err == io.EOF {
			break
		}
		if err := stream.Send(&dcpb.ReadResponse{Data: copyBuf[:n]}); err != nil {
			return err
		}
	}

	c.log.Debugf("Read(%q) succeeded (user prefix: %s)", ResourceIsolationString(rn), up)
	return err
}

func (c *CacheProxy) callHintedHandoffCB(ctx context.Context, peer string, r *rspb.ResourceName) {
	if c.hintedHandoffCallback != nil {
		c.hintedHandoffCallback(ctx, peer, r)
	}
}

func (c *CacheProxy) Write(stream dcpb.DistributedCache_WriteServer) error {
	ctx, err := c.readWriteContext(stream.Context())
	if err != nil {
		return err
	}
	up, _ := prefix.UserPrefixFromContext(ctx)

	var bytesWritten int64
	var writeCloser interfaces.CommittedWriteCloser
	handoffPeer := ""
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		rn := getResource(req.GetResource(), req.GetIsolation(), req.GetKey())
		if writeCloser == nil {
			wc, err := c.cache.Writer(ctx, rn)
			if err != nil {
				c.log.Debugf("Write(%q) failed (user prefix: %s), err: %s", ResourceIsolationString(rn), up, err)
				return err
			}
			defer wc.Close()
			writeCloser = wc
			handoffPeer = req.GetHandoffPeer()
		}
		n, err := writeCloser.Write(req.Data)
		if err != nil {
			return err
		}
		bytesWritten += int64(n)
		if req.FinishWrite {
			if err := writeCloser.Commit(); err != nil {
				return err
			}
			// TODO(vadim): use handoff peer from request once client is including it in the FinishWrite request.
			if handoffPeer != "" {
				c.callHintedHandoffCB(ctx, handoffPeer, rn)
			}
			c.log.Debugf("Write(%q) succeeded (user prefix: %s)", ResourceIsolationString(rn), up)
			return stream.SendAndClose(&dcpb.WriteResponse{
				CommittedSize: bytesWritten,
			})
		}
	}
	return nil
}

func (c *CacheProxy) Heartbeat(ctx context.Context, req *dcpb.HeartbeatRequest) (*dcpb.HeartbeatResponse, error) {
	if req.GetSource() == "" {
		return nil, status.InvalidArgumentError("A source is required.")
	}
	if c.heartbeatCallback != nil {
		c.heartbeatCallback(ctx, req.GetSource())
	}
	return &dcpb.HeartbeatResponse{}, nil
}

func (c *CacheProxy) RemoteContains(ctx context.Context, peer string, r *rspb.ResourceName) (bool, error) {
	isolation := &dcpb.Isolation{
		CacheType:          r.GetCacheType(),
		RemoteInstanceName: r.GetInstanceName(),
	}
	missing, err := c.RemoteFindMissing(ctx, peer, isolation, []*rspb.ResourceName{r})
	if err != nil {
		return false, err
	}
	return len(missing) == 0, nil
}

func (c *CacheProxy) RemoteMetadata(ctx context.Context, peer string, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	isolation := &dcpb.Isolation{
		CacheType:          r.GetCacheType(),
		RemoteInstanceName: r.GetInstanceName(),
	}
	req := &dcpb.MetadataRequest{
		Isolation: isolation,
		Key:       digestToKey(r.GetDigest()),
		Resource:  r,
	}
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}

	md, err := client.Metadata(ctx, req)
	if err != nil {
		return nil, err
	}
	return &interfaces.CacheMetadata{
		StoredSizeBytes:    md.GetStoredSizeBytes(),
		DigestSizeBytes:    md.GetDigestSizeBytes(),
		LastAccessTimeUsec: md.GetLastAccessUsec(),
		LastModifyTimeUsec: md.GetLastModifyUsec(),
	}, nil
}

func (c *CacheProxy) RemoteFindMissing(ctx context.Context, peer string, isolation *dcpb.Isolation, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	req := &dcpb.FindMissingRequest{
		Isolation: isolation,
	}
	for _, r := range resources {
		key := digestToKey(r.GetDigest())
		req.Key = append(req.Key, key)
		req.Resources = append(req.Resources, r)
	}
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}
	rsp, err := client.FindMissing(ctx, req)
	if err != nil {
		return nil, err
	}
	var missing []*repb.Digest
	for _, k := range rsp.GetMissing() {
		missing = append(missing, &repb.Digest{
			Hash:      k.GetKey(),
			SizeBytes: k.GetSizeBytes(),
		})
	}
	return missing, nil
}

func (c *CacheProxy) RemoteDelete(ctx context.Context, peer string, r *rspb.ResourceName) error {
	isolation := &dcpb.Isolation{
		CacheType:          r.GetCacheType(),
		RemoteInstanceName: r.GetInstanceName(),
	}
	req := &dcpb.DeleteRequest{
		Isolation: isolation,
		Key:       digestToKey(r.GetDigest()),
		Resource:  r,
	}
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return err
	}
	_, err = client.Delete(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (c *CacheProxy) RemoteGetMulti(ctx context.Context, peer string, isolation *dcpb.Isolation, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	req := &dcpb.GetMultiRequest{
		Isolation: isolation,
	}
	hashDigests := make(map[string]*repb.Digest, len(resources))
	for _, r := range resources {
		key := digestToKey(r.GetDigest())
		hashDigests[r.GetDigest().GetHash()] = r.GetDigest()
		req.Key = append(req.Key, key)
		req.Resources = append(req.Resources, r)
	}
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}
	rsp, err := client.GetMulti(ctx, req)
	if err != nil {
		return nil, err
	}
	resultMap := make(map[*repb.Digest][]byte, len(rsp.GetKeyValue()))
	for _, keyValue := range rsp.GetKeyValue() {
		d, ok := hashDigests[keyValue.GetKey().GetKey()]
		if ok {
			resultMap[d] = keyValue.GetValue()
		}
	}
	return resultMap, nil
}

func (c *CacheProxy) RemoteReader(ctx context.Context, peer string, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	isolation := &dcpb.Isolation{
		CacheType:          r.GetCacheType(),
		RemoteInstanceName: r.GetInstanceName(),
	}
	req := &dcpb.ReadRequest{
		Isolation: isolation,
		Key:       digestToKey(r.GetDigest()),
		Offset:    offset,
		Limit:     limit,
		Resource:  r,
	}

	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}

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
	if err == io.EOF && r.GetDigest().GetSizeBytes() == offset {
		return reader, nil
	}
	return reader, err
}

type streamWriteCloser struct {
	cancelFunc    context.CancelFunc
	stream        dcpb.DistributedCache_WriteClient
	r             *rspb.ResourceName
	key           *dcpb.Key
	isolation     *dcpb.Isolation
	handoffPeer   string
	bytesUploaded int64
}

func (wc *streamWriteCloser) Write(data []byte) (int, error) {
	req := &dcpb.WriteRequest{
		Isolation:   wc.isolation,
		Key:         wc.key,
		Data:        data,
		FinishWrite: false,
		HandoffPeer: wc.handoffPeer,
		Resource:    wc.r,
	}
	err := wc.stream.Send(req)
	if err == io.EOF {
		_, streamErr := wc.stream.CloseAndRecv()
		if streamErr != nil {
			return 0, streamErr
		}
	}
	return len(data), err
}

func (wc *streamWriteCloser) Commit() error {
	req := &dcpb.WriteRequest{
		Isolation:   wc.isolation,
		Key:         wc.key,
		FinishWrite: true,
		HandoffPeer: wc.handoffPeer,
		Resource:    wc.r,
	}
	if err := wc.stream.Send(req); err != nil {
		return err
	}
	_, err := wc.stream.CloseAndRecv()
	return err
}

func (wc *streamWriteCloser) Close() error {
	wc.cancelFunc()
	return nil
}

type bufferedStreamWriteCloser struct {
	swc            *streamWriteCloser
	bufferedWriter *bufio.Writer
	returnWriter   func()
}

func (bc *bufferedStreamWriteCloser) Write(data []byte) (int, error) {
	return bc.bufferedWriter.Write(data)
}

func (bc *bufferedStreamWriteCloser) Commit() error {
	if err := bc.bufferedWriter.Flush(); err != nil {
		return err
	}
	return bc.swc.Commit()
}

func (bc *bufferedStreamWriteCloser) Close() error {
	bc.returnWriter()
	return bc.swc.Close()
}

func (c *CacheProxy) newBufferedStreamWriteCloser(swc *streamWriteCloser) *bufferedStreamWriteCloser {
	bufWriter := c.writeBufPool.Get().(*bufio.Writer)
	bufWriter.Reset(swc)
	return &bufferedStreamWriteCloser{
		swc:            swc,
		bufferedWriter: bufWriter,
		returnWriter: func() {
			c.writeBufPool.Put(bufWriter)
		},
	}
}

func (c *CacheProxy) RemoteWriter(ctx context.Context, peer, handoffPeer string, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	// Stopping a write mid-stream is difficult because Write streams are
	// unidirectional. The server can close the stream early, but this does
	// not necessarily save the client any work. So, to attempt to reduce
	// duplicate writes, we call Contains before writing a new digest, and
	// if it already exists, we'll return a devnull writecloser so no bytes
	// are transmitted over the network.
	if r.GetCacheType() == rspb.CacheType_CAS {
		if alreadyExists, err := c.RemoteContains(ctx, peer, r); err == nil && alreadyExists {
			log.Debugf("Skipping duplicate CAS write of %q", r.GetDigest().GetHash())
			return ioutil.DiscardWriteCloser(), nil
		}
	}
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}

	isolation := &dcpb.Isolation{
		CacheType:          r.GetCacheType(),
		RemoteInstanceName: r.GetInstanceName(),
	}

	ctx, cancel := context.WithCancel(ctx)
	stream, err := client.Write(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	wc := &streamWriteCloser{
		cancelFunc:    cancel,
		isolation:     isolation,
		handoffPeer:   handoffPeer,
		key:           digestToKey(r.GetDigest()),
		bytesUploaded: 0,
		stream:        stream,
		r:             r,
	}
	return c.newBufferedStreamWriteCloser(wc), nil
}

func (c *CacheProxy) SendHeartbeat(ctx context.Context, peer string) error {
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return err
	}
	req := &dcpb.HeartbeatRequest{
		Source: c.listenAddr,
	}
	_, err = client.Heartbeat(c.prepareContext(ctx), req)
	return err
}
