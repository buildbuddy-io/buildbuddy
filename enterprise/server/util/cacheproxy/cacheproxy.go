package cacheproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/devnull"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"

	dcpb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const readBufSizeBytes = 1000000 // 1MB

type dcClient struct {
	dcpb.DistributedCacheClient
	conn         *grpc.ClientConn
	wasEverReady bool
}

type CacheProxy struct {
	env                   environment.Env
	cache                 interfaces.Cache
	log                   log.Logger
	bufferPool            *bytebufferpool.Pool
	mu                    *sync.Mutex
	server                *grpc.Server
	clients               map[string]*dcClient
	heartbeatCallback     func(ctx context.Context, peer string)
	hintedHandoffCallback func(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest)
	listenAddr            string
	zone                  string
}

func NewCacheProxy(env environment.Env, c interfaces.Cache, listenAddr string) *CacheProxy {
	proxy := &CacheProxy{
		env:        env,
		cache:      c,
		log:        log.NamedSubLogger(fmt.Sprintf("CacheProxy(%s)", listenAddr)),
		bufferPool: bytebufferpool.New(readBufSizeBytes),
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

func (c *CacheProxy) SetHintedHandoffCallbackFunc(fn func(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest)) {
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
	conn, err := grpc_client.DialTarget("grpc://" + peer)
	if err != nil {
		return nil, err
	}
	client := dcpb.NewDistributedCacheClient(conn)
	c.clients[peer] = &dcClient{DistributedCacheClient: client, conn: conn}
	return client, nil
}

func ProtoCacheTypeToCacheType(cacheType dcpb.Isolation_CacheType) (interfaces.CacheTypeDeprecated, error) {
	switch cacheType {
	case dcpb.Isolation_CAS_CACHE:
		return interfaces.CASCacheType, nil
	case dcpb.Isolation_ACTION_CACHE:
		return interfaces.ActionCacheType, nil
	default:
		return interfaces.UnknownCacheType, status.InvalidArgumentErrorf("unknown cache type %v", cacheType)
	}
}

func (c *CacheProxy) getCache(ctx context.Context, isolation *dcpb.Isolation) (interfaces.Cache, error) {
	ct, err := ProtoCacheTypeToCacheType(isolation.GetCacheType())
	if err != nil {
		return nil, err
	}
	return c.cache.WithIsolation(ctx, ct, isolation.GetRemoteInstanceName())
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

func (c *CacheProxy) FindMissing(ctx context.Context, req *dcpb.FindMissingRequest) (*dcpb.FindMissingResponse, error) {
	ctx, err := c.readWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	digests := make([]*repb.Digest, 0)
	for _, k := range req.GetKey() {
		digests = append(digests, digestFromKey(k))
	}
	cache, err := c.getCache(ctx, req.GetIsolation())
	if err != nil {
		return nil, err
	}
	missing, err := cache.FindMissing(ctx, digests)
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
	cache, err := c.getCache(ctx, req.GetIsolation())
	if err != nil {
		return nil, err
	}
	d := digestFromKey(req.GetKey())
	md, err := cache.Metadata(ctx, d)
	if err != nil {
		return nil, err
	}
	return &dcpb.MetadataResponse{
		SizeBytes:      md.SizeBytes,
		LastModifyUsec: md.LastModifyTimeUsec,
		LastAccessUsec: md.LastAccessTimeUsec,
	}, nil
}

// IsolationToString returns a compact representation of the Isolation proto suitable for logging.
func IsolationToString(isolation *dcpb.Isolation) string {
	ct, err := ProtoCacheTypeToCacheType(isolation.GetCacheType())
	if err != nil {
		alert.UnexpectedEvent("unknown_cache_type", "isolation: %s", isolation)
		ct = interfaces.UnknownCacheType
	}
	rep := filepath.Join(isolation.GetRemoteInstanceName(), ct.Prefix())
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
	cache, err := c.getCache(ctx, req.GetIsolation())
	if err != nil {
		return nil, err
	}
	d := digestFromKey(req.GetKey())
	err = cache.Delete(ctx, d)
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
	digests := make([]*repb.Digest, 0)
	for _, k := range req.GetKey() {
		digests = append(digests, digestFromKey(k))
	}
	cache, err := c.getCache(ctx, req.GetIsolation())
	if err != nil {
		return nil, err
	}
	found, err := cache.GetMulti(ctx, digests)
	if err != nil {
		return nil, err
	}
	rsp := &dcpb.GetMultiResponse{}
	for d, buf := range found {
		if len(buf) == 0 {
			c.log.Warningf("returned a zero-length response for %s", IsolationToString(req.GetIsolation())+d.GetHash())
		}
		rsp.KeyValue = append(rsp.KeyValue, &dcpb.KV{
			Key:   digestToKey(d),
			Value: buf,
		})
	}
	return rsp, nil
}

type streamWriter struct {
	stream dcpb.DistributedCache_ReadServer
}

func (w *streamWriter) Write(buf []byte) (int, error) {
	err := w.stream.Send(&dcpb.ReadResponse{
		Data: buf,
	})
	return len(buf), err
}

func (c *CacheProxy) Read(req *dcpb.ReadRequest, stream dcpb.DistributedCache_ReadServer) error {
	ctx, err := c.readWriteContext(stream.Context())
	if err != nil {
		return err
	}
	up, _ := prefix.UserPrefixFromContext(ctx)
	d := digestFromKey(req.GetKey())
	cache, err := c.getCache(ctx, req.GetIsolation())
	if err != nil {
		return err
	}
	reader, err := cache.Reader(ctx, d, req.GetOffset(), req.GetLimit())
	if err != nil {
		c.log.Debugf("Read(%q) failed (user prefix: %s), err: %s", IsolationToString(req.GetIsolation())+d.GetHash(), up, err)
		return err
	}
	defer reader.Close()

	bufSize := int64(readBufSizeBytes)
	if d.GetSizeBytes() > 0 && d.GetSizeBytes() < bufSize {
		bufSize = d.GetSizeBytes()
	}
	copyBuf := c.bufferPool.Get(bufSize)
	_, err = io.CopyBuffer(&streamWriter{stream}, reader, copyBuf[:bufSize])
	c.bufferPool.Put(copyBuf)
	c.log.Debugf("Read(%q) succeeded (user prefix: %s)", IsolationToString(req.GetIsolation())+d.GetHash(), up)
	return err
}

func (c *CacheProxy) callHintedHandoffCB(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest) {
	if c.hintedHandoffCallback != nil {
		c.hintedHandoffCallback(ctx, peer, isolation, d)
	}
}

func (c *CacheProxy) Write(stream dcpb.DistributedCache_WriteServer) error {
	ctx, err := c.readWriteContext(stream.Context())
	if err != nil {
		return err
	}
	up, _ := prefix.UserPrefixFromContext(ctx)

	var bytesWritten int64
	var writeCloser io.WriteCloser
	handoffPeer := ""
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if writeCloser == nil {
			d := digestFromKey(req.GetKey())
			cache, err := c.getCache(ctx, req.GetIsolation())
			if err != nil {
				return err
			}
			wc, err := cache.Writer(ctx, d)
			if err != nil {
				c.log.Debugf("Write(%q) failed (user prefix: %s), err: %s", IsolationToString(req.GetIsolation())+d.GetHash(), up, err)
				return err
			}
			writeCloser = wc
			handoffPeer = req.GetHandoffPeer()
		}
		n, err := writeCloser.Write(req.Data)
		if err != nil {
			return err
		}
		bytesWritten += int64(n)
		if req.FinishWrite {
			if err := writeCloser.Close(); err != nil {
				return err
			}
			// TODO(vadim): use handoff peer from request once client is including it in the FinishWrite request.
			if handoffPeer != "" {
				c.callHintedHandoffCB(ctx, handoffPeer, req.GetIsolation(), digestFromKey(req.GetKey()))
			}
			c.log.Debugf("Write(%q) succeeded (user prefix: %s)", IsolationToString(req.GetIsolation())+req.GetKey().GetKey(), up)
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

func (c *CacheProxy) RemoteContains(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest) (bool, error) {
	missing, err := c.RemoteFindMissing(ctx, peer, isolation, []*repb.Digest{d})
	if err != nil {
		return false, err
	}
	return len(missing) == 0, nil
}

func (c *CacheProxy) RemoteMetadata(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	req := &dcpb.MetadataRequest{
		Isolation: isolation,
		Key:       digestToKey(d),
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
		SizeBytes:          md.GetSizeBytes(),
		LastAccessTimeUsec: md.GetLastAccessUsec(),
		LastModifyTimeUsec: md.GetLastModifyUsec(),
	}, nil
}

func (c *CacheProxy) RemoteFindMissing(ctx context.Context, peer string, isolation *dcpb.Isolation, digests []*repb.Digest) ([]*repb.Digest, error) {
	req := &dcpb.FindMissingRequest{
		Isolation: isolation,
	}
	hashDigests := make(map[string]*repb.Digest, len(digests))
	for _, d := range digests {
		key := digestToKey(d)
		hashDigests[d.GetHash()] = d
		req.Key = append(req.Key, key)
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

func (c *CacheProxy) RemoteDelete(ctx context.Context, peer string, isolation *dcpb.Isolation, digest *repb.Digest) error {
	req := &dcpb.DeleteRequest{
		Isolation: isolation,
		Key:       digestToKey(digest),
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

func (c *CacheProxy) RemoteGetMulti(ctx context.Context, peer string, isolation *dcpb.Isolation, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	req := &dcpb.GetMultiRequest{
		Isolation: isolation,
	}
	hashDigests := make(map[string]*repb.Digest, len(digests))
	for _, d := range digests {
		key := digestToKey(d)
		hashDigests[d.GetHash()] = d
		req.Key = append(req.Key, key)
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

func (c *CacheProxy) RemoteReader(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	req := &dcpb.ReadRequest{
		Isolation: isolation,
		Key:       digestToKey(d),
		Offset:    offset,
		Limit:     limit,
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
	if err == io.EOF && d.GetSizeBytes() == offset {
		return reader, nil
	}
	return reader, err
}

type streamWriteCloser struct {
	stream        dcpb.DistributedCache_WriteClient
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
	}
	err := wc.stream.Send(req)
	return len(data), err
}

func (wc *streamWriteCloser) Close() error {
	req := &dcpb.WriteRequest{
		Isolation:   wc.isolation,
		Key:         wc.key,
		FinishWrite: true,
		HandoffPeer: wc.handoffPeer,
	}
	if err := wc.stream.Send(req); err != nil {
		return err
	}
	_, err := wc.stream.CloseAndRecv()
	return err
}

func (c *CacheProxy) RemoteWriter(ctx context.Context, peer, handoffPeer string, isolation *dcpb.Isolation, d *repb.Digest) (io.WriteCloser, error) {
	// Stopping a write mid-stream is difficult because Write streams are
	// unidirectional. The server can close the stream early, but this does
	// not necessarily save the client any work. So, to attempt to reduce
	// duplicate writes, we call Contains before writing a new digest, and
	// if it already exists, we'll return a devnull writecloser so no bytes
	// are transmitted over the network.
	if isolation.GetCacheType() == dcpb.Isolation_CAS_CACHE {
		if alreadyExists, err := c.RemoteContains(ctx, peer, isolation, d); err == nil && alreadyExists {
			log.Debugf("Skipping duplicate CAS write of %q", d.GetHash())
			return devnull.NewWriteCloser(), nil
		}
	}
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}
	stream, err := client.Write(ctx)
	if err != nil {
		return nil, err
	}
	wc := &streamWriteCloser{
		isolation:     isolation,
		handoffPeer:   handoffPeer,
		key:           digestToKey(d),
		bytesUploaded: 0,
		stream:        stream,
	}
	return wc, nil
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
