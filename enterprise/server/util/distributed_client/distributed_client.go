package distributed_client

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

type Proxy struct {
	env                   environment.Env
	cache                 interfaces.Cache
	log                   log.Logger
	readBufPool           *bytebufferpool.VariableSizePool
	writeBufPool          *bytebufferpool.VariableWriteBufPool
	mu                    *sync.Mutex
	server                *grpc.Server
	clients               map[string]*grpc_client.ClientConnPool
	heartbeatCallback     func(ctx context.Context, peer string)
	hintedHandoffCallback func(ctx context.Context, peer string, r *rspb.ResourceName)
	listenAddr            string
	zone                  string
}

func New(env environment.Env, c interfaces.Cache, listenAddr string) *Proxy {
	proxy := &Proxy{
		env:          env,
		cache:        c,
		log:          log.NamedSubLogger(fmt.Sprintf("Proxy(%s)", listenAddr)),
		readBufPool:  bytebufferpool.VariableSize(readBufSizeBytes),
		writeBufPool: bytebufferpool.NewVariableWriteBufPool(readBufSizeBytes),
		listenAddr:   listenAddr,
		mu:           &sync.Mutex{},
		// server goes here
		clients: make(map[string]*grpc_client.ClientConnPool),
	}
	if zone := resources.GetZone(); zone != "" {
		proxy.zone = zone
	}
	return proxy
}

func (c *Proxy) StartListening() error {
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

func (c *Proxy) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.server == nil {
		return status.FailedPreconditionError("The server was already stopped.")
	}
	err := grpc_server.GRPCShutdown(ctx, c.server)
	c.server = nil
	return err
}

func (c *Proxy) SetHeartbeatCallbackFunc(fn func(ctx context.Context, peer string)) {
	c.heartbeatCallback = fn
}

func (c *Proxy) SetHintedHandoffCallbackFunc(fn func(ctx context.Context, peer string, r *rspb.ResourceName)) {
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

func (c *Proxy) getClient(ctx context.Context, peer string) (dcpb.DistributedCacheClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.clients[peer]; ok {
		conn, err := client.GetReadyConnection()
		if err != nil {
			return nil, status.UnavailableErrorf("no connections to peer %q are ready", peer)
		}
		return dcpb.NewDistributedCacheClient(conn), nil
	}
	log.Debugf("Creating new client for peer: %q", peer)
	conn, err := grpc_client.DialInternal(c.env, "grpc://"+peer)
	if err != nil {
		return nil, err
	}
	c.clients[peer] = conn
	return dcpb.NewDistributedCacheClient(conn), nil
}

func (c *Proxy) prepareContext(ctx context.Context) context.Context {
	if c.zone != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, resources.ZoneHeader, c.zone)
	}
	return ctx
}

func (c *Proxy) readWriteContext(ctx context.Context) (context.Context, error) {
	ctx, err := prefix.AttachUserPrefixToContext(c.prepareContext(ctx), c.env.GetAuthenticator())
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

func (c *Proxy) FindMissing(ctx context.Context, req *dcpb.FindMissingRequest) (*dcpb.FindMissingResponse, error) {
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

func (c *Proxy) Metadata(ctx context.Context, req *dcpb.MetadataRequest) (*dcpb.MetadataResponse, error) {
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

func (c *Proxy) Delete(ctx context.Context, req *dcpb.DeleteRequest) (*dcpb.DeleteResponse, error) {
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

func (c *Proxy) GetMulti(ctx context.Context, req *dcpb.GetMultiRequest) (*dcpb.GetMultiResponse, error) {
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

func (c *Proxy) Read(req *dcpb.ReadRequest, stream dcpb.DistributedCache_ReadServer) error {
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

func (c *Proxy) callHintedHandoffCB(ctx context.Context, peer string, r *rspb.ResourceName) {
	if c.hintedHandoffCallback != nil {
		c.hintedHandoffCallback(ctx, peer, r)
	}
}

func (c *Proxy) Write(stream dcpb.DistributedCache_WriteServer) error {
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
		if rn.GetCacheType() == rspb.CacheType_CAS && req.GetCheckAlreadyExists() {
			missing, err := c.cache.FindMissing(ctx, []*rspb.ResourceName{rn})
			if err == nil && len(missing) == 0 {
				return status.AlreadyExistsError("CAS digest already exists")
			}
		}
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

func (c *Proxy) Heartbeat(ctx context.Context, req *dcpb.HeartbeatRequest) (*dcpb.HeartbeatResponse, error) {
	if req.GetSource() == "" {
		return nil, status.InvalidArgumentError("A source is required.")
	}
	if c.heartbeatCallback != nil {
		c.heartbeatCallback(ctx, req.GetSource())
	}
	return &dcpb.HeartbeatResponse{}, nil
}

func (c *Proxy) RemoteContains(ctx context.Context, peer string, r *rspb.ResourceName) (bool, error) {
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

func (c *Proxy) RemoteMetadata(ctx context.Context, peer string, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
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

func (c *Proxy) RemoteFindMissing(ctx context.Context, peer string, isolation *dcpb.Isolation, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
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

func (c *Proxy) RemoteDelete(ctx context.Context, peer string, r *rspb.ResourceName) error {
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

func (c *Proxy) RemoteGetMulti(ctx context.Context, peer string, isolation *dcpb.Isolation, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
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

func (c *Proxy) RemoteReader(ctx context.Context, peer string, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}
	req := &dcpb.ReadRequest{
		Isolation: &dcpb.Isolation{
			CacheType:          r.GetCacheType(),
			RemoteInstanceName: r.GetInstanceName(),
		},
		Key:      digestToKey(r.GetDigest()),
		Offset:   offset,
		Limit:    limit,
		Resource: r,
	}
	stream, err := client.Read(ctx, req)
	if err != nil {
		return nil, err
	}
	return newDistributedCacheReader(stream, r.GetDigest().GetSizeBytes() == offset)
}

type distributedCacheReader struct {
	stream dcpb.DistributedCache_ReadClient
	rsp    *dcpb.ReadResponse
	err    error
}

func newDistributedCacheReader(stream dcpb.DistributedCache_ReadClient, expectEOF bool) (*distributedCacheReader, error) {
	r := &distributedCacheReader{
		stream: stream,
		rsp:    dcpb.ReadResponseFromVTPool(),
	}
	// Bit annoying here -- the gRPC stream won't give us an error until
	// we've called Recv on it. But we don't want to return a reader that
	// we know will error on first read with NotFound -- we want to return
	// that error now. So read the first message here and return any unexpected
	// error.
	r.moreData()
	if r.err == nil || (r.err == io.EOF && expectEOF) {
		return r, nil
	}
	return nil, r.err
}

// moreData fetches the next batch of data if necessary, and returns true if
// there is more data.
func (r *distributedCacheReader) moreData() bool {
	if r.err == nil && len(r.rsp.GetData()) == 0 {
		r.err = r.stream.RecvMsg(r.rsp)
	}
	return r.err == nil || len(r.rsp.GetData()) > 0
}

func (r *distributedCacheReader) Read(out []byte) (int, error) {
	if !r.moreData() {
		return 0, r.err
	}
	n := copy(out, r.rsp.GetData())
	r.rsp.Data = r.rsp.Data[n:]
	if !r.moreData() {
		// If there is no more data, allow returning a possible EOF. This lets
		// the client skip making another Read call just to get EOF.
		return n, r.err
	}
	return n, nil
}

func (r *distributedCacheReader) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for r.moreData() {
		n, err := w.Write(r.rsp.GetData())
		total += int64(n)
		if err != nil {
			return total, err
		}
		r.rsp.Data = r.rsp.Data[n:]
	}
	if r.err == io.EOF {
		return total, nil
	}
	return total, r.err
}

func (r *distributedCacheReader) Close() error {
	r.rsp.ReturnToVTPool()
	return r.stream.CloseSend()
}

type streamWriteCloser struct {
	cancelFunc    context.CancelFunc
	stream        dcpb.DistributedCache_WriteClient
	r             *rspb.ResourceName
	key           *dcpb.Key
	isolation     *dcpb.Isolation
	handoffPeer   string
	alreadyExists bool
}

func (wc *streamWriteCloser) Write(data []byte) (int, error) {
	if wc.alreadyExists {
		return len(data), nil
	}
	req := &dcpb.WriteRequest{
		Isolation:          wc.isolation,
		Key:                wc.key,
		Data:               data,
		FinishWrite:        false,
		CheckAlreadyExists: true,
		HandoffPeer:        wc.handoffPeer,
		Resource:           wc.r,
	}
	err := wc.stream.Send(req)
	if err == io.EOF {
		_, streamErr := wc.stream.CloseAndRecv()
		if status.IsAlreadyExistsError(streamErr) {
			wc.alreadyExists = true
			err = nil
		} else if streamErr != nil {
			return 0, streamErr
		} else {
			return 0, io.ErrShortWrite
		}
	}
	return len(data), err
}

func (wc *streamWriteCloser) Commit() error {
	if wc.alreadyExists {
		return nil
	}

	req := &dcpb.WriteRequest{
		Isolation:          wc.isolation,
		Key:                wc.key,
		FinishWrite:        true,
		CheckAlreadyExists: true,
		HandoffPeer:        wc.handoffPeer,
		Resource:           wc.r,
	}
	sendErr := wc.stream.Send(req)
	if sendErr != nil && sendErr != io.EOF {
		return sendErr
	}
	_, err := wc.stream.CloseAndRecv()
	if status.IsAlreadyExistsError(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return sendErr
}

func (wc *streamWriteCloser) Close() error {
	wc.cancelFunc()
	return nil
}

type bufferedStreamWriteCloser struct {
	swc            *streamWriteCloser
	bufferedWriter *bytebufferpool.BufioWriter
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

func safeBufferSize(r *rspb.ResourceName) int64 {
	size := int64(4096 * 4) // low / safe / default
	if r.GetCacheType() == rspb.CacheType_CAS {
		size = r.GetDigest().GetSizeBytes()
	}
	if size > readBufSizeBytes {
		size = readBufSizeBytes
	}
	return size
}

func (c *Proxy) newBufferedStreamWriteCloser(swc *streamWriteCloser) *bufferedStreamWriteCloser {
	bufWriter := c.writeBufPool.Get(safeBufferSize(swc.r))
	bufWriter.Reset(swc)
	return &bufferedStreamWriteCloser{
		swc:            swc,
		bufferedWriter: bufWriter,
		returnWriter: func() {
			c.writeBufPool.Put(bufWriter)
		},
	}
}

func (c *Proxy) RemoteWriter(ctx context.Context, peer, handoffPeer string, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
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
		cancelFunc:  cancel,
		isolation:   isolation,
		handoffPeer: handoffPeer,
		key:         digestToKey(r.GetDigest()),
		stream:      stream,
		r:           r,
	}
	return c.newBufferedStreamWriteCloser(wc), nil
}

func (c *Proxy) SendHeartbeat(ctx context.Context, peer string) error {
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
