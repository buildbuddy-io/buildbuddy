package cacheproxy

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	dcpb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	jwtHeader      = "x-buildbuddy-jwt"
	maxDialTimeout = 10 * time.Second
)

type CacheProxy struct {
	env        environment.Env
	cache      interfaces.Cache
	listenAddr string
	mu         *sync.Mutex
	server     *grpc.Server
	clients    map[string]dcpb.DistributedCacheClient
}

func NewCacheProxy(env environment.Env, c interfaces.Cache, listenAddr string) *CacheProxy {
	proxy := &CacheProxy{
		env:        env,
		cache:      c,
		listenAddr: listenAddr,
		mu:         &sync.Mutex{},
		// server goes here
		clients: make(map[string]dcpb.DistributedCacheClient, 0),
	}

	grpcOptions := grpc_server.CommonGRPCServerOptions(env)
	grpcServer := grpc.NewServer(grpcOptions...)
	reflection.Register(grpcServer)
	dcpb.RegisterDistributedCacheServer(grpcServer, proxy)
	proxy.server = grpcServer
	return proxy
}

func (c *CacheProxy) StartListening() error {
	lis, err := net.Listen("tcp", c.listenAddr)
	if err != nil {
		return err
	}
	go func() {
		log.Printf("Listening on %s", c.listenAddr)
		c.server.Serve(lis)
	}()
	return nil
}

func (c *CacheProxy) Shutdown(ctx context.Context) error {
	return grpc_server.GRPCShutdown(ctx, c.server)
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

func dialTimeout(ctx context.Context) time.Duration {
	timeoutDuration := maxDialTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) < timeoutDuration {
			timeoutDuration = deadline.Sub(time.Now())
		}
	}
	return timeoutDuration
}

func (c *CacheProxy) getClient(ctx context.Context, peer string) (dcpb.DistributedCacheClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.clients[peer]; ok {
		return client, nil
	}
	dialOptions := grpc_client.CommonGRPCClientOptions()
	dialOptions = append(dialOptions, grpc.WithInsecure())
	conn, err := grpc_client.DialTargetWithOptions("grpc://"+peer, true, grpc.WithTimeout(dialTimeout(ctx)), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	client := dcpb.NewDistributedCacheClient(conn)
	c.clients[peer] = client
	return client, nil
}

func (c *CacheProxy) ContainsMulti(ctx context.Context, req *dcpb.ContainsMultiRequest) (*dcpb.ContainsMultiResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, c.env)
	if err != nil {
		return nil, err
	}
	digests := make([]*repb.Digest, 0)
	for _, k := range req.GetKey() {
		digests = append(digests, digestFromKey(k))
	}
	found, err := c.cache.WithPrefix(req.GetPrefix()).ContainsMulti(ctx, digests)
	if err != nil {
		return nil, err
	}
	rsp := &dcpb.ContainsMultiResponse{}
	for d, exists := range found {
		rsp.KeysFound = append(rsp.KeysFound, &dcpb.ContainsMultiResponse_KeysFound{
			Key:    digestToKey(d),
			Exists: exists,
		})
	}
	return rsp, nil
}

func (c *CacheProxy) GetMulti(ctx context.Context, req *dcpb.GetMultiRequest) (*dcpb.GetMultiResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, c.env)
	if err != nil {
		return nil, err
	}
	digests := make([]*repb.Digest, 0)
	for _, k := range req.GetKey() {
		digests = append(digests, digestFromKey(k))
	}
	found, err := c.cache.WithPrefix(req.GetPrefix()).GetMulti(ctx, digests)
	if err != nil {
		return nil, err
	}
	rsp := &dcpb.GetMultiResponse{}
	for d, buf := range found {
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
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), c.env)
	if err != nil {
		return err
	}
	d := digestFromKey(req.GetKey())
	reader, err := c.cache.WithPrefix(req.GetPrefix()).Reader(ctx, d, req.GetOffset())
	if err != nil {
		return err
	}
	defer reader.Close()

	_, err = io.Copy(&streamWriter{stream}, reader)
	return err
}

func (c *CacheProxy) Write(stream dcpb.DistributedCache_WriteServer) error {
	ctx, err := prefix.AttachUserPrefixToContext(stream.Context(), c.env)
	if err != nil {
		return err
	}
	var bytesWritten int64
	var writeCloser io.WriteCloser
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if writeCloser == nil {
			wc, err := c.cache.WithPrefix(req.GetPrefix()).Writer(ctx, digestFromKey(req.GetKey()))
			if err != nil {
				return err
			}
			writeCloser = wc
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
			return stream.SendAndClose(&dcpb.WriteResponse{
				CommittedSize: bytesWritten,
			})
		}
	}
	return nil
}

func (c *CacheProxy) RemoteContains(ctx context.Context, peer, prefix string, d *repb.Digest) (bool, error) {
	multiRsp, err := c.RemoteContainsMulti(ctx, peer, prefix, []*repb.Digest{d})
	if err != nil {
		return false, err
	}
	exists, ok := multiRsp[d]
	return ok && exists, nil
}

func (c *CacheProxy) RemoteContainsMulti(ctx context.Context, peer, prefix string, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	req := &dcpb.ContainsMultiRequest{
		Prefix: prefix,
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
	rsp, err := client.ContainsMulti(ctx, req)
	if err != nil {
		return nil, err
	}
	resultMap := make(map[*repb.Digest]bool, len(rsp.GetKeysFound()))
	for _, keyFound := range rsp.GetKeysFound() {
		d, ok := hashDigests[keyFound.GetKey().GetKey()]
		if ok {
			resultMap[d] = keyFound.GetExists()
		}
	}
	return resultMap, nil
}

func (c *CacheProxy) RemoteGetMulti(ctx context.Context, peer, prefix string, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	req := &dcpb.GetMultiRequest{
		Prefix: prefix,
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

func (c *CacheProxy) RemoteReader(ctx context.Context, peer, prefix string, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	req := &dcpb.ReadRequest{
		Prefix: prefix,
		Key:    digestToKey(d),
		Offset: offset,
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
	go func() {
		for {
			rsp, err := stream.Recv()
			if err == io.EOF {
				writer.Close()
				return
			}
			if err != nil {
				writer.CloseWithError(err)
				return
			}
			writer.Write(rsp.Data)
		}
	}()
	return reader, nil
}

type streamWriteCloser struct {
	prefix        string
	key           *dcpb.Key
	bytesUploaded int64
	stream        dcpb.DistributedCache_WriteClient
}

func (wc *streamWriteCloser) Write(data []byte) (int, error) {
	req := &dcpb.WriteRequest{
		Prefix:      wc.prefix,
		Key:         wc.key,
		Data:        data,
		FinishWrite: false,
	}
	err := wc.stream.Send(req)
	return len(data), err
}

func (wc *streamWriteCloser) Close() error {
	req := &dcpb.WriteRequest{
		Prefix:      wc.prefix,
		Key:         wc.key,
		FinishWrite: true,
	}
	if err := wc.stream.Send(req); err != nil {
		return err
	}
	_, err := wc.stream.CloseAndRecv()
	return err
}

func (c *CacheProxy) RemoteWriter(ctx context.Context, peer, prefix string, d *repb.Digest) (io.WriteCloser, error) {
	client, err := c.getClient(ctx, peer)
	if err != nil {
		return nil, err
	}
	stream, err := client.Write(ctx)
	if err != nil {
		return nil, err
	}
	return &streamWriteCloser{
		prefix:        prefix,
		key:           digestToKey(d),
		bytesUploaded: 0,
		stream:        stream,
	}, nil
}
