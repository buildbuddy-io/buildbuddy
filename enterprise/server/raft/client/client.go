package client

import (
	"context"
)

type apiConn struct {
	rfpb.ApiClient
	conn *grpc.ClientConn
}

type APIClient struct {
	env                   environment.Env
	mu                    *sync.Mutex
	clients               map[string]*apiConn
}

func (c *CacheProxy) getClient(ctx context.Context, peer string) (rfpb.ApiClient, error) {
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
	c.clients[peer] = &dcClient{ApiClient: client, conn: conn}
	return client, nil
}

func (c *CacheProxy) RemoteReader(ctx context.Context, peer string, isolation *dcpb.Isolation, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	req := &dcpb.ReadRequest{
		Isolation: isolation,
		Key:       digestToKey(d),
		Offset:    offset,
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
