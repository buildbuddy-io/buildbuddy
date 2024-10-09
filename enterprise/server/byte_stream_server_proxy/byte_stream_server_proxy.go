package byte_stream_server_proxy

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/proxy_peers"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"

	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/metadata"
)

type ByteStreamPeer struct {
	debugString string
	client      bspb.ByteStreamClient
}

type ByteStreamServerProxy struct {
	atimeUpdater interfaces.AtimeUpdater
	local        bspb.ByteStreamClient
	remote       bspb.ByteStreamClient
	peers        proxy_peers.Peers
	env          environment.Env
}

func Register(env *real_environment.RealEnv, peers proxy_peers.Peers) error {
	proxy, err := New(env, peers)
	if err != nil {
		return status.InternalErrorf("Error initializing ByteStreamServerProxy: %s", err)
	}
	env.SetByteStreamServer(proxy)
	return nil
}

func New(env environment.Env, peers proxy_peers.Peers) (*ByteStreamServerProxy, error) {
	atimeUpdater := env.GetAtimeUpdater()
	if atimeUpdater == nil {
		return nil, fmt.Errorf("An AtimeUpdater is required to enable ByteStreamServerProxy")
	}
	local := env.GetLocalByteStreamClient()
	if local == nil {
		return nil, fmt.Errorf("A local ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	remote := env.GetByteStreamClient()
	if remote == nil {
		return nil, fmt.Errorf("A remote ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	return &ByteStreamServerProxy{
		atimeUpdater: atimeUpdater,
		local:        local,
		remote:       remote,
		peers:        peers,
		env:          env,
	}, nil
}

func (s *ByteStreamServerProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	// Determine which peer is responsible for the requested digest.
	rn, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	key := digest.NewKey(rn.GetDigest())
	peer, err := s.peers.Get(key)
	if err != nil {
		log.Warningf("Error locating peer responsible for %s: %s", key.Hash, err)
		err, _ = read(ctx, req, stream, s.remote, nil /*=writeTo*/, nil /*=atimeUpdater*/)
		return err
	}

	if !peer.Local {
		// If a peer is responsible for this digest, attempt to proxy the
		// request to the peer, without local-writing or remote-atime-updating.
		err, recoverable := read(ctx, req, stream, peer.BSClient, nil /*=writeTo*/, nil /*=atimeUpdater*/)

		// TODO(iain): improve error checking here.
		if err != nil && recoverable {
			err, _ = read(ctx, req, stream, s.remote, nil /*=writeTo*/, nil /*=atimeUpdater*/)
		}
		return err
	}

	// If it's this host, attempt to read from the local ByteStreamServer,
	// enqueueing a remote atime update.
	err, recoverable := read(ctx, req, stream, s.local, nil /*=writeTo*/, s.atimeUpdater)
	if err == nil {
		metrics.ByteStreamProxyReads.With(
			prometheus.Labels{metrics.CacheHitMissStatus: "hit"}).Inc()
		return nil
	}

	if !status.IsNotFoundError(err) {
		log.CtxInfof(ctx, "Error reading from local bytestream client: %s", err)
	}
	metrics.ByteStreamProxyReads.With(
		prometheus.Labels{metrics.CacheHitMissStatus: "miss"}).Inc()

	// Return non-recoverable errors to the client.
	if !recoverable {
		return err
	}

	// If a recoverable error was encountered, attempt to read from the
	// remote ByteStreamServer, writing the result to the local
	// ByteStreamServer.
	err, _ = read(ctx, req, stream, s.remote, s.local, nil /*=atimeUpdater*/)
	return err
}

// The Write() RPC requires the client keep track of some state. The
// implementations of this interface take care of that.
type localWriter interface {
	send(data []byte) error
	commit() error
}

// A localWriter that discards everything sent to it.
type discardingLocalWriter struct {
}

func (s *discardingLocalWriter) send(data []byte) error {
	return nil
}

func (s *discardingLocalWriter) commit() error {
	return nil
}

// A localWriter that writes data to a local ByteStream_WriteClient.
type realLocalWriter struct {
	ctx          context.Context
	local        bspb.ByteStream_WriteClient
	resourceName string
	initialized  bool
	offset       int64
}

func (s *realLocalWriter) send(data []byte) error {
	req := &bspb.WriteRequest{WriteOffset: s.offset, Data: data}
	if !s.initialized {
		// Rewrite the resource name so we can write to the local server.
		rn, err := digest.ParseDownloadResourceName(s.resourceName)
		if err != nil {
			return err
		}
		urn, err := rn.UploadString()
		if err != nil {
			return err
		}
		req.ResourceName = urn
		s.initialized = true
	}
	s.offset += int64(len(data))
	return s.local.Send(req)
}

func (s *realLocalWriter) commit() error {
	if err := s.local.Send(&bspb.WriteRequest{WriteOffset: s.offset, FinishWrite: true}); err != nil {
		return err
	}
	// Ignore the local response (but not the error)
	_, err := s.local.CloseAndRecv()
	return err
}

func read(ctx context.Context, req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer, readFrom bspb.ByteStreamClient, writeTo bspb.ByteStreamClient, atimeUpdater interfaces.AtimeUpdater) (error, bool) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()

	readStream, err := readFrom.Read(ctx, req)
	if err != nil {
		log.CtxInfof(ctx, "error reading from ByteStreamServer: %s", err)
		return err, true
	}

	if atimeUpdater != nil {
		atimeUpdater.EnqueueByResourceName(ctx, req.ResourceName)
	}

	var writeStream localWriter = &discardingLocalWriter{}
	if writeTo != nil && req.ReadOffset == 0 {
		localStream, err := writeTo.Write(ctx)
		if err == nil {
			writeStream = &realLocalWriter{
				ctx:          ctx,
				local:        localStream,
				resourceName: req.ResourceName,
				initialized:  false,
				offset:       int64(0),
			}
		} else {
			log.CtxInfof(ctx, "error opening Write to ByteStreamServer: %s", err)
		}
	}

	recoverable := true
	for {
		rsp, err := readStream.Recv()
		if err != nil {
			if err == io.EOF {
				if err := writeStream.commit(); err != nil {
					log.CtxInfof(ctx, "error committing local write: %s", err)
				}
				break
			}
			log.CtxInfof(ctx, "error receiving frame from ByteStreamServer.Read: %s", err)
			return err, recoverable
		}

		if err := writeStream.send(rsp.Data); err != nil {
			log.CtxInfof(ctx, "error sending Write to ByteStreamServer: %s", err)
			writeStream = &discardingLocalWriter{}
		}
		if err = stream.Send(rsp); err != nil {
			recoverable = false
			return err, recoverable
		}

		// If frames have been sent to the client, treat subsequent errors as
		// unrecoverable. We could try to do something clever like do an offset
		// read from the remote to pick up where we left off, but keep it
		// simple for now.
		recoverable = false
	}
	return nil, false
}

func peekable(stream bspb.ByteStream_WriteServer) peekableWriteServer {
	peekSent := false
	return peekableWriteServer{
		peekSent: &peekSent,
		stream:   stream,
	}
}

// Wrapper around ByteStream_WriteServer with peeking support.
// TODO(iain): generalize if this would be useful elsewhere.
type peekableWriteServer struct {
	firstMsg *bspb.WriteRequest
	firstErr error
	peekSent *bool

	stream bspb.ByteStream_WriteServer
}

func (s *peekableWriteServer) peek() (*bspb.WriteRequest, error) {
	if *s.peekSent {
		panic("Cannot peek() after Recv()ing")
	}

	if s.firstMsg == nil && s.firstErr == nil {
		req, err := s.stream.Recv()
		s.firstMsg = req
		s.firstErr = err
	}
	return s.firstMsg, s.firstErr
}
func (s peekableWriteServer) SendAndClose(resp *bspb.WriteResponse) error {
	return s.stream.SendAndClose(resp)
}
func (s peekableWriteServer) Recv() (*bspb.WriteRequest, error) {
	if *s.peekSent {
		return s.stream.Recv()
	}
	req, err := s.peek()
	*s.peekSent = true
	return req, err
}
func (s peekableWriteServer) SetHeader(header metadata.MD) error {
	return s.stream.SetHeader(header)
}
func (s peekableWriteServer) SendHeader(header metadata.MD) error {
	return s.stream.SendHeader(header)
}
func (s peekableWriteServer) SetTrailer(trailer metadata.MD) {
	s.stream.SetTrailer(trailer)
}
func (s peekableWriteServer) Context() context.Context {
	return s.stream.Context()
}
func (s peekableWriteServer) SendMsg(m any) error {
	return s.stream.SendMsg(m)
}
func (s peekableWriteServer) RecvMsg(m any) error {
	if *s.peekSent {
		return s.stream.RecvMsg(m)
	}
	s.firstErr = s.stream.RecvMsg(s.firstMsg)
	*s.peekSent = true
	return s.firstErr
}

func (s *ByteStreamServerProxy) Write(inStream bspb.ByteStream_WriteServer) error {
	ctx, spn := tracing.StartSpan(inStream.Context())
	defer spn.End()

	stream := peekable(inStream)
	req, err := stream.peek()
	if err != nil {
		return err
	}
	rn, err := digest.ParseUploadResourceName(req.ResourceName)
	if err != nil {
		return err
	}

	key := digest.NewKey(rn.GetDigest())

	peer, err := s.peers.Get(key)
	if err != nil {
		return err
	}
	log.Debugf("peermap has declared '%s' is responsible for hash %s", peer, key.Hash)
	//peer := s.peers.Get(rn.GetDigest().GetHash())
	// log.Debugf("Write(%s)", rn.GetDigest().GetHash())
	if peer == "" {
		log.Debugf("Attempting to serve write locally")
		return s.writeToLocal(ctx, stream)
	}
	log.Debugf("Proxying write to peer %s", peer)
	conn, err := grpc_client.DialInternal(s.env, fmt.Sprintf("grpcs://%s:1986", peer))
	if err != nil {
		return status.InternalErrorf("CacheProxy: error starting local bytestream gRPC server: %s", err.Error())
	}

	c := bspb.NewByteStreamClient(conn)
	err = proxyWrite(ctx, c, stream)
	// TODO(iain): improve this error check to avoid amplifying load the the remote cache.
	if err != nil {
		log.Debugf("Proxying write to peer %s failed (%s), proxying to remote", peer, err)
		return proxyWrite(ctx, s.remote, stream)
	}
	return err
}

func proxyWrite(ctx context.Context, client bspb.ByteStreamClient, stream bspb.ByteStream_WriteServer) error {
	remoteStream, err := client.Write(stream.Context())
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		writeDone := req.GetFinishWrite()
		if err := remoteStream.Send(req); err != nil {
			if err == io.EOF {
				writeDone = true
			} else {
				return err
			}
		}
		if writeDone {
			lastRsp, err := remoteStream.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(lastRsp)
		}
	}
}

func (s *ByteStreamServerProxy) writeToLocal(ctx context.Context, stream bspb.ByteStream_WriteServer) error {
	local, err := s.local.Write(ctx)
	if err != nil {
		log.CtxInfof(ctx, "error opening local bytestream write stream for write: %s", err)
		local = nil
	}
	remote, err := s.remote.Write(ctx)
	if err != nil {
		return err
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		// Send to the local ByteStreamServer (if it hasn't errored)
		localDone := req.GetFinishWrite()
		if local != nil {
			if err := local.Send(req); err != nil {
				if err == io.EOF {
					localDone = true
				} else {
					log.CtxInfof(ctx, "error writing to local bytestream server for write: %s", err)
				}
				local = nil
			}
		}

		// Send to the remote ByteStreamServer
		done := req.GetFinishWrite()
		if err := remote.Send(req); err != nil {
			if err == io.EOF {
				done = true
			} else {
				return err
			}
		}

		// If the client or the remote server told us the write is done, send the
		// response to the client.
		if done {
			if local != nil {
				if !localDone {
					log.CtxInfo(ctx, "remote write done but local write is not")
				}
				if _, err := local.CloseAndRecv(); err != nil {
					log.CtxInfof(ctx, "error closing local write stream: %s", err)
				}
			}
			resp, err := remote.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(resp)
		} else if localDone {
			log.CtxInfo(ctx, "local write done but remote write is not")
		}
	}
}

func (s *ByteStreamServerProxy) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	return s.remote.QueryWriteStatus(ctx, req)
}
