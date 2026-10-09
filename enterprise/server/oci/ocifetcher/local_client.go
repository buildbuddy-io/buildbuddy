package ocifetcher

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
)

// NewLocalClient returns an OCIFetcherClient that calls the given server
// in-process, without a gRPC connection. Call options are ignored.
func NewLocalClient(server ofpb.OCIFetcherServer) ofpb.OCIFetcherClient {
	return &localClient{server: server}
}

type localClient struct {
	server ofpb.OCIFetcherServer
}

func (c *localClient) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest, _ ...grpc.CallOption) (*ofpb.FetchManifestResponse, error) {
	return c.server.FetchManifest(ctx, req)
}

func (c *localClient) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest, _ ...grpc.CallOption) (*ofpb.FetchManifestMetadataResponse, error) {
	return c.server.FetchManifestMetadata(ctx, req)
}

func (c *localClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, _ ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	return c.server.FetchBlobMetadata(ctx, req)
}

// FetchBlob runs the server's FetchBlob in a goroutine and hands each
// response to the returned stream. Cancelling ctx stops the server.
func (c *localClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[ofpb.FetchBlobResponse], error) {
	ctx, cancel := context.WithCancel(ctx)
	s := &localBlobStream{
		ctx:       ctx,
		responses: make(chan *ofpb.FetchBlobResponse),
		done:      make(chan struct{}),
	}
	go func() {
		defer cancel()
		s.err = c.server.FetchBlob(req, &localBlobServerStream{s: s})
		close(s.done)
	}()
	return s, nil
}

// localBlobStream connects the server and client halves of an in-process
// FetchBlob call.
type localBlobStream struct {
	ctx       context.Context
	responses chan *ofpb.FetchBlobResponse

	// done is closed once the server returns, after err is set.
	done chan struct{}
	err  error
}

func (s *localBlobStream) Recv() (*ofpb.FetchBlobResponse, error) {
	select {
	case resp := <-s.responses:
		return resp, nil
	case <-s.done:
		return nil, s.result()
	case <-s.ctx.Done():
		// The context is also canceled when the server returns, after done
		// is closed, so check for that first.
		select {
		case <-s.done:
			return nil, s.result()
		default:
			return nil, status.FromContextError(s.ctx)
		}
	}
}

func (s *localBlobStream) result() error {
	if s.err != nil {
		return s.err
	}
	return io.EOF
}

func (s *localBlobStream) Header() (metadata.MD, error) { return nil, nil }
func (s *localBlobStream) Trailer() metadata.MD         { return nil }
func (s *localBlobStream) CloseSend() error             { return nil }
func (s *localBlobStream) Context() context.Context     { return s.ctx }
func (s *localBlobStream) SendMsg(m any) error {
	return status.UnimplementedError("SendMsg is not supported on a server-streaming call")
}
func (s *localBlobStream) RecvMsg(m any) error {
	return status.UnimplementedError("RecvMsg is not supported on a local stream; use Recv")
}

// localBlobServerStream is the server's view of a localBlobStream.
type localBlobServerStream struct {
	s *localBlobStream
}

// Send copies the data before handing it off, since the server reuses
// its buffer as soon as Send returns.
func (ss *localBlobServerStream) Send(resp *ofpb.FetchBlobResponse) error {
	data := make([]byte, len(resp.GetData()))
	copy(data, resp.GetData())
	select {
	case ss.s.responses <- &ofpb.FetchBlobResponse{Data: data}:
		return nil
	case <-ss.s.ctx.Done():
		return status.FromContextError(ss.s.ctx)
	}
}

func (ss *localBlobServerStream) SetHeader(metadata.MD) error  { return nil }
func (ss *localBlobServerStream) SendHeader(metadata.MD) error { return nil }
func (ss *localBlobServerStream) SetTrailer(metadata.MD)       {}
func (ss *localBlobServerStream) Context() context.Context     { return ss.s.ctx }
func (ss *localBlobServerStream) SendMsg(m any) error {
	resp, ok := m.(*ofpb.FetchBlobResponse)
	if !ok {
		return status.InvalidArgumentErrorf("unexpected message type %T", m)
	}
	return ss.Send(resp)
}
func (ss *localBlobServerStream) RecvMsg(m any) error {
	return status.UnimplementedError("RecvMsg is not supported on a server-streaming call")
}
