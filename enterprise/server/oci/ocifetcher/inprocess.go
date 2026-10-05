package ocifetcher

import (
	"context"
	"io"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
)

// NewInProcessClient returns an OCIFetcherClient that calls server directly,
// without going through gRPC.
func NewInProcessClient(server ofpb.OCIFetcherServer) ofpb.OCIFetcherClient {
	return &inProcessClient{server: server}
}

type inProcessClient struct {
	server ofpb.OCIFetcherServer
}

func (c *inProcessClient) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest, _ ...grpc.CallOption) (*ofpb.FetchManifestResponse, error) {
	return c.server.FetchManifest(ctx, req)
}

func (c *inProcessClient) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest, _ ...grpc.CallOption) (*ofpb.FetchManifestMetadataResponse, error) {
	return c.server.FetchManifestMetadata(ctx, req)
}

func (c *inProcessClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, _ ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	return c.server.FetchBlobMetadata(ctx, req)
}

// FetchBlob runs the server's FetchBlob in a goroutine, handing each response
// to the returned stream. Canceling ctx stops the server.
func (c *inProcessClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[ofpb.FetchBlobResponse], error) {
	ctx, cancel := context.WithCancel(ctx)
	s := &inProcessBlobStream{
		ctx:       ctx,
		responses: make(chan *ofpb.FetchBlobResponse),
		done:      make(chan struct{}),
	}
	go func() {
		defer cancel()
		s.err = c.server.FetchBlob(req, &inProcessBlobServerStream{s: s})
		close(s.done)
	}()
	return s, nil
}

// inProcessBlobStream is the client side of an in-process FetchBlob call.
type inProcessBlobStream struct {
	ctx       context.Context
	responses chan *ofpb.FetchBlobResponse
	// done is closed after the server returns. err is set before then.
	done chan struct{}
	err  error
}

func (s *inProcessBlobStream) Recv() (*ofpb.FetchBlobResponse, error) {
	// responses is unbuffered and the server only returns after its last
	// send completes, so once done is closed there is nothing left to read.
	select {
	case resp := <-s.responses:
		return resp, nil
	case <-s.done:
		if s.err != nil {
			return nil, s.err
		}
		return nil, io.EOF
	}
}

func (s *inProcessBlobStream) Header() (metadata.MD, error) { return nil, nil }
func (s *inProcessBlobStream) Trailer() metadata.MD         { return nil }
func (s *inProcessBlobStream) CloseSend() error             { return nil }
func (s *inProcessBlobStream) Context() context.Context     { return s.ctx }
func (s *inProcessBlobStream) SendMsg(m any) error          { return nil }
func (s *inProcessBlobStream) RecvMsg(m any) error {
	resp, err := s.Recv()
	if err != nil {
		return err
	}
	m.(*ofpb.FetchBlobResponse).Data = resp.GetData()
	return nil
}

// inProcessBlobServerStream is the server side of an in-process FetchBlob
// call.
type inProcessBlobServerStream struct {
	s *inProcessBlobStream
}

func (ss *inProcessBlobServerStream) Send(resp *ofpb.FetchBlobResponse) error {
	// The server reuses its buffer after Send returns, so copy the data the
	// way gRPC serialization would.
	resp = &ofpb.FetchBlobResponse{Data: slices.Clone(resp.GetData())}
	select {
	case ss.s.responses <- resp:
		return nil
	case <-ss.s.ctx.Done():
		return ss.s.ctx.Err()
	}
}

func (ss *inProcessBlobServerStream) SetHeader(metadata.MD) error  { return nil }
func (ss *inProcessBlobServerStream) SendHeader(metadata.MD) error { return nil }
func (ss *inProcessBlobServerStream) SetTrailer(metadata.MD)       {}
func (ss *inProcessBlobServerStream) Context() context.Context     { return ss.s.ctx }
func (ss *inProcessBlobServerStream) SendMsg(m any) error {
	return ss.Send(m.(*ofpb.FetchBlobResponse))
}
func (ss *inProcessBlobServerStream) RecvMsg(m any) error { return nil }
