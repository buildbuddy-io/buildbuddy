package oci_byte_stream

import (
	"context"
	"fmt"
	"io"

	gcr "github.com/google/go-containerregistry/pkg/v1"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// OCIByteStreamClient wraps a ByteStreamClient and adds OCI metadata to requests
type OCIByteStreamClient struct {
	underlying bspb.ByteStreamClient
}

// NewOCIByteStreamClient creates a new OCI byte stream client that wraps an existing client
func NewOCIByteStreamClient(underlying bspb.ByteStreamClient) *OCIByteStreamClient {
	return &OCIByteStreamClient{
		underlying: underlying,
	}
}

// OCIReadOptions contains OCI-specific metadata for a read operation
type OCIReadOptions struct {
	Registry    string
	Repository  gcrname.Repository
	Digest      gcr.Hash
	Credentials *rgpb.Credentials
	MediaType   string
	ContentSize int64
}

// Read wraps the underlying client's Read method, optionally adding OCI headers if provided
func (c *OCIByteStreamClient) Read(ctx context.Context, req *bspb.ReadRequest, opts ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
	// Check if OCI options are passed via the context
	if ociOpts, ok := ctx.Value(ociReadOptionsKey{}).(*OCIReadOptions); ok && ociOpts != nil {
		ctx = addOCIHeaders(ctx, ociOpts)
	}

	return c.underlying.Read(ctx, req, opts...)
}

// Write delegates to the underlying client (though OCI byte stream server doesn't support writes)
func (c *OCIByteStreamClient) Write(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	return c.underlying.Write(ctx, opts...)
}

// QueryWriteStatus delegates to the underlying client
func (c *OCIByteStreamClient) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest, opts ...grpc.CallOption) (*bspb.QueryWriteStatusResponse, error) {
	return c.underlying.QueryWriteStatus(ctx, req, opts...)
}

// ociReadOptionsKey is used as a context key for OCI read options
type ociReadOptionsKey struct{}

// ContextWithOCIReadOptions adds OCI read options to a context
func ContextWithOCIReadOptions(ctx context.Context, opts *OCIReadOptions) context.Context {
	return context.WithValue(ctx, ociReadOptionsKey{}, opts)
}

// addOCIHeaders adds OCI metadata as gRPC headers to the outgoing context
func addOCIHeaders(ctx context.Context, opts *OCIReadOptions) context.Context {
	md := metadata.MD{}

	if opts.Registry != "" {
		md.Set(ociRegistryHeader, opts.Registry)
	}

	if opts.Repository.String() != "" {
		// Extract just the repository path (without registry)
		repoPath := opts.Repository.RepositoryStr()
		md.Set(ociRepositoryHeader, repoPath)
	}

	if opts.Digest.String() != "" {
		md.Set(ociDigestHeader, opts.Digest.String())
	}

	if opts.Credentials != nil {
		if opts.Credentials.Username != "" {
			md.Set(ociUsernameHeader, opts.Credentials.Username)
		}
		if opts.Credentials.Password != "" {
			md.Set(ociPasswordHeader, opts.Credentials.Password)
		}
	}

	if opts.MediaType != "" {
		md.Set(ociMediaTypeHeader, opts.MediaType)
	}

	if opts.ContentSize > 0 {
		md.Set(ociContentSizeHeader, fmt.Sprintf("%d", opts.ContentSize))
	}

	return metadata.NewOutgoingContext(ctx, md)
}

// byteStreamReadCloser adapts a ByteStream_ReadClient to an io.ReadCloser
type byteStreamReadCloser struct {
	stream bspb.ByteStream_ReadClient
	buf    []byte
	pos    int
}

// ReadFromByteStream creates an io.ReadCloser that reads from a byte stream client
func ReadFromByteStream(ctx context.Context, client bspb.ByteStreamClient, req *bspb.ReadRequest) (io.ReadCloser, error) {
	stream, err := client.Read(ctx, req)
	if err != nil {
		return nil, err
	}

	return &byteStreamReadCloser{
		stream: stream,
		buf:    nil,
		pos:    0,
	}, nil
}

func (r *byteStreamReadCloser) Read(p []byte) (int, error) {
	if r.buf == nil || r.pos >= len(r.buf) {
		// Need to receive next chunk from stream
		resp, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		r.buf = resp.GetData()
		r.pos = 0
	}

	n := copy(p, r.buf[r.pos:])
	r.pos += n
	return n, nil
}

func (r *byteStreamReadCloser) Close() error {
	// gRPC streams don't have a Close method on the client side,
	// but we can call CloseSend to indicate we're done
	return r.stream.CloseSend()
}
