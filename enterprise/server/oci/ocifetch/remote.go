package ocifetch

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"google.golang.org/protobuf/proto"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
)

// RemoteFetcherUpstream is an Upstream backed by another OCIFetcher service:
// the executor's cache target, or the app behind a cache proxy. It is not a
// registry, so Options.BypassRegistry is forwarded rather than refused and
// the fetcher on the far side applies the rule.
type RemoteFetcherUpstream struct {
	client ofpb.OCIFetcherClient
}

var _ Upstream = (*RemoteFetcherUpstream)(nil)

func NewRemoteFetcherUpstream(client ofpb.OCIFetcherClient) (*RemoteFetcherUpstream, error) {
	if client == nil {
		return nil, status.FailedPreconditionError("ocifetch: an OCIFetcherClient is required")
	}
	return &RemoteFetcherUpstream{client: client}, nil
}

func (u *RemoteFetcherUpstream) Head(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, error) {
	resp, err := u.client.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{
		Ref:            ref.String(),
		Credentials:    creds,
		BypassRegistry: opts.BypassRegistry,
	})
	if err != nil {
		return nil, err
	}
	h, err := ctr.NewHash(resp.GetDigest())
	if err != nil {
		return nil, status.InternalErrorf("invalid digest %q from OCI fetcher: %s", resp.GetDigest(), err)
	}
	return &ctr.Descriptor{Digest: h, Size: resp.GetSize(), MediaType: types.MediaType(resp.GetMediaType())}, nil
}

func (u *RemoteFetcherUpstream) Manifest(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, []byte, error) {
	resp, err := u.client.FetchManifest(ctx, &ofpb.FetchManifestRequest{
		Ref:            ref.String(),
		Credentials:    creds,
		BypassRegistry: opts.BypassRegistry,
	})
	if err != nil {
		return nil, nil, err
	}
	h, err := ctr.NewHash(resp.GetDigest())
	if err != nil {
		return nil, nil, status.InternalErrorf("invalid digest %q from OCI fetcher: %s", resp.GetDigest(), err)
	}
	return &ctr.Descriptor{Digest: h, Size: resp.GetSize(), MediaType: types.MediaType(resp.GetMediaType())}, resp.GetManifest(), nil
}

func (u *RemoteFetcherUpstream) BlobMetadata(ctx context.Context, ref ctrname.Digest, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, error) {
	h, err := blobHash(ref)
	if err != nil {
		return nil, err
	}
	resp, err := u.client.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
		Ref:            ref.String(),
		Credentials:    creds,
		BypassRegistry: opts.BypassRegistry,
	})
	if err != nil {
		return nil, err
	}
	return &ctr.Descriptor{Digest: h, Size: resp.GetSize(), MediaType: types.MediaType(resp.GetMediaType())}, nil
}

// Blob opens a FetchBlob stream, forwarding the size and media type hints
// when the caller has them. The stream itself carries no size or media type,
// so the returned descriptor only echoes the hints; callers that need the
// size and do not have it should call BlobMetadata first.
func (u *RemoteFetcherUpstream) Blob(ctx context.Context, ref ctrname.Digest, creds *rgpb.Credentials, opts Options) (io.ReadCloser, *ctr.Descriptor, error) {
	h, err := blobHash(ref)
	if err != nil {
		return nil, nil, err
	}
	req := &ofpb.FetchBlobRequest{
		Ref:            ref.String(),
		Credentials:    creds,
		BypassRegistry: opts.BypassRegistry,
	}
	if opts.SizeBytes > 0 {
		req.Size = proto.Int64(opts.SizeBytes)
	}
	if opts.MediaType != "" {
		req.MediaType = proto.String(opts.MediaType)
	}
	// A cancellable context lets Close abort the stream if the caller does
	// not read to EOF.
	ctx, cancel := context.WithCancel(ctx)
	stream, err := u.client.FetchBlob(ctx, req)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return &streamReader{stream: stream, cancel: cancel}, &ctr.Descriptor{Digest: h, Size: opts.SizeBytes, MediaType: types.MediaType(opts.MediaType)}, nil
}

// streamReader adapts a FetchBlob stream to an io.ReadCloser.
type streamReader struct {
	stream ofpb.OCIFetcher_FetchBlobClient
	cancel context.CancelFunc
	buf    []byte
}

func (r *streamReader) Read(p []byte) (int, error) {
	if len(r.buf) == 0 {
		resp, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		r.buf = resp.GetData()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// Close cancels the stream's context to release its resources. It is safe to
// call after the stream has been fully read.
func (r *streamReader) Close() error {
	r.cancel()
	return nil
}
