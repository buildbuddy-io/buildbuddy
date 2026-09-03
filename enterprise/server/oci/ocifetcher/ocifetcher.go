// Package ocifetcher provides the OCIFetcher gRPC service.
//
// The service is a thin wrapper around an ocifetch.Fetcher: it parses
// references, authorises registry bypass, and adapts the gRPC stream. The
// same server type runs on the app, where the Fetcher reads registries and
// writes the remote cache, and on the cache proxy, where the Fetcher reads
// the app's OCIFetcher service and writes the proxy's local cache.
package ocifetcher

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetch"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var enabled = flag.Bool("ocifetcher.enabled", false, "Whether to enable the OCI fetcher service.")

type ociFetcherServer struct {
	fetcher *ocifetch.Fetcher
}

// NewServer wraps fetcher as an OCIFetcherServer.
func NewServer(fetcher *ocifetch.Fetcher) (ofpb.OCIFetcherServer, error) {
	if fetcher == nil {
		return nil, status.FailedPreconditionError("OCIFetcherServer requires a non-nil fetcher")
	}
	return &ociFetcherServer{fetcher: fetcher}, nil
}

// NewAppServer returns the app's OCIFetcher service: container registries
// upstream, the remote cache (ActionCache and ByteStream) as the store.
//
// It is preferred to construct only one server per process, so that there is
// only one puller cache.
func NewAppServer(bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient) (ofpb.OCIFetcherServer, error) {
	if bsClient == nil {
		return nil, status.FailedPreconditionError("OCIFetcherServer requires a non-nil byte stream client")
	}
	if acClient == nil {
		return nil, status.FailedPreconditionError("OCIFetcherServer requires a non-nil action cache client")
	}
	upstream, err := ocifetch.NewRegistryUpstream("oci_fetcher")
	if err != nil {
		return nil, err
	}
	store, err := ocifetch.NewCacheStore(bsClient, acClient)
	if err != nil {
		return nil, err
	}
	fetcher, err := ocifetch.New(upstream, store)
	if err != nil {
		return nil, err
	}
	return NewServer(fetcher)
}

// NewProxyServer returns the cache proxy's OCIFetcher service: the app's
// OCIFetcher service upstream, the proxy's local ByteStream server as a
// blob-only store. Manifests and blob metadata pass through to the app.
func NewProxyServer(remote ofpb.OCIFetcherClient, localBSClient bspb.ByteStreamClient) (ofpb.OCIFetcherServer, error) {
	if remote == nil {
		return nil, status.FailedPreconditionError("An OCIFetcherClient is required to enable the OCIFetcher proxy")
	}
	if localBSClient == nil {
		return nil, status.FailedPreconditionError("A LocalByteStreamClient is required to enable the OCIFetcher proxy")
	}
	upstream, err := ocifetch.NewRemoteFetcherUpstream(remote)
	if err != nil {
		return nil, err
	}
	store, err := ocifetch.NewLocalBlobStore(localBSClient)
	if err != nil {
		return nil, err
	}
	fetcher, err := ocifetch.New(upstream, store)
	if err != nil {
		return nil, err
	}
	return NewServer(fetcher)
}

// RegisterServer registers the app's OCIFetcher service if enabled.
func RegisterServer(env *real_environment.RealEnv) error {
	if !*enabled {
		return nil
	}
	server, err := NewAppServer(env.GetByteStreamClient(), env.GetActionCacheClient())
	if err != nil {
		return err
	}
	env.SetOCIFetcherServer(server)
	return nil
}

// RegisterProxyServer registers the cache proxy's OCIFetcher service.
func RegisterProxyServer(env *real_environment.RealEnv) error {
	server, err := NewProxyServer(env.GetOCIFetcherClient(), env.GetLocalByteStreamClient())
	if err != nil {
		return status.InternalErrorf("Error initializing OCIFetcher proxy: %s", err)
	}
	env.SetOCIFetcherServer(server)
	return nil
}

// FetchBlob streams an OCI blob from the store if present, otherwise from
// upstream, writing it to the store at the same time. Concurrent requests
// for the same blob share one upstream fetch. The optional size and
// media_type fields let a caller that has the manifest descriptor skip the
// store's metadata lookup; they are hints for addressing the store only.
//
// Requests may have a bypass_registry flag set. Server admins can bypass the
// registry: the blob is streamed from the store if present, and FetchBlob
// will not fall back to the remote registry.
func (s *ociFetcherServer) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	ctx := stream.Context()
	if err := validateBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return err
	}
	digestRef, err := parseBlobDigestRef(req.GetRef())
	if err != nil {
		return err
	}
	_, err = s.fetcher.FetchBlob(ctx, &grpcStreamWriter{stream: stream}, digestRef, req.GetCredentials(), ocifetch.Options{
		BypassRegistry: req.GetBypassRegistry(),
		SizeBytes:      req.GetSize(),
		MediaType:      req.GetMediaType(),
	})
	return err
}

// FetchBlobMetadata returns OCI blob metadata (size, media type) from the
// store when the caller has recently proven access, falling back to upstream.
//
// Server admins can bypass the registry: the metadata is served from the
// store if present, and FetchBlobMetadata will not fall back to the registry.
func (s *ociFetcherServer) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	if err := validateBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return nil, err
	}
	digestRef, err := parseBlobDigestRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	desc, err := s.fetcher.FetchBlobMetadata(ctx, digestRef, req.GetCredentials(), ocifetch.Options{
		BypassRegistry: req.GetBypassRegistry(),
	})
	if err != nil {
		return nil, err
	}
	return &ofpb.FetchBlobMetadataResponse{
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
	}, nil
}

// FetchManifest returns an OCI manifest from the store if present, falling
// back to upstream and writing the manifest to the store.
//
// Server admins can bypass the registry: the manifest is served from the
// store if present, and FetchManifest will not fall back to the registry.
func (s *ociFetcherServer) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	if err := validateBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return nil, err
	}
	imageRef, err := parseManifestRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	desc, raw, err := s.fetcher.FetchManifest(ctx, imageRef, req.GetCredentials(), ocifetch.Options{
		BypassRegistry: req.GetBypassRegistry(),
	})
	if err != nil {
		return nil, err
	}
	return &ofpb.FetchManifestResponse{
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
		Manifest:  raw,
	}, nil
}

// FetchManifestMetadata fetches metadata (digest, size, media type) for an OCI
// manifest from upstream. It never consults the store, so callers may rely on
// a successful response as an indication that the credentials grant access to
// the image. Bypassing the registry is not possible.
func (s *ociFetcherServer) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	if err := validateUnsupportedBypassRegistry(ctx, req.GetBypassRegistry()); err != nil {
		return nil, err
	}
	imageRef, err := parseManifestRef(req.GetRef())
	if err != nil {
		return nil, err
	}
	desc, err := s.fetcher.FetchManifestMetadata(ctx, imageRef, req.GetCredentials(), ocifetch.Options{})
	if err != nil {
		return nil, err
	}
	return &ofpb.FetchManifestMetadataResponse{
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
		MediaType: string(desc.MediaType),
	}, nil
}

// validateBypassRegistry checks if bypass_registry is enabled and if so,
// verifies the caller has server admin permissions. Returns an error if
// bypass_registry is true but the caller is not a server admin.
func validateBypassRegistry(ctx context.Context, bypassRegistry bool) error {
	if !bypassRegistry {
		return nil
	}
	if err := claims.AuthorizeServerAdmin(ctx); err != nil {
		return status.PermissionDeniedErrorf("not authorized to bypass registry: %s", err)
	}
	return nil
}

// validateUnsupportedBypassRegistry is used by FetchManifestMetadata which does not support
// bypass_registry at all (it always needs registry access for credential validation).
func validateUnsupportedBypassRegistry(ctx context.Context, bypassRegistry bool) error {
	if !bypassRegistry {
		return nil
	}
	if err := claims.AuthorizeServerAdmin(ctx); err != nil {
		return status.PermissionDeniedErrorf("authorize bypass_registry: %s", err)
	}
	return status.NotFoundError("bypass_registry is not yet supported")
}

func parseBlobDigestRef(ref string) (ctrname.Digest, error) {
	blobRef, err := ctrname.ParseReference(ref)
	if err != nil {
		return ctrname.Digest{}, status.InvalidArgumentErrorf("invalid blob reference %q: %s", ref, err)
	}
	digestRef, ok := blobRef.(ctrname.Digest)
	if !ok {
		return ctrname.Digest{}, status.InvalidArgumentErrorf("blob reference must be a digest reference (e.g., repo@sha256:...), got %q", ref)
	}
	if _, err := ctr.NewHash(digestRef.DigestStr()); err != nil {
		return ctrname.Digest{}, status.InvalidArgumentErrorf("invalid digest format %q: %s", digestRef.DigestStr(), err)
	}
	return digestRef, nil
}

func parseManifestRef(ref string) (ctrname.Reference, error) {
	imageRef, err := ctrname.ParseReference(ref)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image reference %q: %s", ref, err)
	}
	return imageRef, nil
}

type grpcStreamWriter struct {
	stream ofpb.OCIFetcher_FetchBlobServer
}

func (w *grpcStreamWriter) Write(p []byte) (int, error) {
	if err := w.stream.Send(&ofpb.FetchBlobResponse{Data: p}); err != nil {
		return 0, status.WrapError(err, "send")
	}
	return len(p), nil
}
