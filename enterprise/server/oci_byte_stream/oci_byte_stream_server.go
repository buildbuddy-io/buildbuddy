package oci_byte_stream

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	gcr "github.com/google/go-containerregistry/pkg/v1"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// gRPC headers for OCI metadata
	ociRegistryHeader    = "x-buildbuddy-oci-registry"
	ociRepositoryHeader  = "x-buildbuddy-oci-repository"
	ociUsernameHeader    = "x-buildbuddy-oci-username"
	ociPasswordHeader    = "x-buildbuddy-oci-password"
	ociDigestHeader      = "x-buildbuddy-oci-digest"
	ociMediaTypeHeader   = "x-buildbuddy-oci-mediatype"
	ociContentSizeHeader = "x-buildbuddy-oci-contentsize"
)

// OCIByteStreamServer is a read-only byte stream server that can fetch blobs
// from OCI registries and cache them in the underlying byte stream client.
type OCIByteStreamServer struct {
	env        environment.Env
	bsClient   bspb.ByteStreamClient
	acClient   repb.ActionCacheClient
	underlying interfaces.ByteStreamServer
}

// ociMetadata contains OCI-specific information extracted from gRPC headers
type ociMetadata struct {
	registry    string
	repository  string
	username    string
	password    string
	digest      string
	mediaType   string
	contentSize int64
}

func Register(env *real_environment.RealEnv) error {
	// This Register function is not used in tests, as tests set up the server manually.
	// It could be used in production code if needed.
	return nil
}

func NewOCIByteStreamServer(env environment.Env, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, underlying interfaces.ByteStreamServer) (*OCIByteStreamServer, error) {
	if bsClient == nil {
		return nil, status.FailedPreconditionError("A ByteStreamClient is required for OCIByteStreamServer")
	}
	if acClient == nil {
		return nil, status.FailedPreconditionError("An ActionCacheClient is required for OCIByteStreamServer")
	}
	if underlying == nil {
		return nil, status.FailedPreconditionError("An underlying ByteStreamServer is required for OCIByteStreamServer")
	}

	return &OCIByteStreamServer{
		env:        env,
		bsClient:   bsClient,
		acClient:   acClient,
		underlying: underlying,
	}, nil
}

// extractOCIMetadata extracts OCI-specific headers from the gRPC context
func extractOCIMetadata(ctx context.Context) (*ociMetadata, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, nil // No metadata, not an OCI request
	}

	// Check if this is an OCI request by looking for the registry header
	registryValues := md.Get(ociRegistryHeader)
	if len(registryValues) == 0 {
		return nil, nil // Not an OCI request
	}

	metadata := &ociMetadata{
		registry: registryValues[0],
	}

	if repoValues := md.Get(ociRepositoryHeader); len(repoValues) > 0 {
		metadata.repository = repoValues[0]
	}
	if usernameValues := md.Get(ociUsernameHeader); len(usernameValues) > 0 {
		metadata.username = usernameValues[0]
	}
	if passwordValues := md.Get(ociPasswordHeader); len(passwordValues) > 0 {
		metadata.password = passwordValues[0]
	}
	if digestValues := md.Get(ociDigestHeader); len(digestValues) > 0 {
		metadata.digest = digestValues[0]
	}
	if mediaTypeValues := md.Get(ociMediaTypeHeader); len(mediaTypeValues) > 0 {
		metadata.mediaType = mediaTypeValues[0]
	}
	// Note: contentSize is set from the header in ociMetadata if provided,
	// but we'll get it from the layer if not available

	return metadata, nil
}

func (s *OCIByteStreamServer) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	rn, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	return s.ReadCASResource(stream.Context(), rn, req.GetReadOffset(), req.GetReadLimit(), stream)
}

func (s *OCIByteStreamServer) ReadCASResource(ctx context.Context, r *digest.CASResourceName, offset, limit int64, stream bspb.ByteStream_ReadServer) error {
	// Extract OCI metadata from headers
	ociMeta, err := extractOCIMetadata(ctx)
	if err != nil {
		return err
	}

	// If this is not an OCI request, delegate to the underlying server
	if ociMeta == nil {
		return s.underlying.ReadCASResource(ctx, r, offset, limit, stream)
	}

	// Try to fetch from cache first via underlying server
	err = s.underlying.ReadCASResource(ctx, r, offset, limit, stream)
	if err == nil {
		// Cache hit, return successfully
		return nil
	}

	// Check if error is a cache miss (not found)
	if !status.IsNotFoundError(err) {
		// Some other error, return it
		return err
	}

	// Cache miss - fetch from OCI registry and write through to cache
	return s.fetchFromOCIAndCache(ctx, r, ociMeta, offset, limit, stream)
}

func (s *OCIByteStreamServer) fetchFromOCIAndCache(ctx context.Context, r *digest.CASResourceName, ociMeta *ociMetadata, offset, limit int64, stream bspb.ByteStream_ReadServer) error {
	// Construct the repository reference
	if ociMeta.repository == "" {
		return status.InvalidArgumentError("OCI repository must be specified in headers")
	}

	repoStr := fmt.Sprintf("%s/%s", ociMeta.registry, ociMeta.repository)
	repo, err := gcrname.NewRepository(repoStr)
	if err != nil {
		return status.InvalidArgumentErrorf("Invalid OCI repository %q: %s", repoStr, err)
	}

	// Create authenticator for the registry
	var auth authn.Authenticator
	if ociMeta.username != "" && ociMeta.password != "" {
		auth = &authn.Basic{
			Username: ociMeta.username,
			Password: ociMeta.password,
		}
	} else {
		auth = authn.Anonymous
	}

	// Parse the digest from the resource name or OCI metadata
	digestStr := r.GetDigest().GetHash()
	if ociMeta.digest != "" {
		digestStr = ociMeta.digest
	}

	hash, err := gcr.NewHash(digestStr)
	if err != nil {
		return status.InvalidArgumentErrorf("Invalid digest %q: %s", digestStr, err)
	}

	// Create a reference to the blob
	ref := repo.Digest(hash.String())

	// Fetch the layer from the remote registry
	layer, err := remote.Layer(ref, remote.WithAuth(auth))
	if err != nil {
		return status.NotFoundErrorf("Failed to fetch layer from OCI registry: %s", err)
	}

	// Get the compressed layer contents
	upstream, err := layer.Compressed()
	if err != nil {
		return status.InternalErrorf("Failed to get compressed layer: %s", err)
	}
	defer upstream.Close()

	// Get media type and content length for caching
	mediaType := ociMeta.mediaType
	if mediaType == "" {
		mt, err := layer.MediaType()
		if err != nil {
			log.CtxWarningf(ctx, "Could not get media type for layer: %s", err)
			mediaType = ""
		} else {
			mediaType = string(mt)
		}
	}

	contentLength := ociMeta.contentSize
	if contentLength == 0 {
		size, err := layer.Size()
		if err != nil {
			log.CtxWarningf(ctx, "Could not get size for layer: %s", err)
		} else {
			contentLength = size
		}
	}

	// Wrap in a read-through cacher that writes to the byte stream while reading
	rc, err := ocicache.NewBlobReadThroughCacher(
		ctx,
		upstream,
		s.bsClient,
		s.acClient,
		repo,
		hash,
		mediaType,
		contentLength,
	)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to create read-through cacher, streaming without caching: %s", err)
		rc = upstream
	}
	defer rc.Close()

	// Handle offset if specified
	if offset > 0 {
		_, err = io.CopyN(io.Discard, rc, offset)
		if err != nil {
			return status.InternalErrorf("Failed to skip to offset: %s", err)
		}
	}

	// Stream to the client
	buf := make([]byte, 256*1024) // 256KB buffer
	totalSent := int64(0)
	for {
		if limit > 0 && totalSent >= limit {
			break
		}

		n, err := rc.Read(buf)
		if n > 0 {
			toSend := n
			if limit > 0 && totalSent+int64(n) > limit {
				toSend = int(limit - totalSent)
			}

			if err := stream.Send(&bspb.ReadResponse{Data: buf[:toSend]}); err != nil {
				return err
			}
			totalSent += int64(toSend)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return status.InternalErrorf("Failed to read from layer: %s", err)
		}
	}

	return nil
}

// Write is not supported for the OCI byte stream server (read-only)
func (s *OCIByteStreamServer) Write(stream bspb.ByteStream_WriteServer) error {
	return status.UnimplementedError("OCI ByteStream server is read-only")
}

// QueryWriteStatus is not supported for the OCI byte stream server (read-only)
func (s *OCIByteStreamServer) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return nil, status.UnimplementedError("OCI ByteStream server is read-only")
}
