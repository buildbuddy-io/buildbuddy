package oci_fetch_server_proxy

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/ocirefactor/fetch"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"

	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/metadata"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// Max chunk size for streaming blob data (1MB)
	maxChunkSize = 1024 * 1024
)

type OCIFetchServerProxy struct {
	env     environment.Env
	fetcher fetch.Fetcher

	// Singleflight groups to deduplicate concurrent requests
	manifestGroup   singleflight.Group[string, []byte]
	blobMetaGroup   singleflight.Group[string, *blobMetadata]
	blobStreamGroup singleflight.Group[string, *repb.Digest]
}

type blobMetadata struct {
	sizeBytes   int64
	contentType string
}

// credKey returns a key for the given credentials to use in singleflight.
// This prevents authorization leakage where one user could receive data
// fetched with another user's credentials.
func credKey(creds *rgpb.Credentials) string {
	if creds == nil || (creds.GetUsername() == "" && creds.GetPassword() == "") {
		return "anon"
	}
	// Hash to avoid credential leakage in logs
	return hash.Strings(creds.GetUsername(), creds.GetPassword())
}

// parseDigestFromBlobRef extracts the digest from an OCI blob reference.
// Blob refs are in the format: registry.example.com/repo@sha256:hash
// Returns the digest in the format: sha256:hash
func parseDigestFromBlobRef(blobRef string) (string, error) {
	parts := strings.Split(blobRef, "@")
	if len(parts) != 2 {
		return "", status.InvalidArgumentErrorf("invalid blob ref format: %s", blobRef)
	}
	return parts[1], nil
}

// ociStreamAdapter adapts OCI stream to ByteStream read server interface
type ociStreamAdapter struct {
	stream ocipb.OCIFetchService_FetchBlobServer
}

func (a *ociStreamAdapter) Send(rsp *bspb.ReadResponse) error {
	return a.stream.Send(&ocipb.FetchBlobResponse{
		Data: rsp.Data,
	})
}

func (a *ociStreamAdapter) Context() context.Context {
	return a.stream.Context()
}

func (a *ociStreamAdapter) SetHeader(metadata.MD) error {
	return nil
}

func (a *ociStreamAdapter) SendHeader(metadata.MD) error {
	return nil
}

func (a *ociStreamAdapter) SetTrailer(metadata.MD) {
}

func (a *ociStreamAdapter) RecvMsg(m interface{}) error {
	return nil
}

func (a *ociStreamAdapter) SendMsg(m interface{}) error {
	return nil
}

func Register(env *real_environment.RealEnv) error {
	proxy, err := New(env)
	if err != nil {
		return status.InternalErrorf("Error initializing OCIFetchServerProxy: %s", err)
	}
	env.SetOCIFetchServer(proxy)
	return nil
}

func New(env environment.Env) (*OCIFetchServerProxy, error) {
	// Read OCI cache configuration flags
	// These flags control whether OCI content is cached and what secret is used
	cacheSecret := oci.GetCacheSecret()
	useCachePercent := oci.GetUseCachePercent()

	// Create the appropriate fetcher based on cache configuration
	var f fetch.Fetcher

	if useCachePercent == 0 {
		// No caching - use RegistryFetcher only
		// This respects the executor's cache configuration
		f = fetch.NewRegistryFetcher(nil)
	} else {
		// Caching enabled - use CachingFetcher with the same secret as executors
		// This ensures cache keys match between proxy and executors
		acClient := env.GetActionCacheClient()
		if acClient == nil {
			return nil, fmt.Errorf("An ActionCacheClient is required to enable OCIFetchServerProxy with caching")
		}
		bsClient := env.GetByteStreamClient()
		if bsClient == nil {
			return nil, fmt.Errorf("A ByteStreamClient is required to enable OCIFetchServerProxy with caching")
		}

		// Use the cache proxy's local cache as well as the remote app cache
		f = fetch.NewCachingFetcher(acClient, bsClient, nil, cacheSecret)
	}

	return &OCIFetchServerProxy{
		env:     env,
		fetcher: f,
	}, nil
}

func (s *OCIFetchServerProxy) FetchManifest(ctx context.Context, req *ocipb.FetchManifestRequest) (*ocipb.FetchManifestResponse, error) {
	if req.GetImageRef() == "" {
		return nil, status.InvalidArgumentError("image_ref is required")
	}

	// Use singleflight to deduplicate concurrent requests for the same manifest
	// Include credentials in the key to prevent authorization leakage
	key := fmt.Sprintf("%s:%s", req.GetImageRef(), credKey(req.GetCredentials()))
	manifest, _, err := s.manifestGroup.Do(ctx, key, func(ctx context.Context) ([]byte, error) {
		var platform *repb.Platform
		if req.GetPlatform() != nil {
			platform = req.GetPlatform()
		}

		var creds *rgpb.Credentials
		if req.GetCredentials() != nil {
			creds = req.GetCredentials()
		}

		return s.fetcher.FetchManifest(ctx, req.GetImageRef(), platform, creds)
	})

	if err != nil {
		log.CtxWarningf(ctx, "Error fetching manifest for %s: %s", req.GetImageRef(), err)
		return nil, err
	}

	return &ocipb.FetchManifestResponse{
		Manifest: manifest,
	}, nil
}

// writeBlobToCAS writes blob data from a reader to the CAS (Content Addressable Storage).
// This ensures the blob is available for streaming to all concurrent requesters.
func (s *OCIFetchServerProxy) writeBlobToCAS(ctx context.Context, d *repb.Digest, reader io.Reader) error {
	cache := s.env.GetCache()
	if cache == nil {
		return status.UnavailableError("cache not available for blob storage")
	}

	// Create CAS resource name for the blob
	rn := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)

	// Get a writer from the cache
	writer, err := cache.Writer(ctx, rn.ToProto())
	if err != nil {
		return err
	}

	// Stream data from reader to cache writer
	_, err = io.Copy(writer, reader)
	if err != nil {
		writer.Close()
		return status.InternalErrorf("failed to write blob to CAS: %s", err)
	}

	// Commit the write to CAS
	if err := writer.Commit(); err != nil {
		return status.InternalErrorf("failed to commit blob to CAS: %s", err)
	}

	return nil
}

func (s *OCIFetchServerProxy) FetchBlob(req *ocipb.FetchBlobRequest, stream ocipb.OCIFetchService_FetchBlobServer) error {
	ctx := stream.Context()

	if req.GetBlobRef() == "" {
		return status.InvalidArgumentError("blob_ref is required")
	}

	// First, get blob metadata to determine the size
	// This is needed to construct the complete Digest
	metaReq := &ocipb.FetchBlobMetadataRequest{
		BlobRef:     req.GetBlobRef(),
		Credentials: req.GetCredentials(),
	}
	metaResp, err := s.FetchBlobMetadata(ctx, metaReq)
	if err != nil {
		return err
	}

	// Parse digest from blob ref (format: registry.example.com/repo@sha256:hash)
	digestStr, err := parseDigestFromBlobRef(req.GetBlobRef())
	if err != nil {
		return err
	}

	// Extract hash (remove "sha256:" or other algorithm prefix)
	hashStr := strings.TrimPrefix(digestStr, "sha256:")
	hashStr = strings.TrimPrefix(hashStr, "sha1:")
	hashStr = strings.TrimPrefix(hashStr, "blake3:")

	// Construct complete Digest with hash and size
	d := &repb.Digest{
		Hash:      hashStr,
		SizeBytes: metaResp.GetSizeBytes(),
	}

	// Use singleflight to coordinate writing the blob to CAS
	// Include credentials in the key to prevent authorization leakage
	// All requests (first + concurrent) wait for the write to complete, then stream from CAS
	key := fmt.Sprintf("%s:%s", req.GetBlobRef(), credKey(req.GetCredentials()))
	_, _, err = s.blobStreamGroup.Do(ctx, key, func(ctx context.Context) (*repb.Digest, error) {
		var creds *rgpb.Credentials
		if req.GetCredentials() != nil {
			creds = req.GetCredentials()
		}

		// Fetch blob from registry (or cache if using CachingFetcher)
		rc, err := s.fetcher.FetchBlob(ctx, req.GetBlobRef(), creds)
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		// Write blob to CAS
		if err := s.writeBlobToCAS(ctx, d, rc); err != nil {
			return nil, err
		}

		return d, nil
	})

	if err != nil {
		log.CtxWarningf(ctx, "Error fetching blob for %s: %s", req.GetBlobRef(), err)
		return err
	}

	// Stream from CAS to all clients (first + concurrent requesters)
	// Create CAS resource name for reading
	casRN := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)
	adapter := &ociStreamAdapter{stream: stream}
	bsServer := s.env.GetLocalByteStreamServer()
	if bsServer == nil {
		return status.UnavailableError("local ByteStreamServer not available")
	}

	// Read entire blob (offset=0, limit=0 means read all)
	return bsServer.ReadCASResource(ctx, casRN, 0, 0, adapter)
}

func (s *OCIFetchServerProxy) FetchBlobMetadata(ctx context.Context, req *ocipb.FetchBlobMetadataRequest) (*ocipb.FetchBlobMetadataResponse, error) {
	if req.GetBlobRef() == "" {
		return nil, status.InvalidArgumentError("blob_ref is required")
	}

	// Use singleflight to deduplicate concurrent requests for the same blob metadata
	// Include credentials in the key to prevent authorization leakage
	key := fmt.Sprintf("%s:%s", req.GetBlobRef(), credKey(req.GetCredentials()))
	meta, _, err := s.blobMetaGroup.Do(ctx, key, func(ctx context.Context) (*blobMetadata, error) {
		var creds *rgpb.Credentials
		if req.GetCredentials() != nil {
			creds = req.GetCredentials()
		}

		sizeBytes, contentType, err := s.fetcher.FetchBlobMetadata(ctx, req.GetBlobRef(), creds)
		if err != nil {
			return nil, err
		}

		return &blobMetadata{
			sizeBytes:   sizeBytes,
			contentType: contentType,
		}, nil
	})

	if err != nil {
		log.CtxWarningf(ctx, "Error fetching blob metadata for %s: %s", req.GetBlobRef(), err)
		return nil, err
	}

	return &ocipb.FetchBlobMetadataResponse{
		SizeBytes:   meta.sizeBytes,
		ContentType: meta.contentType,
	}, nil
}
