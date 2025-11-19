package oci_fetch_server_proxy

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/ocirefactor/fetch"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// Max chunk size for streaming blob data (1MB)
	maxChunkSize = 1024 * 1024
)

type OCIFetchServerProxy struct {
	fetcher fetch.Fetcher

	// Singleflight groups to deduplicate concurrent requests
	manifestGroup  singleflight.Group[string, []byte]
	blobMetaGroup  singleflight.Group[string, *blobMetadata]
	blobStreamGroup singleflight.Group[string, []byte]
}

type blobMetadata struct {
	sizeBytes   int64
	contentType string
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
	// Check for required dependencies
	acClient := env.GetActionCacheClient()
	if acClient == nil {
		return nil, fmt.Errorf("An ActionCacheClient is required to enable OCIFetchServerProxy")
	}
	bsClient := env.GetByteStreamClient()
	if bsClient == nil {
		return nil, fmt.Errorf("A ByteStreamClient is required to enable OCIFetchServerProxy")
	}

	// Create a CachingFetcher that will use the cache proxy's local cache
	// as well as the remote app cache
	f := fetch.NewCachingFetcher(acClient, bsClient, nil, "")

	return &OCIFetchServerProxy{
		fetcher: f,
	}, nil
}

func (s *OCIFetchServerProxy) FetchManifest(ctx context.Context, req *ocipb.FetchManifestRequest) (*ocipb.FetchManifestResponse, error) {
	if req.GetImageRef() == "" {
		return nil, status.InvalidArgumentError("image_ref is required")
	}

	// Use singleflight to deduplicate concurrent requests for the same manifest
	manifest, _, err := s.manifestGroup.Do(ctx, req.GetImageRef(), func(ctx context.Context) ([]byte, error) {
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

func (s *OCIFetchServerProxy) FetchBlob(req *ocipb.FetchBlobRequest, stream ocipb.OCIFetchService_FetchBlobServer) error {
	ctx := stream.Context()

	if req.GetBlobRef() == "" {
		return status.InvalidArgumentError("blob_ref is required")
	}

	// Use singleflight to deduplicate concurrent requests for the same blob
	// Note: This loads the entire blob into memory, which may not be ideal for very large blobs.
	// An alternative would be to implement a more sophisticated caching mechanism.
	blobData, _, err := s.blobStreamGroup.Do(ctx, req.GetBlobRef(), func(ctx context.Context) ([]byte, error) {
		var creds *rgpb.Credentials
		if req.GetCredentials() != nil {
			creds = req.GetCredentials()
		}

		rc, err := s.fetcher.FetchBlob(ctx, req.GetBlobRef(), creds)
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		// Read the entire blob into memory
		// TODO(dan): Consider implementing a more efficient streaming approach
		// that doesn't require loading the entire blob into memory
		return io.ReadAll(rc)
	})

	if err != nil {
		log.CtxWarningf(ctx, "Error fetching blob for %s: %s", req.GetBlobRef(), err)
		return err
	}

	// Stream the blob data in chunks
	for offset := 0; offset < len(blobData); offset += maxChunkSize {
		end := offset + maxChunkSize
		if end > len(blobData) {
			end = len(blobData)
		}

		chunk := blobData[offset:end]
		if err := stream.Send(&ocipb.FetchBlobResponse{
			Data: chunk,
		}); err != nil {
			log.CtxWarningf(ctx, "Error streaming blob chunk for %s: %s", req.GetBlobRef(), err)
			return err
		}
	}

	return nil
}

func (s *OCIFetchServerProxy) FetchBlobMetadata(ctx context.Context, req *ocipb.FetchBlobMetadataRequest) (*ocipb.FetchBlobMetadataResponse, error) {
	if req.GetBlobRef() == "" {
		return nil, status.InvalidArgumentError("blob_ref is required")
	}

	// Use singleflight to deduplicate concurrent requests for the same blob metadata
	meta, _, err := s.blobMetaGroup.Do(ctx, req.GetBlobRef(), func(ctx context.Context) (*blobMetadata, error) {
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
