package fetch

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"google.golang.org/protobuf/types/known/anypb"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Instance name prefix for OCI cache entries
	actionResultInstanceName    = interfaces.OCIImageInstanceNamePrefix
	manifestContentInstanceName = interfaces.OCIImageInstanceNamePrefix + "_manifest_content_"

	// Cache digest function (always SHA256 for OCI)
	cacheDigestFunction = repb.DigestFunction_SHA256

	// Output file paths for blob metadata in ActionResult
	blobOutputFilePath         = "_bb_ociregistry_blob_"
	blobMetadataOutputFilePath = "_bb_ociregistry_blob_metadata_"
)

// Fetcher is an interface for fetching OCI blobs and manifests.
type Fetcher interface {
	// FetchBlob fetches a blob by OCI reference (e.g., gcr.io/foo/bar@sha256:xxx)
	// Returns an io.ReadCloser that yields compressed bytes (as stored in registry/CAS)
	FetchBlob(ctx context.Context, ref string, creds *rgpb.Credentials) (io.ReadCloser, error)

	// FetchBlobMetadata returns the size and media type for a blob
	FetchBlobMetadata(ctx context.Context, ref string, creds *rgpb.Credentials) (int64, string, error)

	// FetchManifest fetches raw manifest bytes for the given reference
	// Returns whatever manifest exists at the ref (image index or single manifest)
	// Platform is used for logging/potential future use, but caller is responsible
	// for platform selection from image indices
	FetchManifest(ctx context.Context, ref string, platform *repb.Platform, creds *rgpb.Credentials) ([]byte, error)
}

// parseDigestRef parses an OCI reference that must contain a digest.
// Returns an error if the reference doesn't contain a digest.
func parseDigestRef(ref string) (ctrname.Digest, error) {
	digestRef, err := ctrname.NewDigest(ref)
	if err != nil {
		return ctrname.Digest{}, status.InvalidArgumentErrorf("ref must contain digest, got %q: %s", ref, err)
	}
	return digestRef, nil
}

// convertCredentials converts proto credentials to an authn.Authenticator
func convertCredentials(creds *rgpb.Credentials) authn.Authenticator {
	if creds == nil || (creds.GetUsername() == "" && creds.GetPassword() == "") {
		return authn.Anonymous
	}
	return &authn.Basic{
		Username: creds.GetUsername(),
		Password: creds.GetPassword(),
	}
}

// manifestACKey generates an AC key for a manifest, following the pattern from ocicache.
// Uses hash of (registry, repo, MANIFEST, algorithm, hex, secret) as the digest.
func manifestACKey(repo ctrname.Repository, refhash ctr.Hash, secret string) (*digest.ACResourceName, error) {
	s := hash.Strings(
		repo.RegistryStr(),
		repo.RepositoryStr(),
		ocipb.OCIResourceType_MANIFEST.String(),
		refhash.Algorithm,
		refhash.Hex,
		secret,
	)
	arDigest, err := digest.Compute(bytes.NewBufferString(s), cacheDigestFunction)
	if err != nil {
		return nil, err
	}
	return digest.NewACResourceName(
		arDigest,
		manifestContentInstanceName,
		cacheDigestFunction,
	), nil
}

// blobMetadataACKey generates an AC key for blob metadata.
// Uses proto-marshaled OCIActionResultKey as the digest input.
func blobMetadataACKey(repo ctrname.Repository, hash ctr.Hash) (*digest.ACResourceName, error) {
	arKey := &ocipb.OCIActionResultKey{
		Registry:      repo.RegistryStr(),
		Repository:    repo.RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return nil, err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		return nil, err
	}
	return digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		cacheDigestFunction,
	), nil
}

// blobCASResourceName generates a CAS resource name for a blob.
// Blobs are stored with ZSTD compression in CAS (matching ocicache behavior).
func blobCASResourceName(hash ctr.Hash, contentLength int64) *digest.CASResourceName {
	blobDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	rn := digest.NewCASResourceName(
		blobDigest,
		"",
		cacheDigestFunction,
	)
	rn.SetCompressor(repb.Compressor_ZSTD)
	return rn
}

// CacheOnlyFetcher fetches OCI artifacts from BuildBuddy's Action Cache and CAS.
type CacheOnlyFetcher struct {
	acClient repb.ActionCacheClient
	bsClient bspb.ByteStreamClient
	secret   string // Secret to add to cache keys
}

// NewCacheOnlyFetcher creates a new CacheOnlyFetcher.
func NewCacheOnlyFetcher(acClient repb.ActionCacheClient, bsClient bspb.ByteStreamClient, secret string) *CacheOnlyFetcher {
	return &CacheOnlyFetcher{
		acClient: acClient,
		bsClient: bsClient,
		secret:   secret,
	}
}

// FetchManifest fetches a manifest from the Action Cache.
func (f *CacheOnlyFetcher) FetchManifest(ctx context.Context, ref string, platform *repb.Platform, creds *rgpb.Credentials) ([]byte, error) {
	digestRef, err := parseDigestRef(ref)
	if err != nil {
		return nil, err
	}

	repo := digestRef.Context()
	hash, err := ctr.NewHash(digestRef.DigestStr())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid digest in ref %q: %s", ref, err)
	}

	arRN, err := manifestACKey(repo, hash, f.secret)
	if err != nil {
		log.CtxWarningf(ctx, "Error creating key for manifest %s@%s: %s", repo, hash, err)
		return nil, err
	}

	ar, err := cachetools.GetActionResult(ctx, f.acClient, arRN)
	if err != nil {
		if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Error getting action result for manifest %s@%s: %s", repo, hash, err)
		}
		return nil, err
	}

	meta := ar.GetExecutionMetadata()
	if meta == nil {
		log.CtxWarningf(ctx, "Missing execution metadata for manifest %s@%s", repo, hash)
		return nil, status.InternalErrorf("missing execution metadata for manifest in %q", repo)
	}

	aux := meta.GetAuxiliaryMetadata()
	if aux == nil || len(aux) != 1 {
		log.CtxWarningf(ctx, "Missing auxiliary metadata for manifest %s@%s", repo, hash)
		return nil, status.InternalErrorf("missing auxiliary metadata for manifest %s@%s", repo, hash)
	}

	var mc ocipb.OCIManifestContent
	if err := aux[0].UnmarshalTo(&mc); err != nil {
		log.CtxWarningf(ctx, "Error unmarshalling manifest content %s@%s: %s", repo, hash, err)
		return nil, status.InternalErrorf("could not unmarshal metadata for manifest %s@%s: %s", repo, hash, err)
	}

	log.CtxInfof(ctx, "Cache hit for manifest %s@%s", repo, hash)
	return mc.GetRaw(), nil
}

// FetchBlobMetadata fetches blob metadata (size and media type) from the Action Cache.
func (f *CacheOnlyFetcher) FetchBlobMetadata(ctx context.Context, ref string, creds *rgpb.Credentials) (int64, string, error) {
	digestRef, err := parseDigestRef(ref)
	if err != nil {
		return 0, "", err
	}

	repo := digestRef.Context()
	hash, err := ctr.NewHash(digestRef.DigestStr())
	if err != nil {
		return 0, "", status.InvalidArgumentErrorf("invalid digest in ref %q: %s", ref, err)
	}

	arRN, err := blobMetadataACKey(repo, hash)
	if err != nil {
		return 0, "", err
	}

	ar, err := cachetools.GetActionResult(ctx, f.acClient, arRN)
	if err != nil {
		return 0, "", err
	}

	// Find the blob metadata digest in the output files
	var blobMetadataCASDigest *repb.Digest
	for _, outputFile := range ar.GetOutputFiles() {
		if outputFile.GetPath() == blobMetadataOutputFilePath {
			blobMetadataCASDigest = outputFile.GetDigest()
			break
		}
	}

	if blobMetadataCASDigest == nil {
		return 0, "", status.NotFoundErrorf("missing blob metadata digest for %s", ref)
	}

	blobMetadataRN := digest.NewCASResourceName(
		blobMetadataCASDigest,
		"",
		cacheDigestFunction,
	)

	var blobMetadata ocipb.OCIBlobMetadata
	if err := cachetools.GetBlobAsProto(ctx, f.bsClient, blobMetadataRN, &blobMetadata); err != nil {
		return 0, "", err
	}

	log.CtxDebugf(ctx, "Cache hit for blob metadata %s", ref)
	return blobMetadata.GetContentLength(), blobMetadata.GetContentType(), nil
}

// FetchBlob fetches a blob from the CAS.
// Returns a ReadCloser that streams the compressed blob data.
func (f *CacheOnlyFetcher) FetchBlob(ctx context.Context, ref string, creds *rgpb.Credentials) (io.ReadCloser, error) {
	// First get the metadata to find the content length
	// This acts as an existence check - if metadata is not in AC, blob is not cached
	contentLength, _, err := f.FetchBlobMetadata(ctx, ref, creds)
	if err != nil {
		return nil, err
	}

	digestRef, err := parseDigestRef(ref)
	if err != nil {
		return nil, err
	}

	hash, err := ctr.NewHash(digestRef.DigestStr())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid digest in ref %q: %s", ref, err)
	}

	blobRN := blobCASResourceName(hash, contentLength)

	// Stream blob from CAS using a pipe (matches oci/ocicache pattern)
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		if err := cachetools.GetBlob(ctx, f.bsClient, blobRN, pw); err != nil {
			pw.CloseWithError(err)
			return
		}
	}()

	log.CtxDebugf(ctx, "Cache hit for blob %s", ref)
	return pr, nil
}

// Cache write helpers (used by CachingFetcher)

// writeManifestToCache writes a manifest to the Action Cache.
func writeManifestToCache(ctx context.Context, acClient repb.ActionCacheClient, ref string, manifestBytes []byte, contentType string, secret string) error {
	digestRef, err := parseDigestRef(ref)
	if err != nil {
		return err
	}

	repo := digestRef.Context()
	refhash, err := ctr.NewHash(digestRef.DigestStr())
	if err != nil {
		return status.InvalidArgumentErrorf("invalid digest in ref %q: %s", ref, err)
	}

	// Build cache key
	s := hash.Strings(
		repo.RegistryStr(),
		repo.RepositoryStr(),
		ocipb.OCIResourceType_MANIFEST.String(),
		refhash.Algorithm,
		refhash.Hex,
		secret,
	)
	arDigest, err := digest.Compute(bytes.NewBufferString(s), cacheDigestFunction)
	if err != nil {
		return err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		manifestContentInstanceName,
		cacheDigestFunction,
	)

	// Create manifest content proto
	m := &ocipb.OCIManifestContent{
		Raw:         manifestBytes,
		ContentType: contentType,
	}
	any, err := anypb.New(m)
	if err != nil {
		return err
	}

	// Upload to AC
	ar := &repb.ActionResult{
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{any},
		},
	}
	return cachetools.UploadActionResult(ctx, acClient, arRN, ar)
}

// writeBlobMetadataToCache writes blob metadata to the Action Cache.
func writeBlobMetadataToCache(ctx context.Context, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref string, contentType string, contentLength int64, secret string) error {
	digestRef, err := parseDigestRef(ref)
	if err != nil {
		return err
	}

	repo := digestRef.Context()
	refhash, err := ctr.NewHash(digestRef.DigestStr())
	if err != nil {
		return status.InvalidArgumentErrorf("invalid digest in ref %q: %s", ref, err)
	}

	// Write blob metadata to CAS
	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	blobMetadataCASDigest, err := cachetools.UploadProto(ctx, bsClient, "", cacheDigestFunction, blobMetadata)
	if err != nil {
		return err
	}

	// Create ActionResult with only the metadata reference
	// Note: We don't include a reference to the blob itself because CacheOnlyFetcher
	// only needs the metadata to determine the content length, then reads from CAS directly.
	arKey := &ocipb.OCIActionResultKey{
		Registry:      repo.RegistryStr(),
		Repository:    repo.RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: refhash.Algorithm,
		HashHex:       refhash.Hex,
	}

	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{
				Path:   blobMetadataOutputFilePath,
				Digest: blobMetadataCASDigest,
			},
		},
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		return err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		cacheDigestFunction,
	)
	return cachetools.UploadActionResult(ctx, acClient, arRN, ar)
}

// blobWriteThroughCacher wraps an io.ReadCloser and writes blob data to CAS as it's read.
type blobWriteThroughCacher struct {
	rc            io.ReadCloser                   // upstream reader (from registry)
	uploader      interfaces.CommittedWriteCloser // writes to CAS
	ctx           context.Context
	bsClient      bspb.ByteStreamClient
	acClient      repb.ActionCacheClient
	ref           string
	contentType   string
	contentLength int64
	secret        string
	cacheErr      error // tracks caching errors (doesn't fail the read)
	eofReached    bool  // tracks if we've reached EOF and committed
}

// newBlobWriteThroughCacher creates a new write-through cacher for blobs.
func newBlobWriteThroughCacher(
	ctx context.Context,
	rc io.ReadCloser,
	bsClient bspb.ByteStreamClient,
	acClient repb.ActionCacheClient,
	ref string,
	contentType string,
	contentLength int64,
	secret string,
) (io.ReadCloser, error) {
	// Parse ref to get hash
	digestRef, err := parseDigestRef(ref)
	if err != nil {
		return nil, err
	}
	hash, err := ctr.NewHash(digestRef.DigestStr())
	if err != nil {
		return nil, err
	}

	// Create ByteStream uploader for the blob
	blobRN := blobCASResourceName(hash, contentLength)
	log.CtxInfof(ctx, "Creating write-through cacher for blob %s (hash: %s, size: %d)", ref, hash.Hex, contentLength)
	uploader, err := cachetools.NewUploadWriter(ctx, bsClient, blobRN)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to create UploadWriter for %s: %s", ref, err)
		return nil, err
	}

	log.CtxInfof(ctx, "Write-through cacher created successfully for %s", ref)
	return &blobWriteThroughCacher{
		rc:            rc,
		uploader:      uploader,
		ctx:           ctx,
		bsClient:      bsClient,
		acClient:      acClient,
		ref:           ref,
		contentType:   contentType,
		contentLength: contentLength,
		secret:        secret,
	}, nil
}

// Read reads from upstream and writes to cache.
func (r *blobWriteThroughCacher) Read(p []byte) (int, error) {
	n, readErr := r.rc.Read(p)

	// Write what we read to the uploader (if we haven't had a cache error)
	if n > 0 && r.cacheErr == nil {
		written, writeErr := r.uploader.Write(p[:n])
		if writeErr != nil {
			log.CtxWarningf(r.ctx, "Error writing to cache for %s: %s", r.ref, writeErr)
			r.cacheErr = writeErr
		} else if written != n {
			log.CtxWarningf(r.ctx, "Partial write to cache for %s: wrote %d of %d bytes", r.ref, written, n)
		}
	}

	// If we reached EOF and haven't committed yet, commit the uploader
	if readErr == io.EOF && !r.eofReached && r.cacheErr == nil {
		r.eofReached = true
		log.CtxInfof(r.ctx, "Committing blob upload for %s (expected length: %d)", r.ref, r.contentLength)
		if err := r.uploader.Commit(); err != nil {
			log.CtxWarningf(r.ctx, "Error committing blob to cache for %s: %s", r.ref, err)
			r.cacheErr = err
		} else {
			log.CtxInfof(r.ctx, "Successfully cached blob %s", r.ref)

			// Now cache the metadata (since blob is successfully in CAS)
			if err := writeBlobMetadataToCache(r.ctx, r.bsClient, r.acClient, r.ref, r.contentType, r.contentLength, r.secret); err != nil {
				log.CtxWarningf(r.ctx, "Error caching blob metadata for %s: %s", r.ref, err)
			} else {
				log.CtxDebugf(r.ctx, "Successfully cached blob metadata for %s", r.ref)
			}
		}
	}

	return n, readErr
}

// Close closes both the upstream reader and the uploader.
func (r *blobWriteThroughCacher) Close() error {
	// Close upstream first
	rcErr := r.rc.Close()

	// Close uploader
	uploaderErr := r.uploader.Close()

	// Return first non-nil error
	if rcErr != nil {
		return rcErr
	}
	return uploaderErr
}

// RegistryFetcher fetches OCI artifacts from upstream OCI registries.
type RegistryFetcher struct {
	// Optional transport for custom HTTP behavior (mirrors, etc.)
	transport http.RoundTripper
}

// NewRegistryFetcher creates a new RegistryFetcher.
func NewRegistryFetcher(transport http.RoundTripper) *RegistryFetcher {
	return &RegistryFetcher{
		transport: transport,
	}
}

// getRemoteOpts builds options for go-containerregistry remote operations.
func (f *RegistryFetcher) getRemoteOpts(ctx context.Context, platform *repb.Platform, creds *rgpb.Credentials) []remote.Option {
	opts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithAuth(convertCredentials(creds)),
	}

	if platform != nil {
		// Convert repb.Platform to ctr.Platform by extracting arch/os from properties
		// For now, we assume simple platform format. In production, you'd parse
		// platform.Properties to extract "OSFamily", "Arch", etc.
		opts = append(opts, remote.WithPlatform(ctr.Platform{
			Architecture: "amd64", // TODO: extract from platform.Properties
			OS:           "linux",
		}))
	}

	if f.transport != nil {
		opts = append(opts, remote.WithTransport(f.transport))
	}

	return opts
}

// FetchManifest fetches a manifest from an upstream OCI registry.
func (f *RegistryFetcher) FetchManifest(ctx context.Context, ref string, platform *repb.Platform, creds *rgpb.Credentials) ([]byte, error) {
	imageRef, err := ctrname.ParseReference(ref)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image ref %q: %s", ref, err)
	}

	log.CtxInfof(ctx, "Fetching manifest from registry for %s", imageRef)

	remoteOpts := f.getRemoteOpts(ctx, platform, creds)
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}

	desc, err := puller.Get(ctx, imageRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to retrieve manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from registry: %s", err)
	}

	manifestBytes, err := desc.RawManifest()
	if err != nil {
		return nil, status.InternalErrorf("could not get raw manifest bytes: %s", err)
	}

	log.CtxInfof(ctx, "Successfully fetched manifest from registry for %s (size: %d bytes)", imageRef, len(manifestBytes))
	return manifestBytes, nil
}

// FetchBlobMetadata fetches blob metadata from an upstream OCI registry using HEAD request.
func (f *RegistryFetcher) FetchBlobMetadata(ctx context.Context, ref string, creds *rgpb.Credentials) (int64, string, error) {
	digestRef, err := parseDigestRef(ref)
	if err != nil {
		return 0, "", err
	}

	log.CtxInfof(ctx, "Fetching blob metadata from registry for %s", digestRef)

	remoteOpts := f.getRemoteOpts(ctx, nil, creds)

	// Use go-containerregistry to get the layer
	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return 0, "", status.InternalErrorf("error creating puller: %s", err)
	}

	layer, err := puller.Layer(ctx, digestRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return 0, "", status.PermissionDeniedErrorf("not authorized to retrieve blob: %s", err)
		}
		return 0, "", status.UnavailableErrorf("could not retrieve blob metadata from registry: %s", err)
	}

	size, err := layer.Size()
	if err != nil {
		return 0, "", status.InternalErrorf("could not get blob size: %s", err)
	}

	mediaType, err := layer.MediaType()
	if err != nil {
		return 0, "", status.InternalErrorf("could not get blob media type: %s", err)
	}

	log.CtxInfof(ctx, "Successfully fetched blob metadata from registry for %s (size: %d)", digestRef, size)
	return size, string(mediaType), nil
}

// FetchBlob fetches a blob from an upstream OCI registry.
func (f *RegistryFetcher) FetchBlob(ctx context.Context, ref string, creds *rgpb.Credentials) (io.ReadCloser, error) {
	digestRef, err := parseDigestRef(ref)
	if err != nil {
		return nil, err
	}

	log.CtxInfof(ctx, "Fetching blob from registry for %s", digestRef)

	remoteOpts := f.getRemoteOpts(ctx, nil, creds)

	puller, err := remote.NewPuller(remoteOpts...)
	if err != nil {
		return nil, status.InternalErrorf("error creating puller: %s", err)
	}

	layer, err := puller.Layer(ctx, digestRef)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("not authorized to retrieve blob: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve blob from registry: %s", err)
	}

	// Get the compressed data (as stored in the registry)
	rc, err := layer.Compressed()
	if err != nil {
		return nil, status.InternalErrorf("could not get compressed blob data: %s", err)
	}

	log.CtxInfof(ctx, "Successfully fetched blob from registry for %s", digestRef)
	return rc, nil
}

// CachingFetcher is a composite fetcher that tries cache first, then registry,
// and writes through to cache on registry hits.
type CachingFetcher struct {
	cacheFetcher    *CacheOnlyFetcher
	registryFetcher *RegistryFetcher
	bsClient        bspb.ByteStreamClient
	acClient        repb.ActionCacheClient
	secret          string
}

// NewCachingFetcher creates a new CachingFetcher.
func NewCachingFetcher(
	acClient repb.ActionCacheClient,
	bsClient bspb.ByteStreamClient,
	transport http.RoundTripper,
	secret string,
) *CachingFetcher {
	return &CachingFetcher{
		cacheFetcher:    NewCacheOnlyFetcher(acClient, bsClient, secret),
		registryFetcher: NewRegistryFetcher(transport),
		bsClient:        bsClient,
		acClient:        acClient,
		secret:          secret,
	}
}

// FetchManifest tries cache first, then falls back to registry with write-through caching.
func (f *CachingFetcher) FetchManifest(ctx context.Context, ref string, platform *repb.Platform, creds *rgpb.Credentials) ([]byte, error) {
	// Try cache first
	manifestBytes, err := f.cacheFetcher.FetchManifest(ctx, ref, platform, creds)
	if err == nil {
		return manifestBytes, nil
	}

	// If not found in cache, try registry
	if !status.IsNotFoundError(err) {
		log.CtxWarningf(ctx, "Error fetching manifest from cache for %s: %s", ref, err)
	}

	manifestBytes, err = f.registryFetcher.FetchManifest(ctx, ref, platform, creds)
	if err != nil {
		return nil, err
	}

	// Get the media type from the manifest to store in cache
	// Parse it to determine content type
	digestRef, parseErr := parseDigestRef(ref)
	if parseErr != nil {
		return manifestBytes, nil // Return manifest even if we can't cache it
	}

	// Fetch the descriptor to get the content type
	remoteOpts := f.registryFetcher.getRemoteOpts(ctx, platform, creds)
	puller, err := remote.NewPuller(remoteOpts...)
	if err == nil {
		desc, err := puller.Get(ctx, digestRef)
		if err == nil {
			contentType := string(desc.MediaType)
			// Write manifest to cache (best effort - don't fail on cache errors)
			if err := writeManifestToCache(ctx, f.acClient, ref, manifestBytes, contentType, f.secret); err != nil {
				log.CtxWarningf(ctx, "Error writing manifest to cache for %s: %s", ref, err)
			} else {
				log.CtxInfof(ctx, "Successfully cached manifest %s", ref)
			}
		}
	}

	return manifestBytes, nil
}

// FetchBlobMetadata tries cache first, then falls back to registry (read-only, no caching).
// Metadata is only cached when the blob itself is cached via FetchBlob's write-through caching.
func (f *CachingFetcher) FetchBlobMetadata(ctx context.Context, ref string, creds *rgpb.Credentials) (int64, string, error) {
	// Try cache first
	contentLength, contentType, err := f.cacheFetcher.FetchBlobMetadata(ctx, ref, creds)
	if err == nil {
		return contentLength, contentType, nil
	}

	// If not found in cache, fetch from registry (but don't cache it)
	// Metadata will be cached when FetchBlob is called with write-through caching
	if !status.IsNotFoundError(err) {
		log.CtxWarningf(ctx, "Error fetching blob metadata from cache for %s: %s", ref, err)
	}

	return f.registryFetcher.FetchBlobMetadata(ctx, ref, creds)
}

// FetchBlob tries cache first, then falls back to registry with write-through caching.
func (f *CachingFetcher) FetchBlob(ctx context.Context, ref string, creds *rgpb.Credentials) (io.ReadCloser, error) {
	// Try to fetch blob from cache (cacheFetcher.FetchBlob checks metadata internally)
	rc, err := f.cacheFetcher.FetchBlob(ctx, ref, creds)
	if err == nil {
		return rc, nil
	}

	// If not found in cache, fetch metadata and blob from registry with write-through caching
	if !status.IsNotFoundError(err) {
		log.CtxWarningf(ctx, "Error fetching blob from cache for %s: %s", ref, err)
	}

	// Fetch metadata from registry (don't cache it - write-through cacher will cache it with blob)
	contentLength, contentType, metaErr := f.registryFetcher.FetchBlobMetadata(ctx, ref, creds)
	if metaErr != nil {
		return nil, metaErr
	}

	rc, err = f.registryFetcher.FetchBlob(ctx, ref, creds)
	if err != nil {
		return nil, err
	}

	// Wrap with write-through cacher (best effort - returns error only if wrapper creation fails)
	cachedRC, err := newBlobWriteThroughCacher(ctx, rc, f.bsClient, f.acClient, ref, contentType, contentLength, f.secret)
	if err != nil {
		log.CtxWarningf(ctx, "Error creating write-through cacher for %s: %s (returning uncached reader)", ref, err)
		return rc, nil // Return the raw reader if we can't create the cacher
	}

	return cachedRC, nil
}

// GRPCFetcher implements the Fetcher interface by calling a remote OCIFetchService.
// This is used by executors to fetch OCI content through cache proxy nodes.
type GRPCFetcher struct {
	client ocipb.OCIFetchServiceClient
}

// NewGRPCFetcher creates a new GRPCFetcher that calls the given OCIFetchService client.
func NewGRPCFetcher(client ocipb.OCIFetchServiceClient) *GRPCFetcher {
	return &GRPCFetcher{
		client: client,
	}
}

// FetchManifest fetches a manifest from the remote OCIFetchService.
func (f *GRPCFetcher) FetchManifest(ctx context.Context, ref string, platform *repb.Platform, creds *rgpb.Credentials) ([]byte, error) {
	req := &ocipb.FetchManifestRequest{
		ImageRef:    ref,
		Platform:    platform,
		Credentials: creds,
	}

	resp, err := f.client.FetchManifest(ctx, req)
	if err != nil {
		return nil, status.UnavailableErrorf("could not retrieve manifest from OCIFetchService: %s", err)
	}

	return resp.GetManifest(), nil
}

// FetchBlobMetadata fetches blob metadata from the remote OCIFetchService.
func (f *GRPCFetcher) FetchBlobMetadata(ctx context.Context, ref string, creds *rgpb.Credentials) (int64, string, error) {
	req := &ocipb.FetchBlobMetadataRequest{
		BlobRef:     ref,
		Credentials: creds,
	}

	resp, err := f.client.FetchBlobMetadata(ctx, req)
	if err != nil {
		return 0, "", status.UnavailableErrorf("could not retrieve blob metadata from OCIFetchService: %s", err)
	}

	return resp.GetSizeBytes(), resp.GetContentType(), nil
}

// FetchBlob fetches a blob from the remote OCIFetchService.
// The blob data is streamed from the server.
func (f *GRPCFetcher) FetchBlob(ctx context.Context, ref string, creds *rgpb.Credentials) (io.ReadCloser, error) {
	req := &ocipb.FetchBlobRequest{
		BlobRef:     ref,
		Credentials: creds,
	}

	stream, err := f.client.FetchBlob(ctx, req)
	if err != nil {
		return nil, status.UnavailableErrorf("could not start blob stream from OCIFetchService: %s", err)
	}

	return &grpcBlobReader{stream: stream}, nil
}

// grpcBlobReader implements io.ReadCloser for streaming blob data from the gRPC service.
type grpcBlobReader struct {
	stream    ocipb.OCIFetchService_FetchBlobClient
	buf       []byte // Buffer for current chunk
	bufOffset int    // Offset into current chunk
}

func (r *grpcBlobReader) Read(p []byte) (int, error) {
	totalRead := 0

	for totalRead < len(p) {
		// If we've consumed the current chunk, fetch the next one
		if r.bufOffset >= len(r.buf) {
			resp, err := r.stream.Recv()
			if err == io.EOF {
				if totalRead > 0 {
					return totalRead, nil
				}
				return 0, io.EOF
			}
			if err != nil {
				return totalRead, status.UnavailableErrorf("error receiving blob chunk: %s", err)
			}

			r.buf = resp.GetData()
			r.bufOffset = 0

			// If we got an empty chunk, continue to next one
			if len(r.buf) == 0 {
				continue
			}
		}

		// Copy from current chunk to output buffer
		n := copy(p[totalRead:], r.buf[r.bufOffset:])
		r.bufOffset += n
		totalRead += n
	}

	return totalRead, nil
}

func (r *grpcBlobReader) Close() error {
	// gRPC streams don't need explicit closing, but we can drain remaining data
	// to avoid leaving the stream hanging
	return nil
}
