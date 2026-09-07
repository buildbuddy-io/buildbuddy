package ocifetch

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocicache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// CacheStore is a Store backed by BuildBuddy's ActionCache and ByteStream
// services, using the key layout in package ocicache: manifests and per-blob
// metadata rows in the AC, blob bytes in the CAS.
//
// The ActionCache client may be nil. Then only blob bytes are stored:
// manifest and metadata reads miss, manifest writes are no-ops, and blobs
// are written to the CAS without a metadata row. That is the configuration
// the cache proxy runs with.
type CacheStore struct {
	bs bspb.ByteStreamClient
	ac repb.ActionCacheClient
	// instanceName is the remote instance name used for CAS blobs.
	instanceName string
}

var _ Store = (*CacheStore)(nil)

// NewCacheStore returns a Store over the given clients using the ocicache
// instance name for blobs. bs is required; ac may be nil (see CacheStore).
func NewCacheStore(bs bspb.ByteStreamClient, ac repb.ActionCacheClient) (*CacheStore, error) {
	return newCacheStore(bs, ac, ocicache.BlobInstanceName)
}

// NewLocalBlobStore returns a Store that keeps only blob bytes, under the
// empty instance name, in the given ByteStream service. The cache proxy
// uses it with its local ByteStream server.
func NewLocalBlobStore(bs bspb.ByteStreamClient) (*CacheStore, error) {
	return newCacheStore(bs, nil, "")
}

func newCacheStore(bs bspb.ByteStreamClient, ac repb.ActionCacheClient, instanceName string) (*CacheStore, error) {
	if bs == nil {
		return nil, status.FailedPreconditionError("ocifetch: a ByteStream client is required")
	}
	return &CacheStore{bs: bs, ac: ac, instanceName: instanceName}, nil
}

func (s *CacheStore) Manifest(ctx context.Context, repo ctrname.Repository, hash ctr.Hash) (*ocipb.OCIManifestContent, error) {
	if s.ac == nil {
		return nil, status.NotFoundErrorf("manifest %s@%s: this store keeps no manifests", repo, hash)
	}
	return ocicache.FetchManifestFromAC(ctx, s.ac, repo, hash, repo.Digest(hash.String()))
}

func (s *CacheStore) PutManifest(ctx context.Context, repo ctrname.Repository, hash ctr.Hash, mediaType string, raw []byte) error {
	if s.ac == nil {
		return nil
	}
	return ocicache.WriteManifestToAC(ctx, raw, s.ac, repo, hash, mediaType, repo.Digest(hash.String()))
}

func (s *CacheStore) BlobMetadata(ctx context.Context, repo ctrname.Repository, hash ctr.Hash) (*ocipb.OCIBlobMetadata, error) {
	if s.ac == nil {
		return nil, status.NotFoundErrorf("blob metadata %s@%s: this store keeps no metadata", repo, hash)
	}
	return ocicache.FetchBlobMetadataFromCache(ctx, s.bs, s.ac, repo, hash)
}

func (s *CacheStore) ReadBlob(ctx context.Context, w io.Writer, hash ctr.Hash, size int64) error {
	return ocicache.ReadBlob(ctx, w, s.bs, s.instanceName, hash, size)
}

func (s *CacheStore) BlobWriter(ctx context.Context, repo ctrname.Repository, hash ctr.Hash, mediaType string, size int64) (interfaces.CommittedWriteCloser, error) {
	if s.ac == nil {
		return ocicache.NewCASBlobUploader(ctx, s.bs, s.instanceName, hash, size)
	}
	if mediaType == "" {
		// A metadata row without a content type would be served as such to
		// docker clients by the registry, so store only the bytes.
		log.CtxWarningf(ctx, "Blob %s@%s has unknown media type; caching bytes without a metadata row", repo, hash)
		return ocicache.NewCASBlobUploader(ctx, s.bs, s.instanceName, hash, size)
	}
	return ocicache.NewBlobUploader(ctx, s.bs, s.ac, repo, hash, mediaType, size)
}
