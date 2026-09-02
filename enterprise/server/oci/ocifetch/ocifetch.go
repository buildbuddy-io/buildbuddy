// Package ocifetch is the single implementation of OCI manifest and blob
// fetching shared by the executor, the cache proxy, and the app.
//
// A Fetcher runs one sequence: consult the Store, go to the Upstream, write
// back to the Store. It is parameterised only by those two dependencies.
// The host it runs on decides what the Upstream is (a container registry, or
// another fetcher service) and where the Store writes (the remote cache, a
// proxy's local cache, or nowhere at all). Nothing else about the sequence
// changes between hosts.
package ocifetch

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocicache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/google/go-containerregistry/pkg/v1/types"
	gstatus "google.golang.org/grpc/status"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
)

const (
	accessProofCacheTTL        = 15 * time.Minute
	accessProofCacheMaxEntries = 1000

	// blobChunkSize matches the cachetools buffer size.
	blobChunkSize = 256 * 1000
)

var blobBufPool = bytebufferpool.VariableSize(blobChunkSize)

// Options carries per-request settings through a Fetcher.
type Options struct {
	// BypassRegistry serves only from caches and never contacts a container
	// registry. Callers are responsible for authorising the requester to
	// bypass (server admins only). A RegistryUpstream refuses every request
	// made with BypassRegistry set; a RemoteFetcherUpstream forwards the flag
	// so that the fetcher on the far side applies the same rule.
	BypassRegistry bool

	// SizeBytes and MediaType describe the blob when the caller already knows
	// them from a manifest descriptor; zero when unknown. They are only used
	// to address the cache and to write the cache metadata row; they are
	// never treated as proof that the blob exists or that the caller may
	// access it.
	SizeBytes int64
	MediaType string
}

// Upstream is where content comes from when the Store does not have it.
type Upstream interface {
	// Head resolves ref (a tag or a digest) to a descriptor. A successful
	// Head proves that creds grant access to the repository.
	Head(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, error)
	// Manifest returns the manifest bytes for ref and their descriptor.
	Manifest(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, []byte, error)
	// BlobMetadata returns the size and media type of a blob. A successful
	// call proves that creds grant access to the repository.
	BlobMetadata(ctx context.Context, ref ctrname.Digest, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, error)
	// Blob opens the blob for reading. The returned descriptor carries
	// whatever size and media type the upstream knows; either may be zero.
	// When opts.SizeBytes is set the upstream may use it instead of asking.
	Blob(ctx context.Context, ref ctrname.Digest, creds *rgpb.Credentials, opts Options) (io.ReadCloser, *ctr.Descriptor, error)
}

// Store caches manifests and blobs. Reads return a NotFound error on a miss.
// A nil Store caches nothing.
type Store interface {
	Manifest(ctx context.Context, repo ctrname.Repository, hash ctr.Hash) (*ocipb.OCIManifestContent, error)
	PutManifest(ctx context.Context, repo ctrname.Repository, hash ctr.Hash, mediaType string, raw []byte) error
	BlobMetadata(ctx context.Context, repo ctrname.Repository, hash ctr.Hash) (*ocipb.OCIBlobMetadata, error)
	ReadBlob(ctx context.Context, w io.Writer, hash ctr.Hash, size int64) error
	// BlobWriter returns a writer that stores exactly size bytes of the blob.
	// mediaType may be empty when the caller does not know it.
	BlobWriter(ctx context.Context, repo ctrname.Repository, hash ctr.Hash, mediaType string, size int64) (interfaces.CommittedWriteCloser, error)
}

// Fetcher fetches OCI manifests and blobs through a Store from an Upstream.
type Fetcher struct {
	upstream Upstream
	store    Store

	// proofs records (repository, credentials) pairs for which an upstream
	// access check recently succeeded. Registry pull authorisation is
	// repository-scoped, so one success lets every manifest and blob in the
	// repository be served from the Store without another upstream request
	// until the entry expires.
	proofs lru.LRU[struct{}]

	// blobGroup deduplicates concurrent fetches of the same blob with the
	// same credentials. The leader streams from upstream into the Store and
	// to its own caller; waiters read from the Store once the leader is done.
	blobGroup singleflight.Group[blobGroupKey, int64]
}

// blobGroupKey identifies one round of a coalesced blob fetch. Rounds have
// distinct keys because the singleflight keeps a finished call in its map
// until its last waiter has left, so a waiter that re-entered under the same
// key could join the stale result instead of starting a new fetch.
type blobGroupKey struct {
	key   ocicache.BlobFetchKey
	round int
}

// New returns a Fetcher. store may be nil, in which case nothing is cached
// and every request goes to upstream.
func New(upstream Upstream, store Store) (*Fetcher, error) {
	if upstream == nil {
		return nil, status.FailedPreconditionError("ocifetch: an Upstream is required")
	}
	proofs, err := lru.New[struct{}](&lru.Config[struct{}]{
		SizeFn:     func(_ struct{}) int64 { return 1 },
		MaxSize:    int64(accessProofCacheMaxEntries),
		TTL:        accessProofCacheTTL,
		ThreadSafe: true,
	})
	if err != nil {
		return nil, status.InternalErrorf("ocifetch: initializing access proof cache: %s", err)
	}
	return &Fetcher{upstream: upstream, store: store, proofs: proofs}, nil
}

// ResolveDigest resolves ref to a manifest digest. For a digest ref this
// proves repository access (unless bypassing) and returns the digest without
// contacting the Store. For a tag ref it asks the Upstream.
func (f *Fetcher) ResolveDigest(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts Options) (ctr.Hash, error) {
	if d, ok := ref.(ctrname.Digest); ok {
		h, err := ctr.NewHash(d.DigestStr())
		if err != nil {
			return ctr.Hash{}, status.InvalidArgumentErrorf("invalid digest %q: %s", d.DigestStr(), err)
		}
		if !opts.BypassRegistry {
			if err := f.proveManifestAccess(ctx, ref, creds, opts); err != nil {
				return ctr.Hash{}, err
			}
		}
		return h, nil
	}
	desc, err := f.upstream.Head(ctx, ref, creds, opts)
	if err != nil {
		return ctr.Hash{}, err
	}
	f.addProof(ref.Context(), creds, opts)
	return desc.Digest, nil
}

// FetchManifestMetadata asks the Upstream for the manifest descriptor. It
// never consults the Store, so a successful call is always fresh proof that
// creds grant access to the image.
func (f *Fetcher) FetchManifestMetadata(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, error) {
	desc, err := f.upstream.Head(ctx, ref, creds, opts)
	if err != nil {
		return nil, err
	}
	f.addProof(ref.Context(), creds, opts)
	return desc, nil
}

// FetchManifest returns the manifest for ref from the Store if present,
// otherwise from the Upstream, writing it to the Store on the way back.
func (f *Fetcher) FetchManifest(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, []byte, error) {
	repo := ref.Context()
	if f.store == nil {
		// Nothing to consult: one upstream request resolves the tag, proves
		// access, and returns the bytes.
		desc, raw, err := f.upstream.Manifest(ctx, ref, creds, opts)
		if err != nil {
			return nil, nil, err
		}
		f.addProof(repo, creds, opts)
		return desc, raw, nil
	}

	h, err := f.ResolveDigest(ctx, ref, creds, opts)
	if err != nil {
		return nil, nil, err
	}
	if mc, err := f.store.Manifest(ctx, repo, h); err == nil {
		return &ctr.Descriptor{
			Digest:    h,
			Size:      int64(len(mc.GetRaw())),
			MediaType: types.MediaType(mc.GetContentType()),
		}, mc.GetRaw(), nil
	} else if !status.IsNotFoundError(err) {
		log.CtxWarningf(ctx, "Error reading manifest %s@%s from cache: %s", repo, h, err)
	}

	// Fetch by the resolved digest, not the original tag, so a tag that moves
	// between resolution and fetch cannot be stored under the wrong key.
	desc, raw, err := f.upstream.Manifest(ctx, repo.Digest(h.String()), creds, opts)
	if err != nil {
		return nil, nil, err
	}
	f.addProof(repo, creds, opts)
	if err := f.store.PutManifest(ctx, repo, desc.Digest, string(desc.MediaType), raw); err != nil {
		log.CtxWarningf(ctx, "Error writing manifest %s@%s to cache: %s", repo, desc.Digest, err)
	}
	return desc, raw, nil
}

// FetchBlobMetadata returns a blob's size and media type from the Store when
// the caller has recently proven access to the repository (or is bypassing
// the registry), otherwise from the Upstream.
func (f *Fetcher) FetchBlobMetadata(ctx context.Context, ref ctrname.Digest, creds *rgpb.Credentials, opts Options) (*ctr.Descriptor, error) {
	repo := ref.Context()
	h, err := blobHash(ref)
	if err != nil {
		return nil, err
	}
	if f.store != nil && (opts.BypassRegistry || f.hasProof(repo, creds)) {
		md, err := f.store.BlobMetadata(ctx, repo, h)
		if err == nil {
			return &ctr.Descriptor{Digest: h, Size: md.GetContentLength(), MediaType: types.MediaType(md.GetContentType())}, nil
		}
		if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Error reading blob metadata %s@%s from cache: %s", repo, h, err)
		}
	}
	desc, err := f.upstream.BlobMetadata(ctx, ref, creds, opts)
	if err != nil {
		return nil, err
	}
	f.addProof(repo, creds, opts)
	return desc, nil
}

// FetchBlob writes the blob to w and returns the number of bytes written.
//
// With a Store, concurrent fetches of the same blob with the same
// credentials are coalesced: the leader streams from the Upstream to w and
// into the Store at the same time, and waiters read from the Store once the
// leader has finished (see fetchBlobShared for what happens when the leader
// could not cache the blob). Cached bytes are only served behind proof that
// creds grant access to the repository.
//
// Requests that bypass the registry never join the group: they must neither
// wait on, nor be served by, a fetch that contacts a registry.
//
// Without a Store the blob is streamed straight from the Upstream.
func (f *Fetcher) FetchBlob(ctx context.Context, w io.Writer, ref ctrname.Digest, creds *rgpb.Credentials, opts Options) (int64, error) {
	h, err := blobHash(ref)
	if err != nil {
		return 0, err
	}
	cw := &countingWriter{w: w}
	if f.store == nil {
		return cw.n, f.streamFromUpstream(ctx, cw, ref, creds, opts)
	}
	if opts.BypassRegistry {
		_, err := f.leadBlobFetch(ctx, cw, ref, h, creds, opts)
		return cw.n, err
	}

	return cw.n, f.fetchBlobShared(ctx, cw, ref, h, creds, opts, 2)
}

// fetchBlobShared runs one singleflight round for the blob. The leader
// streams from the Upstream into the Store and to its caller; waiters read
// the Store afterwards. A waiter that finds nothing to read (the leader could
// not cache the blob, or it was evicted already) runs another round while
// rounds remain, so that a transient cache failure still coalesces, and
// finally fetches the blob from the Upstream itself. Bytes cannot be shared
// without a Store, so a persistent cache outage costs one upstream fetch per
// caller, as it did before when waiters failed and their clients retried.
func (f *Fetcher) fetchBlobShared(ctx context.Context, cw *countingWriter, ref ctrname.Digest, h ctr.Hash, creds *rgpb.Credentials, opts Options, rounds int) error {
	start := time.Now()
	isLeader := false
	key := blobGroupKey{key: ocicache.NewBlobFetchKey(ref.Context(), h, creds), round: rounds}
	size, _, err := f.blobGroup.Do(ctx, key, func(ctx context.Context) (int64, error) {
		isLeader = true
		return f.leadBlobFetch(ctx, cw, ref, h, creds, opts)
	})
	if isLeader {
		recordFetchBlobMetrics(metrics.OCIFetcherRoleLeader, err, time.Since(start))
		return err
	}
	if err == nil && size > 0 {
		err = f.store.ReadBlob(ctx, cw, h, size)
	}
	if (err == nil && size == 0) || (status.IsNotFoundError(err) && cw.n == 0) {
		if rounds > 1 {
			return f.fetchBlobShared(ctx, cw, ref, h, creds, opts, rounds-1)
		}
		err = f.streamFromUpstream(ctx, cw, ref, creds, opts)
	}
	recordFetchBlobMetrics(metrics.OCIFetcherRoleWaiter, err, time.Since(start))
	return err
}

// streamFromUpstream copies the blob from the Upstream to w without caching.
func (f *Fetcher) streamFromUpstream(ctx context.Context, w io.Writer, ref ctrname.Digest, creds *rgpb.Credentials, opts Options) error {
	rc, _, err := f.upstream.Blob(ctx, ref, creds, opts)
	if err != nil {
		return err
	}
	defer rc.Close()
	f.addProof(ref.Context(), creds, opts)
	if _, err := io.Copy(w, rc); err != nil {
		return upstreamReadError(err)
	}
	return nil
}

// leadBlobFetch serves the blob to cw from the Store when it is present and
// access has been proven, and otherwise from the Upstream while writing it
// through to the Store. It returns the size the blob was stored under, or 0
// if it was streamed without being cached.
func (f *Fetcher) leadBlobFetch(ctx context.Context, cw *countingWriter, ref ctrname.Digest, h ctr.Hash, creds *rgpb.Credentials, opts Options) (int64, error) {
	repo := ref.Context()

	// The Store addresses blobs by digest and size, so learn the size: from
	// the caller, else from the Store's metadata row, else from the Upstream.
	size, mediaType := opts.SizeBytes, opts.MediaType
	if size == 0 {
		md, err := f.store.BlobMetadata(ctx, repo, h)
		if err == nil {
			size = md.GetContentLength()
			if mediaType == "" {
				mediaType = md.GetContentType()
			}
		} else if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Error reading blob metadata %s@%s from cache: %s", repo, h, err)
		}
	}
	if size == 0 {
		desc, err := f.upstream.BlobMetadata(ctx, ref, creds, opts)
		if err == nil {
			f.addProof(repo, creds, opts)
			size = desc.Size
			if mediaType == "" {
				mediaType = string(desc.MediaType)
			}
		} else if isAccessError(err) {
			// The blob fetch would fail the same way; do not try it.
			return 0, err
		} else {
			log.CtxWarningf(ctx, "Error fetching blob metadata %s@%s from upstream; the blob will be streamed without caching: %s", repo, h, err)
		}
	}

	if size > 0 {
		// Cached bytes may exist. Prove that the caller may access the
		// repository before serving any of them.
		if !opts.BypassRegistry {
			if err := f.proveBlobAccess(ctx, ref, creds, opts); err != nil {
				return 0, err
			}
		}
		err := f.store.ReadBlob(ctx, cw, h, size)
		if err == nil {
			return size, nil
		}
		if cw.n > 0 {
			// Bytes have already been sent; replaying from upstream would
			// corrupt the caller's stream.
			return 0, err
		}
		if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Error reading blob %s@%s from cache before streaming any bytes; falling back to upstream: %s", repo, h, err)
		}
	}

	// Miss: stream from the Upstream and write through to the Store.
	upstreamOpts := opts
	upstreamOpts.SizeBytes, upstreamOpts.MediaType = size, mediaType
	rc, desc, err := f.upstream.Blob(ctx, ref, creds, upstreamOpts)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	f.addProof(repo, creds, opts)
	if desc != nil {
		if desc.Size > 0 {
			size = desc.Size
		}
		if desc.MediaType != "" {
			mediaType = string(desc.MediaType)
		}
	}
	if size <= 0 {
		if _, err := io.Copy(cw, rc); err != nil {
			return 0, upstreamReadError(err)
		}
		return 0, nil
	}
	bw, err := f.store.BlobWriter(ctx, repo, h, mediaType, size)
	if err != nil {
		log.CtxWarningf(ctx, "Error creating cache writer for blob %s@%s; streaming without caching: %s", repo, h, err)
		if _, err := io.Copy(cw, rc); err != nil {
			return 0, upstreamReadError(err)
		}
		return 0, nil
	}
	cached, err := copyThrough(ctx, rc, cw, bw)
	if err != nil {
		return 0, err
	}
	if !cached {
		return 0, nil
	}
	return size, nil
}

// isAccessError reports whether err means the upstream will not serve the
// repository to this caller at all, as opposed to a transient failure.
func isAccessError(err error) bool {
	return status.IsUnauthenticatedError(err) || status.IsPermissionDeniedError(err) || status.IsNotFoundError(err)
}

// copyThrough copies upstream to both the caller and the Store. It reports
// whether the blob was committed to the Store. A failure to write to the
// caller does not stop the copy into the Store: waiters of the same fetch,
// and every later caller, still get the blob. A failure to write to or
// commit the Store is logged and the caller keeps being served.
func copyThrough(ctx context.Context, upstream io.Reader, caller io.Writer, store interfaces.CommittedWriteCloser) (cached bool, err error) {
	defer func() {
		if err := store.Close(); err != nil {
			log.CtxWarningf(ctx, "Error closing cache writer: %s", err)
		}
	}()
	buf := blobBufPool.Get(blobChunkSize)
	defer blobBufPool.Put(buf)
	var callerErr, storeErr error
	for {
		n, readErr := upstream.Read(buf)
		if n > 0 {
			if callerErr == nil {
				if _, err := caller.Write(buf[:n]); err != nil {
					callerErr = status.WrapError(err, "send blob")
				}
			}
			if storeErr == nil {
				if written, err := store.Write(buf[:n]); err != nil {
					storeErr = err
					if !status.IsAlreadyExistsError(err) {
						log.CtxWarningf(ctx, "Error writing blob to cache: %s", err)
					}
				} else if written < n {
					storeErr = io.ErrShortWrite
					log.CtxWarningf(ctx, "Short write to cache: wanted %d, wrote %d", n, written)
				}
			}
			if callerErr != nil && storeErr != nil {
				return false, callerErr
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			if callerErr != nil {
				return false, callerErr
			}
			return false, upstreamReadError(readErr)
		}
	}
	if storeErr != nil {
		return false, callerErr
	}
	if err := store.Commit(); err != nil {
		log.CtxWarningf(ctx, "Error committing blob to cache: %s", err)
		return false, callerErr
	}
	return true, callerErr
}

// upstreamReadError classifies an error from reading an upstream blob
// stream. Context errors and errors that already carry a status (for
// example a gRPC status from a RemoteFetcherUpstream) are returned as they
// are; anything else becomes Unavailable.
func upstreamReadError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if _, ok := gstatus.FromError(err); ok {
		return err
	}
	return status.UnavailableErrorf("read blob from upstream: %s", err)
}

func (f *Fetcher) proveManifestAccess(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts Options) error {
	repo := ref.Context()
	if f.hasProof(repo, creds) {
		return nil
	}
	if _, err := f.upstream.Head(ctx, ref, creds, opts); err != nil {
		return err
	}
	f.addProof(repo, creds, opts)
	return nil
}

func (f *Fetcher) proveBlobAccess(ctx context.Context, ref ctrname.Digest, creds *rgpb.Credentials, opts Options) error {
	repo := ref.Context()
	if f.hasProof(repo, creds) {
		return nil
	}
	if _, err := f.upstream.BlobMetadata(ctx, ref, creds, opts); err != nil {
		return err
	}
	f.addProof(repo, creds, opts)
	return nil
}

func (f *Fetcher) hasProof(repo ctrname.Repository, creds *rgpb.Credentials) bool {
	return f.proofs.Contains(repoAccessKey(repo, creds))
}

// addProof records that creds were just accepted for repo, unless the request
// bypassed the registry: a bypass read is served by a cache (possibly through
// another fetcher) and proves nothing about the registry's authorisation.
func (f *Fetcher) addProof(repo ctrname.Repository, creds *rgpb.Credentials, opts Options) {
	if opts.BypassRegistry {
		return
	}
	f.proofs.Add(repoAccessKey(repo, creds), struct{}{})
}

// repoAccessKey is the access-proof cache key for a repository and
// credentials. Pull authorisation is repository-scoped, so the key is
// deliberately not specific to any one manifest or blob.
func repoAccessKey(repo ctrname.Repository, creds *rgpb.Credentials) string {
	return hash.Strings(repo.Name(), creds.GetUsername(), creds.GetPassword())
}

func blobHash(ref ctrname.Digest) (ctr.Hash, error) {
	h, err := ctr.NewHash(ref.DigestStr())
	if err != nil {
		return ctr.Hash{}, status.InvalidArgumentErrorf("invalid digest %q: %s", ref.DigestStr(), err)
	}
	return h, nil
}

func recordFetchBlobMetrics(role string, err error, duration time.Duration) {
	statusLabel := metrics.OCIFetcherStatusOK
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || status.IsDeadlineExceededError(err) {
			statusLabel = metrics.OCIFetcherStatusTimeout
		} else if errors.Is(err, context.Canceled) || status.IsCanceledError(err) {
			statusLabel = metrics.OCIFetcherStatusCanceled
		} else {
			statusLabel = metrics.OCIFetcherStatusError
		}
	}
	metrics.OCIFetcherRequestCount.WithLabelValues(metrics.OCIFetcherMethodFetchBlob, role, statusLabel).Inc()
	metrics.OCIFetcherRequestDurationUsec.WithLabelValues(metrics.OCIFetcherMethodFetchBlob, role).Observe(float64(duration.Microseconds()))
}

// countingWriter counts the bytes written so the leader can tell whether a
// cache read failed before or after it started streaming to the caller.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
