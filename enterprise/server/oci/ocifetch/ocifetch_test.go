package ocifetch_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetch"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/require"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	ctr "github.com/google/go-containerregistry/pkg/v1"
)

const testBlob = "0123456789abcdef0123456789abcdef"

var (
	testCreds = &rgpb.Credentials{Username: "user", Password: "pass"}
	testRef   = mustDigestRef(testBlob)
)

func mustDigestRef(content string) ctrname.Digest {
	h, _, err := ctr.SHA256(bytes.NewReader([]byte(content)))
	if err != nil {
		panic(err)
	}
	ref, err := ctrname.NewDigest("example.com/repo@" + h.String())
	if err != nil {
		panic(err)
	}
	return ref
}

// fakeUpstream serves one blob and counts requests. Like a registry upstream
// it refuses every request made with BypassRegistry set, unless
// forwardsBypass is set, in which case it behaves like a remote fetcher that
// served the request from its own cache. Errors can be injected per method.
// gate, when set, blocks Blob until released.
type fakeUpstream struct {
	blob           []byte
	mediaType      string
	forwardsBypass bool

	headErr, metadataErr, blobErr error
	// blobReadErr is returned from the blob reader after blobReadErrAfter bytes.
	blobReadErr      error
	blobReadErrAfter int

	heads, metadatas, blobs atomic.Int32
	gate                    chan struct{}
}

func (u *fakeUpstream) Head(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts ocifetch.Options) (*ctr.Descriptor, error) {
	u.heads.Add(1)
	if opts.BypassRegistry && !u.forwardsBypass {
		return nil, status.NotFoundError("bypassing registry")
	}
	if u.headErr != nil {
		return nil, u.headErr
	}
	return &ctr.Descriptor{}, nil
}

func (u *fakeUpstream) Manifest(ctx context.Context, ref ctrname.Reference, creds *rgpb.Credentials, opts ocifetch.Options) (*ctr.Descriptor, []byte, error) {
	return nil, nil, status.UnimplementedError("not used")
}

func (u *fakeUpstream) BlobMetadata(ctx context.Context, ref ctrname.Digest, creds *rgpb.Credentials, opts ocifetch.Options) (*ctr.Descriptor, error) {
	u.metadatas.Add(1)
	if opts.BypassRegistry && !u.forwardsBypass {
		return nil, status.NotFoundError("bypassing registry")
	}
	if u.metadataErr != nil {
		return nil, u.metadataErr
	}
	h, _ := ctr.NewHash(ref.DigestStr())
	return &ctr.Descriptor{Digest: h, Size: int64(len(u.blob)), MediaType: types.MediaType(u.mediaType)}, nil
}

func (u *fakeUpstream) Blob(ctx context.Context, ref ctrname.Digest, creds *rgpb.Credentials, opts ocifetch.Options) (io.ReadCloser, *ctr.Descriptor, error) {
	u.blobs.Add(1)
	if opts.BypassRegistry && !u.forwardsBypass {
		return nil, nil, status.NotFoundError("bypassing registry")
	}
	if u.blobErr != nil {
		return nil, nil, u.blobErr
	}
	if u.gate != nil {
		select {
		case <-u.gate:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	h, _ := ctr.NewHash(ref.DigestStr())
	var r io.Reader = bytes.NewReader(u.blob)
	if u.blobReadErr != nil {
		r = &failingReader{r: io.LimitReader(r, int64(u.blobReadErrAfter)), err: u.blobReadErr}
	}
	return io.NopCloser(r), &ctr.Descriptor{Digest: h, Size: opts.SizeBytes, MediaType: types.MediaType(opts.MediaType)}, nil
}

type failingReader struct {
	r   io.Reader
	err error
}

func (f *failingReader) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if err == io.EOF {
		return n, f.err
	}
	return n, err
}

// fakeStore keeps blobs and metadata rows in memory. commitFailures makes
// that many blob writes fail at commit time (negative: all of them).
type fakeStore struct {
	mu             sync.Mutex
	blobs          map[string][]byte // key: hex/size
	metadata       map[string]*ocipb.OCIBlobMetadata
	commitFailures atomic.Int32
	reads          atomic.Int32
}

func newFakeStore() *fakeStore {
	return &fakeStore{blobs: map[string][]byte{}, metadata: map[string]*ocipb.OCIBlobMetadata{}}
}

func blobKey(hash ctr.Hash, size int64) string { return hash.Hex + "/" + strconv.FormatInt(size, 10) }

func (s *fakeStore) Manifest(ctx context.Context, repo ctrname.Repository, hash ctr.Hash) (*ocipb.OCIManifestContent, error) {
	return nil, status.NotFoundError("no manifests")
}

func (s *fakeStore) PutManifest(ctx context.Context, repo ctrname.Repository, hash ctr.Hash, mediaType string, raw []byte) error {
	return nil
}

func (s *fakeStore) BlobMetadata(ctx context.Context, repo ctrname.Repository, hash ctr.Hash) (*ocipb.OCIBlobMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if md, ok := s.metadata[hash.Hex]; ok {
		return md, nil
	}
	return nil, status.NotFoundError("no metadata")
}

func (s *fakeStore) ReadBlob(ctx context.Context, w io.Writer, hash ctr.Hash, size int64) error {
	s.reads.Add(1)
	s.mu.Lock()
	b, ok := s.blobs[blobKey(hash, size)]
	s.mu.Unlock()
	if !ok {
		return status.NotFoundError("no blob")
	}
	_, err := w.Write(b)
	return err
}

func (s *fakeStore) BlobWriter(ctx context.Context, repo ctrname.Repository, hash ctr.Hash, mediaType string, size int64) (interfaces.CommittedWriteCloser, error) {
	return &fakeWriter{store: s, hash: hash, mediaType: mediaType, size: size}, nil
}

type fakeWriter struct {
	store     *fakeStore
	hash      ctr.Hash
	mediaType string
	size      int64
	buf       bytes.Buffer
}

func (w *fakeWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *fakeWriter) Close() error                { return nil }
func (w *fakeWriter) Commit() error {
	if n := w.store.commitFailures.Load(); n != 0 {
		if n > 0 {
			w.store.commitFailures.Add(-1)
		}
		return status.UnavailableError("cache down")
	}
	if int64(w.buf.Len()) != w.size {
		return status.DataLossErrorf("wrote %d bytes, expected %d", w.buf.Len(), w.size)
	}
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.store.blobs[blobKey(w.hash, w.size)] = w.buf.Bytes()
	w.store.metadata[w.hash.Hex] = &ocipb.OCIBlobMetadata{ContentLength: w.size, ContentType: w.mediaType}
	return nil
}

func newFetcher(t *testing.T, up ocifetch.Upstream, store ocifetch.Store) *ocifetch.Fetcher {
	f, err := ocifetch.New(up, store)
	require.NoError(t, err)
	return f
}

func TestFetchBlob_NoStore_StreamsFromUpstream(t *testing.T) {
	up := &fakeUpstream{blob: []byte(testBlob)}
	f := newFetcher(t, up, nil)
	var out bytes.Buffer
	n, err := f.FetchBlob(context.Background(), &out, testRef, testCreds, ocifetch.Options{})
	require.NoError(t, err)
	require.Equal(t, int64(len(testBlob)), n)
	require.Equal(t, testBlob, out.String())
	require.Equal(t, int32(0), up.metadatas.Load(), "no store, so no need to learn the size")
	require.Equal(t, int32(1), up.blobs.Load())
}

func TestFetchBlob_MissThenHit(t *testing.T) {
	up := &fakeUpstream{blob: []byte(testBlob), mediaType: "application/x-layer"}
	store := newFakeStore()
	f := newFetcher(t, up, store)

	var out bytes.Buffer
	_, err := f.FetchBlob(context.Background(), &out, testRef, testCreds, ocifetch.Options{})
	require.NoError(t, err)
	require.Equal(t, testBlob, out.String())
	require.Equal(t, int32(1), up.metadatas.Load(), "size learned from upstream")
	require.Equal(t, int32(1), up.blobs.Load())
	require.Equal(t, "application/x-layer", store.metadata[testRef.DigestStr()[len("sha256:"):]].GetContentType())

	// Second fetch: served from the store. Access was proven by the first
	// fetch, so no upstream request at all.
	out.Reset()
	_, err = f.FetchBlob(context.Background(), &out, testRef, testCreds, ocifetch.Options{})
	require.NoError(t, err)
	require.Equal(t, testBlob, out.String())
	require.Equal(t, int32(1), up.metadatas.Load())
	require.Equal(t, int32(1), up.blobs.Load())

	// Different credentials must prove access before reading the cache.
	out.Reset()
	other := &rgpb.Credentials{Username: "other", Password: "pass"}
	_, err = f.FetchBlob(context.Background(), &out, testRef, other, ocifetch.Options{})
	require.NoError(t, err)
	require.Equal(t, testBlob, out.String())
	require.Equal(t, int32(2), up.metadatas.Load(), "one HEAD to prove access")
	require.Equal(t, int32(1), up.blobs.Load())
}

func TestFetchBlob_SizeHintSkipsMetadataAndKeepsMediaType(t *testing.T) {
	up := &fakeUpstream{blob: []byte(testBlob)}
	store := newFakeStore()
	f := newFetcher(t, up, store)

	var out bytes.Buffer
	opts := ocifetch.Options{SizeBytes: int64(len(testBlob)), MediaType: "application/x-hinted"}
	_, err := f.FetchBlob(context.Background(), &out, testRef, testCreds, opts)
	require.NoError(t, err)
	require.Equal(t, testBlob, out.String())
	require.Equal(t, int32(1), up.metadatas.Load(), "one HEAD to prove access; the size came from the caller")
	require.Equal(t, "application/x-hinted", store.metadata[testRef.DigestStr()[len("sha256:"):]].GetContentType())
}

func TestFetchBlob_MetadataAccessErrorFailsFast(t *testing.T) {
	up := &fakeUpstream{blob: []byte(testBlob), metadataErr: status.UnauthenticatedError("bad creds")}
	f := newFetcher(t, up, newFakeStore())
	var out bytes.Buffer
	_, err := f.FetchBlob(context.Background(), &out, testRef, testCreds, ocifetch.Options{})
	require.True(t, status.IsUnauthenticatedError(err), "got %v", err)
	require.Equal(t, int32(0), up.blobs.Load(), "no blob GET after an access failure")
}

func TestFetchBlob_MetadataTransientErrorStreamsUncached(t *testing.T) {
	up := &fakeUpstream{blob: []byte(testBlob), metadataErr: status.UnavailableError("registry 500")}
	store := newFakeStore()
	f := newFetcher(t, up, store)
	var out bytes.Buffer
	n, err := f.FetchBlob(context.Background(), &out, testRef, testCreds, ocifetch.Options{})
	require.NoError(t, err)
	require.Equal(t, int64(len(testBlob)), n)
	require.Equal(t, testBlob, out.String())
	require.Empty(t, store.blobs, "unknown size, so nothing cached")
}

func TestFetchBlob_BypassNeverContactsUpstreamOrJoinsSingleflight(t *testing.T) {
	up := &fakeUpstream{blob: []byte(testBlob), gate: make(chan struct{})}
	store := newFakeStore()
	f := newFetcher(t, up, store)
	ctx := context.Background()

	// A normal caller is mid-fetch (blocked at the gate).
	var normalOut bytes.Buffer
	var normalErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, normalErr = f.FetchBlob(ctx, &normalOut, testRef, testCreds, ocifetch.Options{SizeBytes: int64(len(testBlob))})
	}()
	require.Eventually(t, func() bool { return up.blobs.Load() == 1 }, 5e9, 1e6)

	// A bypass caller for the same blob must not wait for, or be served by,
	// the normal caller's fetch: it gets NotFound from its own cache-only path.
	var bypassOut bytes.Buffer
	_, err := f.FetchBlob(ctx, &bypassOut, testRef, testCreds, ocifetch.Options{BypassRegistry: true, SizeBytes: int64(len(testBlob))})
	require.True(t, status.IsNotFoundError(err), "got %v", err)

	close(up.gate)
	<-done
	require.NoError(t, normalErr)
	require.Equal(t, testBlob, normalOut.String())

	// Now the blob is cached; bypass serves it without touching upstream.
	blobs := up.blobs.Load()
	bypassOut.Reset()
	_, err = f.FetchBlob(ctx, &bypassOut, testRef, testCreds, ocifetch.Options{BypassRegistry: true, SizeBytes: int64(len(testBlob))})
	require.NoError(t, err)
	require.Equal(t, testBlob, bypassOut.String())
	require.Equal(t, blobs, up.blobs.Load())
}

func TestFetchBlob_CommitFailureDoesNotStrandWaiters(t *testing.T) {
	for _, tc := range []struct {
		name           string
		commitFailures int32
		callers        int
		wantFetches    int32
	}{
		// One failed commit: the first waiter to retry becomes the leader of
		// a second coalesced round, caches the blob, and the rest read it.
		{"transient", 1, 4, 2},
		// The cache stays down: after the retry round every caller fetches
		// for itself. Bytes cannot be shared without a store.
		{"persistent", -1, 4, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := &fakeUpstream{blob: []byte(testBlob), gate: make(chan struct{})}
			store := newFakeStore()
			store.commitFailures.Store(tc.commitFailures)
			f := newFetcher(t, up, store)
			ctx := context.Background()
			opts := ocifetch.Options{SizeBytes: int64(len(testBlob))}

			outs := make([]bytes.Buffer, tc.callers)
			errs := make([]error, tc.callers)
			var wg sync.WaitGroup
			for i := 0; i < tc.callers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					_, errs[i] = f.FetchBlob(ctx, &outs[i], testRef, testCreds, opts)
				}(i)
			}
			// Exactly one caller reaches upstream while the gate is closed.
			require.Eventually(t, func() bool { return up.blobs.Load() == 1 }, 5e9, 1e6)
			close(up.gate)
			wg.Wait()

			for i := range errs {
				require.NoError(t, errs[i], "caller %d", i)
				require.Equal(t, testBlob, outs[i].String(), "caller %d", i)
			}
			require.Equal(t, tc.wantFetches, up.blobs.Load())
		})
	}
}

func TestFetchBlob_BypassDoesNotRecordProof(t *testing.T) {
	// A remote-fetcher-like upstream serves bypass requests from its own
	// cache. That must not count as proof of registry access.
	up := &fakeUpstream{blob: []byte(testBlob), forwardsBypass: true}
	store := newFakeStore()
	f := newFetcher(t, up, store)
	ctx := context.Background()
	opts := ocifetch.Options{SizeBytes: int64(len(testBlob))}
	anon := &rgpb.Credentials{}

	var out bytes.Buffer
	bypass := opts
	bypass.BypassRegistry = true
	_, err := f.FetchBlob(ctx, &out, testRef, anon, bypass)
	require.NoError(t, err)
	require.Equal(t, testBlob, out.String())
	require.Equal(t, int32(0), up.metadatas.Load())

	// The blob is now in the store. An ordinary request with the same
	// credentials must still prove access before it is served.
	out.Reset()
	_, err = f.FetchBlob(ctx, &out, testRef, anon, opts)
	require.NoError(t, err)
	require.Equal(t, testBlob, out.String())
	require.Equal(t, int32(1), up.metadatas.Load(), "access must be proven; the bypass read proved nothing")
}

func TestFetchBlob_WaitersReadFromStore(t *testing.T) {
	up := &fakeUpstream{blob: []byte(testBlob), gate: make(chan struct{})}
	store := newFakeStore()
	f := newFetcher(t, up, store)
	ctx := context.Background()
	opts := ocifetch.Options{SizeBytes: int64(len(testBlob))}

	const callers = 4
	outs := make([]bytes.Buffer, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = f.FetchBlob(ctx, &outs[i], testRef, testCreds, opts)
		}(i)
	}
	require.Eventually(t, func() bool { return up.blobs.Load() == 1 }, 5e9, 1e6)
	close(up.gate)
	wg.Wait()
	for i := range errs {
		require.NoError(t, errs[i], "caller %d", i)
		require.Equal(t, testBlob, outs[i].String(), "caller %d", i)
	}
	require.Equal(t, int32(1), up.blobs.Load(), "one upstream fetch shared by all callers")
}

func TestFetchBlob_CallerWriteFailureStillFillsStore(t *testing.T) {
	up := &fakeUpstream{blob: []byte(testBlob)}
	store := newFakeStore()
	f := newFetcher(t, up, store)
	opts := ocifetch.Options{SizeBytes: int64(len(testBlob))}

	_, err := f.FetchBlob(context.Background(), &failingWriter{}, testRef, testCreds, opts)
	require.Error(t, err)
	require.Len(t, store.blobs, 1, "the store was filled even though the caller went away")

	var out bytes.Buffer
	_, err = f.FetchBlob(context.Background(), &out, testRef, testCreds, opts)
	require.NoError(t, err)
	require.Equal(t, testBlob, out.String())
	require.Equal(t, int32(1), up.blobs.Load())
}

type failingWriter struct{}

func (failingWriter) Write(p []byte) (int, error) { return 0, errors.New("caller went away") }

func TestFetchBlob_UpstreamReadErrorKeepsStatus(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		check func(error) bool
	}{
		{"canceled", context.Canceled, func(err error) bool { return errors.Is(err, context.Canceled) }},
		{"deadline", context.DeadlineExceeded, func(err error) bool { return errors.Is(err, context.DeadlineExceeded) }},
		{"status", status.PermissionDeniedError("nope"), status.IsPermissionDeniedError},
		{"plain", errors.New("connection reset"), status.IsUnavailableError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := &fakeUpstream{blob: []byte(testBlob), blobReadErr: tc.err, blobReadErrAfter: 4}
			for _, store := range []ocifetch.Store{nil, newFakeStore()} {
				f := newFetcher(t, up, store)
				var out bytes.Buffer
				_, err := f.FetchBlob(context.Background(), &out, testRef, testCreds, ocifetch.Options{SizeBytes: int64(len(testBlob))})
				require.Error(t, err)
				require.True(t, tc.check(err), "store=%v: got %v", store != nil, err)
			}
		})
	}
}
