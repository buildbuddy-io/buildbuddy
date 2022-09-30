package cacheproxy_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cacheproxy"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/require"

	dcpb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	noHandoff = ""
)

var (
	emptyUserMap = testauth.TestUsers()
)

func getTestEnv(t *testing.T, users map[string]interfaces.UserInfo) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(users))
	return te
}

type randomDataMaker struct {
	src rand.Source
}

func (r *randomDataMaker) Read(p []byte) (n int, err error) {
	todo := len(p)
	offset := 0
	for {
		val := int64(r.src.Int63())
		for i := 0; i < 8; i++ {
			p[offset] = byte(val & 0xff)
			todo--
			if todo == 0 {
				return len(p), nil
			}
			offset++
			val >>= 8
		}
	}
}

func waitUntilServerIsAlive(addr string) {
	for {
		_, err := net.DialTimeout("tcp", addr, 10*time.Millisecond)
		if err == nil {
			return
		}
	}
}

func copyAndClose(wc io.WriteCloser, r io.Reader) error {
	if _, err := io.Copy(wc, r); err != nil {
		return err
	}
	return wc.Close()
}

func TestReaderMaxOffset(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	randomSrc := &randomDataMaker{rand.NewSource(time.Now().Unix())}

	// Read some random bytes.
	buf := new(bytes.Buffer)
	io.CopyN(buf, randomSrc, 100)
	readSeeker := bytes.NewReader(buf.Bytes())

	// Compute a digest for the random bytes.
	d, err := digest.Compute(readSeeker)
	if err != nil {
		t.Fatal(err)
	}
	readSeeker.Seek(0, 0)

	instanceName := "foo"
	isolation := &dcpb.Isolation{
		RemoteInstanceName: instanceName,
		CacheType:          dcpb.Isolation_CAS_CACHE,
	}
	// Set the random bytes in the cache (with a prefix)
	cache, err := te.GetCache().WithIsolation(ctx, interfaces.CASCacheType, instanceName)
	require.NoError(t, err)

	err = cache.Set(ctx, d, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	// Remote-read the random bytes back.
	r, err := c.RemoteReader(ctx, peer, isolation, d, d.GetSizeBytes(), 0)
	if err != nil {
		t.Fatal(err)
	}
	d2 := testdigest.ReadDigestAndClose(t, r)
	emptyHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if emptyHash != d2.GetHash() {
		t.Fatalf("Digest uploaded %q != %q downloaded", emptyHash, d2.GetHash())
	}

}

type snitchCache struct {
	interfaces.Cache
	writeCount map[string]int
}

func (s *snitchCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheTypeDeprecated, remoteInstanceName string) (interfaces.Cache, error) {
	c, err := s.Cache.WithIsolation(ctx, cacheType, remoteInstanceName)
	if err != nil {
		return nil, err
	}
	return &snitchCache{
		c,
		s.writeCount,
	}, nil
}
func (s *snitchCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	wc, err := s.Cache.Writer(ctx, d)
	if err != nil {
		return nil, err
	}
	s.writeCount[d.GetHash()] += 1
	return wc, nil
}

func TestWriteAlreadyExistsCAS(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	writeCounts := make(map[string]int, 0)
	sc := snitchCache{te.GetCache(), writeCounts}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, &sc, peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}

	waitUntilServerIsAlive(peer)

	testSize := int64(10000000)
	d, readSeeker := testdigest.NewRandomDigestReader(t, testSize)
	isolation := &dcpb.Isolation{
		CacheType: dcpb.Isolation_CAS_CACHE,
	}

	// Remote-write the random bytes to the cache (with a prefix).
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, isolation, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyAndClose(wc, readSeeker); err != nil {
		t.Fatal(err)
	}

	if writeCounts[d.GetHash()] != 1 {
		t.Fatalf("Snitch cache was not written to. It should have been.")
	}

	// Reset readSeeker.
	readSeeker.Seek(0, 0)
	wc, err = c.RemoteWriter(ctx, peer, noHandoff, isolation, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyAndClose(wc, readSeeker); err != nil {
		t.Fatal(err)
	}

	if writeCounts[d.GetHash()] != 1 {
		t.Fatalf("Snitch cache was written to, but digest already existed.")
	}
}

func TestWriteAlreadyExistsAC(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	writeCounts := make(map[string]int, 0)
	sc := snitchCache{te.GetCache(), writeCounts}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, &sc, peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}

	waitUntilServerIsAlive(peer)

	testSize := int64(10000000)
	d, readSeeker := testdigest.NewRandomDigestReader(t, testSize)

	isolation := &dcpb.Isolation{
		CacheType: dcpb.Isolation_ACTION_CACHE,
	}

	// Remote-write the random bytes to the cache (with a prefix).
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, isolation, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyAndClose(wc, readSeeker); err != nil {
		t.Fatal(err)
	}

	if writeCounts[d.GetHash()] != 1 {
		t.Fatalf("Snitch cache was not written to. It should have been.")
	}

	// Reset readSeeker.
	readSeeker.Seek(0, 0)
	wc, err = c.RemoteWriter(ctx, peer, noHandoff, isolation, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyAndClose(wc, readSeeker); err != nil {
		t.Fatal(err)
	}

	if writeCounts[d.GetHash()] != 2 {
		t.Fatalf("Snitch cache should have been written to twice.")
	}
}

func TestReader(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	randomSrc := &randomDataMaker{rand.NewSource(time.Now().Unix())}
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}

	for _, testSize := range testSizes {
		remoteInstanceName := fmt.Sprintf("prefix/%d", testSize)
		isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE, RemoteInstanceName: remoteInstanceName}

		// Read some random bytes.
		buf := new(bytes.Buffer)
		io.CopyN(buf, randomSrc, testSize)
		readSeeker := bytes.NewReader(buf.Bytes())

		// Compute a digest for the random bytes.
		d, err := digest.Compute(readSeeker)
		if err != nil {
			t.Fatal(err)
		}
		readSeeker.Seek(0, 0)

		// Set the random bytes in the cache (with a prefix)
		cache, err := te.GetCache().WithIsolation(ctx, interfaces.CASCacheType, remoteInstanceName)
		require.NoError(t, err)
		err = cache.Set(ctx, d, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		// Remote-read the random bytes back.
		r, err := c.RemoteReader(ctx, peer, isolation, d, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		d2 := testdigest.ReadDigestAndClose(t, r)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Digest uploaded %q != %q downloaded", d.GetHash(), d2.GetHash())
		}
	}
}

func TestReadOffsetLimit(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	require.NoError(t, err)

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	size := int64(10)
	d, buf := testdigest.NewRandomDigestBuf(t, size)
	err = te.GetCache().Set(ctx, d, buf)
	require.NoError(t, err)

	offset := int64(2)
	limit := int64(3)
	isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE}
	reader, err := c.RemoteReader(ctx, peer, isolation, d, offset, limit)
	require.NoError(t, err)

	readBuf := make([]byte, size)
	n, err := reader.Read(readBuf)
	require.EqualValues(t, limit, n)
	require.Equal(t, buf[offset:offset+limit], readBuf[:limit])
}

func TestWriter(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}

	waitUntilServerIsAlive(peer)

	randomSrc := &randomDataMaker{rand.NewSource(time.Now().Unix())}
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}

	for _, testSize := range testSizes {
		remoteInstanceName := fmt.Sprintf("prefix/%d", testSize)
		isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE, RemoteInstanceName: remoteInstanceName}

		// Read some random bytes.
		buf := new(bytes.Buffer)
		io.CopyN(buf, randomSrc, testSize)
		readSeeker := bytes.NewReader(buf.Bytes())

		// Compute a digest for the random bytes.
		d, err := digest.Compute(readSeeker)
		if err != nil {
			t.Fatal(err)
		}
		readSeeker.Seek(0, 0)

		// Remote-write the random bytes to the cache (with a prefix).
		wc, err := c.RemoteWriter(ctx, peer, noHandoff, isolation, d)
		if err != nil {
			t.Fatal(err)
		}
		if err := copyAndClose(wc, readSeeker); err != nil {
			t.Fatal(err)
		}

		// Read the bytes back directly from the cache and check that
		// they match..
		cache, err := te.GetCache().WithIsolation(ctx, interfaces.CASCacheType, remoteInstanceName)
		require.NoError(t, err)
		r, err := cache.Reader(ctx, d, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		d2 := testdigest.ReadDigestAndClose(t, r)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Digest uploaded %q != %q downloaded", d.GetHash(), d2.GetHash())
		}
	}
}

func TestWriteAlreadyExists(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	writeCounts := make(map[string]int, 0)
	sc := snitchCache{te.GetCache(), writeCounts}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, &sc, peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}

	waitUntilServerIsAlive(peer)

	testSize := int64(10000000)
	d, readSeeker := testdigest.NewRandomDigestReader(t, testSize)
	remoteInstanceName := ""
	isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE, RemoteInstanceName: remoteInstanceName}

	// Remote-write the random bytes to the cache (with a prefix).
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, isolation, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyAndClose(wc, readSeeker); err != nil {
		t.Fatal(err)
	}

	if writeCounts[d.GetHash()] != 1 {
		t.Fatalf("Snitch cache was not written to. It should have been.")
	}

	// Reset readSeeker.
	readSeeker.Seek(0, 0)
	wc, err = c.RemoteWriter(ctx, peer, noHandoff, isolation, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyAndClose(wc, readSeeker); err != nil {
		t.Fatal(err)
	}

	if writeCounts[d.GetHash()] != 1 {
		t.Fatalf("Snitch cache was written to, but digest already existed.")
	}
}

func TestContains(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	randomSrc := &randomDataMaker{rand.NewSource(time.Now().Unix())}
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}

	for _, testSize := range testSizes {
		remoteInstanceName := fmt.Sprintf("prefix/%d", testSize)
		isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE, RemoteInstanceName: remoteInstanceName}

		// Read some random bytes.
		buf := new(bytes.Buffer)
		io.CopyN(buf, randomSrc, testSize)
		readSeeker := bytes.NewReader(buf.Bytes())

		// Compute a digest for the random bytes.
		d, err := digest.Compute(readSeeker)
		if err != nil {
			t.Fatal(err)
		}

		// Set the random bytes in the cache (with a prefix)
		cache, err := te.GetCache().WithIsolation(ctx, interfaces.CASCacheType, remoteInstanceName)
		require.NoError(t, err)
		err = cache.Set(ctx, d, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		// Ensure key exists.
		ok, err := c.RemoteContains(ctx, peer, isolation, d)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("Digest %q was uploaded but is not contained in cache", d.GetHash())
		}

		// Delete the key.
		err = cache.Delete(ctx, d)
		if err != nil {
			t.Fatal(err)
		}

		// Ensure it no longer exists.
		ok, err = c.RemoteContains(ctx, peer, isolation, d)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatalf("Digest %q was removed but is still contained in cache", d.GetHash())
		}
	}
}

func TestOversizeBlobs(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}

	waitUntilServerIsAlive(peer)

	randomSrc := &randomDataMaker{rand.NewSource(time.Now().Unix())}
	testSizes := []int64{
		1, 10, 100, 1000, 10000, 1000000, 10000000,
	}

	for _, testSize := range testSizes {
		remoteInstanceName := fmt.Sprintf("prefix/%d", testSize)
		isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE, RemoteInstanceName: remoteInstanceName}

		// Read some random bytes.
		buf := new(bytes.Buffer)
		io.CopyN(buf, randomSrc, testSize)
		readSeeker := bytes.NewReader(buf.Bytes())

		// Compute a digest for the random bytes.
		d, err := digest.Compute(readSeeker)
		if err != nil {
			t.Fatal(err)
		}

		// Now tack on a little bit of "extra" data.
		buf.Write([]byte("overload"))
		readSeeker = bytes.NewReader(buf.Bytes())

		// Remote-write the random bytes to the cache (with a prefix).
		wc, err := c.RemoteWriter(ctx, peer, noHandoff, isolation, d)
		if err != nil {
			t.Fatal(err)
		}
		if err := copyAndClose(wc, readSeeker); err != nil {
			t.Fatal(err)
		}

		// Ensure that the bytes remotely read back match the
		// bytes that were uploaded, even though they are keyed
		// under a different digest.
		readSeeker.Seek(0, 0)
		d1, err := digest.Compute(readSeeker)
		if err != nil {
			t.Fatal(err)
		}

		// Remote-read the random bytes back.
		r, err := c.RemoteReader(ctx, peer, isolation, d, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		d2 := testdigest.ReadDigestAndClose(t, r)
		if d1.GetHash() != d2.GetHash() {
			t.Fatalf("Digest of uploaded contents %q != %q downloaded contents", d.GetHash(), d2.GetHash())
		}
	}
}

func TestFindMissing(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := net.JoinHostPort("localhost", fmt.Sprintf("%d", testport.FindFree(t)))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error starting cache proxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	randomSrc := &randomDataMaker{rand.NewSource(time.Now().Unix())}

	type testCase struct {
		numExistingDigests int
		numMissingDigests  int
	}

	for _, tc := range []testCase{
		{numExistingDigests: 1, numMissingDigests: 0},
		{numExistingDigests: 10, numMissingDigests: 1},
		{numExistingDigests: 100, numMissingDigests: 10},
		{numExistingDigests: 1000, numMissingDigests: 10},
		{numExistingDigests: 10000, numMissingDigests: 10},
	} {
		remoteInstanceName := fmt.Sprintf("prefix/%d", tc.numExistingDigests)
		isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE, RemoteInstanceName: remoteInstanceName}

		existingDigests := make([]*repb.Digest, 0, tc.numExistingDigests)
		for i := 0; i < tc.numExistingDigests; i++ {
			// Read some random bytes.
			buf := new(bytes.Buffer)
			io.CopyN(buf, randomSrc, 100)
			readSeeker := bytes.NewReader(buf.Bytes())

			// Compute a digest for the random bytes.
			d, err := digest.Compute(readSeeker)
			if err != nil {
				t.Fatal(err)
			}
			existingDigests = append(existingDigests, d)
			// Set the random bytes in the cache (with a prefix)
			cache, err := te.GetCache().WithIsolation(ctx, interfaces.CASCacheType, remoteInstanceName)
			require.NoError(t, err)
			err = cache.Set(ctx, d, buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
		}

		var missingDigests []*repb.Digest
		for i := 0; i < tc.numMissingDigests; i++ {
			d, _ := testdigest.NewRandomDigestBuf(t, 1000)
			missingDigests = append(missingDigests, d)
		}

		remoteMissing, err := c.RemoteFindMissing(ctx, peer, isolation, append(existingDigests, missingDigests...))
		require.NoError(t, err)
		require.ElementsMatch(t, remoteMissing, missingDigests)
	}
}

func TestGetMulti(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	randomSrc := &randomDataMaker{rand.NewSource(time.Now().Unix())}
	testSizes := []int{
		1, 10, 100, 1000, 10000,
	}

	for _, numDigests := range testSizes {
		remoteInstanceName := fmt.Sprintf("prefix/%d", numDigests)
		isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE, RemoteInstanceName: remoteInstanceName}

		digests := make([]*repb.Digest, 0, numDigests)
		for i := 0; i < numDigests; i++ {
			// Read some random bytes.
			buf := new(bytes.Buffer)
			io.CopyN(buf, randomSrc, 100)
			readSeeker := bytes.NewReader(buf.Bytes())

			// Compute a digest for the random bytes.
			d, err := digest.Compute(readSeeker)
			if err != nil {
				t.Fatal(err)
			}
			digests = append(digests, d)
			// Set the random bytes in the cache (with a prefix)
			cache, err := te.GetCache().WithIsolation(ctx, interfaces.CASCacheType, remoteInstanceName)
			require.NoError(t, err)
			err = cache.Set(ctx, d, buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
		}

		// Ensure key exists.
		gotMap, err := c.RemoteGetMulti(ctx, peer, isolation, digests)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range digests {
			buf, ok := gotMap[d]
			if !ok || int64(len(buf)) != d.GetSizeBytes() {
				t.Fatalf("Digest %q was uploaded but is not contained in cache", d.GetHash())
			}
		}
	}
}

func TestEmptyRead(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	remoteInstanceName := "null"
	isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE, RemoteInstanceName: remoteInstanceName}
	d := &repb.Digest{
		Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		SizeBytes: 0,
	}
	cache, err := te.GetCache().WithIsolation(ctx, interfaces.CASCacheType, remoteInstanceName)
	require.NoError(t, err)
	err = cache.Set(ctx, d, []byte{})
	if err != nil {
		t.Fatal(err)
	}

	r, err := c.RemoteReader(ctx, peer, isolation, d, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = r.Read(nil)
	if err != io.EOF {
		t.Fatal(err)
	}

}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Fatalf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	remoteInstanceName := "remote/instance"
	isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE, RemoteInstanceName: remoteInstanceName}

	// Write to the cache (with a prefix)
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	cache, err := te.GetCache().WithIsolation(ctx, interfaces.CASCacheType, remoteInstanceName)
	if err != nil {
		t.Fatal(err)
	}
	err = cache.Set(ctx, d, buf)
	if err != nil {
		t.Fatal(err)
	}
	exists, err := c.RemoteContains(ctx, peer, isolation, d)
	require.NoError(t, err)
	require.True(t, exists)

	err = c.RemoteDelete(ctx, peer, isolation, d)
	require.NoError(t, err)

	// Ensure it no longer exists
	exists, err = c.RemoteContains(ctx, peer, isolation, d)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestMetadata(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Fatalf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	remoteInstanceName := "remote/instance"
	isolation := &dcpb.Isolation{CacheType: dcpb.Isolation_CAS_CACHE, RemoteInstanceName: remoteInstanceName}
	cache, err := te.GetCache().WithIsolation(ctx, interfaces.CASCacheType, remoteInstanceName)

	// Write to the cache
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	if err != nil {
		t.Fatal(err)
	}
	err = cache.Set(ctx, d, buf)
	if err != nil {
		t.Fatal(err)
	}

	// Verify cacheproxy returns same metadata as underlying cache
	cacheproxyMetadata, err := c.Metadata(ctx, &dcpb.MetadataRequest{
		Isolation: isolation,
		Key: &dcpb.Key{
			Key:       d.GetHash(),
			SizeBytes: d.GetSizeBytes(),
		},
	})
	if err != nil {
		t.Fatalf("Error fetching metadata from cacheproxy: %s", err)
	}
	cacheMetadata, err := cache.Metadata(ctx, d)
	if err != nil {
		t.Fatalf("Error fetching metadata from underlying cache: %s", err)
	}
	require.NoError(t, err)
	require.Equal(t, cacheMetadata.SizeBytes, cacheproxyMetadata.SizeBytes)
	require.Equal(t, cacheMetadata.LastAccessTimeUsec, cacheproxyMetadata.LastAccessUsec)
	require.Equal(t, cacheMetadata.LastModifyTimeUsec, cacheproxyMetadata.LastModifyUsec)
}
