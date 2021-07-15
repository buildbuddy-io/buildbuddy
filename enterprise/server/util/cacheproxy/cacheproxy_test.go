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
	dcpb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

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

func TestReader(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
		prefix := fmt.Sprintf("prefix/%d", testSize)

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
		err = te.GetCache().WithPrefix(prefix).Set(ctx, d, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		// Remote-read the random bytes back.
		r, err := c.RemoteReader(ctx, peer, prefix, &dcpb.Isolation{}, d, 0)
		if err != nil {
			t.Fatal(err)
		}
		d2 := testdigest.ReadDigestAndClose(t, r)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Digest uploaded %q != %q downloaded", d.GetHash(), d2.GetHash())
		}
	}
}

func TestWriter(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
		prefix := fmt.Sprintf("prefix/%d", testSize)

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
		wc, err := c.RemoteWriter(ctx, peer, noHandoff, prefix, &dcpb.Isolation{}, d)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(wc, readSeeker)
		if err != nil {
			t.Fatal(err)
		}
		if err := wc.Close(); err != nil {
			t.Fatal(err)
		}

		// Read the bytes back directly from the cache and check that
		// they match..
		r, err := te.GetCache().WithPrefix(prefix).Reader(ctx, d, 0)
		if err != nil {
			t.Fatal(err)
		}
		d2 := testdigest.ReadDigestAndClose(t, r)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Digest uploaded %q != %q downloaded", d.GetHash(), d2.GetHash())
		}
	}
}

type snitchCache struct {
	interfaces.Cache
	writeCount map[string]int
}

func (s *snitchCache) WithPrefix(prefix string) interfaces.Cache {
	return &snitchCache{
		s.Cache.WithPrefix(prefix),
		s.writeCount,
	}
}

func (s *snitchCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
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

func TestWriteAlreadyExists(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	writeCounts := make(map[string]int, 0)
	sc := snitchCache{te.GetCache(), writeCounts}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
	c := cacheproxy.NewCacheProxy(te, &sc, peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}

	waitUntilServerIsAlive(peer)

	testSize := int64(10000000)
	d, readSeeker := testdigest.NewRandomDigestReader(t, testSize)
	prefix := ""

	// Remote-write the random bytes to the cache (with a prefix).
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, prefix, &dcpb.Isolation{}, d)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(wc, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}

	if writeCounts[d.GetHash()] != 1 {
		t.Fatalf("Snitch cache was not written to. It should have been.")
	}

	// Reset readSeeker.
	readSeeker.Seek(0, 0)
	wc, err = c.RemoteWriter(ctx, peer, noHandoff, prefix, &dcpb.Isolation{}, d)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(wc, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}

	if writeCounts[d.GetHash()] != 1 {
		t.Fatalf("Snitch cache was written to, but digest already existed.")
	}
}

func TestContains(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
		prefix := fmt.Sprintf("prefix/%d", testSize)

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
		err = te.GetCache().WithPrefix(prefix).Set(ctx, d, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		// Ensure key exists.
		ok, err := c.RemoteContains(ctx, peer, prefix, &dcpb.Isolation{}, d)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("Digest %q was uploaded but is not contained in cache", d.GetHash())
		}

		// Delete the key.
		err = te.GetCache().WithPrefix(prefix).Delete(ctx, d)
		if err != nil {
			t.Fatal(err)
		}

		// Ensure it no longer exists.
		ok, err = c.RemoteContains(ctx, peer, prefix, &dcpb.Isolation{}, d)
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
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
		prefix := fmt.Sprintf("prefix/%d", testSize)

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
		wc, err := c.RemoteWriter(ctx, peer, noHandoff, prefix, &dcpb.Isolation{}, d)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(wc, readSeeker)
		if err != nil {
			t.Fatal(err)
		}
		if err := wc.Close(); err != nil {
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
		r, err := c.RemoteReader(ctx, peer, prefix, &dcpb.Isolation{}, d, 0)
		if err != nil {
			t.Fatal(err)
		}
		d2 := testdigest.ReadDigestAndClose(t, r)
		if d1.GetHash() != d2.GetHash() {
			t.Fatalf("Digest of uploaded contents %q != %q downloaded contents", d.GetHash(), d2.GetHash())
		}
	}
}

func TestContainsMulti(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := net.JoinHostPort("localhost", fmt.Sprintf("%d", app.FreePort(t)))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error starting cache proxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	randomSrc := &randomDataMaker{rand.NewSource(time.Now().Unix())}
	testSizes := []int{
		1, 10, 100, 1000, 10000,
	}

	for _, numDigests := range testSizes {
		prefix := fmt.Sprintf("prefix/%d", numDigests)

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
			err = te.GetCache().WithPrefix(prefix).Set(ctx, d, buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
		}

		// Ensure key exists.
		foundMap, err := c.RemoteContainsMulti(ctx, peer, prefix, &dcpb.Isolation{}, digests)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range digests {
			exists, ok := foundMap[d]
			if !ok || !exists {
				t.Fatalf("Digest %q was uploaded but is not contained in cache", d.GetHash())
			}
		}
	}
}

func TestGetMulti(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
		prefix := fmt.Sprintf("prefix/%d", numDigests)

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
			err = te.GetCache().WithPrefix(prefix).Set(ctx, d, buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
		}

		// Ensure key exists.
		gotMap, err := c.RemoteGetMulti(ctx, peer, prefix, &dcpb.Isolation{}, digests)
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
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up cacheproxy: %s", err)
	}
	waitUntilServerIsAlive(peer)

	prefix := "null"
	d := &repb.Digest{
		Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		SizeBytes: 0,
	}
	err = te.GetCache().WithPrefix(prefix).Set(ctx, d, []byte{})
	if err != nil {
		t.Fatal(err)
	}

	// Remote-read the random bytes back.
	_, err = c.RemoteReader(ctx, peer, prefix, &dcpb.Isolation{}, d, 0)
	if err != io.EOF {
		t.Fatal(err)
	}
}

func TestReader_Isolation(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
		r, err := c.RemoteReader(ctx, peer, remoteInstanceName, isolation, d, 0)
		if err != nil {
			t.Fatal(err)
		}
		d2 := testdigest.ReadDigestAndClose(t, r)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Digest uploaded %q != %q downloaded", d.GetHash(), d2.GetHash())
		}
	}
}

func TestWriter_Isolation(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
		wc, err := c.RemoteWriter(ctx, peer, noHandoff, remoteInstanceName, isolation, d)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(wc, readSeeker)
		if err != nil {
			t.Fatal(err)
		}
		if err := wc.Close(); err != nil {
			t.Fatal(err)
		}

		// Read the bytes back directly from the cache and check that
		// they match..
		cache, err := te.GetCache().WithIsolation(ctx, interfaces.CASCacheType, remoteInstanceName)
		require.NoError(t, err)
		r, err := cache.Reader(ctx, d, 0)
		if err != nil {
			t.Fatal(err)
		}
		d2 := testdigest.ReadDigestAndClose(t, r)
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Digest uploaded %q != %q downloaded", d.GetHash(), d2.GetHash())
		}
	}
}

func TestWriteAlreadyExists_Isolation(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	writeCounts := make(map[string]int, 0)
	sc := snitchCache{te.GetCache(), writeCounts}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, remoteInstanceName, isolation, d)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(wc, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}

	if writeCounts[d.GetHash()] != 1 {
		t.Fatalf("Snitch cache was not written to. It should have been.")
	}

	// Reset readSeeker.
	readSeeker.Seek(0, 0)
	wc, err = c.RemoteWriter(ctx, peer, noHandoff, remoteInstanceName, isolation, d)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(wc, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}

	if writeCounts[d.GetHash()] != 1 {
		t.Fatalf("Snitch cache was written to, but digest already existed.")
	}
}

func TestContains_Isolation(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
		ok, err := c.RemoteContains(ctx, peer, remoteInstanceName, isolation, d)
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
		ok, err = c.RemoteContains(ctx, peer, remoteInstanceName, isolation, d)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatalf("Digest %q was removed but is still contained in cache", d.GetHash())
		}
	}
}

func TestOversizeBlobs_Isolation(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
		wc, err := c.RemoteWriter(ctx, peer, noHandoff, remoteInstanceName, isolation, d)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(wc, readSeeker)
		if err != nil {
			t.Fatal(err)
		}
		if err := wc.Close(); err != nil {
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
		r, err := c.RemoteReader(ctx, peer, remoteInstanceName, isolation, d, 0)
		if err != nil {
			t.Fatal(err)
		}
		d2 := testdigest.ReadDigestAndClose(t, r)
		if d1.GetHash() != d2.GetHash() {
			t.Fatalf("Digest of uploaded contents %q != %q downloaded contents", d.GetHash(), d2.GetHash())
		}
	}
}

func TestContainsMulti_Isolation(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := net.JoinHostPort("localhost", fmt.Sprintf("%d", app.FreePort(t)))
	c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error starting cache proxy: %s", err)
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
		foundMap, err := c.RemoteContainsMulti(ctx, peer, remoteInstanceName, isolation, digests)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range digests {
			exists, ok := foundMap[d]
			if !ok || !exists {
				t.Fatalf("Digest %q was uploaded but is not contained in cache", d.GetHash())
			}
		}
	}
}

func TestGetMulti_Isolation(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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
		gotMap, err := c.RemoteGetMulti(ctx, peer, remoteInstanceName, isolation, digests)
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

func TestEmptyRead_Isolation(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
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

	// Remote-read the random bytes back.
	_, err = c.RemoteReader(ctx, peer, remoteInstanceName, isolation, d, 0)
	if err != io.EOF {
		t.Fatal(err)
	}
}
