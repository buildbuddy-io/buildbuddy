package cacheproxy_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cacheproxy"
	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

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

func copyAndClose(wc interfaces.CommittedWriteCloser, r io.Reader) error {
	if _, err := io.Copy(wc, r); err != nil {
		return err
	}
	if err := wc.Commit(); err != nil {
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
	rn := &resource.ResourceName{
		Digest:       d,
		CacheType:    resource.CacheType_CAS,
		InstanceName: instanceName,
	}
	// Set the random bytes in the cache (with a prefix)
	err = te.GetCache().Set(ctx, rn, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	// Remote-read the random bytes back.
	r, err := c.RemoteReader(ctx, peer, rn, d.GetSizeBytes(), 0)
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

func (s *snitchCache) Writer(ctx context.Context, r *resource.ResourceName) (interfaces.CommittedWriteCloser, error) {
	wc, err := s.Cache.Writer(ctx, r)
	if err != nil {
		return nil, err
	}
	s.writeCount[r.GetDigest().GetHash()] += 1
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
	rn := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}

	// Remote-write the random bytes to the cache (with a prefix).
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, rn)
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
	wc, err = c.RemoteWriter(ctx, peer, noHandoff, rn)
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
	rn := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_AC,
	}

	// Remote-write the random bytes to the cache (with a prefix).
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, rn)
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
	wc, err = c.RemoteWriter(ctx, peer, noHandoff, rn)
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

		// Read some random bytes.
		buf := new(bytes.Buffer)
		io.CopyN(buf, randomSrc, testSize)
		readSeeker := bytes.NewReader(buf.Bytes())

		// Compute a digest for the random bytes.
		d, err := digest.Compute(readSeeker)
		if err != nil {
			t.Fatal(err)
		}
		rn := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: remoteInstanceName,
		}
		readSeeker.Seek(0, 0)

		// Set the random bytes in the cache (with a prefix)
		err = te.GetCache().Set(ctx, rn, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		// Remote-read the random bytes back.
		r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
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
	r := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}
	err = te.GetCache().Set(ctx, r, buf)
	require.NoError(t, err)

	offset := int64(2)
	limit := int64(3)
	reader, err := c.RemoteReader(ctx, peer, r, offset, limit)
	require.NoError(t, err)

	readBuf := make([]byte, size)
	n, err := io.ReadFull(reader, readBuf)
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

		// Read some random bytes.
		buf := new(bytes.Buffer)
		io.CopyN(buf, randomSrc, testSize)
		readSeeker := bytes.NewReader(buf.Bytes())

		// Compute a digest for the random bytes.
		d, err := digest.Compute(readSeeker)
		if err != nil {
			t.Fatal(err)
		}
		rn := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: remoteInstanceName,
		}
		readSeeker.Seek(0, 0)

		// Remote-write the random bytes to the cache (with a prefix).
		wc, err := c.RemoteWriter(ctx, peer, noHandoff, rn)
		if err != nil {
			t.Fatal(err)
		}
		if err := copyAndClose(wc, readSeeker); err != nil {
			t.Fatal(err)
		}

		// Read the bytes back directly from the cache and check that
		// they match..
		r, err := te.GetCache().Reader(ctx, rn, 0, 0)
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
	rn := &resource.ResourceName{
		Digest:    d,
		CacheType: resource.CacheType_CAS,
	}

	// Remote-write the random bytes to the cache (with a prefix).
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, rn)
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
	wc, err = c.RemoteWriter(ctx, peer, noHandoff, rn)
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

func TestReadWrite_Compressed(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	testCases := []struct {
		name             string
		writeCompression repb.Compressor_Value
		readCompression  repb.Compressor_Value
	}{
		{
			name:             "Write compressed, read compressed",
			writeCompression: repb.Compressor_ZSTD,
			readCompression:  repb.Compressor_ZSTD,
		},
		{
			name:             "Write compressed, read decompressed",
			writeCompression: repb.Compressor_ZSTD,
			readCompression:  repb.Compressor_IDENTITY,
		},
		{
			name:             "Write decompressed, read decompressed",
			writeCompression: repb.Compressor_IDENTITY,
			readCompression:  repb.Compressor_IDENTITY,
		},
		{
			name:             "Write decompressed, read compressed",
			writeCompression: repb.Compressor_IDENTITY,
			readCompression:  repb.Compressor_ZSTD,
		},
	}

	for _, tc := range testCases {
		peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
		te.SetCache(&testcompression.CompressionCache{Cache: te.GetCache()})
		c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
		if err := c.StartListening(); err != nil {
			t.Fatalf("Error setting up cacheproxy: %s", err)
		}
		waitUntilServerIsAlive(peer)

		d, buf := testdigest.NewRandomDigestBuf(t, 100)
		writeRN := &resource.ResourceName{
			Digest:     d,
			CacheType:  resource.CacheType_CAS,
			Compressor: tc.writeCompression,
		}
		compressedBuf := compression.CompressZstd(nil, buf)

		wc, err := c.RemoteWriter(ctx, peer, noHandoff, writeRN)
		require.NoError(t, err)

		bufToWrite := buf
		if tc.writeCompression == repb.Compressor_ZSTD {
			bufToWrite = compressedBuf
		}
		_, err = wc.Write(bufToWrite)
		require.NoError(t, err)
		err = wc.Commit()
		require.NoError(t, err)
		err = wc.Close()
		require.NoError(t, err)

		readRN := &resource.ResourceName{
			Digest:     d,
			CacheType:  resource.CacheType_CAS,
			Compressor: tc.readCompression,
		}
		r, err := c.RemoteReader(ctx, peer, readRN, 0, 0)
		require.NoError(t, err)

		expected := buf
		if tc.readCompression == repb.Compressor_ZSTD {
			expected = compressedBuf
		}
		readBuf, err := ioutil.ReadAll(r)
		require.NoError(t, err)
		require.True(t, bytes.Equal(expected, readBuf))
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

		// Read some random bytes.
		buf := new(bytes.Buffer)
		io.CopyN(buf, randomSrc, testSize)
		readSeeker := bytes.NewReader(buf.Bytes())

		// Compute a digest for the random bytes.
		d, err := digest.Compute(readSeeker)
		if err != nil {
			t.Fatal(err)
		}
		r := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: remoteInstanceName,
		}

		// Set the random bytes in the cache (with a prefix)
		err = te.GetCache().Set(ctx, r, buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		// Ensure key exists.
		ok, err := c.RemoteContains(ctx, peer, r)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("Digest %q was uploaded but is not contained in cache", d.GetHash())
		}

		// Delete the key.
		err = te.GetCache().Delete(ctx, r)
		if err != nil {
			t.Fatal(err)
		}

		// Ensure it no longer exists.
		ok, err = c.RemoteContains(ctx, peer, r)
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

		// Read some random bytes.
		buf := new(bytes.Buffer)
		io.CopyN(buf, randomSrc, testSize)
		readSeeker := bytes.NewReader(buf.Bytes())

		// Compute a digest for the random bytes.
		d, err := digest.Compute(readSeeker)
		if err != nil {
			t.Fatal(err)
		}
		rn := &resource.ResourceName{
			Digest:       d,
			CacheType:    resource.CacheType_CAS,
			InstanceName: remoteInstanceName,
		}

		// Now tack on a little bit of "extra" data.
		buf.Write([]byte("overload"))
		readSeeker = bytes.NewReader(buf.Bytes())

		// Remote-write the random bytes to the cache (with a prefix).
		wc, err := c.RemoteWriter(ctx, peer, noHandoff, rn)
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
		r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
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
		isolation := &dcpb.Isolation{CacheType: resource.CacheType_CAS, RemoteInstanceName: remoteInstanceName}

		existingDigests := make([]*resource.ResourceName, 0, tc.numExistingDigests)
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
			r := &resource.ResourceName{
				Digest:       d,
				CacheType:    resource.CacheType_CAS,
				InstanceName: remoteInstanceName,
			}
			existingDigests = append(existingDigests, r)
			// Set the random bytes in the cache (with a prefix)
			err = te.GetCache().Set(ctx, r, buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
		}

		var missingResources []*resource.ResourceName
		var missingDigests []*repb.Digest
		for i := 0; i < tc.numMissingDigests; i++ {
			d, _ := testdigest.NewRandomDigestBuf(t, 1000)
			r := &resource.ResourceName{
				Digest:       d,
				CacheType:    resource.CacheType_CAS,
				InstanceName: remoteInstanceName,
			}
			missingResources = append(missingResources, r)
			missingDigests = append(missingDigests, d)
		}

		remoteMissing, err := c.RemoteFindMissing(ctx, peer, isolation, append(existingDigests, missingResources...))
		require.NoError(t, err)
		require.Empty(t, cmp.Diff(missingDigests, remoteMissing, protocmp.Transform()))
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
		isolation := &dcpb.Isolation{CacheType: resource.CacheType_CAS, RemoteInstanceName: remoteInstanceName}

		digests := make([]*resource.ResourceName, 0, numDigests)
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
			r := &resource.ResourceName{
				Digest:       d,
				CacheType:    resource.CacheType_CAS,
				InstanceName: remoteInstanceName,
			}
			digests = append(digests, r)
			// Set the random bytes in the cache (with a prefix)
			err = te.GetCache().Set(ctx, r, buf.Bytes())
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
			buf, ok := gotMap[d.GetDigest()]
			if !ok || int64(len(buf)) != d.GetDigest().GetSizeBytes() {
				t.Fatalf("Digest %q was uploaded but is not contained in cache", d.GetDigest().GetHash())
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
	d := &repb.Digest{
		Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		SizeBytes: 0,
	}
	rn := &resource.ResourceName{
		Digest:       d,
		CacheType:    resource.CacheType_CAS,
		InstanceName: remoteInstanceName,
	}
	err = te.GetCache().Set(ctx, rn, []byte{})
	if err != nil {
		t.Fatal(err)
	}

	r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	n, err := io.ReadFull(r, nil)
	if n != 0 {
		t.Fatal("Empty read failed")
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

	// Write to the cache (with a prefix)
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:       d,
		CacheType:    resource.CacheType_CAS,
		InstanceName: remoteInstanceName,
	}
	err = te.GetCache().Set(ctx, r, buf)
	if err != nil {
		t.Fatal(err)
	}
	exists, err := c.RemoteContains(ctx, peer, r)
	require.NoError(t, err)
	require.True(t, exists)

	err = c.RemoteDelete(ctx, peer, r)
	require.NoError(t, err)

	// Ensure it no longer exists
	exists, err = c.RemoteContains(ctx, peer, r)
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
	isolation := &dcpb.Isolation{CacheType: resource.CacheType_CAS, RemoteInstanceName: remoteInstanceName}

	// Write to the cache
	d, buf := testdigest.NewRandomDigestBuf(t, 100)
	r := &resource.ResourceName{
		Digest:       d,
		CacheType:    resource.CacheType_CAS,
		InstanceName: remoteInstanceName,
	}
	if err != nil {
		t.Fatal(err)
	}
	err = te.GetCache().Set(ctx, r, buf)
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
		Resource: r,
	})
	if err != nil {
		t.Fatalf("Error fetching metadata from cacheproxy: %s", err)
	}
	cacheMetadata, err := te.GetCache().Metadata(ctx, r)
	if err != nil {
		t.Fatalf("Error fetching metadata from underlying cache: %s", err)
	}
	require.NoError(t, err)
	require.Equal(t, cacheMetadata.StoredSizeBytes, cacheproxyMetadata.StoredSizeBytes)
	require.Equal(t, cacheMetadata.DigestSizeBytes, cacheproxyMetadata.DigestSizeBytes)
	require.Equal(t, cacheMetadata.LastAccessTimeUsec, cacheproxyMetadata.LastAccessUsec)
	require.Equal(t, cacheMetadata.LastModifyTimeUsec, cacheproxyMetadata.LastModifyUsec)
}
