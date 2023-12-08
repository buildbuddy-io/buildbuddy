package cacheproxy_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cacheproxy"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/docker/go-units"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	dcpb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	noHandoff = ""

	// Keep under the limit of ~4MB (save 256KB).
	// (Match the readBufSizeBytes in byte_stream_server.go)
	readBufSizeBytes = (1024 * 1024 * 4) - (1024 * 256)
)

var (
	emptyUserMap = testauth.TestUsers()
)

func getTestEnv(t testing.TB, users map[string]interfaces.UserInfo) *testenv.TestEnv {
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
	for {
		if _, err := io.CopyN(wc, r, readBufSizeBytes); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
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
	d, err := digest.Compute(readSeeker, repb.DigestFunction_SHA256)
	if err != nil {
		t.Fatal(err)
	}
	readSeeker.Seek(0, 0)

	instanceName := "foo"
	rn := &rspb.ResourceName{
		Digest:       d,
		CacheType:    rspb.CacheType_CAS,
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

func (s *snitchCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
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
	rn, buf := testdigest.RandomCASResourceBuf(t, testSize)
	readSeeker := bytes.NewReader(buf)

	// Remote-write the random bytes to the cache (with a prefix).
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, rn)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyAndClose(wc, readSeeker); err != nil {
		t.Fatal(err)
	}

	if writeCounts[rn.GetDigest().GetHash()] != 1 {
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

	if writeCounts[rn.GetDigest().GetHash()] != 1 {
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
	rn, buf := testdigest.RandomACResourceBuf(t, testSize)
	readSeeker := bytes.NewReader(buf)

	// Remote-write the random bytes to the cache (with a prefix).
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, rn)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyAndClose(wc, readSeeker); err != nil {
		t.Fatal(err)
	}

	if writeCounts[rn.GetDigest().GetHash()] != 1 {
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

	if writeCounts[rn.GetDigest().GetHash()] != 2 {
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
		d, err := digest.Compute(readSeeker, repb.DigestFunction_SHA256)
		if err != nil {
			t.Fatal(err)
		}
		rn := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_CAS,
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
	r, buf := testdigest.RandomCASResourceBuf(t, size)
	err = te.GetCache().Set(ctx, r, buf)
	require.NoError(t, err)

	offset := int64(2)
	limit := int64(3)
	reader, err := c.RemoteReader(ctx, peer, r, offset, limit)
	require.NoError(t, err)

	readBuf := make([]byte, size)
	n, err := io.ReadFull(reader, readBuf)
	require.Error(t, err)
	require.Equal(t, "unexpected EOF", err.Error())
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
		d, err := digest.Compute(readSeeker, repb.DigestFunction_SHA256)
		if err != nil {
			t.Fatal(err)
		}
		rn := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_CAS,
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
	rn, buf := testdigest.RandomCASResourceBuf(t, testSize)
	readSeeker := bytes.NewReader(buf)

	// Remote-write the random bytes to the cache (with a prefix).
	wc, err := c.RemoteWriter(ctx, peer, noHandoff, rn)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyAndClose(wc, readSeeker); err != nil {
		t.Fatal(err)
	}

	if writeCounts[rn.GetDigest().GetHash()] != 1 {
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

	if writeCounts[rn.GetDigest().GetHash()] != 1 {
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

		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		resourceName := digest.ResourceNameFromProto(rn)
		resourceName.SetCompressor(tc.writeCompression)
		writeRN := resourceName.ToProto()
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

		readResource := digest.ResourceNameFromProto(rn)
		readResource.SetCompressor(tc.readCompression)
		readRN := readResource.ToProto()
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
		d, err := digest.Compute(readSeeker, repb.DigestFunction_SHA256)
		if err != nil {
			t.Fatal(err)
		}
		r := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_CAS,
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
		d, err := digest.Compute(readSeeker, repb.DigestFunction_SHA256)
		if err != nil {
			t.Fatal(err)
		}
		rn := &rspb.ResourceName{
			Digest:       d,
			CacheType:    rspb.CacheType_CAS,
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
		d1, err := digest.Compute(readSeeker, repb.DigestFunction_SHA256)
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
		isolation := &dcpb.Isolation{CacheType: rspb.CacheType_CAS, RemoteInstanceName: remoteInstanceName}

		existingDigests := make([]*rspb.ResourceName, 0, tc.numExistingDigests)
		for i := 0; i < tc.numExistingDigests; i++ {
			// Read some random bytes.
			buf := new(bytes.Buffer)
			io.CopyN(buf, randomSrc, 100)
			readSeeker := bytes.NewReader(buf.Bytes())

			// Compute a digest for the random bytes.
			d, err := digest.Compute(readSeeker, repb.DigestFunction_SHA256)
			if err != nil {
				t.Fatal(err)
			}
			r := &rspb.ResourceName{
				Digest:       d,
				CacheType:    rspb.CacheType_CAS,
				InstanceName: remoteInstanceName,
			}
			existingDigests = append(existingDigests, r)
			// Set the random bytes in the cache (with a prefix)
			err = te.GetCache().Set(ctx, r, buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
		}

		var missingResources []*rspb.ResourceName
		var missingDigests []*repb.Digest
		for i := 0; i < tc.numMissingDigests; i++ {
			r, _ := testdigest.NewRandomResourceAndBuf(t, 1000, rspb.CacheType_CAS, remoteInstanceName)
			missingResources = append(missingResources, r)
			missingDigests = append(missingDigests, r.GetDigest())
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
		isolation := &dcpb.Isolation{CacheType: rspb.CacheType_CAS, RemoteInstanceName: remoteInstanceName}

		digests := make([]*rspb.ResourceName, 0, numDigests)
		for i := 0; i < numDigests; i++ {
			// Read some random bytes.
			buf := new(bytes.Buffer)
			io.CopyN(buf, randomSrc, 100)
			readSeeker := bytes.NewReader(buf.Bytes())

			// Compute a digest for the random bytes.
			d, err := digest.Compute(readSeeker, repb.DigestFunction_SHA256)
			if err != nil {
				t.Fatal(err)
			}
			r := &rspb.ResourceName{
				Digest:       d,
				CacheType:    rspb.CacheType_CAS,
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
	rn := &rspb.ResourceName{
		Digest:       d,
		CacheType:    rspb.CacheType_CAS,
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
	require.NoError(t, err)
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

	// Write to the cache (with a prefix)
	r, buf := testdigest.RandomCASResourceBuf(t, 100)
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

	isolation := &dcpb.Isolation{CacheType: rspb.CacheType_CAS}
	// Write to the cache
	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	err = te.GetCache().Set(ctx, r, buf)
	if err != nil {
		t.Fatal(err)
	}

	// Verify cacheproxy returns same metadata as underlying cache
	cacheproxyMetadata, err := c.Metadata(ctx, &dcpb.MetadataRequest{
		Isolation: isolation,
		Key: &dcpb.Key{
			Key:       r.GetDigest().GetHash(),
			SizeBytes: r.GetDigest().GetSizeBytes(),
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

func copyChunked(t testing.TB, w interfaces.CommittedWriteCloser, data []byte, chunkSize int64) {
	for len(data) > 0 {
		if chunkSize > int64(len(data)) {
			chunkSize = int64(len(data))
		}
		_, err := w.Write(data[:chunkSize])
		require.NoError(t, err)
		data = data[chunkSize:]
	}
	err := w.Commit()
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)
}

func BenchmarkWrite(b *testing.B) {
	flags.Set(b, "app.log_level", "error")
	log.Configure()

	digestSizes := []int64{
		128, 16384, 16_777_216,
	}

	for _, digestSize := range digestSizes {
		for chunkSize := digestSize / 4; chunkSize <= digestSize; chunkSize += digestSize / 4 {
			b.Run(fmt.Sprintf("digest%s_chunk%s", units.BytesSize(float64(digestSize)), units.BytesSize(float64(chunkSize))), func(b *testing.B) {
				b.ReportAllocs()
				ctx := context.Background()
				te := getTestEnv(b, emptyUserMap)

				ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
				require.NoError(b, err)

				peer := fmt.Sprintf("localhost:%d", testport.FindFree(b))
				c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)
				err = c.StartListening()
				require.NoError(b, err)

				waitUntilServerIsAlive(peer)

				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					b.StopTimer()
					rn, buf := testdigest.RandomCASResourceBuf(b, digestSize)
					b.StartTimer()
					wc, err := c.RemoteWriter(ctx, peer, noHandoff, rn)
					require.NoError(b, err)
					copyChunked(b, wc, buf, chunkSize)
					require.NoError(b, err)
				}
			})
		}
	}
}

func BenchmarkRead(b *testing.B) {
	flags.Set(b, "app.log_level", "error")
	log.Configure()
	testSizes := []int64{
		128, 16384, 16_777_216,
	}
	randomSrc := &randomDataMaker{rand.NewSource(time.Now().Unix())}

	for _, testSize := range testSizes {
		b.Run(fmt.Sprintf("digest%s", units.BytesSize(float64(testSize))), func(b *testing.B) {
			b.ReportAllocs()
			ctx := context.Background()
			te := getTestEnv(b, emptyUserMap)
			peer := fmt.Sprintf("localhost:%d", testport.FindFree(b))
			c := cacheproxy.NewCacheProxy(te, te.GetCache(), peer)

			ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
			require.NoError(b, err)
			remoteInstanceName := fmt.Sprintf("prefix/%d", testSize)
			err = c.StartListening()
			require.NoError(b, err)
			waitUntilServerIsAlive(peer)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				buf := new(bytes.Buffer)
				io.CopyN(buf, randomSrc, testSize)
				// Read some random bytes.
				readSeeker := bytes.NewReader(buf.Bytes())
				// Compute a digest for the random bytes.
				d, err := digest.Compute(readSeeker, repb.DigestFunction_SHA256)
				require.NoError(b, err)
				rn := &rspb.ResourceName{
					Digest:       d,
					CacheType:    rspb.CacheType_CAS,
					InstanceName: remoteInstanceName,
				}
				readSeeker.Seek(0, 0)
				// Set the random bytes in the cache (with a prefix)
				err = te.GetCache().Set(ctx, rn, buf.Bytes())
				require.NoError(b, err)
				b.StartTimer()
				// Remote-read the random bytes back.
				r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
				require.NoError(b, err)
				out := testdigest.ReadDigestAndClose(b, r)
				runtime.KeepAlive(out)
			}
		})
	}
}
