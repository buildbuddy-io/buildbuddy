package distributed_client_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/distributed_client"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/docker/go-units"
	"github.com/google/go-cmp/cmp"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/testing/protocmp"

	dcpb "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache"
	refpb "github.com/buildbuddy-io/buildbuddy/proto/reference"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

// spyCache records the resource compressor passed to Reader/GetMulti so tests
// can assert on what was seen at the server side of the wire.
type spyCache struct {
	interfaces.Cache
	mu                  sync.Mutex
	readerCompressors   []repb.Compressor_Value
	getMultiCompressors map[string]repb.Compressor_Value
}

func (s *spyCache) Reader(ctx context.Context, rn *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.readerCompressors = append(s.readerCompressors, rn.GetCompressor())
	s.mu.Unlock()
	return s.Cache.Reader(ctx, rn, offset, limit)
}

func (s *spyCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	s.mu.Lock()
	if s.getMultiCompressors == nil {
		s.getMultiCompressors = map[string]repb.Compressor_Value{}
	}
	for _, r := range resources {
		s.getMultiCompressors[r.GetDigest().GetHash()] = r.GetCompressor()
	}
	s.mu.Unlock()
	return s.Cache.GetMulti(ctx, resources)
}

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
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, users))
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
		conn, err := net.DialTimeout("tcp", addr, 10*time.Millisecond)
		if err == nil {
			conn.Close()
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	writeCounts := make(map[string]int, 0)
	sc := snitchCache{te.GetCache(), writeCounts}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, &sc, peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	writeCounts := make(map[string]int, 0)
	sc := snitchCache{te.GetCache(), writeCounts}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, &sc, peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	writeCounts := make(map[string]int, 0)
	sc := snitchCache{te.GetCache(), writeCounts}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, &sc, peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
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
		c := distributed_client.New(te, te.GetCache(), peer)
		if err := c.StartListening(); err != nil {
			t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := net.JoinHostPort("localhost", fmt.Sprintf("%d", testport.FindFree(t)))
	c := distributed_client.New(te, te.GetCache(), peer)
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

		remoteMissing, err := c.RemoteFindMissing(ctx, peer, append(existingDigests, missingResources...))
		require.NoError(t, err)
		require.Empty(t, cmp.Diff(missingDigests, remoteMissing, protocmp.Transform()))
	}
}

func TestGetMulti(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
	}
	waitUntilServerIsAlive(peer)

	randomSrc := &randomDataMaker{rand.NewSource(time.Now().Unix())}
	testSizes := []int{
		1, 10, 100, 1000, 10000,
	}

	for _, numDigests := range testSizes {
		remoteInstanceName := fmt.Sprintf("prefix/%d", numDigests)

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
		gotMap, err := c.RemoteGetMulti(ctx, peer, digests)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Fatalf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	if err != nil {
		t.Fatalf("error attaching user prefix: %v", err)
	}

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	if err := c.StartListening(); err != nil {
		t.Fatalf("Error setting up distributed_client: %s", err)
	}
	waitUntilServerIsAlive(peer)

	// Write to the cache
	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	err = te.GetCache().Set(ctx, r, buf)
	if err != nil {
		t.Fatal(err)
	}

	// Verify cacheproxy returns same metadata as underlying cache
	cacheproxyMetadata, err := c.Metadata(ctx, &dcpb.MetadataRequest{
		Resource: r,
	})
	if err != nil {
		t.Fatalf("Error fetching metadata from distributed_client: %s", err)
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

func TestGetWithMetadata(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	require.NoError(t, c.StartListening())
	waitUntilServerIsAlive(peer)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	require.NoError(t, te.GetCache().Set(ctx, r, buf))

	// Server handler: data + every metadata field matches the underlying cache.
	rsp, err := c.GetWithMetadata(ctx, &dcpb.GetWithMetadataRequest{Resource: r})
	require.NoError(t, err)
	require.Equal(t, buf, rsp.GetData())

	cacheMD, err := te.GetCache().Metadata(ctx, r)
	require.NoError(t, err)
	require.Equal(t, cacheMD.StoredSizeBytes, rsp.GetMetadata().GetStoredSizeBytes())
	require.Equal(t, cacheMD.DigestSizeBytes, rsp.GetMetadata().GetDigestSizeBytes())
	require.Equal(t, cacheMD.LastAccessTimeUsec, rsp.GetMetadata().GetLastAccessUsec())
	require.Equal(t, cacheMD.LastModifyTimeUsec, rsp.GetMetadata().GetLastModifyUsec())
}

func TestRemoteGetWithMetadata(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	require.NoError(t, c.StartListening())
	waitUntilServerIsAlive(peer)

	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	require.NoError(t, te.GetCache().Set(ctx, r, buf))

	// Client-side: data and metadata round-trip through the RPC.
	data, md, err := c.RemoteGetWithMetadata(ctx, peer, r)
	require.NoError(t, err)
	require.Equal(t, buf, data)

	cacheMD, err := te.GetCache().Metadata(ctx, r)
	require.NoError(t, err)
	require.Equal(t, cacheMD.StoredSizeBytes, md.StoredSizeBytes)
	require.Equal(t, cacheMD.DigestSizeBytes, md.DigestSizeBytes)
	require.Equal(t, cacheMD.LastAccessTimeUsec, md.LastAccessTimeUsec)
	require.Equal(t, cacheMD.LastModifyTimeUsec, md.LastModifyTimeUsec)
}

func TestRemoteGetWithMetadata_NotFound(t *testing.T) {
	ctx := context.Background()
	te := getTestEnv(t, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	require.NoError(t, c.StartListening())
	waitUntilServerIsAlive(peer)

	// Resource was never written; expect NotFound.
	r, _ := testdigest.RandomCASResourceBuf(t, 100)
	_, _, err = c.RemoteGetWithMetadata(ctx, peer, r)
	require.True(t, status.IsNotFoundError(err), "expected NotFound, got: %v", err)
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

	digestSizes := []int64{128, 16384, 16_777_216}

	ctx := context.Background()
	te := getTestEnv(b, emptyUserMap)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(b, err)

	peer := fmt.Sprintf("localhost:%d", testport.FindFree(b))
	c := distributed_client.New(te, te.GetCache(), peer)
	err = c.StartListening()
	require.NoError(b, err)
	waitUntilServerIsAlive(peer)

	for _, digestSize := range digestSizes {
		for chunkSize := digestSize / 4; chunkSize <= digestSize; chunkSize += digestSize / 4 {
			b.Run(fmt.Sprintf("digest%s_chunk%s", units.BytesSize(float64(digestSize)), units.BytesSize(float64(chunkSize))), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					rn, buf := testdigest.RandomCASResourceBuf(b, digestSize)
					b.StartTimer()
					wc, err := c.RemoteWriter(ctx, peer, noHandoff, rn)
					require.NoError(b, err)
					copyChunked(b, wc, buf, chunkSize)
					require.NoError(b, err)
					b.StopTimer()
					// Delete the resource so that background eviction doesn't
					// affect performance. This also ensures that we don't get
					// any accidental collisions with existing resources.
					require.NoError(b, c.RemoteDelete(ctx, peer, rn))
					b.StartTimer()
				}
			})
		}
	}
}

// hangingWriteServer is a DistributedCache server whose Write RPC hangs
// forever, used to test that the client's write timeout fires.
type hangingWriteServer struct {
	dcpb.UnimplementedDistributedCacheServer
}

func (s *hangingWriteServer) Write(stream dcpb.DistributedCache_WriteServer) error {
	// Block until the client gives up.
	<-stream.Context().Done()
	return stream.Context().Err()
}

func startHangingServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	dcpb.RegisterDistributedCacheServer(srv, &hangingWriteServer{})
	t.Cleanup(srv.Stop)
	go srv.Serve(lis)
	return lis.Addr().String()
}

func TestWriteTimeout(t *testing.T) {
	flags.Set(t, "cache.distributed_cache.peer_write_timeout", time.Millisecond)

	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)

	hangingPeer := startHangingServer(t)
	waitUntilServerIsAlive(hangingPeer)
	localPeer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), localPeer)
	require.NoError(t, c.StartListening())
	waitUntilServerIsAlive(localPeer)

	// Use a size just large enough to flush through the double buffer to
	// the underlying stream, where writes will block on the hanging server.
	testSize := int64(3 * readBufSizeBytes)
	rn, buf := testdigest.RandomCASResourceBuf(t, testSize)
	wc, err := c.RemoteWriter(ctx, hangingPeer, noHandoff, rn)
	require.NoError(t, err)
	defer wc.Close()

	_, err = wc.Write(buf)
	require.Error(t, err)
	require.True(t, isCanceledOrDeadlineExceeded(err), "expected cancelled or deadline exceeded, got %s", err)
}

func TestCommitTimeout(t *testing.T) {
	flags.Set(t, "cache.distributed_cache.peer_write_timeout", time.Millisecond)

	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)

	hangingPeer := startHangingServer(t)
	waitUntilServerIsAlive(hangingPeer)
	localPeer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), localPeer)
	require.NoError(t, c.StartListening())
	waitUntilServerIsAlive(localPeer)

	rn, _ := testdigest.RandomCASResourceBuf(t, 4)
	wc, err := c.RemoteWriter(ctx, hangingPeer, noHandoff, rn)
	require.NoError(t, err)
	defer wc.Close()

	err = wc.Commit()
	require.Error(t, err)
	require.True(t, isCanceledOrDeadlineExceeded(err), "expected cancelled or deadline exceeded, got %s", err)
}

func isCanceledOrDeadlineExceeded(err error) bool {
	return status.IsCanceledError(err) || status.IsDeadlineExceededError(err) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
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
			c := distributed_client.New(te, te.GetCache(), peer)

			ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
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

func setupCompressedReadProxy(t *testing.T, enabled bool) (*testenv.TestEnv, *distributed_client.Proxy, *spyCache, string, context.Context) {
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)
	spy := &spyCache{Cache: &testcompression.CompressionCache{Cache: te.GetCache()}}
	te.SetCache(spy)
	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, te.GetCache(), peer)
	c.SetEnableCompressedReads(enabled)
	require.NoError(t, c.StartListening())
	waitUntilServerIsAlive(peer)
	return te, c, spy, peer, ctx
}

func TestRemoteReader_PullsCompressedFromPeer(t *testing.T) {
	cases := []struct {
		name             string
		sizeBytes        int64
		enabled          bool
		offset, limit    int64
		wantSeenAtServer repb.Compressor_Value
	}{
		{"large_flag_on_rewrites", 200, true, 0, 0, repb.Compressor_ZSTD},
		{"small_flag_on_skips", 50, true, 0, 0, repb.Compressor_IDENTITY},
		{"large_flag_off_skips", 200, false, 0, 0, repb.Compressor_IDENTITY},
		{"large_offset_skips", 200, true, 10, 0, repb.Compressor_IDENTITY},
		{"large_limit_skips", 200, true, 0, 50, repb.Compressor_IDENTITY},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			te, c, spy, peer, ctx := setupCompressedReadProxy(t, tc.enabled)
			rn, buf := testdigest.RandomCASResourceBuf(t, tc.sizeBytes)
			require.NoError(t, te.GetCache().Set(ctx, rn, buf))

			r, err := c.RemoteReader(ctx, peer, rn, tc.offset, tc.limit)
			require.NoError(t, err)
			got, err := io.ReadAll(r)
			require.NoError(t, err)
			require.NoError(t, r.Close())

			expected := buf[tc.offset:]
			if tc.limit != 0 && int64(len(expected)) > tc.limit {
				expected = expected[:tc.limit]
			}
			require.Equal(t, expected, got)
			require.Equal(t, []repb.Compressor_Value{tc.wantSeenAtServer}, spy.readerCompressors)
		})
	}
}

func TestRemoteGetMulti_PullsCompressedFromPeer(t *testing.T) {
	te, c, spy, peer, ctx := setupCompressedReadProxy(t, true /*=enableCompressedReads*/)

	smallRN, smallBuf := testdigest.RandomCASResourceBuf(t, 50)
	largeRN, largeBuf := testdigest.RandomCASResourceBuf(t, 200)
	require.NoError(t, te.GetCache().Set(ctx, smallRN, smallBuf))
	require.NoError(t, te.GetCache().Set(ctx, largeRN, largeBuf))

	got, err := c.RemoteGetMulti(ctx, peer, []*rspb.ResourceName{smallRN, largeRN})
	require.NoError(t, err)
	require.Equal(t, smallBuf, got[smallRN.GetDigest()])
	require.Equal(t, largeBuf, got[largeRN.GetDigest()])

	require.Equal(t, repb.Compressor_IDENTITY, spy.getMultiCompressors[smallRN.GetDigest().GetHash()])
	require.Equal(t, repb.Compressor_ZSTD, spy.getMultiCompressors[largeRN.GetDigest().GetHash()])
}

func TestRemoteGetMulti_MultipleCompressedBlobs(t *testing.T) {
	te, c, _, peer, ctx := setupCompressedReadProxy(t, true /*=enableCompressedReads*/)

	const n = 4
	rns := make([]*rspb.ResourceName, n)
	bufs := make([][]byte, n)
	for i := 0; i < n; i++ {
		rns[i], bufs[i] = testdigest.RandomCASResourceBuf(t, 200+int64(i)*50)
		require.NoError(t, te.GetCache().Set(ctx, rns[i], bufs[i]))
	}

	got, err := c.RemoteGetMulti(ctx, peer, rns)
	require.NoError(t, err)
	for i, rn := range rns {
		require.Equalf(t, bufs[i], got[rn.GetDigest()], "blob %d mismatch", i)
	}
}

// referenceReadServer is a DistributedCache server that answers every Read
// with a ReadResponse carrying the configured reference, followed by one data
// message per configured chunk (the verification-mode response shape). It
// records the compressor of the last requested resource so tests can assert
// on what was seen at the server side of the wire.
type referenceReadServer struct {
	dcpb.UnimplementedDistributedCacheServer
	ref        *refpb.Reference
	dataChunks [][]byte

	mu             sync.Mutex
	lastCompressor repb.Compressor_Value
}

func (s *referenceReadServer) Read(req *dcpb.ReadRequest, stream dcpb.DistributedCache_ReadServer) error {
	s.mu.Lock()
	s.lastCompressor = req.GetResource().GetCompressor()
	s.mu.Unlock()
	if err := stream.Send(&dcpb.ReadResponse{Reference: s.ref}); err != nil {
		return err
	}
	for _, chunk := range s.dataChunks {
		if err := stream.Send(&dcpb.ReadResponse{Data: chunk}); err != nil {
			return err
		}
	}
	return nil
}

func (s *referenceReadServer) LastCompressor() repb.Compressor_Value {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastCompressor
}

func startReferenceReadServer(t *testing.T, ref *refpb.Reference) string {
	addr, _ := startReferenceReadServerWithRecorder(t, ref)
	return addr
}

func startReferenceReadServerWithRecorder(t *testing.T, ref *refpb.Reference) (string, *referenceReadServer) {
	return startVerifyingReadServer(t, ref, nil)
}

// startVerifyingReadServer starts a server that responds with ref followed by
// the given data chunks.
func startVerifyingReadServer(t *testing.T, ref *refpb.Reference, dataChunks [][]byte) (string, *referenceReadServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	rrs := &referenceReadServer{ref: ref, dataChunks: dataChunks}
	dcpb.RegisterDistributedCacheServer(srv, rrs)
	t.Cleanup(srv.Stop)
	go srv.Serve(lis)
	waitUntilServerIsAlive(lis.Addr().String())
	return lis.Addr().String(), rrs
}

// fakeReferenceCache implements interfaces.ReferenceCache over an in-memory
// map of blob name -> stored bytes, standing in for shared storage. It records
// the arguments of the last Dereference call so tests can assert on what the
// client passed through; compressor reconciliation itself is the real
// implementation's job and is tested in pebble_cache_test.go.
type fakeReferenceCache struct {
	interfaces.Cache
	blobs map[string][]byte

	mu           sync.Mutex
	lastResource *rspb.ResourceName
	lastOffset   int64
	lastLimit    int64
}

func (c *fakeReferenceCache) ReadReference(ctx context.Context, r *rspb.ResourceName) (*refpb.Reference, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (c *fakeReferenceCache) CreateReference(ctx context.Context, r *rspb.ResourceName) (interfaces.ReferenceWriter, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (c *fakeReferenceCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	return compressor == repb.Compressor_IDENTITY || compressor == repb.Compressor_ZSTD
}

func (c *fakeReferenceCache) Dereference(ctx context.Context, ref *refpb.Reference, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	c.mu.Lock()
	c.lastResource = r
	c.lastOffset = offset
	c.lastLimit = limit
	c.mu.Unlock()
	data, ok := c.blobs[ref.GetMetadata().GetStorageMetadata().GetGcsMetadata().GetBlobName()]
	if !ok {
		return nil, status.NotFoundError("blob not found")
	}
	if offset > int64(len(data)) {
		offset = int64(len(data))
	}
	data = data[offset:]
	if limit != 0 && limit < int64(len(data)) {
		data = data[:limit]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// WriteReference is unused on the read path, but the client only dereferences
// if its cache implements the full interfaces.ReferenceCache.
func (c *fakeReferenceCache) WriteReference(ctx context.Context, ref *refpb.Reference, r *rspb.ResourceName, mustClone bool) error {
	return status.UnimplementedError("not implemented")
}

func (c *fakeReferenceCache) LastDereference() (*rspb.ResourceName, int64, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastResource, c.lastOffset, c.lastLimit
}

func makeReference(rn *rspb.ResourceName, blobName string, compressor repb.Compressor_Value) *refpb.Reference {
	return &refpb.Reference{
		Metadata: &sgpb.FileMetadata{
			FileRecord: &sgpb.FileRecord{
				Isolation: &sgpb.Isolation{
					CacheType:          rn.GetCacheType(),
					RemoteInstanceName: rn.GetInstanceName(),
				},
				Digest:         rn.GetDigest(),
				DigestFunction: rn.GetDigestFunction(),
				Compressor:     compressor,
			},
			StorageMetadata: &sgpb.StorageMetadata{
				GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{BlobName: blobName},
			},
		},
	}
}

func newReferenceTestProxy(t *testing.T, te *testenv.TestEnv, blobs map[string][]byte) (*distributed_client.Proxy, *fakeReferenceCache) {
	t.Helper()
	localPeer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	cache := &fakeReferenceCache{Cache: te.GetCache(), blobs: blobs}
	c := distributed_client.New(te, cache, localPeer)
	require.NoError(t, c.StartListening())
	waitUntilServerIsAlive(localPeer)
	return c, cache
}

// readResponseCount returns the current value of the distributed cache read
// response counter for the given response type and status code.
func readResponseCount(t *testing.T, responseType, statusCode string) float64 {
	return testmetrics.CounterValueForLabels(t, metrics.DistributedCacheReadResponseCount, prometheus.Labels{
		metrics.DistributedCacheReadResponseType: responseType,
		metrics.StatusHumanReadableLabel:         statusCode,
	})
}

func TestRemoteReadReference(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)

	const blobName = "blobs/test-blob"
	rn, buf := testdigest.RandomCASResourceBuf(t, 100)

	t.Run("identity", func(t *testing.T) {
		peer := startReferenceReadServer(t, makeReference(rn, blobName, repb.Compressor_IDENTITY))
		c, _ := newReferenceTestProxy(t, te, map[string][]byte{blobName: buf})
		before := readResponseCount(t, "reference", "OK")
		r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, buf, got)
		require.Equal(t, before+1, readResponseCount(t, "reference", "OK"))
	})

	t.Run("range is forwarded to Dereference", func(t *testing.T) {
		peer := startReferenceReadServer(t, makeReference(rn, blobName, repb.Compressor_IDENTITY))
		c, fake := newReferenceTestProxy(t, te, map[string][]byte{blobName: buf})
		r, err := c.RemoteReader(ctx, peer, rn, 5, 10)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, buf[5:15], got)
		gotRN, gotOffset, gotLimit := fake.LastDereference()
		require.Equal(t, repb.Compressor_IDENTITY, gotRN.GetCompressor())
		require.Equal(t, int64(5), gotOffset)
		require.Equal(t, int64(10), gotLimit)
	})

	t.Run("requested compressor is forwarded to Dereference", func(t *testing.T) {
		peer := startReferenceReadServer(t, makeReference(rn, blobName, repb.Compressor_IDENTITY))
		c, fake := newReferenceTestProxy(t, te, map[string][]byte{blobName: buf})
		rnZstd := rn.CloneVT()
		rnZstd.Compressor = repb.Compressor_ZSTD
		r, err := c.RemoteReader(ctx, peer, rnZstd, 0, 0)
		require.NoError(t, err)
		// The fake serves the mapped bytes verbatim; the client must return
		// Dereference's reader directly without transcoding.
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, buf, got)
		gotRN, _, _ := fake.LastDereference()
		require.Equal(t, repb.Compressor_ZSTD, gotRN.GetCompressor())
	})

	t.Run("decompress transport rewrite", func(t *testing.T) {
		// With compressed reads enabled and no offset/limit, RemoteReader
		// rewrites the request to ZSTD for transport (the blob must be
		// larger than 100 bytes to qualify). On a reference response,
		// Dereference must see the caller's original IDENTITY resource,
		// not the transport-rewritten one.
		bigRN, bigBuf := testdigest.RandomCASResourceBuf(t, 200)
		peer, srv := startReferenceReadServerWithRecorder(t, makeReference(bigRN, blobName, repb.Compressor_ZSTD))
		c, fake := newReferenceTestProxy(t, te, map[string][]byte{blobName: bigBuf})
		c.SetEnableCompressedReads(true)
		r, err := c.RemoteReader(ctx, peer, bigRN, 0, 0)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, bigBuf, got)
		// Confirm the transport rewrite actually happened: the peer saw a
		// ZSTD request even though the caller asked for IDENTITY...
		require.Equal(t, repb.Compressor_ZSTD, srv.LastCompressor())
		// ...but Dereference saw the caller's original request.
		gotRN, _, _ := fake.LastDereference()
		require.Equal(t, repb.Compressor_IDENTITY, gotRN.GetCompressor())
	})

	t.Run("digest mismatch is rejected", func(t *testing.T) {
		otherRN, _ := testdigest.RandomCASResourceBuf(t, 100)
		peer := startReferenceReadServer(t, makeReference(otherRN, blobName, repb.Compressor_IDENTITY))
		c, _ := newReferenceTestProxy(t, te, map[string][]byte{blobName: buf})
		before := readResponseCount(t, "reference", "Internal")
		_, err := c.RemoteReader(ctx, peer, rn, 0, 0)
		require.Error(t, err)
		require.True(t, status.IsInternalError(err), "expected InternalError, got %s", err)
		require.Contains(t, err.Error(), "returned a reference for")
		require.Equal(t, before+1, readResponseCount(t, "reference", "Internal"))
	})

	t.Run("stored instance name may differ", func(t *testing.T) {
		// CAS entries are deduped across instance names, so the reference can
		// carry the first writer's instance name without identifying
		// different content.
		storedRN := rn.CloneVT()
		storedRN.InstanceName = "instance-at-first-write"
		peer := startReferenceReadServer(t, makeReference(storedRN, blobName, repb.Compressor_IDENTITY))
		c, _ := newReferenceTestProxy(t, te, map[string][]byte{blobName: buf})
		r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, buf, got)
	})

	t.Run("cache that cannot dereference is rejected", func(t *testing.T) {
		peer := startReferenceReadServer(t, makeReference(rn, blobName, repb.Compressor_IDENTITY))
		localPeer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
		c := distributed_client.New(te, te.GetCache(), localPeer)
		require.NoError(t, c.StartListening())
		waitUntilServerIsAlive(localPeer)
		_, err := c.RemoteReader(ctx, peer, rn, 0, 0)
		require.Error(t, err)
		require.True(t, status.IsFailedPreconditionError(err), "expected FailedPreconditionError, got %s", err)
	})

	t.Run("missing blob is not found", func(t *testing.T) {
		peer := startReferenceReadServer(t, makeReference(rn, "blobs/no-such-blob", repb.Compressor_IDENTITY))
		c, _ := newReferenceTestProxy(t, te, map[string][]byte{blobName: buf})
		before := readResponseCount(t, "reference", "NotFound")
		beforeOK := readResponseCount(t, "reference", "OK")
		_, err := c.RemoteReader(ctx, peer, rn, 0, 0)
		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err), "expected NotFoundError, got %s", err)
		// The failed dereference is recorded under its status code, not OK.
		require.Equal(t, before+1, readResponseCount(t, "reference", "NotFound"))
		require.Equal(t, beforeOK, readResponseCount(t, "reference", "OK"))
	})
}

// serverReferenceCache implements interfaces.ReferenceCache, returning canned
// references for configured digests and recording accepted reference writes.
type serverReferenceCache struct {
	interfaces.Cache
	refs  map[string]*refpb.Reference
	blobs map[string][]byte

	mu            sync.Mutex
	writtenRef    *refpb.Reference
	writtenRN     *rspb.ResourceName
	writtenCloned bool
	writeRefErr   error
}

func (c *serverReferenceCache) ReadReference(ctx context.Context, r *rspb.ResourceName) (*refpb.Reference, error) {
	if ref, ok := c.refs[r.GetDigest().GetHash()]; ok {
		return ref, nil
	}
	return nil, status.NotFoundError("no reference available")
}

func (c *serverReferenceCache) CreateReference(ctx context.Context, r *rspb.ResourceName) (interfaces.ReferenceWriter, error) {
	return nil, status.UnimplementedError("not implemented")
}

// Dereference serves the configured blob bytes, standing in for shared
// storage. The read server never dereferences, but the write server does
// when reference-write verification is enabled. Like the real
// implementation, it reconciles the reference's stored compressor with the
// compressor requested by r.
func (c *serverReferenceCache) Dereference(ctx context.Context, ref *refpb.Reference, r *rspb.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	data, ok := c.blobs[ref.GetMetadata().GetStorageMetadata().GetGcsMetadata().GetBlobName()]
	if !ok {
		return nil, status.NotFoundError("blob not found")
	}
	stored := ref.GetMetadata().GetFileRecord().GetCompressor()
	requested := r.GetCompressor()
	if stored == repb.Compressor_ZSTD && requested == repb.Compressor_IDENTITY {
		var err error
		data, err = compression.DecompressZstd(nil, data)
		if err != nil {
			return nil, err
		}
	} else if stored == repb.Compressor_IDENTITY && requested == repb.Compressor_ZSTD {
		data = compression.CompressZstd(nil, data)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (c *serverReferenceCache) WriteReference(ctx context.Context, ref *refpb.Reference, r *rspb.ResourceName, mustClone bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeRefErr != nil {
		return c.writeRefErr
	}
	// The server pools WriteRequest protos, so clone anything retained.
	c.writtenRef = ref.CloneVT()
	c.writtenRN = r.CloneVT()
	c.writtenCloned = mustClone
	return nil
}

func (c *serverReferenceCache) lastWriteReference() (*refpb.Reference, *rspb.ResourceName, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writtenRef, c.writtenRN, c.writtenCloned
}

func setReferenceReadExperiments(t *testing.T, te *testenv.TestEnv, readReferences bool, verifyReferences bool) {
	provider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"distributed_cache.read_gcs_references": {
			State:          memprovider.Enabled,
			DefaultVariant: "on",
			Variants:       map[string]any{"on": readReferences},
		},
		"distributed_cache.verify_read_gcs_references": {
			State:          memprovider.Enabled,
			DefaultVariant: "on",
			Variants:       map[string]any{"on": verifyReferences},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(provider))
	fp, err := experiments.NewFlagProvider("")
	require.NoError(t, err)
	te.SetExperimentFlagProvider(fp)
	// Restore the no-experiment-provider state when the (sub)test finishes,
	// so tests do not depend on their ordering. Note that openfeature's
	// provider is process-global state, so tests that use this helper must
	// not run in parallel.
	t.Cleanup(func() {
		te.SetExperimentFlagProvider(nil)
		require.NoError(t, openfeature.SetProviderAndWait(openfeature.NoopProvider{}))
	})
}

// readRawResponses reads rn from peer with a raw gRPC client, returning any
// received reference and the concatenated data bytes.
func readRawResponses(t *testing.T, peer string, rn *rspb.ResourceName) (*refpb.Reference, []byte) {
	t.Helper()
	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	client := dcpb.NewDistributedCacheClient(conn)
	stream, err := client.Read(context.Background(), &dcpb.ReadRequest{Resource: rn})
	require.NoError(t, err)
	var ref *refpb.Reference
	var data []byte
	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if rsp.GetReference() != nil {
			require.Nil(t, ref, "server sent more than one reference")
			require.Empty(t, data, "server sent a reference after data")
			ref = rsp.GetReference()
		}
		data = append(data, rsp.GetData()...)
	}
	return ref, data
}

func TestReadReferenceExperiments(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)

	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	noRefRN, noRefBuf := testdigest.RandomCASResourceBuf(t, 100)
	expectedRef := &refpb.Reference{
		Metadata: &sgpb.FileMetadata{
			FileRecord: &sgpb.FileRecord{Digest: rn.GetDigest()},
			StorageMetadata: &sgpb.StorageMetadata{
				GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{BlobName: "blobs/test-blob"},
			},
		},
	}
	cache := &serverReferenceCache{
		Cache: te.GetCache(),
		refs:  map[string]*refpb.Reference{rn.GetDigest().GetHash(): expectedRef},
	}
	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, cache, peer)
	require.NoError(t, c.StartListening())
	waitUntilServerIsAlive(peer)
	require.NoError(t, te.GetCache().Set(ctx, rn, buf))
	require.NoError(t, te.GetCache().Set(ctx, noRefRN, noRefBuf))

	t.Run("no experiment provider", func(t *testing.T) {
		ref, data := readRawResponses(t, peer, rn)
		require.Nil(t, ref)
		require.Equal(t, buf, data)
	})

	t.Run("experiments off", func(t *testing.T) {
		setReferenceReadExperiments(t, te, false, false)
		ref, data := readRawResponses(t, peer, rn)
		require.Nil(t, ref)
		require.Equal(t, buf, data)
	})

	t.Run("verify flag sends reference and bytes", func(t *testing.T) {
		setReferenceReadExperiments(t, te, false, true)
		ref, data := readRawResponses(t, peer, rn)
		require.Empty(t, cmp.Diff(expectedRef, ref, protocmp.Transform()))
		require.Equal(t, buf, data)
	})

	t.Run("read flag sends reference only", func(t *testing.T) {
		setReferenceReadExperiments(t, te, true, false)
		ref, data := readRawResponses(t, peer, rn)
		require.Empty(t, cmp.Diff(expectedRef, ref, protocmp.Transform()))
		require.Empty(t, data)
	})

	t.Run("read flag falls back to bytes when no reference can be minted", func(t *testing.T) {
		setReferenceReadExperiments(t, te, true, false)
		ref, data := readRawResponses(t, peer, noRefRN)
		require.Nil(t, ref)
		require.Equal(t, noRefBuf, data)
	})
}

// writeRawRequests writes the given requests to peer with a raw gRPC client
// and returns the server's response.
func writeRawRequests(t *testing.T, peer string, reqs []*dcpb.WriteRequest) (*dcpb.WriteResponse, error) {
	t.Helper()
	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	client := dcpb.NewDistributedCacheClient(conn)
	stream, err := client.Write(context.Background())
	require.NoError(t, err)
	for _, req := range reqs {
		require.NoError(t, stream.Send(req))
	}
	return stream.CloseAndRecv()
}

func TestWriteReferenceAccept(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)

	rn, _ := testdigest.RandomCASResourceBuf(t, 100)
	ref := makeReference(rn, "blobs/test-blob", repb.Compressor_IDENTITY)
	ref.Metadata.StoredSizeBytes = 42

	cache := &serverReferenceCache{Cache: te.GetCache()}
	peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	c := distributed_client.New(te, cache, peer)
	require.NoError(t, c.StartListening())
	waitUntilServerIsAlive(peer)

	t.Run("accepted", func(t *testing.T) {
		rsp, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{{
			Resource:    rn,
			Reference:   ref,
			FinishWrite: true,
		}})
		require.NoError(t, err)
		require.Equal(t, int64(42), rsp.GetCommittedSize())
		gotRef, gotRN, gotCloned := cache.lastWriteReference()
		require.Empty(t, cmp.Diff(ref, gotRef, protocmp.Transform()))
		require.Empty(t, cmp.Diff(rn, gotRN, protocmp.Transform()))
		require.False(t, gotCloned)
	})

	t.Run("must-be-cloned bit is passed through", func(t *testing.T) {
		_, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{{
			Resource:              rn,
			Reference:             ref,
			ReferenceMustBeCloned: true,
			FinishWrite:           true,
		}})
		require.NoError(t, err)
		_, _, gotCloned := cache.lastWriteReference()
		require.True(t, gotCloned)
	})

	t.Run("hinted handoff callback fires", func(t *testing.T) {
		var mu sync.Mutex
		var handoffPeer string
		var handoffRN *rspb.ResourceName
		c.SetHintedHandoffCallbackFunc(func(ctx context.Context, peer string, r *rspb.ResourceName) {
			mu.Lock()
			defer mu.Unlock()
			handoffPeer = peer
			handoffRN = r
		})
		t.Cleanup(func() { c.SetHintedHandoffCallbackFunc(nil) })
		_, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{{
			Resource:    rn,
			Reference:   ref,
			HandoffPeer: "some-peer",
			FinishWrite: true,
		}})
		require.NoError(t, err)
		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, "some-peer", handoffPeer)
		require.Empty(t, cmp.Diff(rn, handoffRN, protocmp.Transform()))
	})

	t.Run("cache write errors are returned", func(t *testing.T) {
		cache.mu.Lock()
		cache.writeRefErr = status.NotFoundError("backing object may have expired")
		cache.mu.Unlock()
		t.Cleanup(func() {
			cache.mu.Lock()
			cache.writeRefErr = nil
			cache.mu.Unlock()
		})
		_, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{{
			Resource:    rn,
			Reference:   ref,
			FinishWrite: true,
		}})
		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err), "expected NotFoundError, got %s", err)
	})

	t.Run("reference before the last message is ignored", func(t *testing.T) {
		earlyRN, earlyBuf := testdigest.RandomCASResourceBuf(t, 100)
		earlyRef := makeReference(earlyRN, "blobs/early-blob", repb.Compressor_IDENTITY)
		before := writeVerificationCounts(t)
		rsp, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{
			{Resource: earlyRN, Reference: earlyRef},
			{Resource: earlyRN, Data: earlyBuf, FinishWrite: true},
		})
		require.NoError(t, err)
		require.Equal(t, int64(len(earlyBuf)), rsp.GetCommittedSize())
		got, err := te.GetCache().Get(ctx, earlyRN)
		require.NoError(t, err)
		require.Equal(t, earlyBuf, got)
		// The early reference is neither installed nor verified.
		c.WaitForPendingVerificationsForTesting()
		_, gotRN, _ := cache.lastWriteReference()
		require.NotEqual(t, earlyRN.GetDigest().GetHash(), gotRN.GetDigest().GetHash())
		require.Equal(t, before, writeVerificationCounts(t))
	})

	t.Run("reference with data bytes writes the bytes", func(t *testing.T) {
		shadowRN, shadowBuf := testdigest.RandomCASResourceBuf(t, 100)
		shadowRef := makeReference(shadowRN, "blobs/shadow-blob", repb.Compressor_IDENTITY)
		rsp, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{{
			Resource:    shadowRN,
			Reference:   shadowRef,
			Data:        shadowBuf,
			FinishWrite: true,
		}})
		require.NoError(t, err)
		require.Equal(t, int64(len(shadowBuf)), rsp.GetCommittedSize())
		c.WaitForPendingVerificationsForTesting()
		// The bytes are the write; the reference is not installed.
		got, err := te.GetCache().Get(ctx, shadowRN)
		require.NoError(t, err)
		require.Equal(t, shadowBuf, got)
		_, gotRN, _ := cache.lastWriteReference()
		require.NotEqual(t, shadowRN.GetDigest().GetHash(), gotRN.GetDigest().GetHash())
	})

	t.Run("reference on the last message is verified, not installed", func(t *testing.T) {
		lateRN, lateBuf := testdigest.RandomCASResourceBuf(t, 100)
		lateRef := makeReference(lateRN, "blobs/late-blob", repb.Compressor_IDENTITY)
		before := writeVerificationCounts(t)
		rsp, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{
			{Resource: lateRN, Data: lateBuf[:50]},
			{Resource: lateRN, Reference: lateRef, Data: lateBuf[50:], FinishWrite: true},
		})
		require.NoError(t, err)
		require.Equal(t, int64(len(lateBuf)), rsp.GetCommittedSize())
		got, err := te.GetCache().Get(ctx, lateRN)
		require.NoError(t, err)
		require.Equal(t, lateBuf, got)
		// The reference is verified (the blob is not in shared storage, so
		// verification errors), but never installed.
		c.WaitForPendingVerificationsForTesting()
		_, gotRN, _ := cache.lastWriteReference()
		require.NotEqual(t, lateRN.GetDigest().GetHash(), gotRN.GetDigest().GetHash())
		after := writeVerificationCounts(t)
		require.Equal(t, before[distributed_client.VerificationError]+1, after[distributed_client.VerificationError])
		require.Equal(t, before[distributed_client.VerificationSuccess], after[distributed_client.VerificationSuccess])
		require.Equal(t, before[distributed_client.VerificationFailure], after[distributed_client.VerificationFailure])
	})

	t.Run("existing CAS digest still dedupes", func(t *testing.T) {
		existingRN, existingBuf := testdigest.RandomCASResourceBuf(t, 100)
		require.NoError(t, te.GetCache().Set(ctx, existingRN, existingBuf))
		_, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{{
			Resource:           existingRN,
			Reference:          makeReference(existingRN, "blobs/other-blob", repb.Compressor_IDENTITY),
			CheckAlreadyExists: true,
			FinishWrite:        true,
		}})
		require.Error(t, err)
		require.True(t, status.IsAlreadyExistsError(err), "expected AlreadyExistsError, got %s", err)
	})

	t.Run("cache that cannot accept references is rejected", func(t *testing.T) {
		plainPeer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
		plain := distributed_client.New(te, te.GetCache(), plainPeer)
		require.NoError(t, plain.StartListening())
		waitUntilServerIsAlive(plainPeer)
		_, err := writeRawRequests(t, plainPeer, []*dcpb.WriteRequest{{
			Resource:    rn,
			Reference:   ref,
			FinishWrite: true,
		}})
		require.Error(t, err)
		require.True(t, status.IsUnimplementedError(err), "expected UnimplementedError, got %s", err)
	})
}

func TestRemoteReadVerification(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)

	const blobName = "blobs/verified-blob"
	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	chunks := [][]byte{buf[:40], buf[40:]}

	// deltas returns the nonzero outcome-count changes between two
	// verificationCounts snapshots.
	deltas := func(before, after map[string]float64) map[string]float64 {
		d := map[string]float64{}
		for s, c := range after {
			if diff := c - before[s]; diff != 0 {
				d[s] = diff
			}
		}
		return d
	}

	t.Run("matching bytes", func(t *testing.T) {
		peer, _ := startVerifyingReadServer(t, makeReference(rn, blobName, repb.Compressor_IDENTITY), chunks)
		c, _ := newReferenceTestProxy(t, te, map[string][]byte{blobName: buf})
		beforeBytesOK := readResponseCount(t, "bytes", "OK")
		r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, beforeBytesOK+1, readResponseCount(t, "bytes", "OK"))
		require.Equal(t, buf, got)
	})

	t.Run("ranged", func(t *testing.T) {
		rangedChunks := [][]byte{buf[5:15]}
		peer, _ := startVerifyingReadServer(t, makeReference(rn, blobName, repb.Compressor_IDENTITY), rangedChunks)
		c, _ := newReferenceTestProxy(t, te, map[string][]byte{blobName: buf})
		r, err := c.RemoteReader(ctx, peer, rn, 5, 10)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, buf[5:15], got)
	})

	t.Run("mismatched reference bytes are non-fatal", func(t *testing.T) {
		_, otherBuf := testdigest.RandomCASResourceBuf(t, 100)
		peer, _ := startVerifyingReadServer(t, makeReference(rn, blobName, repb.Compressor_IDENTITY), chunks)
		c, _ := newReferenceTestProxy(t, te, map[string][]byte{blobName: otherBuf})
		r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		// The streamed bytes are authoritative.
		require.Equal(t, buf, got)
	})

	t.Run("mismatched reference digest is non-fatal", func(t *testing.T) {
		otherRN, _ := testdigest.RandomCASResourceBuf(t, 100)
		peer, _ := startVerifyingReadServer(t, makeReference(otherRN, blobName, repb.Compressor_IDENTITY), chunks)
		c, fake := newReferenceTestProxy(t, te, map[string][]byte{blobName: buf})
		before := verificationCounts(t)
		r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		// The streamed bytes are authoritative.
		require.Equal(t, buf, got)
		// The bad reference was counted as a failure and never dereferenced.
		require.Equal(t, map[string]float64{distributed_client.VerificationFailure: 1}, deltas(before, verificationCounts(t)))
		gotRN, _, _ := fake.LastDereference()
		require.Nil(t, gotRN)
	})

	t.Run("stored instance name difference does not fail verification", func(t *testing.T) {
		// CAS entries are deduped across instance names, so the reference can
		// carry the first writer's instance name without identifying
		// different content.
		storedRN := rn.CloneVT()
		storedRN.InstanceName = "instance-at-first-write"
		peer, _ := startVerifyingReadServer(t, makeReference(storedRN, blobName, repb.Compressor_IDENTITY), chunks)
		c, _ := newReferenceTestProxy(t, te, map[string][]byte{blobName: buf})
		before := verificationCounts(t)
		r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, buf, got)
		require.Equal(t, map[string]float64{distributed_client.VerificationSuccess: 1}, deltas(before, verificationCounts(t)))
	})

	t.Run("missing dereferencer is non-fatal", func(t *testing.T) {
		peer, _ := startVerifyingReadServer(t, makeReference(rn, blobName, repb.Compressor_IDENTITY), chunks)
		localPeer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
		c := distributed_client.New(te, te.GetCache(), localPeer)
		require.NoError(t, c.StartListening())
		waitUntilServerIsAlive(localPeer)
		r, err := c.RemoteReader(ctx, peer, rn, 0, 0)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, buf, got)
	})
}

// errorReadCloser fails every read with the given error.
type errorReadCloser struct {
	err error
}

func (e *errorReadCloser) Read(p []byte) (int, error) { return 0, e.err }
func (e *errorReadCloser) Close() error               { return nil }

// verificationCounts returns the current values of the reference verification
// counter, keyed by outcome, summed across error codes.
func verificationCounts(t *testing.T) map[string]float64 {
	counts := map[string]float64{}
	for _, v := range testmetrics.CounterValues(t, metrics.DistributedCacheReferenceVerificationCount) {
		counts[v.Labels[metrics.VerificationOutcomeLabel]] += v.Value
	}
	return counts
}

// writeVerificationCounts returns the current values of the reference write
// verification counter, keyed by outcome.
func writeVerificationCounts(t *testing.T) map[string]float64 {
	counts := map[string]float64{}
	for _, v := range testmetrics.CounterValues(t, metrics.DistributedCacheReferenceWriteVerificationCount) {
		counts[v.Labels[metrics.VerificationOutcomeLabel]] += v.Value
	}
	return counts
}

func TestWriteReferenceVerification(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)

	const blobName = "blobs/test-blob"
	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	ref := makeReference(rn, blobName, repb.Compressor_IDENTITY)

	newProxy := func(t *testing.T, blobs map[string][]byte) (string, *serverReferenceCache, *distributed_client.Proxy) {
		cache := &serverReferenceCache{Cache: te.GetCache(), blobs: blobs}
		peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
		c := distributed_client.New(te, cache, peer)
		require.NoError(t, c.StartListening())
		waitUntilServerIsAlive(peer)
		return peer, cache, c
	}
	// writeShadow writes rn's bytes with ref riding along on the final (and
	// only) message for verification.
	writeShadow := func(t *testing.T, peer string, rn *rspb.ResourceName, ref *refpb.Reference, data []byte) error {
		_, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{{
			Resource:    rn,
			Reference:   ref,
			Data:        data,
			FinishWrite: true,
		}})
		return err
	}
	// deltas returns the nonzero outcome-count changes between two
	// writeVerificationCounts snapshots.
	deltas := func(before, after map[string]float64) map[string]float64 {
		d := map[string]float64{}
		for s, v := range after {
			if v != before[s] {
				d[s] = v - before[s]
			}
		}
		return d
	}
	// assertBytesWritten asserts the byte write landed and no reference was
	// installed: the bytes must stay authoritative regardless of the
	// verification outcome.
	assertBytesWritten := func(t *testing.T, cache *serverReferenceCache, rn *rspb.ResourceName, data []byte) {
		got, err := te.GetCache().Get(ctx, rn)
		require.NoError(t, err)
		require.Equal(t, data, got)
		_, gotRN, _ := cache.lastWriteReference()
		require.Nil(t, gotRN)
	}

	t.Run("writes without a reference are not verified", func(t *testing.T) {
		peer, cache, proxy := newProxy(t, map[string][]byte{blobName: buf})
		before := writeVerificationCounts(t)
		_, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{{
			Resource:    rn,
			Data:        buf,
			FinishWrite: true,
		}})
		require.NoError(t, err)
		proxy.WaitForPendingVerificationsForTesting()
		require.Empty(t, deltas(before, writeVerificationCounts(t)))
		assertBytesWritten(t, cache, rn, buf)
	})

	t.Run("matching content", func(t *testing.T) {
		peer, cache, proxy := newProxy(t, map[string][]byte{blobName: buf})
		before := writeVerificationCounts(t)
		require.NoError(t, writeShadow(t, peer, rn, ref, buf))
		proxy.WaitForPendingVerificationsForTesting()
		require.Equal(t, map[string]float64{distributed_client.VerificationSuccess: 1}, deltas(before, writeVerificationCounts(t)))
		assertBytesWritten(t, cache, rn, buf)
	})

	t.Run("mismatched content does not affect the write", func(t *testing.T) {
		_, wrongBuf := testdigest.RandomCASResourceBuf(t, 100)
		peer, cache, proxy := newProxy(t, map[string][]byte{blobName: wrongBuf})
		before := writeVerificationCounts(t)
		require.NoError(t, writeShadow(t, peer, rn, ref, buf))
		proxy.WaitForPendingVerificationsForTesting()
		require.Equal(t, map[string]float64{distributed_client.VerificationFailure: 1}, deltas(before, writeVerificationCounts(t)))
		assertBytesWritten(t, cache, rn, buf)
	})

	t.Run("unverifiable content does not affect the write", func(t *testing.T) {
		peer, cache, proxy := newProxy(t, map[string][]byte{})
		before := writeVerificationCounts(t)
		require.NoError(t, writeShadow(t, peer, rn, ref, buf))
		proxy.WaitForPendingVerificationsForTesting()
		require.Equal(t, map[string]float64{distributed_client.VerificationError: 1}, deltas(before, writeVerificationCounts(t)))
		assertBytesWritten(t, cache, rn, buf)
	})

	t.Run("reference on the last message of a streamed write", func(t *testing.T) {
		peer, cache, proxy := newProxy(t, map[string][]byte{blobName: buf})
		before := writeVerificationCounts(t)
		_, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{
			{Resource: rn, Data: buf[:50]},
			{Resource: rn, Reference: ref, Data: buf[50:], FinishWrite: true},
		})
		require.NoError(t, err)
		proxy.WaitForPendingVerificationsForTesting()
		require.Equal(t, map[string]float64{distributed_client.VerificationSuccess: 1}, deltas(before, writeVerificationCounts(t)))
		assertBytesWritten(t, cache, rn, buf)
	})

	t.Run("reference on an earlier message is ignored", func(t *testing.T) {
		peer, cache, proxy := newProxy(t, map[string][]byte{blobName: buf})
		before := writeVerificationCounts(t)
		// A write's reference is not known until all of its bytes have been
		// seen, so only the final message's reference counts.
		_, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{
			{Resource: rn, Reference: ref, Data: buf[:50]},
			{Resource: rn, Data: buf[50:], FinishWrite: true},
		})
		require.NoError(t, err)
		proxy.WaitForPendingVerificationsForTesting()
		require.Empty(t, deltas(before, writeVerificationCounts(t)))
		assertBytesWritten(t, cache, rn, buf)
	})

	t.Run("compressed writes are verified against decompressed content", func(t *testing.T) {
		zstdRN := rn.CloneVT()
		zstdRN.Compressor = repb.Compressor_ZSTD
		zstdRef := makeReference(zstdRN, blobName, repb.Compressor_ZSTD)
		compressed := compression.CompressZstd(nil, buf)
		// Shared storage holds the zstd blob; verification must hash the
		// decompressed content.
		peer, _, proxy := newProxy(t, map[string][]byte{blobName: compressed})
		before := writeVerificationCounts(t)
		require.NoError(t, writeShadow(t, peer, zstdRN, zstdRef, compressed))
		proxy.WaitForPendingVerificationsForTesting()
		require.Equal(t, map[string]float64{distributed_client.VerificationSuccess: 1}, deltas(before, writeVerificationCounts(t)))
	})

	t.Run("AC writes are unverifiable", func(t *testing.T) {
		acRN := rn.CloneVT()
		acRN.CacheType = rspb.CacheType_AC
		acRef := makeReference(acRN, blobName, repb.Compressor_IDENTITY)
		peer, _, proxy := newProxy(t, map[string][]byte{blobName: buf})
		before := writeVerificationCounts(t)
		require.NoError(t, writeShadow(t, peer, acRN, acRef, buf))
		proxy.WaitForPendingVerificationsForTesting()
		require.Equal(t, map[string]float64{distributed_client.VerificationError: 1}, deltas(before, writeVerificationCounts(t)))
		got, err := te.GetCache().Get(ctx, acRN)
		require.NoError(t, err)
		require.Equal(t, buf, got)
	})

	t.Run("reference-only writes are not verified", func(t *testing.T) {
		peer, cache, proxy := newProxy(t, map[string][]byte{blobName: buf})
		before := writeVerificationCounts(t)
		_, err := writeRawRequests(t, peer, []*dcpb.WriteRequest{{
			Resource:    rn,
			Reference:   ref,
			FinishWrite: true,
		}})
		require.NoError(t, err)
		proxy.WaitForPendingVerificationsForTesting()
		require.Empty(t, deltas(before, writeVerificationCounts(t)))
		_, gotRN, _ := cache.lastWriteReference()
		require.Empty(t, cmp.Diff(rn, gotRN, protocmp.Transform()))
	})
}

// writeRequestCounts returns the current values of the distributed cache
// write request count and size counters, keyed by request type.
func writeRequestCounts(t *testing.T) (counts, sizes map[string]float64) {
	counts = map[string]float64{}
	sizes = map[string]float64{}
	for _, v := range testmetrics.CounterValues(t, metrics.DistributedCacheWriteRequestCount) {
		counts[v.Labels[metrics.DistributedCacheWriteRequestType]] += v.Value
	}
	for _, v := range testmetrics.CounterValues(t, metrics.DistributedCacheWriteRequestSizeBytes) {
		sizes[v.Labels[metrics.DistributedCacheWriteRequestType]] += v.Value
	}
	return counts, sizes
}

func TestRemoteReferenceWriter(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)

	newPeer := func(t *testing.T) (string, *serverReferenceCache, *distributed_client.Proxy) {
		cache := &serverReferenceCache{Cache: te.GetCache()}
		peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
		c := distributed_client.New(te, cache, peer)
		require.NoError(t, c.StartListening())
		waitUntilServerIsAlive(peer)
		return peer, cache, c
	}
	clientAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	client := distributed_client.New(te, te.GetCache(), clientAddr)
	require.NoError(t, client.StartListening())
	waitUntilServerIsAlive(clientAddr)

	writeRef := func(t *testing.T, peer, handoffPeer string, rn *rspb.ResourceName, ref *refpb.Reference, mustClone bool) error {
		t.Helper()
		wc, err := client.RemoteReferenceWriter(ctx, peer, handoffPeer, rn, ref, mustClone)
		if err != nil {
			return err
		}
		defer wc.Close()
		return wc.Commit()
	}

	t.Run("writes the reference", func(t *testing.T) {
		rn, _ := testdigest.RandomCASResourceBuf(t, 100)
		ref := makeReference(rn, "blobs/rpc-blob", repb.Compressor_IDENTITY)
		peer, cache, _ := newPeer(t)
		beforeCounts, beforeSizes := writeRequestCounts(t)
		require.NoError(t, writeRef(t, peer, "", rn, ref, false /*=mustClone*/))
		gotRef, gotRN, gotCloned := cache.lastWriteReference()
		require.Empty(t, cmp.Diff(ref, gotRef, protocmp.Transform()))
		require.Empty(t, cmp.Diff(rn, gotRN, protocmp.Transform()))
		// mustClone=false lets the peer take ownership of the blob.
		require.False(t, gotCloned)
		afterCounts, afterSizes := writeRequestCounts(t)
		require.Equal(t, beforeCounts["reference"]+1, afterCounts["reference"])
		require.Equal(t, beforeSizes["reference"]+float64(rn.GetDigest().GetSizeBytes()), afterSizes["reference"])
		require.Equal(t, beforeCounts["bytes"], afterCounts["bytes"])
	})

	t.Run("must-be-cloned is passed through", func(t *testing.T) {
		rn, _ := testdigest.RandomCASResourceBuf(t, 100)
		ref := makeReference(rn, "blobs/rpc-blob", repb.Compressor_IDENTITY)
		peer, cache, _ := newPeer(t)
		require.NoError(t, writeRef(t, peer, "", rn, ref, true /*=mustClone*/))
		_, _, gotCloned := cache.lastWriteReference()
		require.True(t, gotCloned)
	})

	t.Run("existing digests are deduped without error", func(t *testing.T) {
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		ref := makeReference(rn, "blobs/rpc-blob", repb.Compressor_IDENTITY)
		require.NoError(t, te.GetCache().Set(ctx, rn, buf))
		peer, cache, _ := newPeer(t)
		dedupedLabels := prometheus.Labels{
			metrics.DistributedCacheWriteRequestType: "reference",
			metrics.StatusHumanReadableLabel:         "AlreadyExists",
		}
		okLabels := prometheus.Labels{
			metrics.DistributedCacheWriteRequestType: "reference",
			metrics.StatusHumanReadableLabel:         "OK",
		}
		beforeDeduped := testmetrics.CounterValueForLabels(t, metrics.DistributedCacheWriteRequestCount, dedupedLabels)
		beforeOK := testmetrics.CounterValueForLabels(t, metrics.DistributedCacheWriteRequestCount, okLabels)
		require.NoError(t, writeRef(t, peer, "", rn, ref, false))
		_, gotRN, _ := cache.lastWriteReference()
		require.Nil(t, gotRN)
		// The deduped commit succeeds but is recorded under AlreadyExists,
		// not OK.
		require.Equal(t, beforeDeduped+1, testmetrics.CounterValueForLabels(t, metrics.DistributedCacheWriteRequestCount, dedupedLabels))
		require.Equal(t, beforeOK, testmetrics.CounterValueForLabels(t, metrics.DistributedCacheWriteRequestCount, okLabels))
	})

	t.Run("handoff peer is propagated", func(t *testing.T) {
		rn, _ := testdigest.RandomCASResourceBuf(t, 100)
		ref := makeReference(rn, "blobs/rpc-blob", repb.Compressor_IDENTITY)
		peer, _, server := newPeer(t)
		var mu sync.Mutex
		var handoffPeer string
		server.SetHintedHandoffCallbackFunc(func(ctx context.Context, peer string, r *rspb.ResourceName) {
			mu.Lock()
			defer mu.Unlock()
			handoffPeer = peer
		})
		require.NoError(t, writeRef(t, peer, "handoff-peer", rn, ref, false))
		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, "handoff-peer", handoffPeer)
	})

	t.Run("peer errors are returned", func(t *testing.T) {
		rn, _ := testdigest.RandomCASResourceBuf(t, 100)
		ref := makeReference(rn, "blobs/rpc-blob", repb.Compressor_IDENTITY)
		peer, cache, _ := newPeer(t)
		cache.mu.Lock()
		cache.writeRefErr = status.NotFoundError("backing object may have expired")
		cache.mu.Unlock()
		notFoundLabels := prometheus.Labels{
			metrics.DistributedCacheWriteRequestType: "reference",
			metrics.StatusHumanReadableLabel:         "NotFound",
		}
		before := testmetrics.CounterValueForLabels(t, metrics.DistributedCacheWriteRequestCount, notFoundLabels)
		err := writeRef(t, peer, "", rn, ref, false)
		require.Error(t, err)
		require.True(t, status.IsNotFoundError(err), "expected NotFoundError, got %s", err)
		// The failed commit is counted under its status code.
		after := testmetrics.CounterValueForLabels(t, metrics.DistributedCacheWriteRequestCount, notFoundLabels)
		require.Equal(t, before+1, after)
	})
}

func TestRemoteVerifiedWriter(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)

	const blobName = "blobs/verified-write-blob"
	clientAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	client := distributed_client.New(te, te.GetCache(), clientAddr)
	require.NoError(t, client.StartListening())
	waitUntilServerIsAlive(clientAddr)

	newPeer := func(t *testing.T, blobs map[string][]byte) (string, *serverReferenceCache, *distributed_client.Proxy) {
		cache := &serverReferenceCache{Cache: te.GetCache(), blobs: blobs}
		peer := fmt.Sprintf("localhost:%d", testport.FindFree(t))
		c := distributed_client.New(te, cache, peer)
		require.NoError(t, c.StartListening())
		waitUntilServerIsAlive(peer)
		return peer, cache, c
	}
	writeAll := func(t *testing.T, peer string, rn *rspb.ResourceName, ref *refpb.Reference, data []byte) {
		t.Helper()
		wc, err := client.RemoteVerifiedWriter(ctx, peer, "", rn)
		require.NoError(t, err)
		_, err = wc.Write(data)
		require.NoError(t, err)
		if ref != nil {
			wc.SetReference(ref)
		}
		require.NoError(t, wc.Commit())
		require.NoError(t, wc.Close())
	}

	t.Run("a bound reference rides on the final message", func(t *testing.T) {
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		ref := makeReference(rn, blobName, repb.Compressor_IDENTITY)
		peer, cache, proxy := newPeer(t, map[string][]byte{blobName: buf})
		before := writeVerificationCounts(t)
		beforeCounts, beforeSizes := writeRequestCounts(t)
		writeAll(t, peer, rn, ref, buf)
		proxy.WaitForPendingVerificationsForTesting()
		// The bytes are the write and the reference was verified.
		got, err := te.GetCache().Get(ctx, rn)
		require.NoError(t, err)
		require.Equal(t, buf, got)
		_, gotRN, _ := cache.lastWriteReference()
		require.Nil(t, gotRN)
		after := writeVerificationCounts(t)
		require.Equal(t, before[distributed_client.VerificationSuccess]+1, after[distributed_client.VerificationSuccess])
		// A verified write is a byte write; the reference only rides along.
		afterCounts, afterSizes := writeRequestCounts(t)
		require.Equal(t, beforeCounts["bytes"]+1, afterCounts["bytes"])
		require.Equal(t, beforeSizes["bytes"]+float64(rn.GetDigest().GetSizeBytes()), afterSizes["bytes"])
		require.Equal(t, beforeCounts["reference"], afterCounts["reference"])
	})

	t.Run("an unbound reference is a plain byte write", func(t *testing.T) {
		rn, buf := testdigest.RandomCASResourceBuf(t, 100)
		peer, cache, proxy := newPeer(t, map[string][]byte{blobName: buf})
		before := writeVerificationCounts(t)
		writeAll(t, peer, rn, nil, buf)
		proxy.WaitForPendingVerificationsForTesting()
		got, err := te.GetCache().Get(ctx, rn)
		require.NoError(t, err)
		require.Equal(t, buf, got)
		_, gotRN, _ := cache.lastWriteReference()
		require.Nil(t, gotRN)
		require.Equal(t, before, writeVerificationCounts(t))
	})
}

func TestVerifyingReadCloser(t *testing.T) {
	newRC := func(data []byte) io.ReadCloser {
		return io.NopCloser(bytes.NewReader(data))
	}
	rn, data := testdigest.RandomCASResourceBuf(t, 1000)

	// run reads through a verifying reader and returns the served bytes plus
	// the change in the verification counter, keyed by outcome.
	run := func(t *testing.T, secondary io.ReadCloser) (gotData []byte, counted map[string]float64) {
		before := verificationCounts(t)
		v := distributed_client.NewVerifyingReadCloser(newRC(data), secondary, log.NamedSubLogger(t.Name()), rn, "test-peer", "GR-test")
		got, err := io.ReadAll(v)
		require.NoError(t, err)
		require.NoError(t, v.Close())
		counted = map[string]float64{}
		for s, c := range verificationCounts(t) {
			if d := c - before[s]; d != 0 {
				counted[s] = d
			}
		}
		return got, counted
	}

	t.Run("matching streams", func(t *testing.T) {
		got, counted := run(t, newRC(data))
		require.Equal(t, data, got)
		require.Equal(t, map[string]float64{distributed_client.VerificationSuccess: 1}, counted)
	})

	t.Run("differing bytes", func(t *testing.T) {
		other := append([]byte{}, data...)
		other[500] ^= 0xff
		got, counted := run(t, newRC(other))
		// Primary bytes are served regardless of the mismatch.
		require.Equal(t, data, got)
		require.Equal(t, map[string]float64{distributed_client.VerificationFailure: 1}, counted)
	})

	t.Run("secondary too short", func(t *testing.T) {
		got, counted := run(t, newRC(data[:900]))
		require.Equal(t, data, got)
		require.Equal(t, map[string]float64{distributed_client.VerificationFailure: 1}, counted)
	})

	t.Run("secondary too long", func(t *testing.T) {
		longer := append(append([]byte{}, data...), 0x01)
		got, counted := run(t, newRC(longer))
		require.Equal(t, data, got)
		require.Equal(t, map[string]float64{distributed_client.VerificationFailure: 1}, counted)
	})

	t.Run("secondary read error", func(t *testing.T) {
		got, counted := run(t, &errorReadCloser{err: errors.New("gcs exploded")})
		require.Equal(t, data, got)
		require.Equal(t, map[string]float64{distributed_client.VerificationError: 1}, counted)
	})

	t.Run("secondary read errors carry their code", func(t *testing.T) {
		canceledLabels := prometheus.Labels{
			metrics.GroupID:                  "GR-test",
			metrics.VerificationOutcomeLabel: distributed_client.VerificationError,
			metrics.StatusHumanReadableLabel: "Canceled",
		}
		before := testmetrics.CounterValueForLabels(t, metrics.DistributedCacheReferenceVerificationCount, canceledLabels)
		got, counted := run(t, &errorReadCloser{err: status.CanceledError("context canceled")})
		require.Equal(t, data, got)
		require.Equal(t, map[string]float64{distributed_client.VerificationError: 1}, counted)
		after := testmetrics.CounterValueForLabels(t, metrics.DistributedCacheReferenceVerificationCount, canceledLabels)
		require.Equal(t, before+1, after)
	})
}
