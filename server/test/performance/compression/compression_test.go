package compression_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/metadata"
)

var (
	backend = flag.String("remote_cache", "", "")
	blobDir = flag.String("blob_dir", "", "")
	zstdLib = flag.String("zstd_lib", "datadog", "")
	// Size to use for ByteStream requests, big or smol
	bsBlobSize = flag.String("bs_blob_size", "big", "")

	compressor *zstd.Encoder
	smolBlobs  []*blob
	bigBlobs   []*blob
	bsBlobs    []*blob
)

func init() {
	rand.Seed(time.Now().UnixNano())
	c, err := zstd.NewWriter(nil)
	if err != nil {
		panic(err)
	}
	compressor = c
}

func TestMain(m *testing.M) {
	flag.Parse()
	ptrs, err := readBlobDir(*blobDir)
	if err != nil {
		panic(err)
	}
	fmt.Println("-zstd_lib=" + *zstdLib)
	fmt.Println("Precompressing...")
	bigBlobs, err = precompress(bigBlobsOnly(ptrs))
	if err != nil {
		panic(err)
	}
	smolBlobs, err = precompress(smolBlobsOnly(ptrs))
	if err != nil {
		panic(err)
	}
	fmt.Println("Done")

	bsBlobs = bigBlobs
	if *bsBlobSize == "smol" {
		bsBlobs = smolBlobs
	}

	m.Run()
}

func BenchmarkBytestreamWrite(t *testing.B) {
	t.RunParallel(func(pb *testing.PB) {
		conn, err := grpc_client.DialTarget(*backend)
		require.NoError(t, err)
		defer conn.Close()
		bsClient := bspb.NewByteStreamClient(conn)
		ctx := context.Background()
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-cache-devnull-writer", "true")
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-cache-zstd-lib", *zstdLib)
		for pb.Next() {
			b := bsBlobs[rand.Intn(len(bsBlobs))]
			mustUploadCompressed(t, ctx, bsClient, b.Digest, b.Data)
		}
	})
}

var uploaded = false

func BenchmarkBytestreamRead(t *testing.B) {
	if !uploaded {
		uploaded = true
		// Pre-upload blobs
		t.Logf("Pre-uploading blobs ...")
		conn, err := grpc_client.DialTarget(*backend)
		require.NoError(t, err)
		defer conn.Close()
		bsClient := bspb.NewByteStreamClient(conn)
		ctx := context.Background()
		for _, b := range bsBlobs {
			mustUploadCompressed(t, ctx, bsClient, b.Digest, b.Data)
		}
		t.Logf("Done.")
	}

	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		conn, err := grpc_client.DialTarget(*backend)
		require.NoError(t, err)
		defer conn.Close()
		bsClient := bspb.NewByteStreamClient(conn)
		ctx := context.Background()
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-cache-zstd-lib", *zstdLib)
		for pb.Next() {
			b := bsBlobs[rand.Intn(len(bsBlobs))]
			r := digest.NewResourceName(b.Digest, "")
			r.SetCompressor(repb.Compressor_ZSTD)
			err := cachetools.GetBlob(ctx, bsClient, r, io.Discard)
			require.NoError(t, err)
		}
	})
}

func precompress(ptrs []*blobPtr) ([]*blob, error) {
	var out []*blob
	mem := 0
	i := 0
	for mem < 250_000_000 {
		p := ptrs[i]
		b, err := p.ReadAll()
		if err != nil {
			return nil, err
		}
		d, s, err := saltedBlob(b)
		if err != nil {
			return nil, err
		}
		compressed := compressor.EncodeAll(s, nil)
		compressor.Reset(nil)
		out = append(out, &blob{d, compressed})
		mem += len(compressed)
		i = (i + 1) % len(ptrs)
	}
	return out, nil
}

type blob struct {
	Digest *repb.Digest
	Data   []byte
}

type blobPtr struct {
	Digest  *repb.Digest
	RelPath string
}

func (b *blobPtr) ReadAll() ([]byte, error) {
	path := filepath.Join(*blobDir, b.RelPath)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func readBlobDir(path string) ([]*blobPtr, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	out := []*blobPtr{}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			// Don't recurse into dirs or follow symlinks.
			continue
		}
		f, err := os.Open(filepath.Join(path, entry.Name()))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		d, err := digest.Compute(f)
		if err != nil {
			return nil, err
		}
		out = append(out, &blobPtr{Digest: d, RelPath: entry.Name()})
	}
	return out, nil
}

func smolBlobsOnly(blobs []*blobPtr) []*blobPtr {
	out := []*blobPtr{}
	for _, b := range blobs {
		if b.Digest.SizeBytes > 2e6 {
			continue
		}
		out = append(out, b)
	}
	return out
}

func bigBlobsOnly(blobs []*blobPtr) []*blobPtr {
	out := []*blobPtr{}
	for _, b := range blobs {
		if b.Digest.SizeBytes < 24e6 {
			continue
		}
		out = append(out, b)
	}
	return out
}

func mustUploadCompressed(t testing.TB, ctx context.Context, bsClient bspb.ByteStreamClient, d *repb.Digest, blob []byte) {
	// blob = compressor.EncodeAll(blob, nil)
	r := digest.NewResourceName(d, "")
	r.SetCompressor(repb.Compressor_ZSTD)
	_, err := cachetools.UploadFromReader(ctx, bsClient, r, bytes.NewReader(blob))
	require.NoError(t, err)
}

func saltedBlob(b []byte) (*repb.Digest, []byte, error) {
	out := make([]byte, len(b))
	copy(out, b)
	saltLen := 24
	if saltLen > len(b) {
		saltLen = len(b)
	}
	_, err := rand.Read(b[:len(b)-saltLen])
	if err != nil {
		return nil, nil, err
	}
	d, err := digest.Compute(bytes.NewReader(out))
	if err != nil {
		return nil, nil, err
	}
	return d, out, nil
}
