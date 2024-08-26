package byte_stream_client

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	zipb "github.com/buildbuddy-io/buildbuddy/proto/zip"
)

var (
	someFilesRunfilePath    string
	noFilesRunfilePath      string
	tooManyFilesRunfilePath string
)

func TestManifest_SomeFilesZip(t *testing.T) {
	expected := &zipb.Manifest{
		Entry: []*zipb.ManifestEntry{
			{
				Name:             "f0.txt",
				UncompressedSize: 5,
				CompressedSize:   7,
				Compression:      zipb.ManifestEntry_COMPRESSION_TYPE_FLATE,
				Crc32:            2745310273,
			},
			{
				Name:             "f1.txt",
				UncompressedSize: 10,
				CompressedSize:   12,
				HeaderOffset:     91,
				Compression:      zipb.ManifestEntry_COMPRESSION_TYPE_FLATE,
				Crc32:            1157536424,
			},
			{
				Name:             "f2.txt",
				UncompressedSize: 36,
				CompressedSize:   38,
				HeaderOffset:     187,
				Compression:      zipb.ManifestEntry_COMPRESSION_TYPE_FLATE,
				Crc32:            869932603,
			},
		},
	}
	path, err := runfiles.Rlocation(someFilesRunfilePath)
	require.NoError(t, err)
	bytes, err := os.ReadFile(path)
	assert.NoError(t, err)
	manifest, err := ParseZipManifestFooter(bytes, 0, int64(len(bytes)))
	assert.NoError(t, err)
	if !proto.Equal(expected, manifest) {
		t.Fatalf("Incorrect manifest. Expected: %v\n Actual: %v", expected, manifest)
	}
}

func TestManifest_NoFilesZip(t *testing.T) {
	expected := &zipb.Manifest{}
	path, err := runfiles.Rlocation(noFilesRunfilePath)
	require.NoError(t, err)
	bytes, err := os.ReadFile(path)
	assert.NoError(t, err)
	manifest, err := ParseZipManifestFooter(bytes, 0, int64(len(bytes)))
	assert.NoError(t, err)
	if !proto.Equal(expected, manifest) {
		t.Fatalf("Incorrect manifest. Expected: %v\n Actual: %v", expected, manifest)
	}
}

func TestManifest_TooManyFilesZip(t *testing.T) {
	path, err := runfiles.Rlocation(tooManyFilesRunfilePath)
	require.NoError(t, err)
	bytes, err := os.ReadFile(path)
	assert.NoError(t, err)

	offset := len(bytes) - 65536
	_, err = ParseZipManifestFooter(bytes[65536:], int64(offset), int64(len(bytes)))
	if !strings.Contains(err.Error(), "code = Unimplemented") {
		t.Fatalf("Unexpectedly parsed very large manifest.")
	}
}

func createBytestreamer(b []byte) Bytestreamer {
	return func(ctx context.Context, url *url.URL, offset int64, limit int64, writer io.Writer) error {
		writer.Write(b[offset : offset+limit])
		return nil
	}
}

func validateZipContents(t *testing.T, ctx context.Context, entry *zipb.ManifestEntry, expectedContent string, streamer Bytestreamer) {
	var buf bytes.Buffer
	streamSingleFileFromBytestreamZipInternal(ctx, nil, entry, &buf, streamer)
	out := make([]byte, entry.GetUncompressedSize())
	_, err := io.ReadFull(&buf, out)
	if err != nil {
		t.Fatal(err.Error())
	}
	if expectedContent != string(out) {
		t.Fatalf("Got unexpected content, expected: %s, actual: %s", expectedContent, string(out))
	}
}

func TestReadZipFileContents(t *testing.T) {
	path, err := runfiles.Rlocation(someFilesRunfilePath)
	require.NoError(t, err)
	b, err := os.ReadFile(path)
	assert.NoError(t, err)
	manifest, err := ParseZipManifestFooter(b, 0, int64(len(b)))
	assert.NoError(t, err)

	streamer := createBytestreamer(b)
	ctx := context.Background()
	validateZipContents(t, ctx, manifest.GetEntry()[0], "bazel", streamer)
	validateZipContents(t, ctx, manifest.GetEntry()[1], "buildbuddy", streamer)
	validateZipContents(t, ctx, manifest.GetEntry()[2], "san dimas high school football rules", streamer)
}
