package bytestream

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"google.golang.org/protobuf/proto"

	zipb "github.com/buildbuddy-io/buildbuddy/proto/zip"
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
	path, _ := bazel.Runfile("some_files.zip")
	bytes, _ := os.ReadFile(path)
	manifest, _ := parseZipManifestFooter(bytes, 0, int64(len(bytes)))
	if !proto.Equal(expected, manifest) {
		t.Fatalf("Incorrect manifest. Expected: %v\n Actual: %v", expected, manifest)
	}
}

func TestManifest_NoFilesZip(t *testing.T) {
	expected := &zipb.Manifest{}
	path, _ := bazel.Runfile("no_files.zip")
	bytes, _ := os.ReadFile(path)
	manifest, _ := parseZipManifestFooter(bytes, 0, int64(len(bytes)))
	if !proto.Equal(expected, manifest) {
		t.Fatalf("Incorrect manifest. Expected: %v\n Actual: %v", expected, manifest)
	}
}

func TestManifest_TooManyFilesZip(t *testing.T) {
	path, _ := bazel.Runfile("too_many_files.zip")
	bytes, _ := os.ReadFile(path)

	offset := len(bytes) - 65536
	_, err := parseZipManifestFooter(bytes[65536:], int64(offset), int64(len(bytes)))
	if !strings.Contains(err.Error(), "code = Unimplemented") {
		t.Fatalf("Unexpectedly parsed very large manifest.")
	}
}

func createBytestreamer(b []byte) Bytestreamer {
	return func(ctx context.Context, env environment.Env, url *url.URL, offset int64, limit int64, writer io.Writer) error {
		writer.Write(b[offset : offset+limit])
		return nil
	}
}

func validateZipContents(t *testing.T, entry *zipb.ManifestEntry, expectedContent string, streamer Bytestreamer) {
	var buf bytes.Buffer
	streamSingleFileFromBytestreamZipInternal(nil, nil, nil, entry, &buf, streamer)
	out := make([]byte, entry.GetUncompressedSize())
	_, err := io.ReadFull(&buf, out)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if expectedContent != string(out) {
		t.Fatalf("Got unexpected content, expected: %s, actual: %s", expectedContent, string(out))
	}
}

func TestReadZipFileContents(t *testing.T) {
	path, _ := bazel.Runfile("some_files.zip")
	b, _ := os.ReadFile(path)
	manifest, _ := parseZipManifestFooter(b, 0, int64(len(b)))

	streamer := createBytestreamer(b)
	validateZipContents(t, manifest.GetEntry()[0], "bazel", streamer)
	validateZipContents(t, manifest.GetEntry()[1], "buildbuddy", streamer)
	validateZipContents(t, manifest.GetEntry()[2], "san dimas high school football rules", streamer)
}
