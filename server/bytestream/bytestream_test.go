package bytestream

import (
	"os"
	"testing"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/stretchr/testify/assert"

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
	assert.Equal(t, expected, manifest)
}

func TestManifest_NoFilesZip(t *testing.T) {
	expected := &zipb.Manifest{}
	path, _ := bazel.Runfile("no_files.zip")
	bytes, _ := os.ReadFile(path)
	manifest, _ := parseZipManifestFooter(bytes, 0, int64(len(bytes)))
	assert.Equal(t, expected, manifest)
}

func TestManifest_TooManyFilesZip(t *testing.T) {
	path, _ := bazel.Runfile("too_many_files.zip")
	bytes, _ := os.ReadFile(path)

	assert.Greater(t, len(bytes), 65536)
	offset := len(bytes) - 65536
	manifest, err := parseZipManifestFooter(bytes[65536:], int64(offset), int64(len(bytes)))
	assert.Nil(t, manifest)
	assert.Contains(t, err.Error(), "code = Unimplemented")
}
