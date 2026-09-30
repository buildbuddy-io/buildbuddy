package download

import (
	"path/filepath"
	"testing"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	"github.com/stretchr/testify/require"
)

func TestArtifactOutputPaths(t *testing.T) {
	outputDir := t.TempDir()
	paths, skipped := artifactOutputPaths([]*apipb.File{
		{Name: "result.patch", Uri: "bytestream://cache/blobs/aaa/1"},
		// Duplicate artifacts with the same name should be skipped.
		{Name: "result.patch", Uri: "bytestream://cache/blobs/bbb/1"},
		{Name: "nested/artifact.txt"},
		// Invalid artifacts with invalid paths should be skipped.
		{Name: "../file"},
		{Name: "/absolute/file"},
	}, outputDir)
	require.Equal(t, 3, skipped)
	want := []string{
		filepath.Join(outputDir, "result.patch"),
		"",
		filepath.Join(outputDir, "nested", "artifact.txt"),
		"",
		"",
	}
	require.Equal(t, want, paths)
}
