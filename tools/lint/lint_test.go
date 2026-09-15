package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListBuildFiles(t *testing.T) {
	root := t.TempDir()
	included := []string{
		"BUILD",
		"pkg/BUILD.bazel",
		"pkg/defs.bzl",
		"WORKSPACE",
		"MODULE.bazel",
	}
	excluded := []string{
		"pkg/main.go",
		"pkg/WORKSPACE.bzlmod",
		".hidden/BUILD",
		"pkg/.hidden/defs.bzl",
	}
	for _, path := range append(slices.Clone(included), excluded...) {
		path := filepath.Join(root, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, nil, 0644))
	}

	files, err := listBuildFiles(root)
	require.NoError(t, err)
	for i, path := range included {
		included[i] = filepath.Join(root, path)
	}
	slices.Sort(included)
	require.Equal(t, included, files)
}
