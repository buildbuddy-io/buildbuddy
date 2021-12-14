package fileresolver_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/fs"
	"github.com/stretchr/testify/require"

	bundle "github.com/buildbuddy-io/buildbuddy/server/util/fileresolver/test_data"
)

func TestFileResolver(t *testing.T) {
	paths := []string{
		"server/util/fileresolver/test_data/embedded_dir/embedded_child.txt",
		"server/util/fileresolver/test_data/embedded_file.txt",
		"server/util/fileresolver/test_data/runfile.txt",
	}

	resolver := fileresolver.New(fs.CtxFSWrapper(bundle.FS), "server/util/fileresolver/test_data")
	ctx := context.Background()
	for _, path := range paths {
		f, err := resolver.Open(ctx, path)
		require.NoError(t, err)
		f.Close(ctx)
	}
}
