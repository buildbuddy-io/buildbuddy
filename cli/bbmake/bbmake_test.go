package bbmake_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/bbmake"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

func TestBuildCPPHello(t *testing.T) {
	ctx := context.Background()
	srcMakefile := testfs.RunfilePath(t, "cli/bbmake/testdata/cpp_hello/Makefile")
	ws := testfs.TempCopyDir(t, filepath.Dir(srcMakefile))
	inv := &bbmake.Invocation{
		Dir:                ws,
		PrintCommands:      true,
		PrintActionOutputs: true,
		// Build the "hello" binary.
		MakeArgs: []string{"hello"},
	}

	err := inv.Run(ctx)

	require.NoError(t, err)
	require.FileExists(t, filepath.Join(ws, "hello"))
}

func TestBuildCPPLibraries(t *testing.T) {
	ctx := context.Background()
	srcMakefile := testfs.RunfilePath(t, "cli/bbmake/testdata/cpp_libraries/Makefile")
	ws := testfs.TempCopyDir(t, filepath.Dir(srcMakefile))
	inv := &bbmake.Invocation{
		Dir:                ws,
		PrintCommands:      true,
		PrintActionOutputs: true,
		// Build the "main" binary.
		MakeArgs: []string{"main"},
	}

	err := inv.Run(ctx)

	require.NoError(t, err)
	require.FileExists(t, filepath.Join(ws, "main"))
}
