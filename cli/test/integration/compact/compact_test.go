package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrintCompactExec(t *testing.T) {
	files, err := filepath.Glob("testdata/*.binpb.zstd")
	require.NoError(t, err)

	ws := testcli.NewWorkspace(t)
	for _, f := range files {
		absPath, err := filepath.Abs(f)
		require.NoError(t, err)

		for _, isSorted := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/isSorted_%t", f, isSorted), func(t *testing.T) {
				expectedSuffix := ".json"
				if isSorted {
					expectedSuffix = ".sorted.json"
				}
				expectedJson := strings.TrimSuffix(absPath, ".binpb.zstd") + expectedSuffix
				b, err := os.ReadFile(expectedJson)
				require.NoError(t, err)

				for i := 0; i < 3; i++ {
					t.Run(fmt.Sprintf("run_%d", i), func(t *testing.T) {
						out, err := testcli.CombinedOutput(testcli.Command(t, ws, "print", "--compact_execution_log", absPath, fmt.Sprintf("--sort=%t", isSorted)))
						assert.NoError(t, err)
						assert.Equal(t, string(b), string(out))
					})
				}
			})
		}
	}
}
