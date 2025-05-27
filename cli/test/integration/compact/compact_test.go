package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/quarantine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrintCompactExec(t *testing.T) {
	// Quarantining this test due to likely flakes on master:
	// - https://app.buildbuddy.io/invocation/9f1cf241-0dd3-4394-95ee-edccc50dcf60
	// - https://app.buildbuddy.io/invocation/8e7a9e14-9d33-47cf-b0a8-17a97a04095a
	quarantine.SkipQuarantinedTest(t)
	files, err := filepath.Glob("testdata/*.binpb.zst")
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
				expectedJson := strings.TrimSuffix(absPath, ".binpb.zst") + expectedSuffix
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
