package compactgraph_test

import (
	"os"
	"path"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/cli/explain/compactgraph"
	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{
			name: "add_comment",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldLogFile, newLogFile := openLogs(t, tc.name)
			defer oldLogFile.Close()
			defer newLogFile.Close()
			oldLog, _, err := compactgraph.ReadCompactLog(oldLogFile)
			require.NoError(t, err)
			newLog, _, err := compactgraph.ReadCompactLog(newLogFile)
			require.NoError(t, err)
			compactgraph.Diff(oldLog, newLog)
		})
	}
}

func openLogs(t *testing.T, name string) (*os.File, *os.File) {
	dir := path.Join("buildbuddy", os.Getenv("PKG"), "testdata")
	oldPath, err := runfiles.Rlocation(path.Join(dir, name+"_old.pb.zstd"))
	require.NoError(t, err)
	newPath, err := runfiles.Rlocation(path.Join(dir, name+"_new.pb.zstd"))
	require.NoError(t, err)
	oldLogFile, err := os.Open(oldPath)
	require.NoError(t, err)
	newLogFile, err := os.Open(newPath)
	require.NoError(t, err)
	return oldLogFile, newLogFile
}
