package compactgraph_test

import (
	"os"
	"path"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/cli/explain/compactgraph"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJavaNoopImplChange(t *testing.T) {
	spawnDiffs := diffLogs(t, "java_noop_impl_change")
	require.Len(t, spawnDiffs, 3)

	sd1 := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib-hjar.jar$", sd1.PrimaryOutput)
	assert.Equal(t, "//src/main/java/com/example/lib:lib", sd1.TargetLabel)
	assert.Equal(t, "Turbine", sd1.Mnemonic)
	assert.Equal(t, spawn_diff.SpawnDiff_MODIFIED, sd1.DiffType)
	assert.Empty(t, sd1.TransitivelyInvalidated)
	require.Len(t, sd1.Diffs, 1)
	sd1d1 := sd1.Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, sd1d1.Diff)
	require.Len(t, sd1d1.GetInputContents().GetFileDiffs(), 1)
	sd1fd1 := sd1d1.GetInputContents().GetFileDiffs()[0]
	assert.Equal(t, "src/main/java/com/example/lib/Lib.java", sd1fd1.Path)
	assert.NotNil(t, sd1fd1.GetOldDigest())
	assert.NotNil(t, sd1fd1.GetNewDigest())
	assert.NotEqual(t, sd1fd1.GetNewDigest().GetHash(), sd1fd1.GetOldDigest().GetHash())

	sd2 := spawnDiffs[1]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib.jar$", sd2.PrimaryOutput)
	assert.Equal(t, "//src/main/java/com/example/lib:lib", sd2.TargetLabel)
	assert.Equal(t, "Javac", sd2.Mnemonic)
	assert.Equal(t, spawn_diff.SpawnDiff_MODIFIED, sd2.DiffType)
	assert.Empty(t, sd2.TransitivelyInvalidated)
	require.Len(t, sd2.Diffs, 1)
	sd2d1 := sd2.Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, sd2d1.Diff)
	require.Len(t, sd2d1.GetInputContents().GetFileDiffs(), 1)
	sd2fd1 := sd2d1.GetInputContents().GetFileDiffs()[0]
	assert.Equal(t, "src/main/java/com/example/lib/Lib.java", sd2fd1.Path)
	assert.NotNil(t, sd2fd1.GetOldDigest())
	assert.NotNil(t, sd2fd1.GetNewDigest())
	assert.NotEqual(t, sd2fd1.GetNewDigest().GetHash(), sd2fd1.GetOldDigest().GetHash())

	sd3 := spawnDiffs[2]
	assert.Regexp(t, "^bazel-out/[^/]+/testlogs/src/test/java/com/example/lib/lib_test/test.xml$", sd3.PrimaryOutput)
	assert.Equal(t, "//src/test/java/com/example/lib:lib_test", sd3.TargetLabel)
	assert.Equal(t, "TestRunner", sd3.Mnemonic)
	assert.Equal(t, spawn_diff.SpawnDiff_MODIFIED, sd3.DiffType)
	assert.Empty(t, sd3.TransitivelyInvalidated)
	require.Len(t, sd3.Diffs, 1)
	sd3d1 := sd3.Diffs[0]
	require.IsType(t, &spawn_diff.Diff_OutputContents{}, sd3d1.Diff)
	require.Len(t, sd3d1.GetOutputContents().GetFileDiffs(), 1)
	sd3fd1 := sd3d1.GetOutputContents().GetFileDiffs()[0]
	assert.Regexp(t, "^bazel-out/[^/]+/testlogs/src/test/java/com/example/lib/lib_test/test.xml$", sd3fd1.Path)
	assert.NotNil(t, sd3fd1.GetOldDigest())
	assert.NotNil(t, sd3fd1.GetNewDigest())
	assert.NotEqual(t, sd3fd1.GetNewDigest().GetHash(), sd3fd1.GetOldDigest().GetHash())
}

func diffLogs(t *testing.T, name string) []*spawn_diff.SpawnDiff {
	dir := path.Join("buildbuddy", os.Getenv("PKG"), "testdata")
	oldPath, err := runfiles.Rlocation(path.Join(dir, name+"_old.pb.zstd"))
	require.NoError(t, err)
	newPath, err := runfiles.Rlocation(path.Join(dir, name+"_new.pb.zstd"))
	require.NoError(t, err)
	oldLogFile, err := os.Open(oldPath)
	require.NoError(t, err)
	defer oldLogFile.Close()
	newLogFile, err := os.Open(newPath)
	require.NoError(t, err)
	defer newLogFile.Close()
	oldLog, _, err := compactgraph.ReadCompactLog(oldLogFile)
	require.NoError(t, err)
	newLog, _, err := compactgraph.ReadCompactLog(newLogFile)
	require.NoError(t, err)
	return compactgraph.Diff(oldLog, newLog)
}
