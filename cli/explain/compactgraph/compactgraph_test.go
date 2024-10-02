package compactgraph_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/cli/explain/compactgraph"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJavaNoopImplChange(t *testing.T) {
	spawnDiffs := diffLogs(t, "java_noop_impl_change", "7.3.1")
	require.Len(t, spawnDiffs, 3)

	sd1 := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib-hjar.jar$", sd1.PrimaryOutput)
	assert.Equal(t, "//src/main/java/com/example/lib:lib", sd1.TargetLabel)
	assert.Equal(t, "Turbine", sd1.Mnemonic)
	assert.Empty(t, sd1.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd1.GetModified().GetDiffs(), 1)
	sd1d1 := sd1.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, sd1d1.Diff)
	require.Len(t, sd1d1.GetInputContents().GetFileDiffs(), 1)
	sd1fd1 := sd1d1.GetInputContents().GetFileDiffs()[0]
	assert.Equal(t, "src/main/java/com/example/lib/Lib.java", sd1fd1.GetOldFile().GetPath())
	assert.NotNil(t, sd1fd1.GetOldFile().GetDigest())
	assert.NotEqual(t, sd1fd1.GetOldFile().GetDigest().GetHash(), sd1fd1.GetNewFile().GetDigest().GetHash())

	sd2 := spawnDiffs[1]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib.jar$", sd2.PrimaryOutput)
	assert.Equal(t, "//src/main/java/com/example/lib:lib", sd2.TargetLabel)
	assert.Equal(t, "Javac", sd2.Mnemonic)
	assert.Empty(t, sd2.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd2.GetModified().GetDiffs(), 1)
	sd2d1 := sd2.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, sd2d1.Diff)
	require.Len(t, sd2d1.GetInputContents().GetFileDiffs(), 1)
	sd2fd1 := sd2d1.GetInputContents().GetFileDiffs()[0]
	assert.Equal(t, "src/main/java/com/example/lib/Lib.java", sd2fd1.GetOldFile().GetPath())
	assert.NotNil(t, sd2fd1.GetOldFile().GetDigest())
	assert.NotEqual(t, sd2fd1.GetOldFile().GetDigest().GetHash(), sd2fd1.GetNewFile().GetDigest().GetHash())

	sd3 := spawnDiffs[2]
	assert.Regexp(t, "^bazel-out/darwin_arm64-fastbuild/testlogs/src/test/java/com/example/lib/lib_test/test.log$", sd3.PrimaryOutput)
	assert.Equal(t, "//src/test/java/com/example/lib:lib_test", sd3.TargetLabel)
	assert.Equal(t, "TestRunner", sd3.Mnemonic)
	assert.Empty(t, sd3.GetModified().GetTransitivelyInvalidated())
	assert.True(t, sd3.GetModified().GetExpected())
	require.Len(t, sd3.GetModified().GetDiffs(), 1)
	sd3d1 := sd3.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_OutputContents{}, sd3d1.Diff)
	require.Len(t, sd3d1.GetOutputContents().GetFileDiffs(), 1)
	sd3fd1 := sd3d1.GetOutputContents().GetFileDiffs()[0]
	assert.Regexp(t, "^bazel-out/[^/]+/testlogs/src/test/java/com/example/lib/lib_test/test.xml$", sd3fd1.GetOldFile().GetPath())
}

func TestJavaImplChange(t *testing.T) {
	spawnDiffs := diffLogs(t, "java_impl_change", "7.3.1")
	require.Len(t, spawnDiffs, 3)

	sd1 := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib-hjar.jar$", sd1.PrimaryOutput)
	assert.Equal(t, "//src/main/java/com/example/lib:lib", sd1.TargetLabel)
	assert.Equal(t, "Turbine", sd1.Mnemonic)
	assert.Empty(t, sd1.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd1.GetModified().GetDiffs(), 1)
	sd1d1 := sd1.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, sd1d1.Diff)
	require.Len(t, sd1d1.GetInputContents().GetFileDiffs(), 1)
	sd1fd1 := sd1d1.GetInputContents().GetFileDiffs()[0]
	assert.Equal(t, "src/main/java/com/example/lib/Lib.java", sd1fd1.GetOldFile().GetPath())
	assert.NotNil(t, sd1fd1.GetOldFile().GetDigest())
	assert.NotEqual(t, sd1fd1.GetOldFile().GetDigest().GetHash(), sd1fd1.GetNewFile().GetDigest().GetHash())

	sd2 := spawnDiffs[1]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib.jar$", sd2.PrimaryOutput)
	assert.Equal(t, "//src/main/java/com/example/lib:lib", sd2.TargetLabel)
	assert.Equal(t, "Javac", sd2.Mnemonic)
	assert.Empty(t, sd2.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd2.GetModified().GetDiffs(), 1)
	sd2d1 := sd2.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, sd2d1.Diff)
	require.Len(t, sd2d1.GetInputContents().GetFileDiffs(), 1)
	sd2fd1 := sd2d1.GetInputContents().GetFileDiffs()[0]
	assert.Equal(t, "src/main/java/com/example/lib/Lib.java", sd2fd1.GetOldFile().GetPath())
	assert.NotNil(t, sd2fd1.GetOldFile().GetDigest())
	assert.NotEqual(t, sd2fd1.GetOldFile().GetDigest().GetHash(), sd2fd1.GetNewFile().GetDigest().GetHash())

	sd3 := spawnDiffs[2]
	assert.Regexp(t, "^bazel-out/darwin_arm64-fastbuild/testlogs/src/test/java/com/example/lib/lib_test/test.log$", sd3.PrimaryOutput)
	assert.Equal(t, "//src/test/java/com/example/lib:lib_test", sd3.TargetLabel)
	assert.Equal(t, "TestRunner", sd3.Mnemonic)
	assert.Empty(t, sd3.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd3.GetModified().GetDiffs(), 1)
	sd3d1 := sd3.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, sd3d1.Diff)
	require.Len(t, sd3d1.GetInputContents().GetFileDiffs(), 1)
	sd3fd1 := sd3d1.GetInputContents().GetFileDiffs()[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/src/test/java/com/example/lib/lib_test.runfiles$", sd3fd1.GetOldDirectory().GetPath())
}

func TestJavaHeaderChange(t *testing.T) {
	for _, bazelVersion := range []string{"7.3.1", "7.4.0"} {
		t.Run(bazelVersion, func(t *testing.T) {
			spawnDiffs := diffLogs(t, "java_header_change", bazelVersion)
			require.Len(t, spawnDiffs, 3)

			sd1 := spawnDiffs[0]
			assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib-hjar.jar$", sd1.PrimaryOutput)
			assert.Equal(t, "//src/main/java/com/example/lib:lib", sd1.TargetLabel)
			assert.Equal(t, "Turbine", sd1.Mnemonic)
			assert.Equal(t, map[string]uint32{"Javac": 2}, sd1.GetModified().GetTransitivelyInvalidated())
			require.Len(t, sd1.GetModified().GetDiffs(), 1)
			sd1d1 := sd1.GetModified().Diffs[0]
			require.IsType(t, &spawn_diff.Diff_InputContents{}, sd1d1.Diff)
			require.Len(t, sd1d1.GetInputContents().GetFileDiffs(), 1)
			sd1fd1 := sd1d1.GetInputContents().GetFileDiffs()[0]
			assert.Equal(t, "src/main/java/com/example/lib/Lib.java", sd1fd1.GetOldFile().GetPath())
			assert.NotNil(t, sd1fd1.GetOldFile().GetDigest())
			assert.NotEqual(t, sd1fd1.GetOldFile().GetDigest().GetHash(), sd1fd1.GetNewFile().GetDigest().GetHash())

			sd2 := spawnDiffs[1]
			assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib.jar$", sd2.PrimaryOutput)
			assert.Equal(t, "//src/main/java/com/example/lib:lib", sd2.TargetLabel)
			assert.Equal(t, "Javac", sd2.Mnemonic)
			assert.Empty(t, sd2.GetModified().GetTransitivelyInvalidated())
			require.Len(t, sd2.GetModified().GetDiffs(), 1)
			sd2d1 := sd2.GetModified().Diffs[0]
			require.IsType(t, &spawn_diff.Diff_InputContents{}, sd2d1.Diff)
			require.Len(t, sd2d1.GetInputContents().GetFileDiffs(), 1)
			sd2fd1 := sd2d1.GetInputContents().GetFileDiffs()[0]
			assert.Equal(t, "src/main/java/com/example/lib/Lib.java", sd2fd1.GetOldFile().GetPath())
			assert.NotNil(t, sd2fd1.GetOldFile().GetDigest())
			assert.NotEqual(t, sd2fd1.GetOldFile().GetDigest().GetHash(), sd2fd1.GetNewFile().GetDigest().GetHash())

			sd3 := spawnDiffs[2]
			assert.Regexp(t, "^bazel-out/darwin_arm64-fastbuild/testlogs/src/test/java/com/example/lib/lib_test/test.log$", sd3.PrimaryOutput)
			assert.Equal(t, "//src/test/java/com/example/lib:lib_test", sd3.TargetLabel)
			assert.Equal(t, "TestRunner", sd3.Mnemonic)
			assert.Empty(t, sd3.GetModified().GetTransitivelyInvalidated())
			require.Len(t, sd3.GetModified().GetDiffs(), 1)
			sd3d1 := sd3.GetModified().Diffs[0]
			require.IsType(t, &spawn_diff.Diff_InputContents{}, sd3d1.Diff)
			require.Len(t, sd3d1.GetInputContents().GetFileDiffs(), 1)
			sd3fd1 := sd3d1.GetInputContents().GetFileDiffs()[0]
			assert.Regexp(t, "^bazel-out/[^/]+/bin/src/test/java/com/example/lib/lib_test.runfiles$", sd3fd1.GetOldDirectory().GetPath())
		})
	}
}

func TestEnvChange(t *testing.T) {
	spawnDiffs := diffLogs(t, "env_change", "7.3.1")
	require.Len(t, spawnDiffs, 1)

	sd1 := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out$", sd1.PrimaryOutput)
	assert.Equal(t, "//pkg:gen", sd1.TargetLabel)
	assert.Equal(t, "Genrule", sd1.Mnemonic)
	assert.Empty(t, sd1.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd1.GetModified().GetDiffs(), 1)
	sd1d1 := sd1.GetModified().Diffs[0]
	assert.Equal(t, map[string]string{
		"OLD_AND_NEW": "old",
		"OLD_ONLY":    "old_only",
	}, sd1d1.GetEnv().GetOldChanged())
	assert.Equal(t, map[string]string{
		"OLD_AND_NEW": "new",
		"NEW_ONLY":    "new_only",
	}, sd1d1.GetEnv().GetNewChanged())
}

func TestNonHermetic(t *testing.T) {
	spawnDiffs := diffLogs(t, "non_hermetic", "7.3.1")
	require.Len(t, spawnDiffs, 1)

	sd1 := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out$", sd1.PrimaryOutput)
	assert.Equal(t, "//pkg:gen", sd1.TargetLabel)
	assert.Equal(t, "Genrule", sd1.Mnemonic)
	assert.Empty(t, sd1.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd1.GetModified().GetDiffs(), 1)
	sd1d1 := sd1.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_OutputContents{}, sd1d1.Diff)
	require.Len(t, sd1d1.GetOutputContents().GetFileDiffs(), 1)
	sd1fd1 := sd1d1.GetOutputContents().GetFileDiffs()[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out$", sd1fd1.GetOldFile().GetPath())
	assert.NotNil(t, sd1fd1.GetOldFile().GetDigest())
	assert.NotEqual(t, sd1fd1.GetOldFile().GetDigest().GetHash(), sd1fd1.GetNewFile().GetDigest().GetHash())
}

func TestSymlinks(t *testing.T) {
	spawnDiffs := diffLogs(t, "symlinks", "7.4.0")
	require.Len(t, spawnDiffs, 1)

	sd1 := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out$", sd1.PrimaryOutput)
	assert.Equal(t, "//pkg:copy", sd1.TargetLabel)
	assert.Equal(t, "CopyFile", sd1.Mnemonic)
	assert.Empty(t, sd1.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd1.GetModified().GetDiffs(), 1)
	sd1d1 := sd1.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, sd1d1.Diff)
	require.Len(t, sd1d1.GetInputContents().GetFileDiffs(), 1)
	sd1fd1 := sd1d1.GetInputContents().GetFileDiffs()[0]
	assert.Equal(t, "pkg/file", sd1fd1.GetOldFile().GetPath())
	assert.Equal(t, "pkg/file", sd1fd1.GetNewFile().GetPath())
	assert.Equal(t, digest("foo\n"), sd1fd1.GetOldFile().GetDigest())
	assert.Equal(t, digest("not_foo\n"), sd1fd1.GetNewFile().GetDigest())
}

func TestTargetRenamed(t *testing.T) {
	spawnDiffs := diffLogs(t, "target_renamed", "7.3.1")
	require.Len(t, spawnDiffs, 4)

	sd1 := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out1$", sd1.PrimaryOutput)
	assert.Equal(t, "//pkg:gen1", sd1.TargetLabel)
	assert.Equal(t, "Genrule", sd1.Mnemonic)
	assert.NotNil(t, sd1.GetOldOnly())
	assert.Equal(t, false, sd1.GetOldOnly().GetTopLevel())

	sd2 := spawnDiffs[1]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out2$", sd2.PrimaryOutput)
	assert.Equal(t, "//pkg:gen2", sd2.TargetLabel)
	assert.Equal(t, "Genrule", sd2.Mnemonic)
	assert.NotNil(t, sd2.GetOldOnly())
	assert.Equal(t, true, sd2.GetOldOnly().GetTopLevel())

	sd3 := spawnDiffs[2]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out3$", sd3.PrimaryOutput)
	assert.Equal(t, "//pkg:gen3", sd3.TargetLabel)
	assert.Equal(t, "Genrule", sd3.Mnemonic)
	assert.NotNil(t, sd3.GetNewOnly())
	assert.Equal(t, false, sd3.GetNewOnly().GetTopLevel())

	sd4 := spawnDiffs[3]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out4$", sd4.PrimaryOutput)
	assert.Equal(t, "//pkg:gen4", sd4.TargetLabel)
	assert.Equal(t, "Genrule", sd4.Mnemonic)
	assert.NotNil(t, sd4.GetNewOnly())
	assert.Equal(t, true, sd4.GetNewOnly().GetTopLevel())
}

func TestFlakyTest(t *testing.T) {
	spawnDiffs := diffLogs(t, "flaky_test", "7.3.1")
	require.Len(t, spawnDiffs, 1)

	sd1 := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/testlogs/pkg/flaky_test/test.log$", sd1.PrimaryOutput)
	assert.Equal(t, "//pkg:flaky_test", sd1.TargetLabel)
	assert.Equal(t, "TestRunner", sd1.Mnemonic)
	assert.Equal(t, map[string]uint32{
		"TestRunner (XML generation)": 1,
	}, sd1.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd1.GetModified().GetDiffs(), 1)
	sd1d1 := sd1.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_ExitCode{}, sd1d1.Diff)
	assert.Equal(t, int32(0), sd1d1.GetExitCode().Old)
	assert.Equal(t, int32(1), sd1d1.GetExitCode().New)
}

func diffLogs(t *testing.T, name, bazelVersion string) []*spawn_diff.SpawnDiff {
	dir := "buildbuddy/cli/explain/compactgraph/testdata"
	oldPath, err := runfiles.Rlocation(path.Join(dir, bazelVersion, name+"_old.pb.zstd"))
	require.NoError(t, err)
	newPath, err := runfiles.Rlocation(path.Join(dir, bazelVersion, name+"_new.pb.zstd"))
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

func digest(content string) *spawn.Digest {
	h := sha256.New()
	h.Write([]byte(content))
	return &spawn.Digest{
		Hash:             hex.EncodeToString(h.Sum(nil)),
		SizeBytes:        int64(len(content)),
		HashFunctionName: "SHA-256",
	}
}
