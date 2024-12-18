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

func TestJavaNoopImplChange_7_3_1(t *testing.T) {
	spawnDiffs := diffLogs(t, "java_noop_impl_change", "7.3.1")
	require.Len(t, spawnDiffs, 3)

	{
		sd := spawnDiffs[0]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib.jar$", sd.PrimaryOutput)
		assert.Equal(t, "//src/main/java/com/example/lib:lib", sd.TargetLabel)
		assert.Equal(t, "Javac", sd.Mnemonic)
		assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
		require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
		fd := d.GetInputContents().GetFileDiffs()[0]
		assert.Equal(t, "src/main/java/com/example/lib/Lib.java", fd.GetOldFile().GetPath())
		assert.NotNil(t, fd.GetOldFile().GetDigest())
		assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
	}

	{
		sd := spawnDiffs[1]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib-hjar.jar$", sd.PrimaryOutput)
		assert.Equal(t, "//src/main/java/com/example/lib:lib", sd.TargetLabel)
		assert.Equal(t, "Turbine", sd.Mnemonic)
		assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
		require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
		fd := d.GetInputContents().GetFileDiffs()[0]
		assert.Equal(t, "src/main/java/com/example/lib/Lib.java", fd.GetOldFile().GetPath())
		assert.NotNil(t, fd.GetOldFile().GetDigest())
		assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
	}

	{
		sd := spawnDiffs[2]
		assert.Regexp(t, "^bazel-out/[^/]+/testlogs/src/test/java/com/example/lib/lib_test/test.log$", sd.PrimaryOutput)
		assert.Equal(t, "//src/test/java/com/example/lib:lib_test", sd.TargetLabel)
		assert.Equal(t, "TestRunner", sd.Mnemonic)
		assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
		assert.True(t, sd.GetModified().GetExpected())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_OutputContents{}, d.Diff)
		require.Len(t, d.GetOutputContents().GetFileDiffs(), 1)
		fd := d.GetOutputContents().GetFileDiffs()[0]
		assert.Regexp(t, "^bazel-out/[^/]+/testlogs/src/test/java/com/example/lib/lib_test/test.xml$", fd.GetOldFile().GetPath())
	}
}

func TestJavaNoopImplChange_8_0_0(t *testing.T) {
	spawnDiffs := diffLogs(t, "java_noop_impl_change", "8.0.0")
	require.Len(t, spawnDiffs, 3)

	{
		sd := spawnDiffs[0]
		assert.Regexp(t, "^bazel-out/[^/]+/testlogs/src/test/java/com/example/lib/lib_test/test.log$", sd.PrimaryOutput)
		assert.Equal(t, "//src/test/java/com/example/lib:lib_test", sd.TargetLabel)
		assert.Equal(t, "TestRunner", sd.Mnemonic)
		assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
		assert.True(t, sd.GetModified().GetExpected())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_OutputContents{}, d.Diff)
		require.Len(t, d.GetOutputContents().GetFileDiffs(), 1)
		fd := d.GetOutputContents().GetFileDiffs()[0]
		assert.Regexp(t, "^bazel-out/[^/]+/testlogs/src/test/java/com/example/lib/lib_test/test.xml$", fd.GetOldFile().GetPath())
	}

	{
		sd := spawnDiffs[1]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib-hjar.jar$", sd.PrimaryOutput)
		assert.Equal(t, "//src/main/java/com/example/lib:lib", sd.TargetLabel)
		assert.Equal(t, "Turbine", sd.Mnemonic)
		assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
		require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
		fd := d.GetInputContents().GetFileDiffs()[0]
		assert.Equal(t, "src/main/java/com/example/lib/Lib.java", fd.GetOldFile().GetPath())
		assert.NotNil(t, fd.GetOldFile().GetDigest())
		assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
	}

	{
		sd := spawnDiffs[2]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib.jar$", sd.PrimaryOutput)
		assert.Equal(t, "//src/main/java/com/example/lib:lib", sd.TargetLabel)
		assert.Equal(t, "Javac", sd.Mnemonic)
		assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
		require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
		fd := d.GetInputContents().GetFileDiffs()[0]
		assert.Equal(t, "src/main/java/com/example/lib/Lib.java", fd.GetOldFile().GetPath())
		assert.NotNil(t, fd.GetOldFile().GetDigest())
		assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
	}
}

func TestJavaImplChange_7_3_1(t *testing.T) {
	spawnDiffs := diffLogs(t, "java_impl_change", "7.3.1")
	require.Len(t, spawnDiffs, 3)

	{
		sd := spawnDiffs[0]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib.jar$", sd.PrimaryOutput)
		assert.Equal(t, "//src/main/java/com/example/lib:lib", sd.TargetLabel)
		assert.Equal(t, "Javac", sd.Mnemonic)
		assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
		require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
		fd := d.GetInputContents().GetFileDiffs()[0]
		assert.Equal(t, "src/main/java/com/example/lib/Lib.java", fd.GetOldFile().GetPath())
		assert.NotNil(t, fd.GetOldFile().GetDigest())
		assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
	}

	{
		sd := spawnDiffs[1]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib-hjar.jar$", sd.PrimaryOutput)
		assert.Equal(t, "//src/main/java/com/example/lib:lib", sd.TargetLabel)
		assert.Equal(t, "Turbine", sd.Mnemonic)
		assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
		require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
		fd := d.GetInputContents().GetFileDiffs()[0]
		assert.Equal(t, "src/main/java/com/example/lib/Lib.java", fd.GetOldFile().GetPath())
		assert.NotNil(t, fd.GetOldFile().GetDigest())
		assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
	}

	{
		sd := spawnDiffs[2]
		assert.Regexp(t, "^bazel-out/[^/]+/testlogs/src/test/java/com/example/lib/lib_test/test.log$", sd.PrimaryOutput)
		assert.Equal(t, "//src/test/java/com/example/lib:lib_test", sd.TargetLabel)
		assert.Equal(t, "TestRunner", sd.Mnemonic)
		assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
		assert.False(t, sd.GetModified().GetExpected())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
		require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
		fd := d.GetInputContents().GetFileDiffs()[0]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/src/test/java/com/example/lib/lib_test.runfiles$", fd.GetOldDirectory().GetPath())
	}
}
func TestJavaImplChange_8_0_0(t *testing.T) {
	spawnDiffs := diffLogs(t, "java_impl_change", "8.0.0")
	require.Len(t, spawnDiffs, 2)

	{
		sd := spawnDiffs[0]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib-hjar.jar$", sd.PrimaryOutput)
		assert.Equal(t, "//src/main/java/com/example/lib:lib", sd.TargetLabel)
		assert.Equal(t, "Turbine", sd.Mnemonic)
		assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
		require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
		fd := d.GetInputContents().GetFileDiffs()[0]
		assert.Equal(t, "src/main/java/com/example/lib/Lib.java", fd.GetOldFile().GetPath())
		assert.NotNil(t, fd.GetOldFile().GetDigest())
		assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
	}

	{
		sd := spawnDiffs[1]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib.jar$", sd.PrimaryOutput)
		assert.Equal(t, "//src/main/java/com/example/lib:lib", sd.TargetLabel)
		assert.Equal(t, "Javac", sd.Mnemonic)
		assert.Equal(t, map[string]uint32{
			"Runfiles directory": 1,
			"TestRunner":         1,
		}, sd.GetModified().GetTransitivelyInvalidated())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		d := sd.GetModified().Diffs[0]
		require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
		require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
		fd := d.GetInputContents().GetFileDiffs()[0]
		assert.Equal(t, "src/main/java/com/example/lib/Lib.java", fd.GetOldFile().GetPath())
		assert.NotNil(t, fd.GetOldFile().GetDigest())
		assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
	}
}

func TestJavaHeaderChange(t *testing.T) {
	for _, bazelVersion := range []string{"7.3.1", "8.0.0"} {
		t.Run(bazelVersion, func(t *testing.T) {
			spawnDiffs := diffLogs(t, "java_header_change", bazelVersion)
			require.Len(t, spawnDiffs, 3)

			{
				sd := spawnDiffs[0]
				assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib.jar$", sd.PrimaryOutput)
				assert.Equal(t, "//src/main/java/com/example/lib:lib", sd.TargetLabel)
				assert.Equal(t, "Javac", sd.Mnemonic)
				assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
				require.Len(t, sd.GetModified().GetDiffs(), 1)
				d := sd.GetModified().Diffs[0]
				require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
				require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
				fd := d.GetInputContents().GetFileDiffs()[0]
				assert.Equal(t, "src/main/java/com/example/lib/Lib.java", fd.GetOldFile().GetPath())
				assert.NotNil(t, fd.GetOldFile().GetDigest())
				assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
			}

			{
				sd := spawnDiffs[1]
				assert.Regexp(t, "^bazel-out/[^/]+/bin/src/main/java/com/example/lib/liblib-hjar.jar$", sd.PrimaryOutput)
				assert.Equal(t, "//src/main/java/com/example/lib:lib", sd.TargetLabel)
				assert.Equal(t, "Turbine", sd.Mnemonic)
				assert.Equal(t, map[string]uint32{"Javac": 2}, sd.GetModified().GetTransitivelyInvalidated())
				require.Len(t, sd.GetModified().GetDiffs(), 1)
				d := sd.GetModified().Diffs[0]
				require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
				require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
				fd := d.GetInputContents().GetFileDiffs()[0]
				assert.Equal(t, "src/main/java/com/example/lib/Lib.java", fd.GetOldFile().GetPath())
				assert.NotNil(t, fd.GetOldFile().GetDigest())
				assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
			}

			{
				sd := spawnDiffs[2]
				assert.Regexp(t, "^bazel-out/[^/]+/testlogs/src/test/java/com/example/lib/lib_test/test.log$", sd.PrimaryOutput)
				assert.Equal(t, "//src/test/java/com/example/lib:lib_test", sd.TargetLabel)
				assert.Equal(t, "TestRunner", sd.Mnemonic)
				assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
				assert.False(t, sd.GetModified().GetExpected())
				require.Len(t, sd.GetModified().GetDiffs(), 1)
				d := sd.GetModified().Diffs[0]
				require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
				require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
				fd := d.GetInputContents().GetFileDiffs()[0]
				assert.Regexp(t, "^bazel-out/[^/]+/bin/src/test/java/com/example/lib/lib_test.runfiles$", fd.GetOldDirectory().GetPath())
			}
		})
	}
}

func TestEnvChange(t *testing.T) {
	for _, bazelVersion := range []string{"7.3.1", "8.0.0"} {
		t.Run(bazelVersion, func(t *testing.T) {
			spawnDiffs := diffLogs(t, "env_change", "7.3.1")
			require.Len(t, spawnDiffs, 1)

			sd := spawnDiffs[0]
			assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out$", sd.PrimaryOutput)
			assert.Equal(t, "//pkg:gen", sd.TargetLabel)
			assert.Equal(t, "Genrule", sd.Mnemonic)
			assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
			require.Len(t, sd.GetModified().GetDiffs(), 1)
			d := sd.GetModified().Diffs[0]
			assert.Equal(t, map[string]string{
				"OLD_AND_NEW": "old",
				"OLD_ONLY":    "old_only",
			}, d.GetEnv().GetOldChanged())
			assert.Equal(t, map[string]string{
				"OLD_AND_NEW": "new",
				"NEW_ONLY":    "new_only",
			}, d.GetEnv().GetNewChanged())
		})
	}
}

func TestTransitiveInvalidation(t *testing.T) {
	spawnDiffs := diffLogs(t, "transitive_invalidation", "8.0.0")
	require.Len(t, spawnDiffs, 2)

	{
		sd := spawnDiffs[0]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out_direct1$", sd.PrimaryOutput)
		assert.Equal(t, "//pkg:direct1", sd.TargetLabel)
		assert.Equal(t, "Genrule", sd.Mnemonic)
		assert.Equal(t, map[string]uint32{"Genrule": 3}, sd.GetModified().GetTransitivelyInvalidated())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		assert.Len(t, sd.GetModified().GetDiffs()[0].GetInputContents().GetFileDiffs(), 1)
	}
	{
		sd := spawnDiffs[1]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out_direct2$", sd.PrimaryOutput)
		assert.Equal(t, "//pkg:direct2", sd.TargetLabel)
		assert.Equal(t, "Genrule", sd.Mnemonic)
		assert.Equal(t, map[string]uint32{"Genrule": 3}, sd.GetModified().GetTransitivelyInvalidated())
		require.Len(t, sd.GetModified().GetDiffs(), 1)
		assert.Len(t, sd.GetModified().GetDiffs()[0].GetInputContents().GetFileDiffs(), 1)
	}
}

func TestNonHermetic(t *testing.T) {
	spawnDiffs := diffLogs(t, "non_hermetic", "7.3.1")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out$", sd.PrimaryOutput)
	assert.Equal(t, "//pkg:gen", sd.TargetLabel)
	assert.Equal(t, "Genrule", sd.Mnemonic)
	assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_OutputContents{}, d.Diff)
	require.Len(t, d.GetOutputContents().GetFileDiffs(), 1)
	fd := d.GetOutputContents().GetFileDiffs()[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out$", fd.GetOldFile().GetPath())
	assert.NotNil(t, fd.GetOldFile().GetDigest())
	assert.NotEqual(t, fd.GetOldFile().GetDigest().GetHash(), fd.GetNewFile().GetDigest().GetHash())
}

func TestSymlinks(t *testing.T) {
	spawnDiffs := diffLogs(t, "symlinks", "8.0.0")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out$", sd.PrimaryOutput)
	assert.Equal(t, "//pkg:copy", sd.TargetLabel)
	assert.Equal(t, "CopyFile", sd.Mnemonic)
	assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
	require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
	fd := d.GetInputContents().GetFileDiffs()[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/symlink2$", fd.GetLogicalPath())
	assert.Equal(t, "pkg/file", fd.GetOldFile().GetPath())
	assert.Equal(t, "pkg/file", fd.GetNewFile().GetPath())
	assert.Equal(t, digest("foo\n"), fd.GetOldFile().GetDigest())
	assert.Equal(t, digest("not_foo\n"), fd.GetNewFile().GetDigest())
}

func TestTargetRenamed(t *testing.T) {
	spawnDiffs := diffLogs(t, "target_renamed", "7.3.1")
	require.Len(t, spawnDiffs, 4)

	{
		sd := spawnDiffs[0]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out1$", sd.PrimaryOutput)
		assert.Equal(t, "//pkg:gen1", sd.TargetLabel)
		assert.Equal(t, "Genrule", sd.Mnemonic)
		assert.NotNil(t, sd.GetOldOnly())
		assert.Equal(t, false, sd.GetOldOnly().GetTopLevel())
	}

	{
		sd := spawnDiffs[1]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out2$", sd.PrimaryOutput)
		assert.Equal(t, "//pkg:gen2", sd.TargetLabel)
		assert.Equal(t, "Genrule", sd.Mnemonic)
		assert.NotNil(t, sd.GetOldOnly())
		assert.Equal(t, true, sd.GetOldOnly().GetTopLevel())
	}

	{
		sd := spawnDiffs[2]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out3$", sd.PrimaryOutput)
		assert.Equal(t, "//pkg:gen3", sd.TargetLabel)
		assert.Equal(t, "Genrule", sd.Mnemonic)
		assert.NotNil(t, sd.GetNewOnly())
		assert.Equal(t, false, sd.GetNewOnly().GetTopLevel())
	}

	{
		sd := spawnDiffs[3]
		assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out4$", sd.PrimaryOutput)
		assert.Equal(t, "//pkg:gen4", sd.TargetLabel)
		assert.Equal(t, "Genrule", sd.Mnemonic)
		assert.NotNil(t, sd.GetNewOnly())
		assert.Equal(t, true, sd.GetNewOnly().GetTopLevel())
	}
}

func TestFlakyTest(t *testing.T) {
	spawnDiffs := diffLogs(t, "flaky_test", "7.3.1")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/testlogs/pkg/flaky_test/test.log$", sd.PrimaryOutput)
	assert.Equal(t, "//pkg:flaky_test", sd.TargetLabel)
	assert.Equal(t, "TestRunner", sd.Mnemonic)
	assert.Equal(t, map[string]uint32{
		"TestRunner (XML generation)": 1,
	}, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_ExitCode{}, d.Diff)
	assert.Equal(t, int32(0), d.GetExitCode().Old)
	assert.Equal(t, int32(1), d.GetExitCode().New)
}

func TestMultipleOutputs(t *testing.T) {
	spawnDiffs := diffLogs(t, "multiple_outputs", "7.3.1")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/outa$", sd.PrimaryOutput)
	assert.Equal(t, "//pkg:gen1", sd.TargetLabel)
	assert.Equal(t, "Genrule", sd.Mnemonic)
	assert.Equal(t, map[string]uint32{"Genrule": 1}, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
	require.Len(t, d.GetInputContents().GetFileDiffs(), 2)
	fd0 := d.GetInputContents().GetFileDiffs()[0]
	assert.Equal(t, "pkg/inb", fd0.GetLogicalPath())
	assert.Equal(t, "pkg/inb", fd0.GetOldFile().GetPath())
	assert.Equal(t, "pkg/inb", fd0.GetNewFile().GetPath())
	assert.Equal(t, digest("b\n"), fd0.GetOldFile().GetDigest())
	assert.Equal(t, digest("b2\n"), fd0.GetNewFile().GetDigest())
	fd1 := d.GetInputContents().GetFileDiffs()[1]
	assert.Equal(t, "pkg/inc", fd1.GetLogicalPath())
	assert.Equal(t, "pkg/inc", fd1.GetOldFile().GetPath())
	assert.Equal(t, "pkg/inc", fd1.GetNewFile().GetPath())
	assert.Equal(t, digest("c\n"), fd1.GetOldFile().GetDigest())
	assert.Equal(t, digest("c2\n"), fd1.GetNewFile().GetDigest())
}

func TestSourceDirectory(t *testing.T) {
	spawnDiffs := diffLogs(t, "source_directory", "7.3.1")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/out$", sd.PrimaryOutput)
	assert.Equal(t, "//pkg:gen", sd.TargetLabel)
	assert.Equal(t, "Genrule", sd.Mnemonic)
	assert.Empty(t, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
	require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
	fd := d.GetInputContents().GetFileDiffs()[0]
	assert.Equal(t, "pkg/src_dir", fd.GetLogicalPath())
	assert.Equal(t, "pkg/src_dir", fd.GetOldDirectory().GetPath())
	assert.Equal(t, "pkg/src_dir", fd.GetNewDirectory().GetPath())

	require.Len(t, fd.GetOldDirectory().GetFiles(), 2)
	assert.Equal(t, "file1.txt", fd.GetOldDirectory().GetFiles()[0].GetPath())
	assert.Equal(t, digest("old\n"), fd.GetOldDirectory().GetFiles()[0].GetDigest())
	assert.Equal(t, "file2.txt", fd.GetOldDirectory().GetFiles()[1].GetPath())
	assert.Equal(t, digest("unchanged\n"), fd.GetOldDirectory().GetFiles()[1].GetDigest())

	require.Len(t, fd.GetNewDirectory().GetFiles(), 3)
	assert.Equal(t, "file1.txt", fd.GetNewDirectory().GetFiles()[0].GetPath())
	assert.Equal(t, digest("new\n"), fd.GetNewDirectory().GetFiles()[0].GetDigest())
	assert.Equal(t, "file2.txt", fd.GetNewDirectory().GetFiles()[1].GetPath())
	assert.Equal(t, digest("unchanged\n"), fd.GetNewDirectory().GetFiles()[1].GetDigest())
	assert.Equal(t, "file3.txt", fd.GetNewDirectory().GetFiles()[2].GetPath())
	assert.Equal(t, digest("new\n"), fd.GetNewDirectory().GetFiles()[2].GetDigest())
}

func TestTreeArtifactPaths(t *testing.T) {
	spawnDiffs := diffLogs(t, "tree_artifact_paths", "7.3.1")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/out/tree_artifact$", sd.PrimaryOutput)
	assert.Equal(t, "//out:tree_artifact", sd.TargetLabel)
	assert.Equal(t, "Action", sd.Mnemonic)
	assert.Equal(t, map[string]uint32{"Action": 1}, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_Args{}, d.Diff)
	assert.NotEqual(t, d.GetArgs().GetOld(), d.GetArgs().GetNew())
}

func TestTreeArtifactContents(t *testing.T) {
	spawnDiffs := diffLogs(t, "tree_artifact_contents", "7.3.1")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/out/tree_artifact$", sd.PrimaryOutput)
	assert.Equal(t, "//out:tree_artifact", sd.TargetLabel)
	assert.Equal(t, "Action", sd.Mnemonic)
	assert.Equal(t, map[string]uint32{"Action": 1}, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_Args{}, d.Diff)
	assert.NotEqual(t, d.GetArgs().GetOld(), d.GetArgs().GetNew())
}

func TestToolRunfilesPaths(t *testing.T) {
	spawnDiffs := diffLogs(t, "tool_runfiles_paths", "8.0.0")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/tool.runfiles", sd.PrimaryOutput)
	assert.Equal(t, "//tools:tool_sh", sd.TargetLabel)
	assert.Equal(t, "Runfiles directory", sd.Mnemonic)
	assert.Equal(t, map[string]uint32{"Genrule": 2}, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputPaths{}, d.Diff)
	assert.Equal(t, []string{"_main/pkg/file3.txt"}, d.GetInputPaths().GetNewOnly())
	assert.Empty(t, d.GetInputPaths().GetOldOnly())
}

func TestToolRunfilesContents(t *testing.T) {
	spawnDiffs := diffLogs(t, "tool_runfiles_contents", "8.0.0")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/pkg/tool.runfiles", sd.PrimaryOutput)
	assert.Equal(t, "//tools:tool_sh", sd.TargetLabel)
	assert.Equal(t, "Runfiles directory", sd.Mnemonic)
	assert.Equal(t, map[string]uint32{"Genrule": 2}, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
	require.Len(t, d.GetInputContents().GetFileDiffs(), 1)
	fd := d.GetInputContents().GetFileDiffs()[0]
	assert.Equal(t, "_main/pkg/file1.txt", fd.GetLogicalPath())
	assert.Equal(t, "pkg/file1.txt", fd.GetOldFile().GetPath())
	assert.Equal(t, "pkg/file1.txt", fd.GetNewFile().GetPath())
	assert.Equal(t, digest("old\n"), fd.GetOldFile().GetDigest())
	assert.Equal(t, digest("new\n"), fd.GetNewFile().GetDigest())
}

func TestToolRunfilesContentsTransitive(t *testing.T) {
	spawnDiffs := diffLogs(t, "tool_runfiles_contents_transitive", "8.0.0")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/tools/tool.sh", sd.PrimaryOutput)
	assert.Equal(t, "//tools:tool_sh", sd.TargetLabel)
	assert.Equal(t, "Genrule", sd.Mnemonic)
	assert.Equal(t, map[string]uint32{"Genrule": 2, "Runfiles directory": 1}, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_Args{}, d.Diff)
	assert.NotEqual(t, d.GetArgs().GetOld(), d.GetArgs().GetNew())
}

func TestToolRunfilesSetStructure(t *testing.T) {
	spawnDiffs := diffLogs(t, "tool_runfiles_set_structure", "8.0.0")
	// The structure of the nested sets in the runfiles tree changed, but the
	// flattened tree is the same, so there should be no diffs.
	assert.Empty(t, spawnDiffs)
}

func TestToolRunfilesSymlinksPaths(t *testing.T) {
	spawnDiffs := diffLogs(t, "tool_runfiles_symlinks_paths", "8.0.0")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/tools/tool.runfiles$", sd.PrimaryOutput)
	assert.Empty(t, sd.TargetLabel)
	assert.Equal(t, "Runfiles directory", sd.Mnemonic)
	assert.Equal(t, map[string]uint32{"Genrule": 1}, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputPaths{}, d.Diff)
	assert.Equal(t, []string{"_main/old_only", "old_only"}, d.GetInputPaths().GetOldOnly())
	assert.Equal(t, []string{"_main/new_only", "new_only"}, d.GetInputPaths().GetNewOnly())
	// Changed file contents are not reported if file paths changed.
}

func TestToolRunfilesSymlinksContents(t *testing.T) {
	spawnDiffs := diffLogs(t, "tool_runfiles_symlinks_contents", "8.0.0")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/tools/tool.runfiles$", sd.PrimaryOutput)
	assert.Empty(t, sd.TargetLabel)
	assert.Equal(t, "Runfiles directory", sd.Mnemonic)
	assert.Equal(t, map[string]uint32{"Genrule": 1}, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_InputContents{}, d.Diff)
	require.Len(t, d.GetInputContents().GetFileDiffs(), 2)
	{
		fd := d.GetInputContents().GetFileDiffs()[0]
		assert.Equal(t, "_main/other/pkg/common", fd.GetLogicalPath())
		assert.Equal(t, "tools/file1.txt", fd.GetOldFile().GetPath())
		assert.Equal(t, digest("old\n"), fd.GetOldFile().GetDigest())
		assert.Equal(t, "tools/file1.txt", fd.GetNewFile().GetPath())
		assert.Equal(t, digest("new\n"), fd.GetNewFile().GetDigest())
	}
	{
		fd := d.GetInputContents().GetFileDiffs()[1]
		assert.Equal(t, "other/pkg/common", fd.GetLogicalPath())
		assert.Equal(t, "tools/file4.txt", fd.GetOldFile().GetPath())
		assert.Equal(t, digest("old\n"), fd.GetOldFile().GetDigest())
		assert.Equal(t, "tools/file4.txt", fd.GetNewFile().GetPath())
		assert.Equal(t, digest("new\n"), fd.GetNewFile().GetDigest())
	}
}

func TestToolRunfilesSymlinksContentsTransitive(t *testing.T) {
	spawnDiffs := diffLogs(t, "tool_runfiles_symlinks_contents_transitive", "8.0.0")
	require.Len(t, spawnDiffs, 1)

	sd := spawnDiffs[0]
	assert.Regexp(t, "^bazel-out/[^/]+/bin/gen/file$", sd.PrimaryOutput)
	assert.Equal(t, "//gen:gen", sd.TargetLabel)
	assert.Equal(t, "Genrule", sd.Mnemonic)
	assert.Equal(t, map[string]uint32{
		"Genrule":            1,
		"Runfiles directory": 1,
	}, sd.GetModified().GetTransitivelyInvalidated())
	require.Len(t, sd.GetModified().GetDiffs(), 1)
	d := sd.GetModified().Diffs[0]
	require.IsType(t, &spawn_diff.Diff_Args{}, d.Diff)
	assert.NotEqual(t, d.GetArgs().GetOld(), d.GetArgs().GetNew())
}

func TestToolRunfilesSymlinksSetStructure(t *testing.T) {
	spawnDiffs := diffLogs(t, "tool_runfiles_symlinks_set_structure", "8.0.0")
	// The structure of the nested sets in the runfiles tree changed, but the
	// flattened tree is the same, so there should be no diffs.
	assert.Empty(t, spawnDiffs)
}

func TestSettings(t *testing.T) {
	_, err := diffLogsAllowingError(t, "settings", "8.0.0")
	require.ErrorContains(t, err, "--enable_bzlmod")
	require.ErrorContains(t, err, "--legacy_external_runfiles")
}

func diffLogsAllowingError(t *testing.T, name, bazelVersion string) ([]*spawn_diff.SpawnDiff, error) {
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
	oldLog, err := compactgraph.ReadCompactLog(oldLogFile)
	require.NoError(t, err)
	newLog, err := compactgraph.ReadCompactLog(newLogFile)
	require.NoError(t, err)
	return compactgraph.Diff(oldLog, newLog)
}

func diffLogs(t *testing.T, name, bazelVersion string) []*spawn_diff.SpawnDiff {
	spawnDiffs, err := diffLogsAllowingError(t, name, bazelVersion)
	require.NoError(t, err)
	return spawnDiffs
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
