package plugin

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/config"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Configure("--verbose=1")
}

func TestGetConfiguredPlugins_CombinesUserAndWorkspaceConfigs(t *testing.T) {
	ws, home := setup(t)
	testfs.WriteAllFileContents(t, home, map[string]string{
		"a/.empty": "",
		"b/.empty": "",
		"buildbuddy.yaml": `
plugins:
  - path: ./a
  - path: ./b
  - repo: foo/bar@v1.0
  - repo: foo/bar@v1.0
    path: ./foobar_subdir1
`,
	})
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"a/.empty": "",
		"b/.empty": "",
		"buildbuddy.yaml": `
plugins:
  - path: ./a
  - path: ./b
  - repo: foo/bar@v2.0
  - repo: foo/bar@v2.0
    path: foobar_subdir2
`,
	})
	setTestHomeDir(t, home)

	plugins, err := getConfiguredPlugins(ws)

	require.NoError(t, err)
	var ids []string
	for _, p := range plugins {
		id, err := p.VersionedID()
		require.NoError(t, err)
		ids = append(ids, id)
	}
	expectedIDs := []string{
		filepath.Join(home, "a"),
		filepath.Join(home, "b"),
		// Note: foo/bar@v1.0 should not be included since the workspace
		// buildbuddy.yaml includes a version override with v2.0

		// repo_subdir1 should be included since the home buildbuddy.yaml
		// doesn't include a specific override for that path:
		"https://github.com/foo/bar@v1.0:foobar_subdir1",
		filepath.Join(ws, "a"),
		filepath.Join(ws, "b"),
		"https://github.com/foo/bar@v2.0",
		"https://github.com/foo/bar@v2.0:foobar_subdir2",
	}
	require.Equal(t, expectedIDs, ids)
}

func TestInstall_ToWorkspaceConfig_LocalAbsolutePath_CreateNewConfig(t *testing.T) {
	ws, _ := setup(t)
	pluginDir := testfs.MakeDirAll(t, ws, "test-plugin")

	exitCode, err := HandleInstall([]string{"--path=" + pluginDir})

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	config := testfs.ReadFileAsString(t, ws, "buildbuddy.yaml")
	expectedConfig := `plugins:
  - path: ` + pluginDir + `
`
	require.Equal(t, expectedConfig, config)
}

func TestInstall_ToWorkspaceConfig_LocalRelativePath_CreateNewConfig(t *testing.T) {
	ws, _ := setup(t)
	testfs.MakeDirAll(t, ws, "test-plugin")

	exitCode, err := HandleInstall([]string{"--path=./test-plugin"})

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	config := testfs.ReadFileAsString(t, ws, "buildbuddy.yaml")
	expectedConfig := `plugins:
  - path: ./test-plugin
`
	require.Equal(t, expectedConfig, config)
}

func TestInstall_ToWorkspaceConfig_OverwriteExistingConfigWithPlugins(t *testing.T) {
	ws, _ := setup(t)
	testfs.MakeDirAll(t, ws, "test-plugin-1")
	testfs.MakeDirAll(t, ws, "test-plugin-2")
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"buildbuddy.yaml": `
actions:
  - name: "Foo"
    steps:
      - run: |
          echo hello
plugins:
  - path: ./test-plugin-1
`,
	})

	exitCode, err := HandleInstall([]string{"--path=./test-plugin-2"})

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	config := testfs.ReadFileAsString(t, ws, "buildbuddy.yaml")
	expectedConfig := `
actions:
  - name: "Foo"
    steps:
      - run: |
          echo hello
plugins:
  - path: ./test-plugin-1
  - path: ./test-plugin-2
`
	require.Equal(t, expectedConfig, config)
}

func TestInstall_ToWorkspaceConfig_OverwriteExistingConfigWithNoPlugins(t *testing.T) {
	ws, _ := setup(t)
	testfs.MakeDirAll(t, ws, "test-plugin-1")
	testfs.MakeDirAll(t, ws, "test-plugin-2")
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"buildbuddy.yaml": `
actions:
  - name: "Foo"
    steps:
      - run: |
          echo hello
`,
	})

	exitCode, err := HandleInstall([]string{"--path=./test-plugin-2"})

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	config := testfs.ReadFileAsString(t, ws, "buildbuddy.yaml")
	expectedConfig := `
actions:
  - name: "Foo"
    steps:
      - run: |
          echo hello

plugins:
  - path: ./test-plugin-2
`
	require.Equal(t, expectedConfig, config)
}

func TestInstall_ToUserConfig_CreateNewConfig(t *testing.T) {
	_, home := setup(t)
	testfs.MakeDirAll(t, home, "test-plugin")

	exitCode, err := HandleInstall([]string{"--path=./test-plugin", "--user"})

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	config := testfs.ReadFileAsString(t, home, "buildbuddy.yaml")
	expectedConfig := `plugins:
  - path: ./test-plugin
`
	require.Equal(t, expectedConfig, config)
}

func TestParsePluginSpec(t *testing.T) {
	for _, tc := range []struct {
		Repo, PathArg  string
		ExpectedConfig *config.PluginConfig
	}{
		{
			":/local/path", "",
			&config.PluginConfig{Path: "/local/path"},
		},
		{
			"foo/bar", "",
			&config.PluginConfig{Repo: "foo/bar"},
		},
		{
			"foo/bar@v1.0", "",
			&config.PluginConfig{Repo: "foo/bar@v1.0"},
		},
		{
			"foo/bar:subdir", "",
			&config.PluginConfig{Repo: "foo/bar", Path: "subdir"},
		},
		{
			"example.com/foo/bar", "",
			&config.PluginConfig{Repo: "example.com/foo/bar"},
		},
		{
			"https://example.com/foo/bar", "",
			&config.PluginConfig{Repo: "example.com/foo/bar"},
		},
		{
			"https://example.com/foo/bar@v1:subdir", "",
			&config.PluginConfig{Repo: "example.com/foo/bar@v1", Path: "subdir"},
		},
		{
			"https://example.com/foo/bar@v1", "subdir",
			&config.PluginConfig{Repo: "example.com/foo/bar@v1", Path: "subdir"},
		},
		{
			"https://example.com/foo/bar@v1:/nested/subdir", "",
			&config.PluginConfig{Repo: "example.com/foo/bar@v1", Path: "/nested/subdir"},
		},
	} {
		cfg, err := parsePluginSpec(tc.Repo, tc.PathArg)

		require.NoError(t, err)
		assert.Equal(t, tc.ExpectedConfig, cfg)
	}
}

func setup(t *testing.T) (ws, home string) {
	root := testfs.MakeTempDir(t)
	ws = testfs.MakeDirAll(t, root, "workspace")
	testfs.WriteAllFileContents(t, ws, map[string]string{"WORKSPACE": ""})
	home = testfs.MakeDirAll(t, root, "home")

	setTestHomeDir(t, home)
	setTestWorkDir(t, ws)
	workspace.SetForTest(ws)

	return ws, home
}

func setTestWorkDir(t *testing.T, path string) {
	original, err := os.Getwd()
	require.NoError(t, err)
	t.Cleanup(func() {
		err := os.Chdir(original)
		require.NoError(t, err)
	})
}

func setTestHomeDir(t *testing.T, path string) {
	original := os.Getenv("HOME")
	err := os.Setenv("HOME", path)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := os.Setenv("HOME", original)
		require.NoError(t, err)
	})
}

func TestPipelineWriter_AllOutputCaptured(t *testing.T) {
	// Regression test: output produced by handle_bazel_output.sh must not be
	// dropped due to pty teardown races. We run the pipeline many times with
	// a script that emits a known number of lines, and verify every line
	// arrives.
	const numLines = 200
	const iterations = 20

	pluginDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, pluginDir, map[string]string{
		// The script reads stdin (to satisfy the pipeline), then prints
		// numLines lines to stdout right before exiting. This stresses the
		// "output at exit" path where the pty teardown race used to drop data.
		"handle_bazel_output.sh": fmt.Sprintf(`#!/usr/bin/env bash
cat > /dev/null
for i in $(seq 1 %d); do
  echo "OUTPUT_LINE_$i"
done
`, numLines),
	})
	testfs.MakeExecutable(t, pluginDir, "handle_bazel_output.sh")

	for i := 0; i < iterations; i++ {
		t.Run(fmt.Sprintf("iteration_%d", i), func(t *testing.T) {
			p := &Plugin{
				config:   &config.PluginConfig{Path: pluginDir},
				tempDir:  t.TempDir(),
			}
			p.configFile = &config.File{Path: filepath.Join(pluginDir, "buildbuddy.yaml")}

			var out bytes.Buffer
			wc, err := PipelineWriter(&out, []*Plugin{p})
			require.NoError(t, err)

			// Write some input and immediately close to trigger plugin exit.
			_, err = wc.Write([]byte("hello\n"))
			require.NoError(t, err)
			err = wc.Close()
			require.NoError(t, err)

			output := out.String()
			for line := 1; line <= numLines; line++ {
				expected := fmt.Sprintf("OUTPUT_LINE_%d", line)
				if !strings.Contains(output, expected) {
					t.Fatalf("missing %s in output (got %d bytes, %d lines)",
						expected, len(output), strings.Count(output, "\n"))
				}
			}
		})
	}
}
