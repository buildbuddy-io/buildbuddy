package plugin

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// TestPipelineWriter_HandlesFinalLine guards against a regression in which
// the plugin output handler dropped the last line of plugin output due to a
// race between draining the pty and closing the pipe. The plugin here only
// emits a single line, so any lost-tail-output bug shows up as a missing line
// in the captured output. See: cli/test/integration/cli TestBazelBuildWithLocalPlugin.
func TestPipelineWriter_HandlesFinalLine(t *testing.T) {
	ws, _ := setup(t)
	pluginDir := testfs.MakeDirAll(t, ws, "plugins/echo")
	testfs.WriteAllFileContents(t, pluginDir, map[string]string{
		"handle_bazel_output.sh": "echo 'final line'\n",
	})

	const trials = 200
	for i := 0; i < trials; i++ {
		cfgFile := &config.File{
			Path:       filepath.Join(ws, "buildbuddy.yaml"),
			RootConfig: &config.RootConfig{},
		}
		plugins := []*Plugin{{
			config:     &config.PluginConfig{Path: "./plugins/echo"},
			configFile: cfgFile,
		}}

		var out bytes.Buffer
		wc, err := PipelineWriter(&out, plugins)
		require.NoError(t, err)
		_, err = wc.Write([]byte("some bazel output\n"))
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() { done <- wc.Close() }()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatalf("trial %d: Close hung", i)
		}
		require.Containsf(t, out.String(), "final line",
			"trial %d: plugin's final output line was dropped; got %q", i, out.String())
	}
}

func TestPipelineWriter_ClosesPluginOutputPipes(t *testing.T) {
	// /dev/fd is a symlink to /proc/self/fd on Linux and a real fdesc
	// filesystem on macOS; either way it lists the calling process's open
	// fds, so the leak check works on both.
	if _, err := os.Stat("/dev/fd"); err != nil {
		t.Skip("/dev/fd is not available")
	}

	ws, _ := setup(t)
	pluginDir := testfs.MakeDirAll(t, ws, "plugins/echo")
	testfs.WriteAllFileContents(t, pluginDir, map[string]string{
		"handle_bazel_output.sh": "cat >/dev/null\necho 'final line'\n",
	})
	cfgFile := &config.File{
		Path:       filepath.Join(ws, "buildbuddy.yaml"),
		RootConfig: &config.RootConfig{},
	}
	plugins := []*Plugin{{
		config:     &config.PluginConfig{Path: "./plugins/echo"},
		configFile: cfgFile,
	}}

	before := openFDCount(t)
	const trials = 20
	for i := 0; i < trials; i++ {
		var out bytes.Buffer
		wc, err := PipelineWriter(&out, plugins)
		require.NoError(t, err)
		_, err = wc.Write([]byte("some bazel output\n"))
		require.NoError(t, err)
		require.NoError(t, wc.Close())
		require.Contains(t, out.String(), "final line")
	}
	after := openFDCount(t)
	require.LessOrEqualf(t, after, before+2,
		"PipelineWriter leaked file descriptors; before=%d after=%d", before, after)
}

func openFDCount(t *testing.T) int {
	t.Helper()
	// Use Readdirnames rather than os.ReadDir: on macOS, /dev/fd is the
	// fdesc filesystem, and os.ReadDir lstats every entry, which fails
	// with EBADF for fds that get closed during iteration (including the
	// directory fd itself).
	f, err := os.Open("/dev/fd")
	require.NoError(t, err)
	defer f.Close()
	names, err := f.Readdirnames(-1)
	require.NoError(t, err)
	return len(names)
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
