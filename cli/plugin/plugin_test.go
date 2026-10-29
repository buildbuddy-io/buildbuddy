package plugin

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/config"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Configure("--verbose=1")
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
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

func TestPreBazel(t *testing.T) {
	originalArgs := []string{"test", "//initial"}
	resolvedOriginalArgs := []string{"--ignore_all_rc_files", "test", "//initial"}
	canonicalizedArgs := []string{
		"test",
		"--compilation_mode=opt",
		"--bes_backend=grpc://new",
		"--remote_header=x-buildbuddy-foo=1",
		"--remote_header=x-buildbuddy-bar=2",
		"//foo",
	}
	resolvedArgs := append([]string{"--ignore_all_rc_files"}, canonicalizedArgs...)

	tests := []struct {
		name                  string
		fileToWrite           string
		expectedForwardedArgs []string
		expectedResolvedArgs  []string
	}{
		{
			name:                  "writes to the forwarded args file",
			fileToWrite:           "$FORWARDED_BAZEL_ARGS_FILE",
			expectedForwardedArgs: canonicalizedArgs,
			expectedResolvedArgs:  resolvedArgs,
		},
		{
			name:                  "writes to the deprecated args file",
			fileToWrite:           "$1",
			expectedForwardedArgs: canonicalizedArgs,
			// In the deprecated flow, we do not re-resolve the args.
			expectedResolvedArgs: canonicalizedArgs,
		},
		// The resolved args file is read-only, so we expect the args to be unchanged.
		{
			name:                  "writes to the resolved args file",
			fileToWrite:           "$RESOLVED_BAZEL_ARGS_FILE",
			expectedForwardedArgs: originalArgs,
			expectedResolvedArgs:  resolvedOriginalArgs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ws, _ := setup(t)
			testfs.WriteAllFileContents(t, ws, map[string]string{
				// Plugin 1 sets a bunch of non-canonicalized flags.
				"plugins/plugin1/pre_bazel.sh": `cat > "` + test.fileToWrite + `" <<'EOF'
test
-c
opt
--bes_backend
grpc://old
--bes_backend
grpc://new
--remote_header
x-buildbuddy-foo=1
--remote_header
x-buildbuddy-bar=2
//foo
EOF
`,
			})

			plugin1 := testPlugin(t, ws, "./plugins/plugin1")
			args, err := arg.NewBazelArgs(originalArgs)
			require.NoError(t, err)

			args, execArgs, err := plugin1.PreBazel(args, []string{"--exec"})
			require.NoError(t, err)

			// We expect the output args to be canonicalized.
			require.Equal(t, test.expectedForwardedArgs, args.Forwarded())
			require.Equal(t, test.expectedResolvedArgs, args.Resolved())
			// Exec args should be passed through unchanged.
			require.Equal(t, []string{"--exec"}, execArgs)
		})
	}
}

func TestPreBazel_AddConfig(t *testing.T) {
	ws, _ := setup(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bazelrc":                    "test:plugin-cfg --test_output=all\n",
		"plugins/config/pre_bazel.sh": `echo '--config=plugin-cfg' >> "$FORWARDED_BAZEL_ARGS_FILE"`,
	})
	p := testPlugin(t, ws, "./plugins/config")
	args, err := arg.NewBazelArgs([]string{"test", "//initial"})
	require.NoError(t, err)

	args, execArgs, err := p.PreBazel(args, nil)
	require.NoError(t, err)
	require.Empty(t, execArgs)

	require.Equal(t, []string{"test", "--config=plugin-cfg", "//initial"}, args.Forwarded())
	require.Equal(t, []string{"--ignore_all_rc_files", "test", "--test_output=all", "//initial"}, args.Resolved())
}

func TestPreBazel_PreservesBBRC(t *testing.T) {
	ws, _ := setup(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc":                       `run:ci --stream_run_logs`,
		"plugins/config/pre_bazel.sh": `echo '--build_metadata=PLUGIN' >> "$FORWARDED_BAZEL_ARGS_FILE"`,
	})
	p := testPlugin(t, ws, "./plugins/config")
	args, err := arg.NewBazelArgs([]string{"run", "--bb_config=ci", "//initial"})
	require.NoError(t, err)

	args, execArgs, err := p.PreBazel(args, nil)
	require.NoError(t, err)
	require.Empty(t, execArgs)

	// The plugin's Bazel flag is applied without losing the named BB config.
	require.Contains(t, args.Unresolved(), "--bb_config=ci")
	require.NotContains(t, args.Forwarded(), "--bb_config=ci")
	require.NotContains(t, args.Resolved(), "--bb_config=ci")

	require.NotContains(t, args.Unresolved(), "--stream_run_logs")
	require.NotContains(t, args.Forwarded(), "--stream_run_logs")
	require.Contains(t, args.Resolved(), "--stream_run_logs")

	require.Contains(t, args.Unresolved(), "--build_metadata=PLUGIN")
	require.Contains(t, args.Forwarded(), "--build_metadata=PLUGIN")
	require.Contains(t, args.Resolved(), "--build_metadata=PLUGIN")
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
	for i := range trials {
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
	for range trials {
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

func testPlugin(t *testing.T, ws, path string) *Plugin {
	t.Helper()
	return &Plugin{
		config: &config.PluginConfig{Path: path},
		configFile: &config.File{
			Path:       filepath.Join(ws, "buildbuddy.yaml"),
			RootConfig: &config.RootConfig{},
		},
		tempDir: t.TempDir(),
	}
}

func setup(t *testing.T) (ws, home string) {
	root := testfs.MakeTempDir(t)
	ws = testfs.MakeDirAll(t, root, "workspace")
	testfs.WriteAllFileContents(t, ws, map[string]string{"WORKSPACE": ""})
	home = testfs.MakeDirAll(t, root, "home")

	setTestHomeDir(t, home)
	setTestWorkDir(t, ws)
	workspace.SetForTest(t, ws)

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

func TestSplitRepoRef(t *testing.T) {
	const sha = "0123456789abcdef0123456789abcdef01234567"
	for _, tc := range []struct {
		spec, repo, ref string
	}{
		{"foo/bar", "foo/bar", ""},
		{"foo/bar@v1.0", "foo/bar", "v1.0"},
		{"foo/bar@" + sha, "foo/bar", sha},
		{"example.com/foo/bar@feature/x", "example.com/foo/bar", "feature/x"},
		{"https://example.com/foo/bar@v1", "https://example.com/foo/bar", "v1"},
		{"git@github.com:foo/bar", "git@github.com:foo/bar", ""},
		{"git@github.com:foo/bar@" + sha, "git@github.com:foo/bar", sha},
		{"ssh://git@github.com/foo/bar", "ssh://git@github.com/foo/bar", ""},
		{"ssh://git@github.com/foo/bar@" + sha, "ssh://git@github.com/foo/bar", sha},
		{"/local/repo@" + sha, "/local/repo", sha},
	} {
		p := &Plugin{config: &config.PluginConfig{Repo: tc.spec}}
		repo, ref := p.splitRepoRef()
		assert.Equal(t, tc.repo, repo, "repo for %q", tc.spec)
		assert.Equal(t, tc.ref, ref, "ref for %q", tc.spec)
	}

	p := &Plugin{config: &config.PluginConfig{Repo: "git@github.com:foo/bar@" + sha}}
	assert.Equal(t, "https://github.com/foo/bar", p.RepoURL())
	id, err := p.VersionedID()
	require.NoError(t, err)
	assert.Equal(t, "https://github.com/foo/bar@"+sha, id)
}

func TestLoad_RejectsInsecureSchemes(t *testing.T) {
	t.Setenv("BUILDBUDDY_CACHE_DIR", testfs.MakeTempDir(t))
	for _, repo := range []string{
		"http://example.com/foo/bar",
		"HTTP://example.com/foo/bar@v1",
		"git://example.com/foo/bar",
	} {
		p := &Plugin{config: &config.PluginConfig{Repo: repo}}
		err := p.load()
		require.Error(t, err, repo)
		assert.Contains(t, err.Error(), "insecure scheme", repo)
	}
}

func TestLoad_PinnedCommit(t *testing.T) {
	t.Setenv("BUILDBUDDY_CACHE_DIR", testfs.MakeTempDir(t))
	repoPath, firstSHA := testgit.MakeTempRepo(t, map[string]string{"pre_bazel.sh": "echo v1"})
	secondSHA := testgit.CommitFiles(t, repoPath, map[string]string{"pre_bazel.sh": "echo v2"})

	p := &Plugin{config: &config.PluginConfig{Repo: repoPath + "@" + firstSHA}}
	require.NoError(t, p.load())
	clonePath, err := p.repoClonePath()
	require.NoError(t, err)
	assert.Equal(t, firstSHA, testgit.CurrentCommitSHA(t, clonePath))

	// A cached clone that isn't at the pinned commit gets replaced.
	testshell.Run(t, clonePath, "git checkout --quiet "+secondSHA)
	require.NoError(t, p.load())
	assert.Equal(t, firstSHA, testgit.CurrentCommitSHA(t, clonePath))
	b, err := os.ReadFile(filepath.Join(clonePath, "pre_bazel.sh"))
	require.NoError(t, err)
	assert.Equal(t, "echo v1", string(b))
}

func TestLoad_UnpinnedRefStillWorks(t *testing.T) {
	t.Setenv("BUILDBUDDY_CACHE_DIR", testfs.MakeTempDir(t))
	repoPath, _ := testgit.MakeTempRepo(t, map[string]string{"pre_bazel.sh": "echo v1"})
	testshell.Run(t, repoPath, "git tag v1")
	testgit.CommitFiles(t, repoPath, map[string]string{"pre_bazel.sh": "echo v2"})

	for _, repo := range []string{repoPath, repoPath + "@v1"} {
		p := &Plugin{config: &config.PluginConfig{Repo: repo}}
		require.NoError(t, p.load(), repo)
		path, err := p.Path()
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(path, "pre_bazel.sh"))
		require.NoError(t, err, repo)
	}
}

func TestCheckHead(t *testing.T) {
	repoPath, firstSHA := testgit.MakeTempRepo(t, map[string]string{"a.txt": "a"})
	secondSHA := testgit.CommitFiles(t, repoPath, map[string]string{"a.txt": "b"})

	require.NoError(t, checkHead(repoPath, secondSHA))
	require.NoError(t, checkHead(repoPath, strings.ToUpper(secondSHA)))
	err := checkHead(repoPath, firstSHA)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "want pinned commit")
}

func TestIsCommitSHA(t *testing.T) {
	assert.True(t, isCommitSHA("0123456789abcdef0123456789abcdef01234567"))
	assert.False(t, isCommitSHA(""))
	assert.False(t, isCommitSHA("v1.0"))
	assert.False(t, isCommitSHA("0123456"))
	assert.False(t, isCommitSHA("0123456789abcdef0123456789abcdef0123456g"))
}
