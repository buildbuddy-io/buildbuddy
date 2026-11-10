package testbazel

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/stretchr/testify/require"
)

var (
	// Version is the bazel version of the embedded test bazel binary.
	//
	// Injected via x_defs.
	Version string

	// bazelRlocationpath specifies the path to the bazel binary used for
	// invocations.
	//
	// This binary should not be run as-is, since it doesn't have the
	// pre-configured install_base which is required to avoid extracting the
	// installation during tests, and it doesn't have the pre-warmed repository
	// cache which is required to avoid making network requests.
	//
	// Injected via x_defs.
	bazelRlocationpath string

	// outdirRlocationpath contains the pre-generated Bazel installation,
	// including the install_base, repository_cache, and MODULE.bazel.lock file.
	//
	// Injected via x_defs.
	outdirRlocationpath string

	initOnce sync.Once
)

// InitModule creates a MODULE.bazel file and MODULE.bazel.lock file in the
// given workspace dir if they don't already exist. The lockfile that is written
// is sufficient for bazel to not make any network requests when using built-in
// rules such as genrule, sh_binary, and sh_test.
//
// sh_binary and sh_test are no longer native rules in Bazel 9, so the
// MODULE.bazel file is amended with a bazel_dep on rules_shell (which is
// already in the pre-warmed repository_cache / vendor dir). Test BUILD files
// must load sh_binary / sh_test from @rules_shell.
func InitModule(t testing.TB, workspaceDir string) {
	var module string
	if testfs.Exists(t, workspaceDir, "MODULE.bazel") {
		module = testfs.ReadFileAsString(t, workspaceDir, "MODULE.bazel")
	}
	if !strings.Contains(module, `"rules_shell"`) {
		if module != "" && !strings.HasSuffix(module, "\n") {
			module += "\n"
		}
		module += `bazel_dep(name = "rules_shell", version = "0.6.1")` + "\n"
		testfs.WriteFile(t, workspaceDir, "MODULE.bazel", module)
	}
	if !testfs.Exists(t, workspaceDir, "MODULE.bazel.lock") {
		lockfileContents := testfs.ReadFileAsString(t, "", lockfilePath(t))
		testfs.WriteFile(t, workspaceDir, "MODULE.bazel.lock", lockfileContents)
	}
}

// BinaryPath returns the path to the test bazel launcher script.
//
// IMPORTANT: To avoid making unnecessary network requests to the bazel central
// registry, be sure use InitWorkspace or MakeTempWorkspace to ensure
// MODULE.bazel.lock is created in the workspace before running bazel commands.
//
// The script runs bazel with a pre-configured install_base and repository_cache
// to ensure that bazel does not need to make any network requests to build
// basic targets like genrule, sh_binary, and sh_test.
//
// NOTE: if you have a test which really needs to fetch external dependencies
// other than the basic genrule, sh_binary, or sh_test rules, you can either:
//   - Write your own rule to generate a pre-warmed repository_cache and
//     MODULE.bazel.lock, and point to those in your test build.
//     Check server/util/bazel/defs.bzl to see how this is done.
//   - Not use this testbazel util and directly use the bazel binary from
//     server/util/bazel instead. This is not recommended, since this means
//     bazel will fetch external dependencies during the test.
func BinaryPath(t testing.TB) string {
	// Write an entrypoint script that runs bazel with --install_base
	// and adds --repository_cache to build commands.
	entrypoint := filepath.Join(os.Getenv("TEST_TMPDIR"), "bazel-"+Version+"_test_entrypoint.sh")
	initOnce.Do(func() {
		path, err := runfiles.Rlocation(bazelRlocationpath)
		require.NoError(t, err, "look up bazel binary path")

		installBase := initInstallBase(t)

		bazelrc := filepath.Join(os.Getenv("TEST_TMPDIR"), "bazel-"+Version+".bazelrc")
		var bazelrcLines []string
		for _, flag := range HermeticFlags(t) {
			bazelrcLines = append(bazelrcLines, "common "+flag)
		}
		err = os.WriteFile(bazelrc, []byte(strings.Join(bazelrcLines, "\n")), 0644)
		require.NoError(t, err)

		script := "#!/usr/bin/env sh\n"
		// All tests use genrule / sh_binary / sh_test currently, which are
		// pre-cached in the repository_cache, so we make it a hard failure if
		// tests aren't properly configured to use the repository_cache.
		script += `
STARTDIR="$PWD"
# Find workspace root.
while ! [ -f MODULE.bazel ] && ! [ $PWD = "/" ]; do cd .. ; done
if ! [ -f MODULE.bazel.lock ]; then
	echo >&2 "[testbazel.go] ERROR: missing MODULE.bazel.lock in bazel workspace."
	echo >&2 "[testbazel.go] This results in network requests to the Bazel Central Registry during the test."
	echo >&2 "[testbazel.go] Use testbazel.InitModule() or testbazel.MakeTempModule() to create the lockfile."
	exit 1
fi
cd "$STARTDIR"
`
		if major, _ := version(t); major < 8 {
			// Bazel < 8's native sh rules need an autodetected C++
			// toolchain, which fails without a C compiler; tests don't
			// compile C++.
			script += `
if [ -z "${CC:-}" ] && ! command -v gcc >/dev/null; then export CC=/bin/true; fi
`
		}
		script += fmt.Sprintf(`exec %q --install_base=%q --bazelrc=%q "$@"`, path, installBase, bazelrc)
		err = os.WriteFile(entrypoint, []byte(script), 0755)
		require.NoError(t, err)
	})
	return entrypoint
}

// HermeticFlags returns the command options that the BinaryPath launcher
// applies through a bazelrc so that bazel does not make network requests.
// Tests that turn off bazelrc processing (for example with
// --ignore_all_rc_files) should pass these options on the command line.
func HermeticFlags(t testing.TB) []string {
	flags := []string{
		// Keep the repository_cache as a safety net for any non-bzlmod
		// (WORKSPACE-style) fetches. For bzlmod deps, we rely on --vendor_dir
		// below instead, since repository_cache entries can be
		// platform-specific and may not cover every host OS/arch.
		"--repository_cache=" + repoCachePath(t),
		"--lockfile_mode=error",
	}
	major, minor := version(t)
	// The repo contents cache (Bazel 8.3+) defaults to a dir inside the
	// shared prewarmed repository_cache, where nested invocations can contend
	// on its lock and hang.
	if major > 8 || (major == 8 && minor >= 3) {
		flags = append(flags, "--repo_contents_cache=")
	}
	// Combined with --lockfile_mode=error, the prewarmed vendor dir keeps
	// bzlmod resolution offline on any host OS/arch.
	vendorDir := vendorDirPath(t)
	require.True(t, testfs.Exists(t, "", vendorDir), "missing prewarmed vendor dir %q for Bazel %s", vendorDir, Version)
	flags = append(flags, "--vendor_dir="+vendorDir)
	return flags
}

// version returns the major and minor version of the embedded test bazel
// binary, e.g. 10, 0 for "10.0.0rc2".
func version(t testing.TB) (major, minor int) {
	parts := strings.SplitN(Version, ".", 3)
	require.Len(t, parts, 3, "parse bazel version %q", Version)
	major, err := strconv.Atoi(parts[0])
	require.NoError(t, err, "parse bazel version %q", Version)
	minor, err = strconv.Atoi(parts[1])
	require.NoError(t, err, "parse bazel version %q", Version)
	return major, minor
}

func repoCachePath(t testing.TB) string {
	outdirPath, err := runfiles.Rlocation(outdirRlocationpath)
	require.NoError(t, err)
	return filepath.Join(outdirPath, "repository_cache")
}

func vendorDirPath(t testing.TB) string {
	outdirPath, err := runfiles.Rlocation(outdirRlocationpath)
	require.NoError(t, err)
	return filepath.Join(outdirPath, "vendor_dir")
}

func lockfilePath(t testing.TB) string {
	outdirPath, err := runfiles.Rlocation(outdirRlocationpath)
	require.NoError(t, err)
	return filepath.Join(outdirPath, "MODULE.bazel.lock")
}

func initInstallBase(t testing.TB) string {
	outdirPath, err := runfiles.Rlocation(outdirRlocationpath)
	require.NoError(t, err)
	installBasePath := filepath.Join(outdirPath, "install_base")
	// Make a physical copy of the install dir (if it's symlinked via bazel
	// sandboxing) and set file mtimes to be in the future so that bazel sees it
	// as a pristine install.
	if target, _ := os.Readlink(filepath.Join(installBasePath, "build-label.txt")); target != "" {
		tmp := os.Getenv("TEST_TMPDIR")
		require.NotEmpty(t, tmp, "TEST_TMPDIR should not be empty")

		copyPath := filepath.Join(tmp, "bazel_install_base")
		cmd := exec.Command("cp", "-R", filepath.Dir(target), copyPath)
		b, err := cmd.CombinedOutput()
		require.NoError(t, err, "failed to init bazel install base: %s", string(b))

		installBasePath = copyPath
	}
	mtime := time.Now().Add(10 * 365 * 24 * time.Hour)
	// The trailing separator makes WalkDir follow installBasePath if it's a
	// symlink.
	err = filepath.WalkDir(installBasePath+string(filepath.Separator), func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		return os.Chtimes(path, mtime, mtime)
	})
	require.NoError(t, err, "set install base mtimes")
	return installBasePath
}

// Invoke the bazel CLI from within the given workspace dir.
func Invoke(ctx context.Context, t testing.TB, workspaceDir string, subCommand string, args ...string) *bazel.InvocationResult {
	bazelBinaryPath := BinaryPath(t)
	return bazel.Invoke(ctx, bazelBinaryPath, workspaceDir, subCommand, args...)
}

// Clean runs `bazel clean` within the given workspace.
func Clean(ctx context.Context, t testing.TB, workspaceDir string) *bazel.InvocationResult {
	return Invoke(ctx, t, workspaceDir, "clean")
}

// MakeTempModule creates a temporary bazel module with the given contents.
// It also calls InitModule to ensure MODULE.bazel and MODULE.bazel.lock are
// created.
func MakeTempModule(t testing.TB, contents map[string]string) string {
	workspaceDir := testfs.MakeTempDir(t)
	for path, fileContents := range contents {
		fullPath := filepath.Join(workspaceDir, path)
		err := os.MkdirAll(filepath.Dir(fullPath), 0777)
		require.NoError(t, err, "failed to create bazel workspace contents")
		err = os.WriteFile(fullPath, []byte(fileContents), 0777)
		require.NoError(t, err, "failed to create bazel workspace contents")
	}
	InitModule(t, workspaceDir)
	return workspaceDir
}
