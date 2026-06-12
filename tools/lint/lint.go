package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
)

var (
	fix        = flag.Bool("fix", false, "If true, attempt to fix lint errors automatically.")
	tool       = flag.Slice("tool", []string{}, "If set, only run the given tool. Can be specified multiple times.")
	exclude    = flag.Slice("exclude", []string{}, "If set, exclude the given tool. Can be specified multiple times.")
	force      = flag.Bool("force", false, "If true, run on all files, not just files changed since the diff base.")
	diffBase   = flag.String("diff_base", "", "If set, use the given git rev as the diff base when determining changed files.")
	bazelArg   = flag.Slice("bazel_arg", []string{}, "Additional argument to pass to nested bazel invocations (e.g. --bazel_arg=--config=linux-workflows). Can be specified multiple times.")
	nogoTarget = flag.Slice("nogo_target", []string{}, "Target patterns to analyze with the GoVet tool. Analyzes //... if unset. Can be specified multiple times.")

	legacyAllFlag = flag.Bool("a", false, "Has no effect (kept for backwards compatibility but will be removed soon)")
)

// BB CLI version is now pinned in deps.bzl (BB_CLI_VERSION) and downloaded
// as a prebuilt binary via //tools/bb.

// Set via x_defs in BUILD file.
var (
	goimportsRlocationpath                     string
	goRlocationpath                            string
	clangFormatRlocationpath                   string
	bbCLIRlocationpath                         string
	prettierRlocationpath                      string
	prettierPluginOrganizeImportsRlocationpath string
)

var (
	// Available tools
	tools = []Tool{
		// Fixes go files - both imports and formatting.
		{Name: "GoFormat", Run: runGoimports},
		// Fixes proto files.
		{Name: "ProtoFormat", Run: runClangFormat},
		// Fixes frontend-related files, configs, and docs.
		{Name: "PrettierFormat", Run: runPrettier},
		// tools/fix_go_deps.sh fixes go.mod, go.sum, deps.bzl, and MODULE.bzl.
		// Runs exclusively because this might change deps.bzl which BuildFiles
		// might also change.
		{Name: "GoModulesFix", Run: runFixGoDeps, WriteLock: true},
		// Fixes build+starlark file formatting and deps (via embedded gazelle).
		// Runs exclusively because this might change deps.bzl which GoDeps
		// might also change.
		{Name: "BuildFix", Run: runBBFix, WriteLock: true},
		// Ensures that MODULE.bazel.lock is up to date.
		{Name: "UpdateLockfile", Run: runBazelModDeps, WriteLock: true},
		// Runs nogo static analysis (go vet, staticcheck, modernize, etc.)
		// over all Go targets. In fix mode, applies the suggested fixes
		// emitted by nogo. Runs exclusively so that it analyzes the sources
		// written by the other tools and so that nobody else writes the
		// files it patches.
		{Name: "GoVet", Run: runNoGo, WriteLock: true},
	}

	// File extensions handled by prettier.
	prettierExtensions = []string{
		".html", ".css",
		".js", ".jsx", ".mjs", ".cjs",
		".ts", ".tsx", ".mts", ".cts",
		".json", ".yaml", ".yml",
		".md", ".mdx",
	}
)

type Tool struct {
	// Name is the tool name.
	Name string

	// WriteLock indicates whether to run the tool exclusively when writing
	// files (fix mode). This should be set for tools that may concurrently
	// modify the same files.
	WriteLock bool

	// Run invokes the tool from the workspace root directory.
	// The tool should run only on the given files.
	// If `fix` is true, the tool should attempt to fix the changed files.
	// An error should be returned if lint errors exist and `fix` is false.
	Run func(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error
}

func runBBFix(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	cmd, err := getRunfileToolCommand(ctx, bbCLIRlocationpath)
	if err != nil {
		return fmt.Errorf("get bb command: %w", err)
	}
	cmd.Args = append(cmd.Args, "fix")
	if !fix {
		cmd.Args = append(cmd.Args, "--diff")
	}
	stdoutCounter := &ioutil.Counter{}
	cmd.Stdout = io.MultiWriter(stdout, stdoutCounter)
	cmd.Stderr = stderr
	// bb fix runs gazelle, which needs 'go' in PATH to resolve imports.
	goPath, err := runfiles.Rlocation(goRlocationpath)
	if err != nil {
		return fmt.Errorf("find go in runfiles: %w", err)
	}
	cmd.Env = append(cmd.Env, "PATH="+filepath.Dir(goPath)+":"+os.Getenv("PATH"))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run bb fix: %w", err)
	}
	// In diff mode, fail if the diff is non-empty.
	if !fix && stdoutCounter.Count() > 0 {
		return fmt.Errorf("bb fix found lint errors")
	}
	return nil
}

func runFixGoDeps(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	// fix_go_deps.sh doesn't exist when run from the internal repo, which is
	// fine since we only want to run it from the external repo anyway.
	if _, err := os.Stat("tools/fix_go_deps.sh"); os.IsNotExist(err) {
		return nil
	}
	cmd := exec.CommandContext(ctx, "tools/fix_go_deps.sh")
	if !fix {
		cmd.Args = append(cmd.Args, "--diff")
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	// Set GO_PATH to runfile tool paths so that we don't have to run nested
	// bazel invocations to build the tools. Also forward runfiles.Env() through
	// env so that the tool can find its runfiles.
	runfilesEnv, err := runfiles.Env()
	if err != nil {
		return fmt.Errorf("get runfiles env: %w", err)
	}
	goRlocation, err := runfiles.Rlocation(goRlocationpath)
	if err != nil {
		return fmt.Errorf("find go in runfiles: %w", err)
	}
	cmd.Env = append(os.Environ(), runfilesEnv...)
	cmd.Env = append(cmd.Env, "GO_PATH="+goRlocation)

	// Run the tool
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run go deps: %w", err)
	}
	return nil
}

func runGoimports(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	files = filterToExtensions(files, []string{".go"})
	if len(files) == 0 {
		return nil
	}
	cmd, err := getRunfileToolCommand(ctx, goimportsRlocationpath)
	if err != nil {
		return fmt.Errorf("get goimports command: %w", err)
	}
	if fix {
		cmd.Args = append(cmd.Args, "-w")
	} else {
		cmd.Args = append(cmd.Args, "-d")
	}
	cmd.Args = append(cmd.Args, files...)
	stdoutCounter := &ioutil.Counter{}
	cmd.Stdout = io.MultiWriter(stdout, stdoutCounter)
	cmd.Stderr = stderr
	// goimports requires 'go' to be in PATH.
	goPath, err := runfiles.Rlocation(goRlocationpath)
	if err != nil {
		return fmt.Errorf("find go in runfiles: %w", err)
	}
	path := os.Getenv("PATH")
	cmd.Env = append(cmd.Env, "PATH="+filepath.Dir(goPath)+":"+path)
	if err := cmd.Run(); err != nil {
		return err
	}
	if stdoutCounter.Count() > 0 {
		return fmt.Errorf("goimports found lint errors")
	}
	return nil
}

func runClangFormat(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	files = filterToExtensions(files, []string{".proto"})
	if len(files) == 0 {
		return nil
	}
	cmd, err := getRunfileToolCommand(ctx, clangFormatRlocationpath)
	if err != nil {
		return fmt.Errorf("get clang format command: %w", err)
	}
	if !fix {
		cmd.Args = append(cmd.Args, "--dry-run")
	}
	cmd.Args = append(cmd.Args, "-i", "--style=Google")
	cmd.Args = append(cmd.Args, files...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}

func runPrettier(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	// If either yarn.lock or .prettierrc have changed, run on all files.
	// Otherwise, run on changed files, filtering by extension.
	if slices.Contains(files, "yarn.lock") || slices.Contains(files, ".prettierrc") {
		var err error
		files, err = gitListFilesWithExtensions(prettierExtensions)
		if err != nil {
			return fmt.Errorf("list files: %w", err)
		}
	} else {
		files = filterToExtensions(files, prettierExtensions)
	}
	if len(files) == 0 {
		return nil
	}
	// Run prettier.
	cmd, err := getRunfileToolCommand(ctx, prettierRlocationpath)
	if err != nil {
		return fmt.Errorf("get prettier command: %w", err)
	}
	prettierPluginOrganizeImports, err := runfiles.Rlocation(prettierPluginOrganizeImportsRlocationpath)
	if err != nil {
		return fmt.Errorf("find prettier-plugin-organize-imports in runfiles: %w", err)
	}
	cmd.Args = append(cmd.Args, "--plugin", filepath.Join(prettierPluginOrganizeImports, "index.js"))
	if fix {
		cmd.Args = append(cmd.Args, "--write")
	} else {
		cmd.Args = append(cmd.Args, "--log-level=warn", "--check")
	}
	cmd.Args = append(cmd.Args, files...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// For why we set BAZEL_BINDIR to ".", see
	// https://github.com/aspect-build/rules_js/tree/dbb5af0d2a9a2bb50e4cf4a96dbc582b27567155#running-nodejs-programs
	cmd.Env = append(cmd.Env, "BAZEL_BINDIR=.")
	return cmd.Run()
}

func runBazelModDeps(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	cmd, err := getRunfileToolCommand(ctx, bbCLIRlocationpath)
	if err != nil {
		return fmt.Errorf("get bb command: %w", err)
	}
	cmd.Args = append(cmd.Args, "mod", "deps")
	if fix {
		cmd.Args = append(cmd.Args, "--lockfile_mode=update")
	} else {
		cmd.Args = append(cmd.Args, "--lockfile_mode=error")
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// bb mod deps may need 'go' in PATH.
	goPath, err := runfiles.Rlocation(goRlocationpath)
	if err != nil {
		return fmt.Errorf("find go in runfiles: %w", err)
	}
	cmd.Env = append(cmd.Env, "PATH="+filepath.Dir(goPath)+":"+os.Getenv("PATH"))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run bb mod deps: %w", err)
	}
	return nil
}

// nogoPatchLineRegexp matches the "patch -p1 < <path>/nogo.patch" hint that a
// failing nogo validation action prints when an analyzer suggested a fix.
// Only the part of the path below the output directory is captured: with
// --experimental_output_paths=strip, actions see config-mapped paths
// (bazel-out/cfg/bin/...) that don't exist on disk, so the patch is resolved
// via the bazel-bin convenience symlink instead.
// Bazel may emit \r\n line endings when it believes it's writing to a
// terminal, hence the \r? before the end anchor.
var nogoPatchLineRegexp = regexp.MustCompile(`(?m)^\$ patch -p1 < \S+?/bin/(\S+nogo\.patch)\r?$`)

// runNoGo runs nogo static analysis by building all Go targets with nogo
// enabled (--config=nogo, see shared.bazelrc). Regular builds skip nogo
// entirely; here, analysis of unchanged packages - including all external
// dependencies - is served from bazel's action cache, so only changed
// packages are re-analyzed.
//
// In fix mode, the suggested fixes emitted by nogo (nogo.patch files) are
// applied to the workspace and the analysis is re-run to report any remaining
// diagnostics that have no automatic fix.
func runNoGo(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	if !slices.ContainsFunc(files, affectsGoBuild) {
		return nil
	}
	bazel, err := findBazelCommand()
	if err != nil {
		return err
	}
	runBuild := func(stderr io.Writer) error {
		// --keep_going so that diagnostics from all packages are reported in
		// a single run rather than stopping at the first failing package.
		args := []string{"build", "--config=nogo", "--keep_going"}
		if fix {
			// Make sure the nogo.patch files referenced by validation
			// failures are materialized in bazel-out, even when building
			// with a remote cache and minimal downloads.
			args = append(args,
				"--output_groups=+nogo_fix",
				"--remote_download_regex=.*_nogo/nogo.patch$",
			)
		}
		args = append(args, *bazelArg...)
		args = append(args, "--")
		if len(*nogoTarget) > 0 {
			args = append(args, *nogoTarget...)
		} else {
			args = append(args, "//...")
		}
		cmd := exec.CommandContext(ctx, bazel, args...)
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		return cmd.Run()
	}
	capture := lockingbuffer.New()
	if err := runBuild(io.MultiWriter(stderr, capture)); err == nil {
		return nil
	}
	if !fix {
		return fmt.Errorf("nogo static analysis found errors - run ./buildfix.sh to attempt automatic fixes")
	}
	// Apply the fixes suggested by the failing validation actions.
	var patchPaths []string
	for _, m := range nogoPatchLineRegexp.FindAllStringSubmatch(capture.String(), -1) {
		patchPath := filepath.Join("bazel-bin", m[1])
		if !slices.Contains(patchPaths, patchPath) {
			patchPaths = append(patchPaths, patchPath)
		}
	}
	if len(patchPaths) == 0 {
		return fmt.Errorf("nogo static analysis found errors with no automatic fixes")
	}
	patchedFiles, err := applyNogoPatches(ctx, stderr, patchPaths)
	if err != nil {
		return fmt.Errorf("apply nogo fixes: %w", err)
	}
	// Suggested fixes are not necessarily gofmt'd; format the patched files.
	if err := runGoimports(ctx, io.Discard, stderr, true, patchedFiles); err != nil {
		return fmt.Errorf("format nogo-fixed files: %w", err)
	}
	log.Infof("[GoVet] applied nogo fixes to: %s", strings.Join(patchedFiles, ", "))
	// Re-run the analysis to report any remaining diagnostics.
	if err := runBuild(stderr); err != nil {
		return fmt.Errorf("nogo static analysis found errors that could not be fixed automatically")
	}
	return nil
}

// applyNogoPatches applies the given nogo.patch files (unified diffs with
// paths relative to the workspace root) and returns the workspace-relative
// paths of the files that were patched.
//
// A source file analyzed in multiple Go archives (e.g. a library and its
// test) gets the same fix suggested in each archive's patch, so per-file
// diffs are deduplicated before being applied. A diff that no longer applies
// (e.g. because it overlaps with one already applied) is skipped with a
// warning; the re-run of the analysis will report its diagnostics again.
func applyNogoPatches(ctx context.Context, stderr io.Writer, patchPaths []string) ([]string, error) {
	applied := make(map[string]bool)
	var patchedFiles []string
	for _, patchPath := range patchPaths {
		patch, err := os.ReadFile(patchPath)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", patchPath, err)
		}
		for _, fileDiff := range splitFileDiffs(string(patch)) {
			if applied[fileDiff] {
				continue
			}
			applied[fileDiff] = true
			cmd := exec.CommandContext(ctx, "git", "apply")
			cmd.Stdin = strings.NewReader(fileDiff)
			cmd.Stdout = io.Discard
			cmd.Stderr = io.Discard
			if err := cmd.Run(); err != nil {
				log.Warningf("[GoVet] skipping a nogo fix from %s that no longer applies", patchPath)
				continue
			}
			if file, ok := strings.CutPrefix(lines(fileDiff)[0], "--- a/"); ok && !slices.Contains(patchedFiles, file) {
				patchedFiles = append(patchedFiles, file)
			}
		}
	}
	return patchedFiles, nil
}

// splitFileDiffs splits a concatenation of unified diffs into one diff per
// file, each starting with a "--- a/<path>" header line.
func splitFileDiffs(patch string) []string {
	var diffs []string
	var current []string
	flush := func() {
		if len(current) > 0 {
			diffs = append(diffs, strings.Join(current, "\n")+"\n")
		}
	}
	for _, line := range lines(patch) {
		if strings.HasPrefix(line, "--- a/") {
			flush()
			current = nil
		}
		current = append(current, line)
	}
	flush()
	return diffs
}

// affectsGoBuild returns whether a change to the given file can affect nogo
// analysis results.
func affectsGoBuild(file string) bool {
	switch filepath.Base(file) {
	case "BUILD", "BUILD.bazel", "go.mod", "go.sum":
		return true
	}
	switch filepath.Ext(file) {
	case ".go", ".proto", ".bzl", ".bazel", ".bazelrc":
		return true
	}
	return false
}

// findBazelCommand returns a bazel-compatible executable from PATH, trying the
// same candidates as buildfix.sh.
func findBazelCommand() (string, error) {
	for _, name := range []string{"bb", "bazelisk", "bazel"} {
		if path, err := exec.LookPath(name); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("could not find bb, bazelisk, or bazel in PATH")
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	ctx := context.Background()
	if err := log.Configure(); err != nil {
		return fmt.Errorf("configure logging: %w", err)
	}
	// Change to workspace root.
	if wd := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); wd != "" {
		if err := os.Chdir(wd); err != nil {
			return fmt.Errorf("change to workspace root: %w", err)
		}
	} else {
		return fmt.Errorf("BUILD_WORKSPACE_DIRECTORY is not set")
	}

	// Let people continue to use "./buildfix.sh -a" for a bit, but log a
	// warning.
	if *legacyAllFlag {
		log.Warningf("The -a flag now has no effect (all tools are now run by default)")
	}
	// Validate tool flags.
	toolNames := make([]string, len(tools))
	for i, t := range tools {
		toolNames[i] = t.Name
	}
	for _, t := range *tool {
		if !slices.Contains(toolNames, t) {
			return fmt.Errorf("tool %q not found", t)
		}
	}
	for _, t := range *exclude {
		if !slices.Contains(toolNames, t) {
			return fmt.Errorf("tool %q not found", t)
		}
	}

	// Get changed files.
	diffBaseRev, err := getDiffBase()
	if err != nil {
		return fmt.Errorf("get diff base: %w", err)
	}
	var files []string
	if *force {
		lsFiles, err := sh("git ls-files")
		if err != nil {
			return fmt.Errorf("get all files: %w", err)
		}
		files = lines(lsFiles)
	} else {
		log.Infof("Linting changes since base revision: %s", diffBaseRev)
		fileDiff, err := sh(fmt.Sprintf("git diff --name-only --diff-filter=AMRCT %s", diffBaseRev))
		if err != nil {
			return fmt.Errorf("get changed files: %w", err)
		}
		files = lines(fileDiff)
	}

	// Start lint tools.
	var eg errgroup.Group
	eg.SetLimit(3)
	var mu sync.RWMutex
	for _, t := range tools {
		if len(*tool) > 0 && !slices.Contains(*tool, t.Name) {
			continue
		}
		if slices.Contains(*exclude, t.Name) {
			continue
		}
		eg.Go(func() error {
			if t.WriteLock && *fix {
				mu.Lock()
				defer mu.Unlock()
			} else {
				mu.RLock()
				defer mu.RUnlock()
			}
			log.Infof("[%s] starting", t.Name)
			out := lockingbuffer.New()
			err := t.Run(ctx, out, out, *fix, files)
			if err != nil {
				// Wait until the end to print all the diffs.
				log.Errorf("[%s] failed: %s: output:\n%s", t.Name, err, out.String())
				return fmt.Errorf("one or more lint checks failed - run ./buildfix.sh to attempt automatic fixes")
			} else {
				log.Infof("[%s] done", t.Name)
			}
			return nil
		})
	}
	return eg.Wait()
}

// getRunfileToolCommand returns an [*exec.Cmd] for the given tool in runfiles.
// The returned command's Env is configured to set runfiles env vars so that the
// tool can find its runfiles.
func getRunfileToolCommand(ctx context.Context, rlocationpath string) (*exec.Cmd, error) {
	rlocation, err := runfiles.Rlocation(rlocationpath)
	if err != nil {
		return nil, fmt.Errorf("find tool in runfiles: %w", err)
	}
	cmd := exec.CommandContext(ctx, rlocation)
	runfilesEnv, err := runfiles.Env()
	if err != nil {
		return nil, fmt.Errorf("get runfiles env: %w", err)
	}
	cmd.Env = append(os.Environ(), runfilesEnv...)
	return cmd, nil
}

func filterToExtensions(files, extensions []string) []string {
	var filteredFiles []string
	for _, file := range files {
		if slices.Contains(extensions, filepath.Ext(file)) {
			filteredFiles = append(filteredFiles, file)
		}
	}
	return filteredFiles
}

// getDiffBase returns the git revision used as the base for diffing to
// determine which files have changed and need to be linted.
func getDiffBase() (string, error) {
	if *diffBase != "" {
		return *diffBase, nil
	}
	// If we're on master, use the previous commit as the diff base.
	branch, err := sh("git rev-parse --abbrev-ref HEAD")
	if err != nil {
		return "", fmt.Errorf("get current branch: %w", err)
	}
	if branch == "master" {
		return "HEAD~1", nil
	}
	// If on a feature branch, use GIT_BASE_BRANCH env var set by BB workflows,
	// or fall back to origin/master (which should usually work when running
	// locally).
	mergeBase, err := sh("git merge-base HEAD origin/${GIT_BASE_BRANCH:-master}")
	if err != nil {
		return "", fmt.Errorf("get merge base: %w", err)
	}
	return mergeBase, nil
}

// gitListFilesWithExtensions lists all files known to git with the given
// extensions.
func gitListFilesWithExtensions(extensions []string) ([]string, error) {
	var cmd strings.Builder
	cmd.WriteString("git ls-files --")
	for _, ext := range extensions {
		cmd.WriteString(fmt.Sprintf(" '*%s'", ext))
	}
	files, err := sh(cmd.String())
	if err != nil {
		return nil, fmt.Errorf("list files with extensions: %w", err)
	}
	return lines(files), nil
}

// sh runs a shell command and returns its stdout as a trimmed string.
func sh(command string) (stdout string, err error) {
	cmd := exec.Command("sh", "-ec", command)
	b, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("run %q: %w: %s", command, err, exitErr.Stderr)
		}
		return "", fmt.Errorf("run %q: %w", command, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// Trims any trailing newline character and returns the remaining split lines.
// Returns an empty slice if the input is empty.
func lines(s string) []string {
	s = strings.TrimSuffix(s, "\n")
	if s == "" {
		return nil // avoids returning []string{""} below
	}
	return strings.Split(s, "\n")
}
