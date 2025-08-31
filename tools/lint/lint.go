package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
)

var (
	fix      = flag.Bool("fix", false, "If true, attempt to fix lint errors automatically.")
	tool     = flag.Slice("tool", []string{}, "If set, only run the given tool. Can be specified multiple times.")
	exclude  = flag.Slice("exclude", []string{}, "If set, exclude the given tool. Can be specified multiple times.")
	force    = flag.Bool("force", false, "If true, run on all files, not just files changed since the diff base.")
	diffBase = flag.String("diff_base", "", "If set, use the given git rev as the diff base when determining changed files.")

	legacyAllFlag = flag.Bool("a", false, "Has no effect (kept for backwards compatibility but will be removed soon)")
)

// Set via x_defs in BUILD file.
var (
	goimportsRlocationpath                     string
	clangFormatRlocationpath                   string
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
	}

	// File extensions handled by prettier.
	prettierExtensions = []string{
		".html", ".css", ".js", ".jsx", ".ts", ".tsx", // TODO: add .mjs
		".json", ".yaml", // TODO: add .yml
		".md", // TODO: add .mdx
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
	// TODO: use a static build of bb instead of having bazelisk download it.
	cmd := exec.CommandContext(ctx, "bazelisk", "fix")
	if !fix {
		cmd.Args = append(cmd.Args, "--diff")
	}
	cmd.Env = append(os.Environ(), "USE_BAZEL_VERSION=buildbuddy-io/5.0.179")
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}

func runFixGoDeps(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	cmd := exec.CommandContext(ctx, "tools/fix_go_deps.sh")
	if !fix {
		cmd.Args = append(cmd.Args, "--diff")
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		// fix_go_deps.sh doesn't exist when this tool is invoked from the
		// internal repo.
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("run go deps: %w", err)
	}
	return nil
}

func runGoimports(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	goimports, err := runfiles.Rlocation(goimportsRlocationpath)
	if err != nil {
		return fmt.Errorf("find goimports in runfiles: %w", err)
	}
	files = filterToExtensions(files, []string{".go"})
	if len(files) == 0 {
		return nil
	}
	var args []string
	if fix {
		args = append(args, "-w")
	} else {
		args = append(args, "-d")
	}
	args = append(args, files...)
	cmd := exec.CommandContext(ctx, goimports, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}

func runClangFormat(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	clangFormat, err := runfiles.Rlocation(clangFormatRlocationpath)
	if err != nil {
		return fmt.Errorf("find clang-format in runfiles: %w", err)
	}
	files = filterToExtensions(files, []string{".proto"})
	if len(files) == 0 {
		return nil
	}
	var args []string
	if !fix {
		args = append(args, "--dry-run")
	}
	args = append(args, "-i", "--style=Google")
	args = append(args, files...)
	cmd := exec.CommandContext(ctx, clangFormat, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}

func runPrettier(ctx context.Context, stdout, stderr io.Writer, fix bool, files []string) error {
	// Find tool paths.
	prettier, err := runfiles.Rlocation(prettierRlocationpath)
	if err != nil {
		return fmt.Errorf("find prettier in runfiles: %w", err)
	}
	prettierPluginOrganizeImports, err := runfiles.Rlocation(prettierPluginOrganizeImportsRlocationpath)
	if err != nil {
		return fmt.Errorf("find prettier-plugin-organize-imports in runfiles: %w", err)
	}
	// If either yarn.lock or .prettierrc have changed, run on all files.
	// Otherwise, run on changed files, filtering by extension.
	if slices.Contains(files, "yarn.lock") || slices.Contains(files, ".prettierrc") {
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
	var args []string
	args = append(args, "--plugin", filepath.Join(prettierPluginOrganizeImports, "index.js"))
	if fix {
		args = append(args, "--write")
	} else {
		args = append(args, "--log-level=warn", "--check")
	}
	args = append(args, files...)
	cmd := exec.CommandContext(ctx, prettier, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// For why we set BAZEL_BINDIR to ".", see
	// https://github.com/aspect-build/rules_js/tree/dbb5af0d2a9a2bb50e4cf4a96dbc582b27567155#running-nodejs-programs
	cmd.Env = append(os.Environ(), "BAZEL_BINDIR=.")
	// Set runfiles env vars so the prettier js_binary can find its runfiles.
	runfilesEnv, err := runfiles.Env()
	if err != nil {
		return fmt.Errorf("get runfiles env: %w", err)
	}
	cmd.Env = append(cmd.Env, runfilesEnv...)
	return cmd.Run()
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
	cmd := "git ls-files --"
	for _, ext := range extensions {
		cmd += fmt.Sprintf(" '*%s'", ext)
	}
	files, err := sh(cmd)
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
