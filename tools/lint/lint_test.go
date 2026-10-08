package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"
)

func TestChangedPathsSince(t *testing.T) {
	repo, base := testgit.MakeTempRepo(t, map[string]string{
		".gitignore":   "ignored.go\n",
		"keep.go":      "package x\n",
		"deleted.go":   "package x\n",
		"renamed.go":   "package x\n",
		"modified.txt": "a\n",
	})
	t.Chdir(repo)

	paths, err := changedPathsSince(base)
	require.NoError(t, err)
	require.NotNil(t, paths, "should be non-nil even if nothing changed")
	require.Empty(t, paths)
	require.False(t, slices.ContainsFunc(paths, isGoModulesInput))

	require.NoError(t, os.WriteFile(filepath.Join(repo, "modified.txt"), []byte("b\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "untracked.go"), []byte("package x\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "ignored.go"), []byte("package x\n"), 0644))
	// git would quote this path by default.
	require.NoError(t, os.WriteFile(filepath.Join(repo, "café.go"), []byte("package x\n"), 0644))
	testshell.Run(t, repo, "git rm -q deleted.go && git mv renamed.go renamed.txt")

	paths, err = changedPathsSince(base)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		"modified.txt",
		// Untracked files count, since `go mod tidy` sees them.
		"untracked.go",
		"café.go",
		"deleted.go",
		// Renames count as both the old and the new path.
		"renamed.go",
		"renamed.txt",
	}, paths)
}

func TestChangedPathsSince_OnlyUntrackedGoFile(t *testing.T) {
	// A new Go file that hasn't been `git add`ed yet must still make
	// GoModulesFix run, since it may import a new module.
	repo, base := testgit.MakeTempRepo(t, map[string]string{"README.md": "hi\n"})
	t.Chdir(repo)
	require.NoError(t, os.WriteFile(filepath.Join(repo, "new.go"), []byte("package x\n"), 0644))

	paths, err := changedPathsSince(base)
	require.NoError(t, err)
	require.True(t, slices.ContainsFunc(paths, isGoModulesInput), "paths: %v", paths)
}

func TestIsGoModulesInput(t *testing.T) {
	for _, f := range []string{"go.mod", "go.sum", "go.work", "go.work.sum", "x.go", "pkg/y_test.go", "sub/go.mod", "tools/fix_go_deps.sh"} {
		require.True(t, isGoModulesInput(f), f)
	}
	for _, f := range []string{"README.md", "BUILD", "x.go.txt", "go.mod.bak", "MODULE.bazel", "other/tools/fix_go_deps.sh"} {
		require.False(t, isGoModulesInput(f), f)
	}
}
