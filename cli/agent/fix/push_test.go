package fix

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func testGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %v: %s", args, output)
	return strings.TrimSpace(string(output))
}

func initRepo(t *testing.T, dir, defaultBranch string, bare bool) {
	t.Helper()
	args := []string{"init"}
	if bare {
		args = append(args, "--bare")
	}
	testGit(t, filepath.Dir(dir), append(args, dir)...)
	testGit(t, dir, "symbolic-ref", "HEAD", "refs/heads/"+defaultBranch)
}

func setupPushRepo(t *testing.T, defaultBranch, checkoutBranch string) (work, bare string) {
	t.Helper()
	root := t.TempDir()
	bare = filepath.Join(root, "remote.git")
	work = filepath.Join(root, "work")
	initRepo(t, bare, defaultBranch, true)
	initRepo(t, work, defaultBranch, false)
	testGit(t, work, "config", "user.name", "Test")
	testGit(t, work, "config", "user.email", "test@example.com")
	testGit(t, work, "remote", "add", "origin", bare)
	require.NoError(t, os.WriteFile(filepath.Join(work, "file.txt"), []byte("original\n"), 0600))
	testGit(t, work, "add", "--all")
	testGit(t, work, "commit", "-m", "initial")
	testGit(t, work, "push", "-u", "origin", defaultBranch)
	if checkoutBranch != defaultBranch {
		testGit(t, work, "checkout", "-b", checkoutBranch)
		testGit(t, work, "push", "-u", "origin", checkoutBranch)
	}
	t.Chdir(work)
	return work, bare
}

func TestCommitAndPush_CreatesNewBranchOffDefaultBranch(t *testing.T) {
	for _, defaultBranch := range []string{"main", "trunk"} {
		t.Run(defaultBranch, func(t *testing.T) {
			work, bare := setupPushRepo(t, defaultBranch, defaultBranch)
			target, err := checkPushPreconditions(context.Background())
			require.NoError(t, err)
			require.True(t, target.createBranch)
			require.NoError(t, os.WriteFile(filepath.Join(work, "file.txt"), []byte("fixed\n"), 0600))
			require.NoError(t, commitAndPush(context.Background(), target, "", "12345678-0000-0000-0000-000000000000"))
			require.Equal(t, target.base, testGit(t, bare, "rev-parse", "refs/heads/"+defaultBranch))
			newBranch := testGit(t, work, "symbolic-ref", "--short", "HEAD")
			require.Contains(t, newBranch, "bb-agent-fix/12345678-")
			require.Equal(t, testGit(t, work, "rev-parse", "HEAD"), testGit(t, bare, "rev-parse", "refs/heads/"+newBranch))
		})
	}
}

func TestCommitAndPush_PushesDirectlyToNonDefaultBranch(t *testing.T) {
	work, bare := setupPushRepo(t, "main", "feature")
	target, err := checkPushPreconditions(context.Background())
	require.NoError(t, err)
	require.False(t, target.createBranch)
	require.NoError(t, os.WriteFile(filepath.Join(work, "file.txt"), []byte("fixed\n"), 0600))
	require.NoError(t, commitAndPush(context.Background(), target, "", "12345678-0000-0000-0000-000000000000"))
	require.Equal(t, "feature", testGit(t, work, "symbolic-ref", "--short", "HEAD"))
	require.Equal(t, testGit(t, work, "rev-parse", "HEAD"), testGit(t, bare, "rev-parse", "refs/heads/feature"))
}

func TestCommitAndPush_CreatesNewBranchWhenDefaultBranchIsUnknown(t *testing.T) {
	work, bare := setupPushRepo(t, "main", "feature")
	// Point HEAD to a nonexistent branch, so it's impossible to determine the default branch.
	testGit(t, bare, "symbolic-ref", "HEAD", "refs/heads/nonexistent")
	target, err := checkPushPreconditions(context.Background())
	require.NoError(t, err)
	require.True(t, target.createBranch)
	require.NoError(t, os.WriteFile(filepath.Join(work, "file.txt"), []byte("fixed\n"), 0600))
	require.NoError(t, commitAndPush(context.Background(), target, "", "12345678-0000-0000-0000-000000000000"))
	require.Equal(t, target.base, testGit(t, bare, "rev-parse", "refs/heads/feature"))
}

func TestCommitAndPush_CreatesNewBranchFromDetachedHead(t *testing.T) {
	work, bare := setupPushRepo(t, "main", "feature")
	testGit(t, work, "checkout", "--detach")
	target, err := checkPushPreconditions(context.Background())
	require.NoError(t, err)
	require.True(t, target.createBranch)
	require.Equal(t, "origin", target.remote)
	require.NoError(t, os.WriteFile(filepath.Join(work, "file.txt"), []byte("fixed\n"), 0600))
	require.NoError(t, commitAndPush(context.Background(), target, "", "12345678-0000-0000-0000-000000000000"))
	require.Equal(t, target.base, testGit(t, bare, "rev-parse", "refs/heads/feature"))
	newBranch := testGit(t, work, "symbolic-ref", "--short", "HEAD")
	require.Contains(t, newBranch, "bb-agent-fix/12345678-")
	require.Equal(t, testGit(t, work, "rev-parse", "HEAD"), testGit(t, bare, "rev-parse", "refs/heads/"+newBranch))
}

func TestCheckPushPreconditions_DetachedHeadUsesOnlyRemote(t *testing.T) {
	work, _ := setupPushRepo(t, "main", "main")
	testGit(t, work, "remote", "rename", "origin", "upstream")
	testGit(t, work, "checkout", "--detach")
	target, err := checkPushPreconditions(context.Background())
	require.NoError(t, err)
	require.Equal(t, "upstream", target.remote)
}

func TestCheckPushPreconditions_DetachedHeadFailsWithUnreachableRemote(t *testing.T) {
	work, bare := setupPushRepo(t, "main", "main")
	testGit(t, work, "checkout", "--detach")
	require.NoError(t, os.RemoveAll(bare))
	_, err := checkPushPreconditions(context.Background())
	require.Error(t, err)
}

func TestCommitAndPush_UsesPushRemoteInsteadOfFetchRemote(t *testing.T) {
	work, fork := setupPushRepo(t, "main", "feature")
	upstream := filepath.Join(filepath.Dir(work), "upstream.git")
	initRepo(t, upstream, "main", true)
	testGit(t, work, "remote", "add", "upstream", upstream)
	testGit(t, work, "push", "upstream", "main", "feature")
	testGit(t, work, "config", "branch.feature.remote", "upstream")
	testGit(t, work, "config", "remote.pushDefault", "origin")
	upstreamBase := testGit(t, upstream, "rev-parse", "refs/heads/feature")

	target, err := checkPushPreconditions(context.Background())
	require.NoError(t, err)
	require.Equal(t, "origin", target.remote)
	require.NoError(t, os.WriteFile(filepath.Join(work, "file.txt"), []byte("fixed\n"), 0600))
	require.NoError(t, commitAndPush(context.Background(), target, "", "12345678-0000-0000-0000-000000000000"))
	require.Equal(t, testGit(t, work, "rev-parse", "HEAD"), testGit(t, fork, "rev-parse", "refs/heads/feature"))
	require.Equal(t, upstreamBase, testGit(t, upstream, "rev-parse", "refs/heads/feature"))
}

func TestCommitAndPush_RemoteRunnerPushesToForkWithoutPushConfig(t *testing.T) {
	work, targetRepo := setupPushRepo(t, "main", "feature")
	fork := filepath.Join(filepath.Dir(work), "fork.git")
	initRepo(t, fork, "main", true)
	testGit(t, work, "remote", "add", "fork", fork)
	testGit(t, work, "push", "fork", "main", "feature")
	t.Setenv("BUILDBUDDY_CI_RUNNER_ROOT_DIR", filepath.Dir(work))
	targetBase := testGit(t, targetRepo, "rev-parse", "refs/heads/feature")

	target, err := checkPushPreconditions(context.Background())
	require.NoError(t, err)
	require.Equal(t, "fork", target.remote)
	require.NoError(t, os.WriteFile(filepath.Join(work, "file.txt"), []byte("fixed\n"), 0600))
	require.NoError(t, commitAndPush(context.Background(), target, "", "12345678-0000-0000-0000-000000000000"))
	require.Equal(t, testGit(t, work, "rev-parse", "HEAD"), testGit(t, fork, "rev-parse", "refs/heads/feature"))
	require.Equal(t, targetBase, testGit(t, targetRepo, "rev-parse", "refs/heads/feature"))
}
