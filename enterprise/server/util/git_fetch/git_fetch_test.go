package git_fetch_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/git_fetch"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"
)

const (
	testOrg  = "test-org"
	testRepo = "test-repo"
)

type remoteRepository struct {
	url       string
	commitSHA string
}

func TestRunFetchesRepository(t *testing.T) {
	// Fetch a real repository from the local Git server. Verify the public API
	// reports transferred bytes and leaves FETCH_HEAD at the remote commit.
	remote := makeRemoteRepository(t, testgit.ServerOptions{LogWriter: io.Discard})
	checkoutDir := makeCheckout(t)
	var output bytes.Buffer
	attempts := 0

	result := git_fetch.Run(t.Context(), git_fetch.Options{Output: &output}, gitCommandRunner(checkoutDir, &attempts),
		"fetch", "--force", "--progress", remote.url, "refs/heads/master")

	require.NoError(t, result.Err, output.String())
	require.Equal(t, 1, attempts)
	require.Positive(t, result.TotalBytes)
	require.Equal(t, remote.commitSHA, strings.TrimSpace(testshell.Run(t, checkoutDir, "git rev-parse FETCH_HEAD")))
}

func TestRunRetriesSlowFetch(t *testing.T) {
	// Limit the server's pack response well below the configured threshold.
	// Verify the public API cancels the real Git process and retries once.
	remote := makeRemoteRepository(t, testgit.ServerOptions{
		LogWriter:                io.Discard,
		UploadPackBytesPerSecond: 64 * 1024,
	})
	checkoutDir := makeCheckout(t)
	var output bytes.Buffer
	attempts := 0

	result := git_fetch.Run(t.Context(), git_fetch.Options{
		Output:                 &output,
		SlowRateBytesPerSecond: 1024 * 1024,
		SlowTimeout:            100 * time.Millisecond,
		Retries:                1,
	}, gitCommandRunner(checkoutDir, &attempts),
		"fetch", "--force", "--progress", remote.url, "refs/heads/master")

	require.ErrorIs(t, result.Err, git_fetch.ErrSlowFetchDetected, output.String())
	require.Equal(t, 2, attempts)
	require.Positive(t, result.TotalBytes)
	require.Contains(t, output.String(), "retry 1 of 1")
	require.Contains(t, output.String(), "retries exhausted")
}

func makeRemoteRepository(t *testing.T, opts testgit.ServerOptions) remoteRepository {
	// More than 100 random blobs keep Git from using its small-fetch unpack path
	// and compressing the pack into a tiny response. This exercises the same
	// index-pack receive progress used by large repository fetches.
	data := make([]byte, 512*1024)
	_, err := rand.Read(data)
	require.NoError(t, err)
	contents := make(map[string]string)
	for i := range 128 {
		contents[fmt.Sprintf("files/%03d.bin", i)] = string(data[i*4096 : (i+1)*4096])
	}
	sourceDir, commitSHA := testgit.MakeTempRepo(t, contents)

	server := testgit.StartServer(t, opts)
	server.CreateProject(testOrg, testRepo, &testgit.ProjectSettings{Public: true})
	server.Push(testOrg, testRepo, server.AccessToken(), sourceDir)
	return remoteRepository{
		url:       server.RepoURL(testOrg, testRepo, ""),
		commitSHA: commitSHA,
	}
}

func makeCheckout(t *testing.T) string {
	dir := t.TempDir()
	testshell.Run(t, dir, "git init --quiet")
	return dir
}

func gitCommandRunner(workDir string, attempts *int) git_fetch.CommandRunner {
	return func(ctx context.Context, out io.Writer, env map[string]string, extraFiles []*os.File, args ...string) (string, error) {
		*attempts = *attempts + 1
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = workDir
		cmd.ExtraFiles = extraFiles
		cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		for name, value := range env {
			cmd.Env = append(cmd.Env, name+"="+value)
		}

		var commandOutput bytes.Buffer
		writer := io.MultiWriter(out, &commandOutput)
		cmd.Stdout = writer
		cmd.Stderr = writer
		err := cmd.Run()
		return strings.TrimSpace(commandOutput.String()), err
	}
}
