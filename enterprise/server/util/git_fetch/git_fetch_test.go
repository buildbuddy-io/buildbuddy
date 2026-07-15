package git_fetch_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
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
	remote := makeRemoteRepository(t)
	checkoutDir := makeCheckout(t)
	var output bytes.Buffer
	attempts := 0

	result := git_fetch.Run(t.Context(), git_fetch.Options{Output: &output}, gitCommandRunner(checkoutDir, &attempts),
		"fetch", "--force", "--progress", remote.url, "refs/heads/master")

	require.NoError(t, result.Err, output.String())
	require.Equal(t, 1, attempts)
	require.Zero(t, result.Retries)
	require.Positive(t, result.TotalBytes)
	require.Equal(t, remote.commitSHA, strings.TrimSpace(testshell.Run(t, checkoutDir, "git rev-parse FETCH_HEAD")))
}

func TestRunRetriesSlowFetch(t *testing.T) {
	// Limit the server's pack response well below the configured threshold.
	// Verify the public API cancels the real Git process and retries once.
	remote := makeRemoteRepository(t)
	remote.url = startRateLimitedProxy(t, remote.url, 64*1024)
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
	require.Equal(t, 1, result.Retries)
	require.Positive(t, result.TotalBytes)
	require.Contains(t, output.String(), "retry 1 of 1")
	require.Contains(t, output.String(), "retries exhausted")
}

func makeRemoteRepository(t *testing.T) remoteRepository {
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

	server := testgit.StartServer(t, testgit.ServerOptions{LogWriter: io.Discard})
	server.CreateProject(testOrg, testRepo, &testgit.ProjectSettings{Public: true})
	server.Push(testOrg, testRepo, server.AccessToken(), sourceDir)
	return remoteRepository{
		url:       server.RepoURL(testOrg, testRepo, ""),
		commitSHA: commitSHA,
	}
}

func startRateLimitedProxy(t *testing.T, upstream string, bytesPerSecond int64) string {
	upstreamURL, err := url.Parse(upstream)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(&url.URL{
		Scheme: upstreamURL.Scheme,
		Host:   upstreamURL.Host,
	})
	proxy.ModifyResponse = func(response *http.Response) error {
		if strings.HasSuffix(response.Request.URL.Path, "/git-upload-pack") {
			response.Body = &rateLimitedReadCloser{
				ReadCloser:     response.Body,
				ctx:            response.Request.Context(),
				bytesPerSecond: bytesPerSecond,
			}
		}
		return nil
	}
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)

	proxyURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	upstreamURL.Scheme = proxyURL.Scheme
	upstreamURL.Host = proxyURL.Host
	return upstreamURL.String()
}

type rateLimitedReadCloser struct {
	io.ReadCloser
	ctx            context.Context
	bytesPerSecond int64
	bytesRead      int64
	start          time.Time
}

func (r *rateLimitedReadCloser) Read(data []byte) (int, error) {
	if r.start.IsZero() {
		r.start = time.Now()
	}
	n, err := r.ReadCloser.Read(data[:min(len(data), 32*1024)])
	if n == 0 {
		return n, err
	}
	r.bytesRead += int64(n)
	targetElapsed := time.Duration(float64(r.bytesRead) / float64(r.bytesPerSecond) * float64(time.Second))
	delay := time.Until(r.start.Add(targetElapsed))
	if delay <= 0 {
		return n, err
	}

	timer := time.NewTimer(delay)
	select {
	case <-r.ctx.Done():
		timer.Stop()
		return 0, r.ctx.Err()
	case <-timer.C:
		return n, err
	}
}

func makeCheckout(t *testing.T) string {
	dir := t.TempDir()
	testshell.Run(t, dir, "git init --quiet")
	return dir
}

func gitCommandRunner(workDir string, attempts *int) git_fetch.CommandRunner {
	return func(ctx context.Context, out io.Writer, configure func(*exec.Cmd) error, args ...string) (string, error) {
		*attempts = *attempts + 1
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = workDir
		cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if err := configure(cmd); err != nil {
			return "", err
		}

		var commandOutput bytes.Buffer
		writer := io.MultiWriter(out, &commandOutput)
		cmd.Stdout = writer
		cmd.Stderr = writer
		err := cmd.Run()
		return strings.TrimSpace(commandOutput.String()), err
	}
}
