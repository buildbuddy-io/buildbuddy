package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsBazelCommandToken(t *testing.T) {
	for _, baseName := range []string{
		bazelBinaryName,
		bazeliskBinaryName,
		bbBinaryName,
	} {
		require.True(t, isBazelCommandToken(baseName))
		require.True(t, isBazelCommandToken(baseName+executableSuffix))
	}
	require.Equal(t, executableSuffix == ".exe", isBazelCommandToken("bazel.exe"))
	require.False(t, isBazelCommandToken("bazelisk-custom"))
}

func TestBazelChildProcessResult_StartError(t *testing.T) {
	err := bazelChildProcessResult(errors.New("executable not found"))
	require.ErrorContains(t, err, "failed to start Bazel")
	var result *actionResult
	require.False(t, errors.As(err, &result))
}

func TestCollectRunfiles_RelativeDirectorySymlink(t *testing.T) {
	rootDir := t.TempDir()
	targetDir := filepath.Join(rootDir, "target")
	assert.NoError(t, os.Mkdir(targetDir, 0755))

	runfilesDir := filepath.Join(rootDir, "binary.runfiles")
	assert.NoError(t, os.Mkdir(runfilesDir, 0755))
	linkPath := filepath.Join(runfilesDir, "relative-directory")
	linkTarget, err := filepath.Rel(filepath.Dir(linkPath), targetDir)
	assert.NoError(t, err)
	assert.NoError(t, os.Symlink(linkTarget, linkPath))

	files, dirs, err := collectRunfiles(runfilesDir)
	assert.NoError(t, err)
	assert.Empty(t, files)
	resolvedTargetDir, err := filepath.EvalSymlinks(targetDir)
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{linkPath: resolvedTargetDir}, dirs)
}

func TestCollectRunfiles_ExecutableMetadata(t *testing.T) {
	runfilesDir := t.TempDir()
	executablePath := filepath.Join(runfilesDir, "executable")
	require.NoError(t, os.WriteFile(executablePath, []byte("executable"), 0644))
	require.NoError(t, os.Chmod(executablePath, 0755))
	nonExecutablePath := filepath.Join(runfilesDir, "non-executable")
	require.NoError(t, os.WriteFile(nonExecutablePath, []byte("non-executable"), 0644))

	files, _, err := collectRunfiles(runfilesDir)
	require.NoError(t, err)

	executableByPath := make(map[string]bool, len(files))
	for _, runfile := range files {
		executableByPath[runfile.logicalPath] = runfile.isExecutable
	}
	assert.Equal(t, map[string]bool{
		executablePath:    true,
		nonExecutablePath: false,
	}, executableByPath)
}

func TestCollectRunfiles_ResolvesFileSymlinks(t *testing.T) {
	rootDir := t.TempDir()
	targetPath := filepath.Join(rootDir, "target")
	require.NoError(t, os.WriteFile(targetPath, []byte("contents"), 0755))

	runfilesDir := filepath.Join(rootDir, "binary.runfiles")
	require.NoError(t, os.Mkdir(runfilesDir, 0755))
	firstLink := filepath.Join(runfilesDir, "first")
	secondLink := filepath.Join(runfilesDir, "second")
	require.NoError(t, os.Symlink(targetPath, firstLink))
	require.NoError(t, os.Symlink(targetPath, secondLink))

	files, dirs, err := collectRunfiles(runfilesDir)
	require.NoError(t, err)
	require.Empty(t, dirs)
	// Output should still contain two entries, one for each symlink,
	// even though the physical path is the same.
	require.Len(t, files, 2)

	filesByPath := make(map[string]*runfile, len(files))
	for _, f := range files {
		filesByPath[f.logicalPath] = f
	}
	resolvedTargetPath, err := filepath.EvalSymlinks(targetPath)
	require.NoError(t, err)
	for _, logicalPath := range []string{firstLink, secondLink} {
		f := filesByPath[logicalPath]
		require.NotNil(t, f)
		require.Equal(t, resolvedTargetPath, f.physicalPath)
		require.True(t, f.isExecutable)
		require.NotNil(t, f.digest)
	}
	require.Equal(t, filesByPath[firstLink].digest, filesByPath[secondLink].digest)
}

type stallingReadCloser struct {
	ctx context.Context
	io.ReadCloser
	started bool
}

func (r *stallingReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if !r.started {
		r.started = true
		return r.ReadCloser.Read(p[:1])
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func TestGitFetchRetriesSlowTransfer(t *testing.T) {
	flags.Set(t, "git_fetch_low_speed_retries", 1)
	flags.Set(t, "git_fetch_low_speed_limit", 1)
	// Git and libcurl configure the low-speed window in whole seconds, so one
	// second is the shortest interval and gives the fastest possible check.
	flags.Set(t, "git_fetch_low_speed_time", time.Second)
	for _, envVar := range []string{"GIT_HTTP_LOW_SPEED_LIMIT", "GIT_HTTP_LOW_SPEED_TIME"} {
		originalValue, wasSet := os.LookupEnv(envVar)
		require.NoError(t, os.Unsetenv(envVar))
		t.Cleanup(func() {
			if wasSet {
				require.NoError(t, os.Setenv(envVar, originalValue))
				return
			}
			require.NoError(t, os.Unsetenv(envVar))
		})
	}
	// Fetch from the proxy URL as-is. Otherwise fetch() embeds the REPO_USER
	// and REPO_TOKEN credentials in the URL if they are set, which they are
	// when this test itself runs in a BuildBuddy workflow.
	t.Setenv("USE_SYSTEM_GIT_CREDENTIALS", "1")

	// Serve a real repository over HTTP.
	remote := testgit.StartServer(t, testgit.ServerOptions{LogWriter: io.Discard})
	remote.CreateProject("test-org", "test-repo", &testgit.ProjectSettings{Public: true})
	sourceDir, wantCommitSHA := testgit.MakeTempRepo(t, map[string]string{"README.md": "test repo"})
	remote.Push("test-org", "test-repo", remote.AccessToken(), sourceDir)

	// Proxy the Git server and leave the first upload-pack response hanging
	// after its first byte until Git aborts it. Later responses pass through.
	upstreamURL, err := url.Parse(remote.RepoURL("test-org", "test-repo", ""))
	require.NoError(t, err)
	var stalledFirstFetch atomic.Bool
	proxy := httputil.NewSingleHostReverseProxy(upstreamURL)
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.Method == http.MethodPost && strings.HasSuffix(response.Request.URL.Path, "/git-upload-pack") && stalledFirstFetch.CompareAndSwap(false, true) {
			response.Body = &stallingReadCloser{ctx: response.Request.Context(), ReadCloser: response.Body}
		}
		return nil
	}
	proxyServer := httptest.NewServer(proxy)
	t.Cleanup(proxyServer.Close)

	checkoutDir := t.TempDir()
	originalWorkingDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(checkoutDir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(originalWorkingDir))
	})
	invocationLog := newInvocationLog(nil)
	invocationLog.writer = io.Discard
	ws := &workspace{
		rootDir: checkoutDir,
		log:     &buildEventReporter{log: invocationLog},
	}
	require.NoError(t, ws.init(t.Context()))

	// The initial fetch should be aborted by Git's low-speed check. Verify the
	// runner retries it and the second real fetch retrieves the expected ref.
	err = ws.fetch(t.Context(), proxyServer.URL, []string{"refs/heads/master"}, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), ws.gitFetchRetryCount)
	fetchedCommitSHA, gitErr := git(t.Context(), io.Discard, "rev-parse", "FETCH_HEAD")
	require.Nil(t, gitErr)
	require.Equal(t, wantCommitSHA, strings.TrimSpace(fetchedCommitSHA))
}

func TestGitCheckoutRetriesSlowLazyFetch(t *testing.T) {
	flags.Set(t, "git_fetch_low_speed_retries", 1)
	flags.Set(t, "git_fetch_low_speed_limit", 1)
	// Git and libcurl configure the low-speed window in whole seconds, so one
	// second is the shortest interval and gives the fastest possible check.
	flags.Set(t, "git_fetch_low_speed_time", time.Second)
	// Fetch with blob:none so that the checkout has to lazily fetch the blobs
	// it needs from the remote.
	flags.Set(t, "git_fetch_filters", []string{"blob:none"})
	flags.Set(t, "pushed_branch", "master")
	for _, envVar := range []string{"GIT_HTTP_LOW_SPEED_LIMIT", "GIT_HTTP_LOW_SPEED_TIME"} {
		originalValue, wasSet := os.LookupEnv(envVar)
		require.NoError(t, os.Unsetenv(envVar))
		t.Cleanup(func() {
			if wasSet {
				require.NoError(t, os.Setenv(envVar, originalValue))
				return
			}
			require.NoError(t, os.Unsetenv(envVar))
		})
	}
	// Fetch from the proxy URL as-is. Otherwise fetch() embeds the REPO_USER
	// and REPO_TOKEN credentials in the URL if they are set, which they are
	// when this test itself runs in a BuildBuddy workflow.
	t.Setenv("USE_SYSTEM_GIT_CREDENTIALS", "1")

	// Serve a real repository over HTTP.
	remote := testgit.StartServer(t, testgit.ServerOptions{LogWriter: io.Discard})
	remote.CreateProject("test-org", "test-repo", &testgit.ProjectSettings{Public: true})
	sourceDir, wantCommitSHA := testgit.MakeTempRepo(t, map[string]string{"README.md": "test repo"})
	remote.Push("test-org", "test-repo", remote.AccessToken(), sourceDir)

	// Proxy the Git server. Once armed, the proxy leaves the next upload-pack
	// response hanging after its first byte until Git aborts it; all other
	// responses pass through.
	upstreamURL, err := url.Parse(remote.RepoURL("test-org", "test-repo", ""))
	require.NoError(t, err)
	var stallNextFetch atomic.Bool
	proxy := httputil.NewSingleHostReverseProxy(upstreamURL)
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.Method == http.MethodPost && strings.HasSuffix(response.Request.URL.Path, "/git-upload-pack") && stallNextFetch.CompareAndSwap(true, false) {
			response.Body = &stallingReadCloser{ctx: response.Request.Context(), ReadCloser: response.Body}
		}
		return nil
	}
	proxyServer := httptest.NewServer(proxy)
	t.Cleanup(proxyServer.Close)

	checkoutDir := t.TempDir()
	originalWorkingDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(checkoutDir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(originalWorkingDir))
	})
	invocationLog := newInvocationLog(nil)
	invocationLog.writer = io.Discard
	ws := &workspace{
		rootDir: checkoutDir,
		log:     &buildEventReporter{log: invocationLog},
	}
	require.NoError(t, ws.init(t.Context()))
	// Filtered fetches require the partialClone extension, which the runner
	// normally enables while configuring the repository.
	_, gitErr := git(t.Context(), io.Discard, "config", "extensions.partialClone", "true")
	require.Nil(t, gitErr)

	// Fetch the pushed branch without blobs. This fetch is not stalled, so
	// expect no retries. Fetched byte counts are not asserted in this test,
	// since the git version on the test executors is too old to log the
	// trace2 progress events that byte counting is parsed from.
	require.NoError(t, ws.fetch(t.Context(), proxyServer.URL, []string{"refs/heads/master"}, 1))
	require.Equal(t, int64(0), ws.gitFetchRetryCount)

	// Check out the fetched branch, stalling the lazy blob fetch that the
	// checkout triggers. Expect the checkout to be aborted by Git's low-speed
	// check and retried.
	stallNextFetch.Store(true)
	require.NoError(t, ws.checkoutRef(t.Context()))
	require.Equal(t, int64(1), ws.gitFetchRetryCount)

	// The checked-out working tree should contain the lazily fetched README
	// at the pushed commit.
	readme, err := os.ReadFile("README.md")
	require.NoError(t, err)
	require.Equal(t, "test repo", string(readme))
	headCommitSHA, gitErr := git(t.Context(), io.Discard, "rev-parse", "HEAD")
	require.Nil(t, gitErr)
	require.Equal(t, wantCommitSHA, strings.TrimSpace(headCommitSHA))
}

func TestGitMergeRetriesSlowLazyFetch(t *testing.T) {
	flags.Set(t, "git_fetch_low_speed_retries", 1)
	flags.Set(t, "git_fetch_low_speed_limit", 1)
	// Git and libcurl configure the low-speed window in whole seconds, so one
	// second is the shortest interval and gives the fastest possible check.
	flags.Set(t, "git_fetch_low_speed_time", time.Second)
	// Fetch with blob:none so that the merge has to lazily fetch the blobs it
	// needs from the remote.
	flags.Set(t, "git_fetch_filters", []string{"blob:none"})
	flags.Set(t, "pushed_branch", "feature")
	flags.Set(t, "target_branch", "master")
	for _, envVar := range []string{"GIT_HTTP_LOW_SPEED_LIMIT", "GIT_HTTP_LOW_SPEED_TIME"} {
		originalValue, wasSet := os.LookupEnv(envVar)
		require.NoError(t, os.Unsetenv(envVar))
		t.Cleanup(func() {
			if wasSet {
				require.NoError(t, os.Setenv(envVar, originalValue))
				return
			}
			require.NoError(t, os.Unsetenv(envVar))
		})
	}
	// Fetch from the proxy URL as-is. Otherwise fetch() embeds the REPO_USER
	// and REPO_TOKEN credentials in the URL if they are set, which they are
	// when this test itself runs in a BuildBuddy workflow.
	t.Setenv("USE_SYSTEM_GIT_CREDENTIALS", "1")

	// Serve a repository over HTTP in which a feature branch and master have
	// diverged, so that merging master into the feature branch needs master's
	// updated README blob.
	remote := testgit.StartServer(t, testgit.ServerOptions{LogWriter: io.Discard})
	remote.CreateProject("test-org", "test-repo", &testgit.ProjectSettings{Public: true})
	sourceDir, _ := testgit.MakeTempRepo(t, map[string]string{"README.md": "base readme"})
	testshell.Run(t, sourceDir, `
		git checkout -q -b feature
		echo -n "feature change" > feature.txt
		git add . && git commit -qm "feature change"
		git checkout -q master
		echo -n "updated readme" > README.md
		git add . && git commit -qm "master change"
	`)
	remote.Push("test-org", "test-repo", remote.AccessToken(), sourceDir)
	// Push the feature branch with the same credential settings that
	// remote.Push uses. In particular the credential helper list must be
	// reset, because on macOS the system gitconfig enables the osxkeychain
	// helper, which hangs forever on unattended hosts waiting for keychain
	// access.
	testshell.Run(t, sourceDir, `
		export GIT_ASKPASS=/usr/bin/true
		git -c credential.helper= push -q origin feature
	`)
	masterReadmeOID := strings.TrimSpace(testshell.Run(t, sourceDir, `git rev-parse master:README.md`))

	// Proxy the Git server. The merge's lazy fetch is identified by its
	// request body wanting master's README blob; once armed, the proxy leaves
	// the response to that fetch hanging after its first byte until Git
	// aborts it. All other responses pass through, including those of the
	// target branch fetch that precedes the merge.
	upstreamURL, err := url.Parse(remote.RepoURL("test-org", "test-repo", ""))
	require.NoError(t, err)
	var stallNextLazyFetch atomic.Bool
	type stallMarker struct{}
	proxy := httputil.NewSingleHostReverseProxy(upstreamURL)
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.Context().Value(stallMarker{}) != nil {
			response.Body = &stallingReadCloser{ctx: response.Request.Context(), ReadCloser: response.Body}
		}
		return nil
	}
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/git-upload-pack") {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			if strings.Contains(string(body), masterReadmeOID) && stallNextLazyFetch.CompareAndSwap(true, false) {
				r = r.WithContext(context.WithValue(r.Context(), stallMarker{}, true))
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(proxyServer.Close)
	flags.Set(t, "pushed_repo_url", proxyServer.URL)
	flags.Set(t, "target_repo_url", proxyServer.URL)

	checkoutDir := t.TempDir()
	originalWorkingDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(checkoutDir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(originalWorkingDir))
	})
	invocationLog := newInvocationLog(nil)
	invocationLog.writer = io.Discard
	ws := &workspace{
		rootDir: checkoutDir,
		log:     &buildEventReporter{log: invocationLog},
	}
	require.NoError(t, ws.init(t.Context()))
	// Filtered fetches require the partialClone extension, and the merge
	// commit needs a committer identity. The runner normally sets both while
	// configuring the repository.
	for _, kv := range [][2]string{
		{"extensions.partialClone", "true"},
		{"user.email", "ci-runner@buildbuddy.io"},
		{"user.name", "BuildBuddy"},
	} {
		_, gitErr := git(t.Context(), io.Discard, "config", kv[0], kv[1])
		require.Nil(t, gitErr)
	}

	// Fetch and check out the feature branch. Neither is stalled, so expect
	// no retries.
	require.NoError(t, ws.fetch(t.Context(), proxyServer.URL, []string{"refs/heads/feature"}, 1))
	require.NoError(t, ws.checkoutRef(t.Context()))
	require.Equal(t, int64(0), ws.gitFetchRetryCount)

	// Merge with the base branch, stalling the lazy fetch of master's README
	// blob that the merge triggers. Expect the merge to be aborted by Git's
	// low-speed check and retried. MergeWithBase is enabled by default for
	// pull request triggers.
	stallNextLazyFetch.Store(true)
	triggers := &config.Triggers{PullRequest: &config.PullRequestTrigger{}}
	require.NoError(t, ws.mergeWithBaseIfRequested(t.Context(), triggers))
	require.Nil(t, ws.setupError, "setup error: %v", ws.setupError)
	require.False(t, stallNextLazyFetch.Load(), "expected the proxy to have stalled the merge's lazy fetch")
	require.Equal(t, int64(1), ws.gitFetchRetryCount)

	// The merge commit should combine both branches, with the lazily fetched
	// README at master's version and the feature branch's file intact.
	readme, err := os.ReadFile("README.md")
	require.NoError(t, err)
	require.Equal(t, "updated readme", string(readme))
	featureFile, err := os.ReadFile("feature.txt")
	require.NoError(t, err)
	require.Equal(t, "feature change", string(featureFile))
	_, gitErr := git(t.Context(), io.Discard, "rev-parse", "--verify", "HEAD^2")
	require.Nil(t, gitErr, "expected HEAD to be a merge commit")
}

func TestIsTransferTooSlow(t *testing.T) {
	// Serve a git HTTP endpoint that starts a ref advertisement and then
	// stalls, so that git's low-speed check aborts the transfer.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		io.WriteString(w, "001e# service=git-upload-pack\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)

	// List the stalled remote with the low-speed check configured to trip
	// after one second (the shortest window git supports). Expect
	// isTransferTooSlow to match the error that git reports.
	_, err := gitWithEnv(t.Context(), io.Discard, map[string]string{
		"GIT_HTTP_LOW_SPEED_LIMIT": "1024",
		"GIT_HTTP_LOW_SPEED_TIME":  "1",
	}, "ls-remote", server.URL)
	require.NotNil(t, err)
	assert.True(t, isTransferTooSlow(err), "expected a low-speed abort, got: %s", err.Output)

	// A git error unrelated to transfer speed should not match.
	_, err = git(t.Context(), io.Discard, "ls-remote", filepath.Join(t.TempDir(), "nonexistent"))
	require.NotNil(t, err)
	assert.False(t, isTransferTooSlow(err), "unexpected low-speed abort: %s", err.Output)
}

func TestParseGitFetchedBytes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		trace2Log string
		want      int64
	}{
		{
			name:      "empty log",
			trace2Log: "",
			want:      0,
		},
		{
			// A fetch with no new objects starts no progress meters, so the
			// log contains only process lifecycle and transfer events.
			name: "fetch with no new objects",
			trace2Log: `{"event":"version","sid":"sid1","thread":"main","time":"2026-07-14T15:23:39.915Z","file":"common-main.c","line":50,"evt":"3","exe":"2.43.0"}
{"event":"start","sid":"sid1","thread":"main","time":"2026-07-14T15:23:39.915Z","file":"common-main.c","line":51,"t_abs":0.001,"argv":["git","fetch","--force","origin","master"]}
{"event":"data","sid":"sid1","thread":"main","time":"2026-07-14T15:23:39.917Z","file":"connect.c","line":175,"t_abs":0.002,"t_rel":0.001,"nesting":2,"category":"transfer","key":"negotiated-version","value":"2"}
{"event":"exit","sid":"sid1","thread":"main","time":"2026-07-14T15:23:40.100Z","file":"git.c","line":712,"t_abs":0.185,"code":0}
`,
			want: 0,
		},
		{
			// Receiving a pack logs a "total_bytes" data event under the
			// "Receiving objects" progress region when the meter completes.
			// Sibling data events such as "total_objects" are not byte
			// counts and are expected to be ignored.
			name: "received pack",
			trace2Log: `{"event":"region_enter","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:23:59.947Z","file":"progress.c","line":270,"repo":1,"nesting":1,"category":"progress","label":"Receiving objects"}
{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":341,"repo":1,"t_abs":0.153,"t_rel":0.153,"nesting":2,"category":"progress","key":"total_objects","value":"155"}
{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":345,"repo":1,"t_abs":0.153,"t_rel":0.153,"nesting":2,"category":"progress","key":"total_bytes","value":"12985331"}
{"event":"region_leave","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":348,"repo":1,"t_rel":0.153,"nesting":1,"category":"progress","label":"Receiving objects"}
`,
			want: 12985331,
		},
		{
			// Small fetches unpack objects directly rather than indexing a
			// pack; the "Unpacking objects" progress meter also logs a
			// "total_bytes" data event.
			name: "unpacked objects",
			trace2Log: `{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":345,"repo":1,"t_abs":0.010,"t_rel":0.010,"nesting":2,"category":"progress","key":"total_bytes","value":"1034"}
`,
			want: 1034,
		},
		{
			// A fetch may receive multiple packs (e.g. submodule fetches
			// append events from their own processes to the same log).
			// Expect the byte counts to be summed.
			name: "multiple packs",
			trace2Log: `{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":345,"repo":1,"nesting":2,"category":"progress","key":"total_bytes","value":"1000"}
{"event":"data","sid":"sid1/sid3","thread":"main","time":"2026-07-14T15:24:01.100Z","file":"progress.c","line":345,"repo":1,"nesting":2,"category":"progress","key":"total_bytes","value":"234"}
`,
			want: 1234,
		},
		{
			// Progress meters without throughput (such as "Resolving
			// deltas") log only object counts. A "total_bytes" key under a
			// different category should not be counted as fetched bytes.
			name: "non-progress categories ignored",
			trace2Log: `{"event":"data","sid":"sid1","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":341,"repo":1,"nesting":2,"category":"progress","key":"total_objects","value":"155"}
{"event":"data","sid":"sid1","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"index-pack.c","line":100,"repo":1,"nesting":2,"category":"pack","key":"total_bytes","value":"999"}
`,
			want: 0,
		},
		{
			// Concurrent processes appending to the log can in rare cases
			// interleave partial lines. Expect malformed lines to be skipped
			// without affecting other events.
			name: "malformed lines skipped",
			trace2Log: `{"event":"data","sid":"sid1","thread":"main","cat
{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":345,"repo":1,"nesting":2,"category":"progress","key":"total_bytes","value":"4096"}
not json at all
{"event":"data","sid":"sid1/sid2","thread":"main","time":"2026-07-14T15:24:00.100Z","file":"progress.c","line":345,"repo":1,"nesting":2,"category":"progress","key":"total_bytes","value":"not-a-number"}
`,
			want: 4096,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, parseGitFetchedBytes(strings.NewReader(tc.trace2Log)))
		})
	}
}
