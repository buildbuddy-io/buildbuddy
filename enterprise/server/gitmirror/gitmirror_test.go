package gitmirror_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror/gitremote"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror/gitstorage"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	exampleOrgName     = "example-org"
	exampleRepoName    = "example-repo"
	exampleAccessToken = "example-access-token"
)

var (
	exampleRepoInitialContents = map[string]string{"README.md": "# Example git repository"}
)

func startMirror(t *testing.T, clock clockwork.Clock) (serverURL, rootDir string) {
	rootDir = testfs.MakeTempDir(t)
	flags.Set(t, "git.mirror.root_directory", rootDir)
	flags.Set(t, "http.client.allow_localhost", true)
	server, err := gitmirror.New(clock)
	if errors.Is(err, gitmirror.ErrInsufficientGitVersion) && os.Getenv("CI") != "true" {
		t.Skip(err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	return httpServer.URL, rootDir
}

func createExampleRepo(t *testing.T, remote *testgit.Server) string {
	remote.CreateProject(exampleOrgName, exampleRepoName, &testgit.ProjectSettings{})
	repoPath, _ := testgit.MakeTempRepo(t, exampleRepoInitialContents)
	remote.Push(exampleOrgName, exampleRepoName, exampleAccessToken, repoPath)
	return repoPath
}

func readPacketLines(r io.Reader) ([]string, error) {
	var lines []string
	scanner := pktline.NewScanner(r)
	for scanner.Scan() {
		lines = append(lines, string(scanner.Bytes()))
	}
	return lines, scanner.Err()
}

func TestPullThroughMirror(t *testing.T) {
	// Create an upstream server configured to serve the example repo.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	repoPath := createExampleRepo(t, remote)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})

	clock := clockwork.NewFakeClock()
	mirrorServerURL, mirrorRootDir := startMirror(t, clock)

	// Pull a basic repo through the mirror. After pulling, repo contents should
	// exist locally.
	mirrorRepoURL := fmt.Sprintf("%s/v1/%s%s", mirrorServerURL, upstreamURL.Host, upstreamURL.Path)
	mirrorRepoURL, err = git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
	require.NoError(t, err)
	clonePath := testfs.MakeTempDir(t)
	testshell.Run(t, clonePath, fmt.Sprintf("git clone %q .", mirrorRepoURL))
	require.Equal(t, exampleRepoInitialContents["README.md"], testfs.ReadFileAsString(t, clonePath, "README.md"))

	// Repo contents should also exist in the mirror, at the expected dir.
	expectedRepo, err := gitremote.RestoreRepo(upstreamURL.String())
	require.NoError(t, err)
	repoID := gitstorage.IDForRepo(expectedRepo)
	mirrorDirName := string(repoID) + "_" + gitstorage.LabelForRepo(expectedRepo) + ".git"
	mirrorRepoPath := filepath.Join(mirrorRootDir, string(repoID[:2]), mirrorDirName)
	require.DirExists(t, mirrorRepoPath)
	require.Equal(t, exampleRepoInitialContents["README.md"], testshell.Run(t, mirrorRepoPath, "git show HEAD:README.md"))

	// Push another commit to the remote, then pull it again through the mirror.
	updatedContents := map[string]string{"README.md": "# Updated example git repository"}
	testgit.CommitFiles(t, repoPath, updatedContents)
	testshell.Run(t, repoPath, "git push")
	testshell.Run(t, clonePath, "git pull --ff-only")
	require.Equal(t, updatedContents["README.md"], testfs.ReadFileAsString(t, clonePath, "README.md"))

	// Push through the mirror, which forwards receive-pack requests upstream.
	// Then pull it through the mirror again.
	pushedContents := map[string]string{"README.md": "# Pushed through the Git mirror"}
	testshell.Run(t, clonePath, "git config user.name 'Test User' && git config user.email 'test@example.com'")
	testgit.CommitFiles(t, clonePath, pushedContents)
	testshell.Run(t, clonePath, "git push")
	testshell.Run(t, repoPath, "git pull --ff-only")
	require.Equal(t, pushedContents["README.md"], testfs.ReadFileAsString(t, repoPath, "README.md"))
}

func TestParseRequest(t *testing.T) {
	for _, testCase := range []struct {
		name              string
		path              string
		wantRawUpstream   string
		wantRawRepository string
		wantGitPath       string
		wantError         bool
	}{
		{
			name:            "unhandled path is preserved for passthrough",
			path:            "/v1/github.com/org/repo/HEAD",
			wantRawUpstream: "github.com/org/repo/HEAD",
		},
		{
			name:              "repository path is extracted",
			path:              "/v1/github.com/org/repo/git-upload-pack",
			wantRawRepository: "github.com/org/repo",
			wantGitPath:       "/git-upload-pack",
		},
		{
			name:              "custom port is preserved",
			path:              "/v1/github.com:8443/org/repo/git-upload-pack",
			wantRawRepository: "github.com:8443/org/repo",
			wantGitPath:       "/git-upload-pack",
		},
		{
			name:              "nested repository path is preserved",
			path:              "/v1/gitlab.com/group/subgroup/repo/git-upload-pack",
			wantRawRepository: "gitlab.com/group/subgroup/repo",
			wantGitPath:       "/git-upload-pack",
		},
		{
			name:              "host-only repository is preserved",
			path:              "/v1/git.example.com/git-upload-pack",
			wantRawRepository: "git.example.com",
			wantGitPath:       "/git-upload-pack",
		},
		{
			name:              "info refs path is identified",
			path:              "/v1/github.com/org/repo/info/refs",
			wantRawRepository: "github.com/org/repo",
			wantGitPath:       "/info/refs",
		},
		{
			name:              "receive pack path is identified",
			path:              "/v1/github.com/org/repo/git-receive-pack",
			wantRawRepository: "github.com/org/repo",
			wantGitPath:       "/git-receive-pack",
		},
		// "Weird" repo URLs - technically valid, so we should accept them. When
		// later calling Resolve, the upstream can reject the URL if it's
		// invalid.
		{
			name: "dots are preserved",
			// Note: gitstorage layer has additional tests to ensure these types
			// of URLs cannot result in path traversal.
			path:              "/v1/github.com/.././repo/git-upload-pack",
			wantRawRepository: "github.com/.././repo",
			wantGitPath:       "/git-upload-pack",
		},
		{
			name:              "backslash is preserved",
			path:              `/v1/github.com/org\repo/git-upload-pack`,
			wantRawUpstream:   "github.com/org%5Crepo/git-upload-pack",
			wantRawRepository: `github.com/org\repo`,
			wantGitPath:       "/git-upload-pack",
		},
		{
			name:              "empty segment is preserved",
			path:              "/v1/github.com//repo/git-upload-pack",
			wantRawRepository: "github.com//repo",
			wantGitPath:       "/git-upload-pack",
		},
		// Error cases
		{name: "missing host is rejected", path: "/v1/git-upload-pack", wantError: true},
		{name: "missing version is rejected", path: "/github.com/org/repo/git-upload-pack", wantError: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			httpReq := httptest.NewRequest(http.MethodPost, testCase.path, nil)
			req, err := gitmirror.ParseRequest(httpReq)
			if testCase.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Same(t, httpReq, req.Request)
			require.Equal(t, "v1", req.Version)
			wantRawUpstream := testCase.wantRawUpstream
			if wantRawUpstream == "" {
				wantRawUpstream = testCase.wantRawRepository + testCase.wantGitPath
			}
			require.Equal(t, wantRawUpstream, req.RawUpstream)
			require.Equal(t, testCase.wantRawRepository, req.RawRepository)
			require.Equal(t, testCase.wantGitPath, req.GitPath)
		})
	}
}

func TestUnhandledRequestIsForwarded(t *testing.T) {
	// Configure an upstream that verifies the mirror forwarded the client's
	// method, escaped path, query, authorization, and custom header unchanged.
	var requestCount atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requestCount.Add(1)
		assert.Equal(t, http.MethodPatch, req.Method)
		assert.Equal(t, "/custom/endpoint/with%2Fescaping", req.URL.EscapedPath())
		assert.Equal(t, "customParam=customValue", req.URL.RawQuery)
		assert.Equal(t, "Bearer secret", req.Header.Get("Authorization"))
		assert.Equal(t, "request", req.Header.Get("X-Custom"))

		w.Header().Set("X-Custom", "response")
		w.WriteHeader(http.StatusAccepted)
		_, err := io.Copy(w, req.Body)
		require.NoError(t, err)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())

	// Send an unhandled request through the mirror with a body, query, auth,
	// and application-specific header that the catch-all proxy must preserve.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPatch,
		fmt.Sprintf("%s/v1/%s/custom/endpoint/with%%2Fescaping?customParam=customValue", mirrorURL, upstreamURL.Host), strings.NewReader("body"))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("X-Custom", "request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Check that upstream header and body were forwarded back as-is.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	require.Equal(t, "response", resp.Header.Get("X-Custom"))
	require.Equal(t, "body", string(responseBody))

	// Make sure we only sent one request to upstream.
	require.Equal(t, int64(1), requestCount.Load())
}

func TestInfoRefsProtocolVersions(t *testing.T) {
	// Configure a private upstream repository containing a commit that older
	// protocol advertisements can identify.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	repoPath := createExampleRepo(t, remote)
	commitID := strings.TrimSpace(testshell.Run(t, repoPath, "git rev-parse HEAD"))
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})

	// Request discovery using every supported protocol. Start each case with an
	// empty mirror so a previous case cannot populate its local repository.
	for _, testCase := range []struct {
		name        string
		protocol    string
		wantVersion string
		wantRefs    bool
	}{
		{
			name:     "protocol v0",
			wantRefs: true,
		},
		{
			name:        "protocol v1",
			protocol:    "version=1",
			wantVersion: "version 1\n",
			wantRefs:    true,
		},
		{
			name:        "protocol v2",
			protocol:    "version=2",
			wantVersion: "version 2\n",
		},
		{
			name:        "protocol v2 after another parameter",
			protocol:    "agent=test:version=2",
			wantVersion: "version 2\n",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Start a fresh mirror and request the upstream repository's smart HTTP
			// advertisement using this protocol version.
			mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
			refsURL := fmt.Sprintf(
				"%s/v1/%s%s/info/refs?service=git-upload-pack",
				mirrorURL, upstreamURL.Host, upstreamURL.Path,
			)
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, refsURL, nil)
			require.NoError(t, err)
			if testCase.protocol != "" {
				request.Header.Set("Git-Protocol", testCase.protocol)
			}
			request.SetBasicAuth("buildbuddy", exampleAccessToken)

			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err)

			// Every supported protocol returns the smart HTTP service announcement
			// followed by a valid Git advertisement.
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(
				t,
				"application/x-git-upload-pack-advertisement",
				response.Header.Get("Content-Type"),
			)
			require.Equal(t, "no-cache", response.Header.Get("Cache-Control"))
			lines, err := readPacketLines(response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.GreaterOrEqual(t, len(lines), 3)
			require.Equal(t, "# service=git-upload-pack\n", lines[0])
			require.Empty(t, lines[1])

			advertisement := strings.Join(lines[2:], "")
			if testCase.wantVersion != "" {
				require.Equal(t, testCase.wantVersion, lines[2])
			}
			if testCase.wantRefs {
				// Protocols v0 and v1 advertise the current repository refs.
				require.Contains(t, advertisement, commitID)
			} else {
				// Protocol v2 advertises commands before the client requests refs.
				require.NotContains(t, advertisement, commitID)
				require.Contains(t, advertisement, "ls-refs")
				require.Contains(t, advertisement, "fetch=")
			}
		})
	}
}

func TestInfoRefsEmptyRepository(t *testing.T) {
	// Configure an authenticated upstream repository without any commits or
	// refs, then start an empty mirror in front of it.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	remote.CreateProject(exampleOrgName, exampleRepoName, &testgit.ProjectSettings{})
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())

	// Request a v0 advertisement, which must encode capabilities even though
	// the repository has no real refs to advertise.
	refsURL := fmt.Sprintf(
		"%s/v1/%s%s/info/refs?service=git-upload-pack",
		mirrorURL, upstreamURL.Host, upstreamURL.Path,
	)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, refsURL, nil)
	require.NoError(t, err)
	request.SetBasicAuth("buildbuddy", exampleAccessToken)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	lines, err := readPacketLines(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	// With no refs, Git advertises capabilities using a synthetic first ref
	// with an all-zero object ID and the name capabilities^{}.
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.GreaterOrEqual(t, len(lines), 3)
	require.Contains(t, lines[2], strings.Repeat("0", 40)+" capabilities^{}")
	advertisement := strings.Join(lines[2:], "")
	require.NotContains(t, advertisement, "refs/heads/")
}

func TestInfoRefsRefreshesRefs(t *testing.T) {
	// Configure a private upstream repository and an empty mirror that clients
	// will use for protocol v0 ref discovery.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	repoPath := createExampleRepo(t, remote)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	mirrorRepoURL := fmt.Sprintf("%s/v1/%s%s", mirrorURL, upstreamURL.Host, upstreamURL.Path)
	mirrorRepoURL, err = git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
	require.NoError(t, err)
	branch := testgit.CurrentBranch(t, repoPath)
	command := fmt.Sprintf("git -c protocol.version=0 ls-remote %q refs/heads/%s", mirrorRepoURL, branch)

	// The first discovery request advertises the initial upstream branch tip.
	clientDir := testfs.MakeTempDir(t)
	initialCommitID := strings.TrimSpace(testshell.Run(t, repoPath, "git rev-parse HEAD"))
	require.Contains(t, testshell.Run(t, clientDir, command), initialCommitID)

	// After the branch advances upstream, the next discovery request refreshes
	// the mirror and advertises the new tip rather than its cached predecessor.
	testgit.CommitFiles(t, repoPath, map[string]string{"new.txt": "new contents"})
	testshell.Run(t, repoPath, "git push")
	updatedCommitID := strings.TrimSpace(testshell.Run(t, repoPath, "git rev-parse HEAD"))
	require.NotEqual(t, initialCommitID, updatedCommitID)
	require.Contains(t, testshell.Run(t, clientDir, command), updatedCommitID)
}

func TestInfoRefsPreservesUpstreamErrors(t *testing.T) {
	// Configure a private upstream repository so discovery can exercise both
	// authentication failures and missing repositories.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	createExampleRepo(t, remote)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())

	for _, testCase := range []struct {
		name          string
		repoName      string
		token         string
		wantStatus    int
		wantChallenge string
	}{
		{
			name:          "missing credentials",
			repoName:      exampleRepoName,
			wantStatus:    http.StatusUnauthorized,
			wantChallenge: "Basic",
		},
		{
			name:          "invalid credentials",
			repoName:      exampleRepoName,
			token:         "invalid-token",
			wantStatus:    http.StatusUnauthorized,
			wantChallenge: "Basic",
		},
		{
			name:       "missing repository",
			repoName:   "missing-repo",
			token:      exampleAccessToken,
			wantStatus: http.StatusNotFound,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Request discovery for the selected repository and credentials.
			refsURL := fmt.Sprintf(
				"%s/v1/%s/%s/%s/info/refs?service=git-upload-pack",
				mirrorURL, upstreamURL.Host, exampleOrgName, testCase.repoName,
			)
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, refsURL, nil)
			require.NoError(t, err)
			if testCase.token != "" {
				request.SetBasicAuth("buildbuddy", testCase.token)
			}
			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err)
			defer response.Body.Close()

			// The mirror preserves the upstream status and authentication scheme.
			// The challenge lets Git ask its credential helper before retrying.
			require.Equal(t, testCase.wantStatus, response.StatusCode)
			require.Equal(t, testCase.wantChallenge, response.Header.Get("WWW-Authenticate"))
		})
	}
}

func TestInfoRefsUnsupportedRequestsAreForwarded(t *testing.T) {
	// Configure an upstream that identifies each request it receives, then put
	// the mirror in front of it.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("X-Received-Request", req.Method+" "+req.URL.RequestURI())
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())

	for _, testCase := range []struct {
		name   string
		method string
		query  string
	}{
		{
			name:   "wrong method",
			method: http.MethodPost,
			query:  "?service=git-upload-pack",
		},
		{
			name:   "wrong service",
			method: http.MethodGet,
			query:  "?service=git-receive-pack",
		},
		{name: "missing service", method: http.MethodGet},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Send an info/refs request that does not select the locally handled
			// GET service=git-upload-pack combination.
			requestURL := fmt.Sprintf(
				"%s/v1/%s/repo/info/refs%s",
				mirrorURL, upstreamURL.Host, testCase.query,
			)
			request, err := http.NewRequestWithContext(t.Context(), testCase.method, requestURL, nil)
			require.NoError(t, err)
			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err)
			defer response.Body.Close()

			// The upstream response proves that the mirror preserved the method,
			// path, and query while forwarding the request.
			require.Equal(t, http.StatusAccepted, response.StatusCode)
			wantRequest := testCase.method + " /repo/info/refs" + testCase.query
			require.Equal(t, wantRequest, response.Header.Get("X-Received-Request"))
		})
	}
}

func TestInfoRefsInitializesAndReloadsMirror(t *testing.T) {
	// Configure a private upstream repository, then start a mirror with an empty
	// storage directory.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	createExampleRepo(t, remote)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})

	clock := clockwork.NewFakeClock()
	mirrorServerURL, mirrorRootDir := startMirror(t, clock)

	// Protocol v2 discovery needs only local capabilities, so it initializes
	// the bare repository without fetching upstream refs.
	refsURL := fmt.Sprintf(
		"%s/v1/%s%s/info/refs?service=git-upload-pack",
		mirrorServerURL, upstreamURL.Host, upstreamURL.Path,
	)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, refsURL, nil)
	require.NoError(t, err)
	request.Header.Set("Git-Protocol", "version=2")
	request.SetBasicAuth("buildbuddy", exampleAccessToken)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())

	// The initialized mirror records its normalized origin URL but contains no
	// local refs because protocol v2 did not request them.
	expectedRepo, err := gitremote.RestoreRepo(upstreamURL.String())
	require.NoError(t, err)
	repoID := gitstorage.IDForRepo(expectedRepo)
	mirrorDirName := string(repoID) + "_" + gitstorage.LabelForRepo(expectedRepo) + ".git"
	mirrorRepoPath := filepath.Join(mirrorRootDir, string(repoID[:2]), mirrorDirName)
	require.DirExists(t, mirrorRepoPath)
	originURL := strings.TrimSpace(testshell.Run(t, mirrorRepoPath, "git remote get-url origin"))
	require.Equal(t, expectedRepo.String(), originURL)
	refs := testshell.Run(t, mirrorRepoPath, "git for-each-ref --format='%(refname)'")
	require.Empty(t, strings.TrimSpace(refs))

	// Restart the mirror from the same storage directory, then request protocol
	// v0 discovery, which must reload and refresh the existing bare repository.
	reloadedServer, err := gitmirror.New(clock)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reloadedServer.Close()) })
	reloadedHTTPServer := httptest.NewServer(reloadedServer)
	t.Cleanup(reloadedHTTPServer.Close)
	refsURL = fmt.Sprintf(
		"%s/v1/%s%s/info/refs?service=git-upload-pack",
		reloadedHTTPServer.URL, upstreamURL.Host, upstreamURL.Path,
	)
	request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, refsURL, nil)
	require.NoError(t, err)
	request.SetBasicAuth("buildbuddy", exampleAccessToken)
	response, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
	// Protocol v0 discovery advertises refs, so it refreshes the repository.
	refs = testshell.Run(t, mirrorRepoPath, "git for-each-ref --format='%(refname)'")
	require.NotEmpty(t, strings.TrimSpace(refs))
}

func TestRetentionEvictsUnusedMirror(t *testing.T) {
	// Create a mirror with a short retention period and initialize a repository
	// through protocol v2 discovery.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	createExampleRepo(t, remote)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	flags.Set(t, "git.mirror.retention_period", time.Minute)
	clock := clockwork.NewFakeClock()
	mirrorServerURL, mirrorRootDir := startMirror(t, clock)
	clock.BlockUntil(1)

	refsURL := fmt.Sprintf(
		"%s/v1/%s%s/info/refs?service=git-upload-pack",
		mirrorServerURL, upstreamURL.Host, upstreamURL.Path,
	)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, refsURL, nil)
	require.NoError(t, err)
	request.Header.Set("Git-Protocol", "version=2")
	request.SetBasicAuth("buildbuddy", exampleAccessToken)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	expectedRepo, err := gitremote.RestoreRepo(upstreamURL.String())
	require.NoError(t, err)
	repoID := gitstorage.IDForRepo(expectedRepo)
	mirrorDirName := string(repoID) + "_" + gitstorage.LabelForRepo(expectedRepo) + ".git"
	mirrorRepoPath := filepath.Join(mirrorRootDir, string(repoID[:2]), mirrorDirName)
	require.DirExists(t, mirrorRepoPath)

	// The first sweep at the retention boundary atomically removes the idle
	// repository after the request has released its lease.
	clock.Advance(time.Minute)
	require.Eventually(t, func() bool {
		_, err := os.Stat(mirrorRepoPath)
		return errors.Is(err, os.ErrNotExist)
	}, time.Second, 10*time.Millisecond)
}

func TestUploadPackProtocolVersions(t *testing.T) {
	// Configure a private upstream repository whose contents can be verified
	// after each protocol completes packfile negotiation.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	createExampleRepo(t, remote)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})

	for _, testCase := range []struct {
		name    string
		version string
	}{
		{name: "protocol v0", version: "0"},
		{name: "protocol v1", version: "1"},
		{name: "protocol v2", version: "2"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Start with an empty mirror so this protocol must fetch from the
			// upstream and complete its own upload-pack negotiation.
			mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())

			// git does not surface the mirror's HTTP response headers, so clone
			// through a reverse proxy that records the headers of each
			// /git-upload-pack response. A protocol v2 clone sends two upload-pack
			// requests (ls-refs, then fetch); the channel buffer holds both so
			// ModifyResponse never blocks.
			mirrorBackend, err := url.Parse(mirrorURL)
			require.NoError(t, err)
			uploadPackHeaders := make(chan http.Header, 2)
			proxy := httputil.NewSingleHostReverseProxy(mirrorBackend)
			proxy.ModifyResponse = func(response *http.Response) error {
				if strings.HasSuffix(response.Request.URL.Path, "/git-upload-pack") {
					uploadPackHeaders <- response.Header.Clone()
				}
				return nil
			}
			recordingProxy := httptest.NewServer(proxy)
			t.Cleanup(recordingProxy.Close)
			mirrorRepoURL := fmt.Sprintf(
				"%s/v1/%s%s", recordingProxy.URL, upstreamURL.Host, upstreamURL.Path,
			)
			authenticatedURL, err := git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
			require.NoError(t, err)
			clonePath := testfs.MakeTempDir(t)
			testshell.Run(t, clonePath, fmt.Sprintf(
				"git -c protocol.version=%s clone %q .", testCase.version, authenticatedURL,
			))

			// The first upload-pack response (the only one for v0 and v1, and the
			// ls-refs response for v2) should use Git's result content type and
			// disable caching.
			responseHeader := <-uploadPackHeaders
			require.Equal(
				t, "application/x-git-upload-pack-result", responseHeader.Get("Content-Type"),
			)
			require.Equal(t, "no-cache", responseHeader.Get("Cache-Control"))

			// A successful checkout shows that discovery, negotiation, and packfile
			// transfer all used a mutually compatible protocol.
			require.Equal(
				t, exampleRepoInitialContents["README.md"],
				testfs.ReadFileAsString(t, clonePath, "README.md"),
			)
		})
	}
}

func TestUploadPackDecompressesGzipRequest(t *testing.T) {
	// Configure a private upstream containing a ref that can be requested with
	// a small protocol v2 ls-refs command.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	repoPath := createExampleRepo(t, remote)
	commitID := strings.TrimSpace(testshell.Run(t, repoPath, "git rev-parse HEAD"))
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	uploadPackURL := fmt.Sprintf(
		"%s/v1/%s%s/git-upload-pack", mirrorURL, upstreamURL.Host, upstreamURL.Path,
	)

	// Compress the small request explicitly so the test does not need enough
	// refs to make Git choose compression automatically.
	var requestBody bytes.Buffer
	compressor := gzip.NewWriter(&requestBody)
	_, err = compressor.Write([]byte("0014command=ls-refs\n00010000"))
	require.NoError(t, err)
	require.NoError(t, compressor.Close())
	request, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, uploadPackURL, &requestBody,
	)
	require.NoError(t, err)
	request.Header.Set("Content-Encoding", "gzip")
	request.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	request.Header.Set("Git-Protocol", "version=2")
	request.SetBasicAuth("buildbuddy", exampleAccessToken)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	responseBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	// The mirror should decompress the command before upload-pack reads it and
	// return the requested refs instead of an HTTP 500.
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(
		t, "application/x-git-upload-pack-result", response.Header.Get("Content-Type"),
	)
	require.Contains(t, string(responseBody), commitID)
}

func TestUploadPackRejectsInvalidContentEncoding(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		upstreamRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	uploadPackURL := fmt.Sprintf("%s/v1/%s/repo/git-upload-pack", mirrorURL, upstreamURL.Host)

	for _, testCase := range []struct {
		name            string
		contentEncoding string
		body            string
		wantStatus      int
		wantBody        string
	}{
		{
			name:            "malformed gzip is rejected",
			contentEncoding: "gzip",
			body:            "not gzip",
			wantStatus:      http.StatusBadRequest,
			wantBody:        "invalid gzip request body\n",
		},
		{
			name:            "unknown encoding is rejected",
			contentEncoding: "br",
			wantStatus:      http.StatusUnsupportedMediaType,
			wantBody:        "unsupported content encoding\n",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Send the invalid encoding to a valid mirror route.
			request, err := http.NewRequestWithContext(
				t.Context(), http.MethodPost, uploadPackURL, strings.NewReader(testCase.body),
			)
			require.NoError(t, err)
			request.Header.Set("Content-Encoding", testCase.contentEncoding)
			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err)
			responseBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())

			// Encoding errors should be public client errors and should not
			// trigger upstream resolution or fetch work.
			require.Equal(t, testCase.wantStatus, response.StatusCode)
			require.Equal(t, testCase.wantBody, string(responseBody))
			require.Zero(t, upstreamRequests.Load())
		})
	}
}

func TestUploadPackClonesEmptyRepository(t *testing.T) {
	// Configure a private upstream repository with no commits or refs, then put
	// an empty mirror in front of it.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	remote.CreateProject(exampleOrgName, exampleRepoName, &testgit.ProjectSettings{})
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	mirrorRepoURL := fmt.Sprintf("%s/v1/%s%s", mirrorURL, upstreamURL.Host, upstreamURL.Path)
	authenticatedURL, err := git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
	require.NoError(t, err)

	// Clone through the mirror to exercise upload-pack with no objects to send.
	clonePath := testfs.MakeTempDir(t)
	testshell.Run(t, clonePath, fmt.Sprintf("git clone %q .", authenticatedURL))

	// The clone should produce a valid worktree without checking out any files
	// because the upstream has no HEAD commit.
	require.Equal(t, "true", strings.TrimSpace(
		testshell.Run(t, clonePath, "git rev-parse --is-inside-work-tree"),
	))
	require.NoFileExists(t, filepath.Join(clonePath, "README.md"))
}

func TestUploadPackPreservesUpstreamErrors(t *testing.T) {
	// Configure a private upstream repository so upload-pack can exercise both
	// authentication failures and missing repositories before negotiation.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	createExampleRepo(t, remote)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())

	for _, testCase := range []struct {
		name          string
		repoName      string
		token         string
		wantStatus    int
		wantChallenge string
	}{
		{
			name:          "missing credentials",
			repoName:      exampleRepoName,
			wantStatus:    http.StatusUnauthorized,
			wantChallenge: "Basic",
		},
		{
			name:          "invalid credentials",
			repoName:      exampleRepoName,
			token:         "invalid-token",
			wantStatus:    http.StatusUnauthorized,
			wantChallenge: "Basic",
		},
		{
			name:       "missing repository",
			repoName:   "missing-repo",
			token:      exampleAccessToken,
			wantStatus: http.StatusNotFound,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Begin upload-pack using the selected repository and credentials. The
			// upstream rejects the request before the packet body is processed.
			uploadPackURL := fmt.Sprintf(
				"%s/v1/%s/%s/%s/git-upload-pack",
				mirrorURL, upstreamURL.Host, exampleOrgName, testCase.repoName,
			)
			request, err := http.NewRequestWithContext(
				t.Context(), http.MethodPost, uploadPackURL, http.NoBody,
			)
			require.NoError(t, err)
			request.Header.Set("Content-Type", "application/x-git-upload-pack-request")
			if testCase.token != "" {
				request.SetBasicAuth("buildbuddy", testCase.token)
			}
			response, err := http.DefaultClient.Do(request)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())

			// The mirror should preserve the upstream status and authentication
			// scheme so Git can ask its credential helper before retrying.
			require.Equal(t, testCase.wantStatus, response.StatusCode)
			require.Equal(t, testCase.wantChallenge, response.Header.Get("WWW-Authenticate"))
		})
	}
}

func TestUploadPackPreservesRetryAfter(t *testing.T) {
	// Configure an upstream that asks clients to wait before retrying, then put
	// the mirror in front of it.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	uploadPackURL := fmt.Sprintf("%s/v1/%s/repo/git-upload-pack", mirrorURL, upstreamURL.Host)

	// Begin upload-pack while the upstream is throttling requests.
	request, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, uploadPackURL, http.NoBody,
	)
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	// The mirror should preserve both the status and Retry-After value so the
	// client can schedule its retry according to the upstream response.
	require.Equal(t, http.StatusTooManyRequests, response.StatusCode)
	require.Equal(t, "60", response.Header.Get("Retry-After"))
}

func TestUploadPackSanitizesFetchFailure(t *testing.T) {
	// Let the resolver's Go HTTP probe succeed, but reject the separate request
	// made by Git so the repository refresh fails before upload-pack starts.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.UserAgent(), "Go-http-client/") {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "sensitive upstream fetch failure", http.StatusInternalServerError)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, mirrorRootDir := startMirror(t, clockwork.NewFakeClock())
	uploadPackURL := fmt.Sprintf("%s/v1/%s/repo/git-upload-pack", mirrorURL, upstreamURL.Host)

	// Begin upload-pack with a valid route so the failure comes from git fetch.
	request, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, uploadPackURL, http.NoBody,
	)
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	responseBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	// The mirror should return a stable public error without exposing Git
	// output or local paths.
	require.Equal(t, http.StatusBadGateway, response.StatusCode)
	require.Equal(t, "failed to refresh repository\n", string(responseBody))
	require.NotContains(t, string(responseBody), "sensitive upstream fetch failure")
	require.NotContains(t, string(responseBody), mirrorRootDir)
}

func TestUploadPackServesCachedExactCommit(t *testing.T) {
	// Configure a private upstream repository and record the initial commit and
	// branch so the branch can be removed after the mirror caches them.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	repoPath := createExampleRepo(t, remote)
	commitID := strings.TrimSpace(testshell.Run(t, repoPath, "git rev-parse HEAD"))
	branch := testgit.CurrentBranch(t, repoPath)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	mirrorRepoURL := fmt.Sprintf("%s/v1/%s%s", mirrorURL, upstreamURL.Host, upstreamURL.Path)
	authenticatedURL, err := git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
	require.NoError(t, err)

	// Populate the mirror while the branch advertises the commit, then remove
	// that branch upstream. The mirror should retain the commit object locally.
	initialClonePath := testfs.MakeTempDir(t)
	testshell.Run(t, initialClonePath, fmt.Sprintf("git clone %q .", authenticatedURL))
	remote.SetDefaultBranch(exampleOrgName, exampleRepoName, "unborn")
	testshell.Run(t, repoPath, fmt.Sprintf("git push origin --delete %q", branch))

	// A fresh client can request the unadvertised commit by its exact object ID
	// because upload-pack is configured to serve objects already in the mirror.
	clientPath := testfs.MakeTempDir(t)
	testshell.Run(t, clientPath, "git init --quiet")
	testshell.Run(t, clientPath, fmt.Sprintf("git fetch %q %s", authenticatedURL, commitID))
	require.Equal(
		t, exampleRepoInitialContents["README.md"],
		testshell.Run(t, clientPath, "git show FETCH_HEAD:README.md"),
	)
}

func TestUploadPackFetchesMissingExactCommit(t *testing.T) {
	// Create a commit that exists upstream only under a pull-request ref. A
	// heads-and-tags-only mirror would not populate this object.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	repoPath := createExampleRepo(t, remote)
	testgit.CommitFiles(t, repoPath, map[string]string{"hidden.txt": "hidden contents"})
	commitID := strings.TrimSpace(testshell.Run(t, repoPath, "git rev-parse HEAD"))
	testshell.Run(t, repoPath, "git push origin HEAD:refs/pull/1/head")
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	mirrorRepoURL := fmt.Sprintf("%s/v1/%s%s", mirrorURL, upstreamURL.Host, upstreamURL.Path)
	authenticatedURL, err := git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
	require.NoError(t, err)

	// Request the known object ID through the mirror.
	clientPath := testfs.MakeTempDir(t)
	testshell.Run(t, clientPath, "git init --quiet")
	testshell.Run(t, clientPath, fmt.Sprintf("git fetch %q %s", authenticatedURL, commitID))

	// The fetch should succeed because the broad refspec populates objects that
	// are reachable only through the provider-specific pull-request ref.
	require.Equal(
		t, "hidden contents",
		testshell.Run(t, clientPath, "git show FETCH_HEAD:hidden.txt"),
	)
}

func TestUploadPackRejectsUnreferencedExactCommit(t *testing.T) {
	// Push a commit under a temporary ref, then delete the ref so its objects
	// remain upstream but upload-pack no longer advertises or permits the commit.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	repoPath := createExampleRepo(t, remote)
	testgit.CommitFiles(t, repoPath, map[string]string{"unreferenced.txt": "contents"})
	commitID := strings.TrimSpace(testshell.Run(t, repoPath, "git rev-parse HEAD"))
	testshell.Run(t, repoPath, "git push origin HEAD:refs/temporary/unreferenced")
	testshell.Run(t, repoPath, "git push origin :refs/temporary/unreferenced")
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	mirrorRepoURL := fmt.Sprintf("%s/v1/%s%s", mirrorURL, upstreamURL.Host, upstreamURL.Path)
	authenticatedURL, err := git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
	require.NoError(t, err)

	// Ask the mirror for the exact object ID after no upstream ref reaches it.
	clientPath := testfs.MakeTempDir(t)
	testshell.Run(t, clientPath, "git init --quiet")
	cmd := exec.CommandContext(t.Context(), "git", "fetch", authenticatedURL, commitID)
	cmd.Dir = clientPath
	output, err := cmd.CombinedOutput()

	// The fetch should fail with "not our ref" because the mirror did not cache
	// the commit before its last upstream ref was deleted.
	require.Error(t, err)
	require.Contains(t, string(output), "not our ref")
}

func TestUploadPackShallowClone(t *testing.T) {
	// Configure a private upstream repository with multiple commits so a
	// depth-one clone has history to omit.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	repoPath := createExampleRepo(t, remote)
	testgit.CommitFiles(t, repoPath, map[string]string{"latest.txt": "latest contents"})
	testshell.Run(t, repoPath, "git push")
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	mirrorRepoURL := fmt.Sprintf("%s/v1/%s%s", mirrorURL, upstreamURL.Host, upstreamURL.Path)
	authenticatedURL, err := git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
	require.NoError(t, err)

	// Request only the latest commit through a depth-one clone.
	clonePath := testfs.MakeTempDir(t)
	testshell.Run(t, clonePath, fmt.Sprintf("git clone --depth=1 %q .", authenticatedURL))

	// The checkout should contain the latest files while Git records that the
	// parent history was intentionally omitted.
	require.Equal(t, "latest contents", testfs.ReadFileAsString(t, clonePath, "latest.txt"))
	require.Equal(t, "true", strings.TrimSpace(
		testshell.Run(t, clonePath, "git rev-parse --is-shallow-repository"),
	))
	require.Equal(t, "1", strings.TrimSpace(
		testshell.Run(t, clonePath, "git rev-list --count HEAD"),
	))
}

func TestUploadPackRejectsMalformedRequest(t *testing.T) {
	// Configure an authenticated upstream repository, then start an empty
	// mirror that will initialize and refresh it before invoking upload-pack.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	createExampleRepo(t, remote)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})
	mirrorURL, mirrorRootDir := startMirror(t, clockwork.NewFakeClock())
	uploadPackURL := fmt.Sprintf(
		"%s/v1/%s%s/git-upload-pack", mirrorURL, upstreamURL.Host, upstreamURL.Path,
	)

	// Send bytes that cannot be decoded as Git's pkt-line protocol.
	request, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, uploadPackURL, strings.NewReader("not a packet line"),
	)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	request.SetBasicAuth("buildbuddy", exampleAccessToken)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	responseBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	// Upload-pack rejects the invalid request without exposing its command
	// error, stderr, or the mirror's local repository path.
	require.Equal(t, http.StatusInternalServerError, response.StatusCode)
	require.Equal(t, "failed to serve repository\n", string(responseBody))
	require.NotContains(t, string(responseBody), mirrorRootDir)
	require.NotContains(t, string(responseBody), "upload-pack")
}

func TestUploadPackConcurrentRefreshPreservesReadAfterWrite(t *testing.T) {
	// Configure a private upstream and pause the first pack request made by the
	// mirror so another client can arrive while its refresh is in progress.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	repoPath := createExampleRepo(t, remote)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	upstreamOrigin := &url.URL{Scheme: upstreamURL.Scheme, Host: upstreamURL.Host}
	upstreamProxy := httputil.NewSingleHostReverseProxy(upstreamOrigin)
	firstFetchStarted := make(chan struct{})
	releaseFirstFetch := make(chan struct{}, 1)
	var blockedFirstFetch atomic.Bool
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/git-upload-pack") &&
			blockedFirstFetch.CompareAndSwap(false, true) {
			close(firstFetchStarted)
			<-releaseFirstFetch
		}
		upstreamProxy.ServeHTTP(w, req)
	}))
	t.Cleanup(proxyServer.Close)
	t.Cleanup(func() {
		select {
		case releaseFirstFetch <- struct{}{}:
		default:
		}
	})
	proxyURL, err := url.Parse(proxyServer.URL)
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{proxyURL.Host})
	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	startClone := func(mirrorOrigin, clonePath string) <-chan error {
		result := make(chan error, 1)
		go func() {
			mirrorRepoURL := fmt.Sprintf(
				"%s/v1/%s%s", mirrorOrigin, proxyURL.Host, upstreamURL.Path,
			)
			authenticatedURL, err := git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
			if err != nil {
				result <- err
				return
			}
			cmd := exec.CommandContext(t.Context(), "git", "clone", authenticatedURL, ".")
			cmd.Dir = clonePath
			if output, err := cmd.CombinedOutput(); err != nil {
				result <- fmt.Errorf("git clone: %w: %q", err, output)
				return
			}
			result <- nil
		}()
		return result
	}

	// Start the first clone and push a commit while its refresh is paused.
	firstClonePath := testfs.MakeTempDir(t)
	firstCloneResult := startClone(mirrorURL, firstClonePath)
	<-firstFetchStarted
	updatedContents := map[string]string{"README.md": "# Written during the first refresh"}
	testgit.CommitFiles(t, repoPath, updatedContents)
	testshell.Run(t, repoPath, "git push")

	// Start another clone and wait until its request reaches the mirror before
	// allowing the older refresh to finish.
	mirrorBackend, err := url.Parse(mirrorURL)
	require.NoError(t, err)
	secondRequestStarted := make(chan struct{}, 1)
	mirrorProxy := httputil.NewSingleHostReverseProxy(mirrorBackend)
	secondClientProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		select {
		case secondRequestStarted <- struct{}{}:
		default:
		}
		mirrorProxy.ServeHTTP(w, req)
	}))
	t.Cleanup(secondClientProxy.Close)
	secondClonePath := testfs.MakeTempDir(t)
	secondCloneResult := startClone(secondClientProxy.URL, secondClonePath)
	<-secondRequestStarted
	releaseFirstFetch <- struct{}{}

	// Both clones should complete without ref-lock failures. The later clone
	// must include the completed write because its refresh began afterward.
	require.NoError(t, <-firstCloneResult)
	require.NoError(t, <-secondCloneResult)
	require.Equal(
		t, updatedContents["README.md"],
		testfs.ReadFileAsString(t, secondClonePath, "README.md"),
	)
}

func TestPullThroughMirrorPreservesDefaultBranch(t *testing.T) {
	// Configure a private upstream whose default branch is main rather than the
	// default branch selected when the test repository was initialized.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	repoPath := createExampleRepo(t, remote)
	testshell.Run(t, repoPath, "git branch -M main && git push --set-upstream origin main")
	remote.SetDefaultBranch(exampleOrgName, exampleRepoName, "main")
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})

	mirrorURL, _ := startMirror(t, clockwork.NewFakeClock())
	mirrorRepoURL := fmt.Sprintf("%s/v1/%s%s", mirrorURL, upstreamURL.Host, upstreamURL.Path)
	mirrorRepoURL, err = git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
	require.NoError(t, err)

	// Clone through the mirror without naming a branch explicitly.
	clonePath := testfs.MakeTempDir(t)
	testshell.Run(t, clonePath, fmt.Sprintf("git clone %q .", mirrorRepoURL))

	// A clone through the mirror should check out the branch selected by the
	// upstream repository's symbolic HEAD.
	require.Equal(t, "main", testgit.CurrentBranch(t, clonePath))
}

func TestPullThroughMirrorAcrossBackends(t *testing.T) {
	// Create a private upstream repository that the client will clone through
	// two independent mirror backends.
	remote := testgit.StartServer(t, testgit.ServerOptions{
		OwnerAccessTokens: map[string][]string{exampleOrgName: {exampleAccessToken}},
	})
	createExampleRepo(t, remote)
	upstreamURL, err := url.Parse(remote.RepoURL(exampleOrgName, exampleRepoName, ""))
	require.NoError(t, err)
	flags.Set(t, "git.mirror.insecure_http_hosts", []string{upstreamURL.Host})

	clock := clockwork.NewFakeClock()
	refsMirrorURL, _ := startMirror(t, clock)
	uploadPackMirrorURL, _ := startMirror(t, clock)
	refsBackend, err := url.Parse(refsMirrorURL)
	require.NoError(t, err)
	uploadPackBackend, err := url.Parse(uploadPackMirrorURL)
	require.NoError(t, err)

	// Run a proxy that routes discovery requests
	// (/info/refs?service=git-upload-pack) to one backend, and all other
	// requests (including /git-upload-pack) to the other backend.
	proxy := testhttp.StartProxy(t, func(req *http.Request) *url.URL {
		if req.Method == http.MethodGet &&
			strings.HasSuffix(req.URL.Path, "/info/refs") {
			return refsBackend
		}
		return uploadPackBackend
	})

	// A smart HTTP clone may send each request to a different backend. The
	// upload-pack backend must not depend on the discovery request having
	// populated its local repository.
	mirrorRepoURL := fmt.Sprintf(
		"%s/v1/%s%s", proxy.URL, upstreamURL.Host, upstreamURL.Path,
	)
	mirrorRepoURL, err = git.AuthRepoURL(mirrorRepoURL, "", exampleAccessToken)
	require.NoError(t, err)
	clonePath := testfs.MakeTempDir(t)
	testshell.Run(t, clonePath, fmt.Sprintf("git clone %q .", mirrorRepoURL))
	require.Equal(
		t, exampleRepoInitialContents["README.md"],
		testfs.ReadFileAsString(t, clonePath, "README.md"),
	)
}
