package gitremote_test

import (
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror/gitremote"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestResolve(t *testing.T) {
	// Start a test git server and create an empty repo
	// example-org/example-repo.
	remote := testgit.StartServer(t, testgit.ServerOptions{})
	remote.CreateProject("example-org", "example-repo", &testgit.ProjectSettings{Public: false})
	remote.CreateProject("example-org", "public-repo", &testgit.ProjectSettings{Public: true})
	targetURL, err := url.Parse(remote.RepoURL("example-org", "example-repo", ""))
	require.NoError(t, err)
	publicURL, err := url.Parse(remote.RepoURL("example-org", "public-repo", ""))
	require.NoError(t, err)

	// Redirect several "legacy" repo paths to the private repo, varying the
	// status and freshness headers so each cache policy can be exercised. This
	// is testing a real scenario - when renaming a GitHub repository for
	// example, GitHub redirects from the old repo to the new repo.
	var redirectCount atomic.Int64
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		redirectCount.Add(1)
		statusCode := http.StatusFound
		switch {
		case strings.HasPrefix(req.URL.Path, "/cacheable/"):
			w.Header().Set("Cache-Control", "max-age=3600")
		case strings.HasPrefix(req.URL.Path, "/short-lived/"):
			w.Header().Set("Cache-Control", "max-age=60")
		case strings.HasPrefix(req.URL.Path, "/long-lived/"):
			w.Header().Set("Cache-Control", "max-age=86400")
		case strings.HasPrefix(req.URL.Path, "/permanent/"):
			statusCode = http.StatusMovedPermanently
		}
		redirectTarget := *targetURL
		redirectTarget.Path += "/info/refs"
		redirectTarget.RawQuery = req.URL.RawQuery
		http.Redirect(w, req, redirectTarget.String(), statusCode)
	}))
	t.Cleanup(redirectServer.Close)
	redirectURL, err := url.Parse(redirectServer.URL)
	require.NoError(t, err)
	// httptest advertises 127.0.0.1, while testgit uses localhost. Use the same
	// hostname on both sides of the redirect so Go preserves Authorization.
	redirectURL.Host = net.JoinHostPort("localhost", redirectURL.Port())

	_, loopbackNet, err := net.ParseCIDR("127.0.0.0/8")
	require.NoError(t, err)

	authRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	authRequest.SetBasicAuth("x-access-token", remote.AccessToken())
	authorization := authRequest.Header.Get("Authorization")
	wantRepo, err := gitremote.RestoreRepo(targetURL.String())
	require.NoError(t, err)
	wantPublicRepo, err := gitremote.RestoreRepo(publicURL.String())
	require.NoError(t, err)
	redirectHost := strings.TrimPrefix(redirectURL.String(), "http://")

	// Test various URL resolutions.
	for _, testCase := range []struct {
		name          string
		repoHostPath  string
		authorization string
		wantRepo      *gitremote.Repo
		wantError     error
		wantRedirects int64
		advance       time.Duration
	}{
		{
			name:          "fresh temporary redirect is cached",
			repoHostPath:  redirectHost + "/cacheable/repo",
			authorization: authorization,
			wantRepo:      wantRepo,
			wantRedirects: 1,
		},
		{
			name:          "bare temporary redirect is not cached",
			repoHostPath:  redirectHost + "/temporary/repo",
			authorization: authorization,
			wantRepo:      wantRepo,
			wantRedirects: 2,
		},
		{
			name:          "permanent redirect uses heuristic lifetime",
			repoHostPath:  redirectHost + "/permanent/repo",
			authorization: authorization,
			wantRepo:      wantRepo,
			wantRedirects: 1,
		},
		{
			name:          "upstream freshness expiration is honored",
			repoHostPath:  redirectHost + "/short-lived/repo",
			authorization: authorization,
			wantRepo:      wantRepo,
			wantRedirects: 2,
			advance:       2 * time.Minute,
		},
		{
			name:          "configured maximum caps upstream freshness",
			repoHostPath:  redirectHost + "/long-lived/repo",
			authorization: authorization,
			wantRepo:      wantRepo,
			wantRedirects: 2,
			advance:       2 * time.Hour,
		},
		{
			name:          "failed probe does not cache redirect",
			repoHostPath:  redirectHost + "/cacheable/repo",
			wantError:     &gitremote.HTTPError{StatusCode: http.StatusUnauthorized},
			wantRedirects: 2,
		},
		{
			name:          "resolves canonical URL to itself",
			repoHostPath:  targetURL.Host + targetURL.Path,
			authorization: authorization,
			wantRepo:      wantRepo,
		},
		{
			name:         "resolves public repo without authorization",
			repoHostPath: publicURL.Host + publicURL.Path,
			wantRepo:     wantPublicRepo,
		},
		{
			name:          "returns not found error for missing repo",
			repoHostPath:  targetURL.Host + "/example-org/missing-repo",
			authorization: authorization,
			wantError:     &gitremote.HTTPError{StatusCode: http.StatusNotFound},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			clock := clockwork.NewFakeClock()
			client, err := gitremote.NewClient(gitremote.ClientOptions{
				Clock:                 clock,
				AuthorizationCacheTTL: 0,
				RedirectCacheTTL:      time.Hour,
				AllowedPrivateIPNets:  []*net.IPNet{loopbackNet},
				InsecureHTTPHosts:     []string{redirectURL.Host, targetURL.Host},
			})
			require.NoError(t, err)
			redirectCount.Store(0)
			// Resolve twice and count redirect handler calls to determine whether
			// the second probe reused the cached target.
			for i := range 2 {
				if i == 1 {
					clock.Advance(testCase.advance)
				}
				repo, err := client.Resolve(
					t.Context(),
					testCase.repoHostPath,
					testCase.authorization,
				)
				if testCase.wantError != nil {
					require.Nil(t, repo)
					require.EqualError(t, err, testCase.wantError.Error())
					continue
				}
				require.NoError(t, err)
				require.Equal(t, testCase.wantRepo.String(), repo.String())
				gitFlags, err := repo.GitFlags()
				require.NoError(t, err)
				require.Equal(t, []string{
					"-c", "http.followRedirects=false",
					"-c", "http.curloptResolve=",
					"-c", "http.curloptResolve=" +
						targetURL.Hostname() + ":" + targetURL.Port() + ":127.0.0.1",
				}, gitFlags)
			}
			require.Equal(t, testCase.wantRedirects, redirectCount.Load())
		})
	}
}

func TestResolvePreservesRepositoryPath(t *testing.T) {
	// Configure an upstream that records the exact path used by each resolver
	// probe.
	paths := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		paths <- req.URL.EscapedPath()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	_, loopbackNet, err := net.ParseCIDR("127.0.0.0/8")
	require.NoError(t, err)
	client, err := gitremote.NewClient(gitremote.ClientOptions{
		Clock:                 clockwork.NewFakeClock(),
		AuthorizationCacheTTL: 0,
		AllowedPrivateIPNets:  []*net.IPNet{loopbackNet},
		InsecureHTTPHosts:     []string{upstreamURL.Host},
	})
	require.NoError(t, err)
	testCases := []struct {
		name             string
		repositoryPath   string
		wantEscapedProbe string
	}{
		{
			name:             "dot segment reaches upstream",
			repositoryPath:   "/org/./repo",
			wantEscapedProbe: "/org/./repo/info/refs",
		},
		{
			name:             "empty segment reaches upstream",
			repositoryPath:   "/org//repo",
			wantEscapedProbe: "/org//repo/info/refs",
		},
		{
			name:             "backslash is URL-escaped for upstream",
			repositoryPath:   `/org\repo`,
			wantEscapedProbe: "/org%5Crepo/info/refs",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Resolve the repository path through the normal upstream probe.
			repo, err := client.Resolve(
				t.Context(), upstreamURL.Host+testCase.repositoryPath, "",
			)
			require.NoError(t, err)

			// The resolver should append /info/refs without cleaning any part of
			// the repository path, and retain that path in the repository URL.
			require.Equal(t, testCase.wantEscapedProbe, <-paths)
			require.Equal(t, testCase.repositoryPath, repo.URL().Path)
		})
	}
}

func TestResolveCachesSuccessfulAuthorization(t *testing.T) {
	var requestCount atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requestCount.Add(1)
		if req.Header.Get("Authorization") != "Bearer valid-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	_, loopbackNet, err := net.ParseCIDR("127.0.0.0/8")
	require.NoError(t, err)
	clock := clockwork.NewFakeClock()
	client, err := gitremote.NewClient(gitremote.ClientOptions{
		Clock:                 clock,
		AuthorizationCacheTTL: time.Minute,
		AllowedPrivateIPNets:  []*net.IPNet{loopbackNet},
		InsecureHTTPHosts:     []string{upstreamURL.Host},
	})
	require.NoError(t, err)
	repoHostPath := upstreamURL.Host + "/example-org/example-repo"

	// Repeated resolutions with credentials accepted by the upstream reuse the
	// successful authorization check.
	for range 2 {
		repo, err := client.Resolve(t.Context(), repoHostPath, "Bearer valid-token")
		require.NoError(t, err)
		require.NotNil(t, repo)
		_, err = repo.GitFlags()
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), requestCount.Load())

	// Different credentials cannot reuse another credential's successful
	// authorization check.
	repo, err := client.Resolve(t.Context(), repoHostPath, "Bearer invalid-token")
	require.Nil(t, repo)
	require.EqualError(t, err, "upstream returned HTTP 401")
	require.Equal(t, int64(2), requestCount.Load())

	// The failed check does not disturb the cached successful check for the
	// valid credentials.
	repo, err = client.Resolve(t.Context(), repoHostPath, "Bearer valid-token")
	require.NoError(t, err)
	require.NotNil(t, repo)
	require.Equal(t, int64(2), requestCount.Load())

	// Once the successful check expires, the next resolution validates the
	// credentials with the upstream again.
	clock.Advance(2 * time.Minute)
	repo, err = client.Resolve(t.Context(), repoHostPath, "Bearer valid-token")
	require.NoError(t, err)
	require.NotNil(t, repo)
	require.Equal(t, int64(3), requestCount.Load())
}

func TestRestoreRepoNormalizesURL(t *testing.T) {
	repo, err := gitremote.RestoreRepo("HTTPS://GitHub.COM/buildbuddy-io/buildbuddy")
	require.NoError(t, err)
	require.Equal(t, "https://github.com:443/buildbuddy-io/buildbuddy", repo.String())
	_, err = repo.GitFlags()
	require.EqualError(t, err, "repo URL has no validated IP addresses")
}
