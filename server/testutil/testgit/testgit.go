package testgit

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/mockgitserver"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-github/v59/github"
	"github.com/stretchr/testify/require"
)

const (
	// FakeWebhookID is the fake webhook ID returned by the provider when
	// creating a webhook.
	FakeWebhookID = "fake-webhook-id"
)

type Status struct {
	AccessToken string
	RepoURL     string
	CommitSHA   string
	Payload     any
}

// FakeProvider implements the git provider interface for tests.
type FakeProvider struct {
	// Captured values

	RegisteredWebhookURL  string
	UnregisteredWebhookID string
	Statuses              chan *Status

	// Faked values

	RegisterWebhookError error
	WebhookData          *interfaces.WebhookData
	FileContents         map[string]string
	TrustedUsers         []string
}

func NewFakeProvider() *FakeProvider {
	return &FakeProvider{
		Statuses: make(chan *Status, 1024),
	}
}

func (p *FakeProvider) MatchRepoURL(u *url.URL) bool {
	return true
}
func (p *FakeProvider) MatchWebhookRequest(req *http.Request) bool {
	return true
}
func (p *FakeProvider) ParseWebhookData(req *http.Request) (*interfaces.WebhookData, error) {
	if p.WebhookData == nil {
		return nil, status.UnimplementedError("Not implemented")
	}
	return p.WebhookData, nil
}
func (p *FakeProvider) RegisterWebhook(ctx context.Context, accessToken, repoURL, webhookURL string) (string, error) {
	if p.RegisterWebhookError != nil {
		return "", p.RegisterWebhookError
	}
	p.RegisteredWebhookURL = webhookURL
	return FakeWebhookID, nil
}
func (p *FakeProvider) UnregisterWebhook(ctx context.Context, accessToken, repoURL, webhookID string) error {
	p.UnregisteredWebhookID = webhookID
	return nil
}
func (p *FakeProvider) GetFileContents(ctx context.Context, accessToken, repoURL, filePath, ref string) ([]byte, error) {
	contents, ok := p.FileContents[filePath]
	if !ok {
		return nil, status.NotFoundError("Not found")
	}
	return []byte(contents), nil
}
func (p *FakeProvider) IsTrusted(ctx context.Context, accessToken, repoURL, user string) (bool, error) {
	for _, u := range p.TrustedUsers {
		if u == user {
			return true, nil
		}
	}
	return false, nil
}
func (p *FakeProvider) CreateStatus(ctx context.Context, accessToken, groupID, repoURL, commitSHA string, payload any) error {
	p.Statuses <- &Status{accessToken, repoURL, commitSHA, payload}
	return nil
}

// MakeTempRepo initializes a Git repository with the given file contents, and
// creates an initial commit of those files. Contents are specified as a map of
// file paths to file contents. Parent directories are created automatically.
// The repository contents are automatically deleted in the test cleanup phase.
// Returns the path to the repo and the SHA of the initial commit.
func MakeTempRepo(t testing.TB, contents map[string]string) (path, commitSHA string) {
	path = testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, path, contents)
	for fileRelPath := range contents {
		filePath := filepath.Join(path, fileRelPath)
		// Make shell scripts executable.
		if strings.HasSuffix(filePath, ".sh") {
			if err := os.Chmod(filePath, 0750); err != nil {
				t.Fatal(err)
			}
		}
	}
	Init(t, path)
	headCommitSHA := strings.TrimSpace(testshell.Run(t, path, `git rev-parse HEAD`))
	return path, headCommitSHA
}

// Init takes a directory which does not already contain a .git dir,
// and initializes the directory with an initial commit of all the existing
// files.
func Init(t testing.TB, dir string) string {
	testshell.Run(t, dir, `git -c init.defaultBranch=master init`)
	configure(t, dir)
	testshell.Run(t, dir, `git add . && git commit -m "Initial commit"`)
	return strings.TrimSpace(testshell.Run(t, dir, `git rev-parse HEAD`))
}

// CommitAll adds and commits all changes in the given git repo path,
// returning the SHA of the new commit.
func CommitAll(t testing.TB, dir, message string) string {
	testshell.Run(t, dir, fmt.Sprintf(`git add . && git commit -m %q`, message))
	commitSHA := strings.TrimSpace(testshell.Run(t, dir, "git rev-parse HEAD"))
	return commitSHA
}

func ConfigureRemoteOrigin(t testing.TB, dir, url string) {
	testshell.Run(t, dir, `git remote add origin `+url)
}

func CurrentBranch(t testing.TB, dir string) string {
	output := testshell.Run(t, dir, `git rev-parse --abbrev-ref HEAD`)
	return strings.TrimSpace(output)
}

func CurrentCommitSHA(t testing.TB, dir string) string {
	output := testshell.Run(t, dir, `git rev-parse HEAD`)
	return strings.TrimSpace(output)
}

// MakeTempRepoClone makes a clone of the git repo at the given path, and cleans
// up the copy after the test is complete.
func MakeTempRepoClone(t testing.TB, path string) string {
	copyPath := testfs.MakeTempDir(t)
	testshell.Run(t, copyPath, fmt.Sprintf(`git clone file://%q .`, path))
	configure(t, copyPath)
	return copyPath
}

// CommitFiles writes the given file contents and creates a new commit with the changes.
// Contents are specified as a map of file path to file contents.
// Returns the sha of the new commit
func CommitFiles(t testing.TB, repoPath string, contents map[string]string) string {
	testfs.WriteAllFileContents(t, repoPath, contents)
	testshell.Run(t, repoPath, `git add . && git commit -m "Initial commit"`)
	commitSHA := strings.TrimSpace(testshell.Run(t, repoPath, `git rev-parse HEAD`))
	return commitSHA
}

func configure(t testing.TB, repoPath string) {
	testshell.Run(t, repoPath, `
		git config user.name "Test"
		git config user.email "test@buildbuddy.io"
	`)
}

// Server simulates a git forge such as GitHub. It allows creating git projects
// associated with orgs. Repos can be private, requiring authorization via a
// statically configured access token (which defaults to "test-access-token").
//
// Example usage:
//
//	remote := testgit.StartServer(t, testgit.ServerOptions{})
//	repo := testgit.MakeTempRepo(t, map[string]string{"README": ""})
//	remote.CreateProject("foo-org", "bar-repo", testgit.ProjectSettings{Public: false})
//	remote.Push("foo-org", "bar-repo", "test-access-token", repo)
type Server struct {
	t              testing.TB
	gitProjectRoot string
	accessToken    string

	server *httptest.Server
}

type ServerOptions = mockgitserver.Options

type ProjectSettings = mockgitserver.ProjectSettings

// StartServer starts a git remote for testing.
// The server is automatically cleaned up when the test is complete.
func StartServer(t testing.TB, opts ServerOptions) *Server {
	if opts.AccessToken == "" {
		opts.AccessToken = "test-access-token"
	}
	gitProjectRoot := testfs.MakeTempDir(t)
	handler := mockgitserver.NewHandler(gitProjectRoot, opts)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &Server{
		t:              t,
		server:         server,
		gitProjectRoot: gitProjectRoot,
		accessToken:    opts.AccessToken,
	}
}

// RepoURL returns the URL of the given owner/repo with an optional access token
// encoded in the URL for basic auth.
//
// If the test server is bound to 127.0.0.1, RepoURL rewrites the host to
// localhost. Remote Bazel preserves plain HTTP for localhost URLs, but coerces
// HTTP 127.0.0.1 URLs to HTTPS before the remote checkout, which breaks tests
// that intentionally use this local HTTP git server without TLS.
func (s *Server) RepoURL(owner, repo, accessToken string) string {
	u, err := url.Parse(s.server.URL)
	require.NoError(s.t, err)
	if u.Scheme == "http" && u.Hostname() == "127.0.0.1" {
		u.Host = net.JoinHostPort("localhost", u.Port())
	}
	u.Path = path.Join(owner, repo)
	if accessToken != "" {
		u.User = url.UserPassword("x-testgit-access-token", accessToken)
	}
	return u.String()
}

// AccessToken returns a token that grants access to all test repositories.
func (s *Server) AccessToken() string {
	return s.accessToken
}

// CreateProject initializes a git repository on the server.
func (s *Server) CreateProject(owner, repo string, settings *ProjectSettings) {
	err := mockgitserver.CreateProject(s.gitProjectRoot, owner, repo, settings)
	require.NoError(s.t, err)
}

// Push pushes the local repo to a project created with CreateProject.
func (s *Server) Push(owner, repo, accessToken, localPath string) {
	testshell.Run(s.t, localPath, `
		git remote add origin `+s.RepoURL(owner, repo, accessToken)+`
		export GIT_ASKPASS=/usr/bin/true
		git push --set-upstream origin "$(git branch --show-current)"
	`)
}

// FakeGitHubApp implements the github app interface for tests.
type FakeGitHubApp struct {
	interfaces.GitHubApp
	Token     string
	MockAppID int64
}

func (a *FakeGitHubApp) GetRepositoryInstallationToken(ctx context.Context, groupID, repoURL string) (string, error) {
	return a.Token, nil
}

func (a *FakeGitHubApp) GetInstallationTokenForInternalUseOnly(ctx context.Context, owner string) (*github.InstallationToken, error) {
	return &github.InstallationToken{Token: &a.Token}, nil
}

func (a *FakeGitHubApp) GetDefaultBranch(ctx context.Context, repoURL string, token string) (string, error) {
	return "main", nil
}

func (a *FakeGitHubApp) AppID() int64 {
	return a.MockAppID
}
