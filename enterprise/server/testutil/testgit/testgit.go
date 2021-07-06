package testgit

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
)

const (
	// FakeWebhookID is the fake webhook ID returned by the provider when
	// creating a webhook.
	FakeWebhookID = "fake-webhook-id"
)

// FakeProvider implements the git provider interface for tests.
type FakeProvider struct {
	RegisteredWebhookURL  string
	UnregisteredWebhookID string
}

func NewFakeProvider() *FakeProvider {
	return &FakeProvider{}
}

func (p *FakeProvider) MatchRepoURL(u *url.URL) bool {
	return true
}
func (p *FakeProvider) MatchWebhookRequest(req *http.Request) bool {
	return true
}
func (p *FakeProvider) ParseWebhookData(req *http.Request) (*interfaces.WebhookData, error) {
	return nil, status.UnimplementedError("Not implemented")
}
func (p *FakeProvider) RegisterWebhook(ctx context.Context, accessToken, repoURL, webhookURL string) (string, error) {
	p.RegisteredWebhookURL = webhookURL
	return FakeWebhookID, nil
}
func (p *FakeProvider) UnregisterWebhook(ctx context.Context, accessToken, repoURL, webhookID string) error {
	p.UnregisteredWebhookID = webhookID
	return nil
}

func bash(t testing.TB, workDir string, script string) string {
	cmd := exec.Command("/usr/bin/env", "bash", "-e", "-c", script)
	cmd.Dir = workDir
	b, err := cmd.CombinedOutput()
	require.NoError(t, err, "script %q failed: %s", script, string(b))
	return string(b)
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
	bash(t, path, `
		git init
		git add .
		git config --local user.name "Test"
		git config --local user.email "test@buildbuddy.io"
		git commit -m "Initial commit"
	`)
	headCommitSHA := strings.TrimSpace(bash(t, path, `git rev-parse HEAD`))
	return path, headCommitSHA
}
