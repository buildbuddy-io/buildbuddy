package gitserver_test

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/gitserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGitServerPushAndPull(t *testing.T) {
	for _, test := range []struct {
		name         string
		repoIsPublic bool
		repoTokens   []string
		userToken    string

		wantCloneError string
		wantPushError  string
	}{
		{
			name:          "can pull public repo as unauthenticated user",
			repoIsPublic:  true,
			wantPushError: "remote: Unauthorized",
		},
		{
			name:          "can pull but not push public repo with invalid access token",
			repoIsPublic:  true,
			userToken:     "invalid-test-token",
			wantPushError: "remote: Unauthorized",
		},
		{
			name:         "can pull and push public repo if authenticated as repo owner",
			repoIsPublic: true,
			userToken:    "test-token",
		},
		{
			name:           "cannot pull private repo as unauthenticated user",
			repoIsPublic:   false,
			wantCloneError: "remote: Unauthorized",
		},
		{
			name:           "cannot pull private repo with invalid access token",
			repoIsPublic:   false,
			userToken:      "invalid-test-token",
			wantCloneError: "remote: Unauthorized",
		},
		{
			name:         "can pull and push private repo if authenticated as repo owner",
			repoIsPublic: false,
			userToken:    "test-token",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// For the purposes of this test, set GIT_ASKPASS to a no-op
			// executable to prevent "No such device or address" errors, and
			// instead just tell git to send empty credentials to the server
			// when the server asks for them.
			t.Setenv("GIT_ASKPASS", "/usr/bin/true")

			remoteRoot := testfs.MakeTempDir(t)

			// Provision a bare repository for testorg/testrepo.
			err := gitserver.CreateProject(remoteRoot, "testorg", "testrepo", &gitserver.ProjectSettings{
				Public: test.repoIsPublic,
			})
			require.NoError(t, err)

			// Serve the repository.
			server := httptest.NewServer(gitserver.NewHandler(remoteRoot, gitserver.Options{
				AccessToken: "test-token",
			}))
			t.Cleanup(server.Close)

			// Try cloning. Note, we need to add the server's TLS certificate,
			// otherwise we cannot use auth.
			remoteURL := server.URL
			if test.userToken != "" {
				u, err := url.Parse(server.URL)
				require.NoError(t, err)
				u.User = url.UserPassword("x-token", test.userToken)
				remoteURL = u.String()
			}
			localRoot := testfs.MakeTempDir(t)
			_, stderr, err := testshell.Try(t, localRoot, `
				git clone `+remoteURL+`/testorg/testrepo clonedir
			`)
			if test.wantCloneError != "" {
				assert.Contains(t, stderr, test.wantCloneError)
				require.Error(t, err, "stderr: %s", stderr)
				return
			} else {
				require.NoError(t, err, "stderr: %s", stderr)
			}

			// Try pushing a commit.
			_, stderr, err = testshell.Try(t, localRoot, `
				cd clonedir
				git config user.name 'Test'
				git config user.email test@test.com
				echo "test-readme-content" > README
				git add .
				git commit -m "Initial commit"
				git push
			`)
			if test.wantPushError != "" {
				assert.Contains(t, stderr, test.wantPushError)
				require.Error(t, err, "stderr: %s", stderr)
				return
			} else {
				require.NoError(t, err, "stderr: %s", stderr)
			}

			// If we got this far, we can both pull and push. Try cloning again
			// and make sure the clone contains our new file.
			stdout, stderr, err := testshell.Try(t, localRoot, `
				mkdir clonedir2
				cd clonedir2
				git init >&2
				cp ../clonedir/.git/config .git/config
				git pull
				cat README
			`)
			assert.Equal(t, string("test-readme-content\n"), stdout)
			require.NoError(t, err, "stderr: %s", stderr)
		})
	}
}
