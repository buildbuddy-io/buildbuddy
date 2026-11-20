package remotebazel

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNetrc(t *testing.T) {
	contents := `
# comment
machine github.com login gh-user password gh-token
machine bitbucket.org
  login bb-user
  account ignored
  password bb-token

macdef init
machine bitbucket.org login macro-user password macro-token

default login default-user password default-token
`
	for _, tc := range []struct {
		host string
		want *netrcCredentials
	}{
		{"github.com", &netrcCredentials{login: "gh-user", password: "gh-token"}},
		{"bitbucket.org", &netrcCredentials{login: "bb-user", password: "bb-token"}},
		{"gitlab.com", &netrcCredentials{login: "default-user", password: "default-token"}},
	} {
		t.Run(tc.host, func(t *testing.T) {
			require.Equal(t, tc.want, parseNetrc(contents, tc.host))
		})
	}
}

func TestParseNetrc_NoMatch(t *testing.T) {
	require.Nil(t, parseNetrc("machine github.com login u password p", "bitbucket.org"))
}

func TestLocalRepoCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "netrc")
	err := os.WriteFile(path, []byte("machine bitbucket.org login bb-user password bb-token\n"), 0600)
	require.NoError(t, err)
	t.Setenv("NETRC", path)

	user, token, err := localRepoCredentials("git@bitbucket.org:org/repo.git", map[string]string{})
	require.NoError(t, err)
	require.Equal(t, "bb-user", user)
	require.Equal(t, "bb-token", token)

	// Credentials explicitly set with --env take precedence.
	user, token, err = localRepoCredentials("https://bitbucket.org/org/repo", map[string]string{"REPO_TOKEN": "env-token"})
	require.NoError(t, err)
	require.Empty(t, user)
	require.Empty(t, token)

	// No entry for the host.
	user, token, err = localRepoCredentials("https://github.com/org/repo", map[string]string{})
	require.NoError(t, err)
	require.Empty(t, user)
	require.Empty(t, token)
}

func TestSecretEnvOverridesHeader(t *testing.T) {
	header := secretEnvOverridesHeader(map[string]string{
		"REPO_USER":  "",
		"REPO_TOKEN": "tok,en",
	})
	require.Equal(t, "x-buildbuddy-platform.secret-env-overrides-base64="+base64.StdEncoding.EncodeToString([]byte("REPO_TOKEN=tok,en")), header)
}
