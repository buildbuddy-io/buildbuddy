package git_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
)

func TestAuthRepoURL(t *testing.T) {
	for _, test := range []struct {
		url      string
		user     string
		token    string
		expected string
	}{
		{"https://github.com/foo/bar", "", "TOKEN", "https://buildbuddy:TOKEN@github.com/foo/bar"},
		{"https://gitlab.com/foo/bar", "", "TOKEN", "https://buildbuddy:TOKEN@gitlab.com/foo/bar"},
		{"https://bitbucket.org/foo/bar", "USER", "TOKEN", "https://USER:TOKEN@bitbucket.org/foo/bar"},
		{"https://github.com/foo-public/bar", "", "", "https://github.com/foo-public/bar"},
	} {
		authURL, err := gitutil.AuthRepoURL(test.url, test.user, test.token)

		assert.NoError(t, err)
		assert.Equal(t, test.expected, authURL)
	}

	authURL, err := gitutil.AuthRepoURL(" http @://INVALID_URL", "USER", "TOKEN")
	assert.Empty(t, authURL)
	assert.Error(t, err)
}
