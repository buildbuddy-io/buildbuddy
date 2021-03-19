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
		{"https://github.com/org/repo", "", "TOKEN", "https://buildbuddy:TOKEN@github.com/org/repo"},
		{"https://gitlab.com/org/repo", "", "TOKEN", "https://buildbuddy:TOKEN@gitlab.com/org/repo"},
		{"https://bitbucket.org/org/repo", "USER", "TOKEN", "https://USER:TOKEN@bitbucket.org/org/repo"},
		{"https://github.com/org-public/repo", "", "", "https://github.com/org-public/repo"},
	} {
		authURL, err := gitutil.AuthRepoURL(test.url, test.user, test.token)

		assert.NoError(t, err)
		assert.Equal(t, test.expected, authURL)
	}
}

func TestStripRepoURLCredentials(t *testing.T) {
	for _, testCase := range []struct {
		url      string
		expected string
	}{
		{"https://github.com/org/repo.git", "https://github.com/org/repo.git"},
		{"https://USER:PASS@github.com/org/repo.git", "https://github.com/org/repo.git"},
		{"https://PASS:@github.com/org/repo.git", "https://github.com/org/repo.git"},
		{"https://:PASS@gitlab.com/org/repo", "https://gitlab.com/org/repo"},
		{"http://USER:PASS@github.com/org/repo.git", "http://github.com/org/repo.git"},
		{"ssh://USER:PASS@github.com/org/repo.git", "ssh://github.com/org/repo.git"},
		{"git@github.com:org/repo.git", "ssh://github.com/org/repo.git"},
		{"INVALID", "file://INVALID"},
	} {
		str := gitutil.StripRepoURLCredentials(testCase.url)

		assert.Equal(t, testCase.expected, str)
	}
}

func TestOwnerRepoFromRepoURL(t *testing.T) {
	for _, url := range []string{
		"https://github.com/org/repo.git",
		"https://USER:PASS@github.com/org/repo.git",
		"https://PASS:@github.com/org/repo.git",
		"https://:PASS@gitlab.com/org/repo",
		"http://USER:PASS@github.com/org/repo.git",
		"ssh://USER:PASS@github.com/org/repo.git",
		"git@github.com:org/repo.git",
	} {
		ownerRepo := gitutil.OwnerRepoFromRepoURL(url)

		assert.Equal(t, "org/repo", ownerRepo)
	}
}
