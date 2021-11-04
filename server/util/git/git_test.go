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
		{" \r\n\t https://USER:PASS@github.com/org/repo.git\r\n\t  ", "https://github.com/org/repo.git"},
		{"https://USER:PASS@github.com/org/repo.git", "https://github.com/org/repo.git"},
		{"https://PASS:@github.com/org/repo.git", "https://github.com/org/repo.git"},
		{"https://:PASS@gitlab.com/org/repo", "https://gitlab.com/org/repo"},
		{"http://USER:PASS@github.com/org/repo.git", "http://github.com/org/repo.git"},
		{"ssh://USER:PASS@github.com/org/repo.git", "ssh://github.com/org/repo.git"},
		{"git@github.com:org/repo.git", "ssh://github.com/org/repo.git"},
		{"github.com/org/repo.git", "https://github.com/org/repo.git"},
		{"bitbucket.org/org/repo", "https://bitbucket.org/org/repo"},
		{"gitlab.com/org/repo", "https://gitlab.com/org/repo"},
		{"10.3.1.5/foo/bar.git", "https://10.3.1.5/foo/bar.git"},
		{"localhost:8888/foo/bar.git", "http://localhost:8888/foo/bar.git"},
		{"/home/user/local-repo", "file:///home/user/local-repo"},
		{"unknown", "https://unknown"},
	} {
		str := gitutil.StripRepoURLCredentials(testCase.url)

		assert.Equal(t, testCase.expected, str)
	}
}

func TestOwnerRepoFromRepoURL(t *testing.T) {
	for _, url := range []string{
		"https://github.com/org/repo.git",
		"https://USER:PASS@github.com/org/repo.git",
		"  https://USER:PASS@github.com/org/repo.git \r\n",
		"https://PASS:@github.com/org/repo.git",
		"https://:PASS@gitlab.com/org/repo",
		"http://USER:PASS@github.com/org/repo.git",
		"ssh://USER:PASS@github.com/org/repo.git",
		"git@github.com:org/repo.git",
		"github.com/org/repo.git",
		"bitbucket.org/org/repo",
		"gitlab.com/org/repo",
	} {
		ownerRepo, err := gitutil.OwnerRepoFromRepoURL(url)

		assert.NoError(t, err)
		assert.Equal(t, "org/repo", ownerRepo)
	}
}

func TestNormalizeRepoURL(t *testing.T) {
	for _, s := range []string{
		"ssh://github.com/buildbuddy-io/buildbuddy",
		"ssh://github.com/buildbuddy-io/buildbuddy.git",
		"git://github.com/buildbuddy-io/buildbuddy",
		"   git://github.com/buildbuddy-io/buildbuddy \r\n\t  ",
		"git://github.com/buildbuddy-io/buildbuddy.git",
		"git@github.com:buildbuddy-io/buildbuddy.git",
		"https://NOTREALTOKEN:@github.com/buildbuddy-io/buildbuddy",
		"https://buildbuddy:NOTREALTOKEN@github.com/buildbuddy-io/buildbuddy.git",
		"https://github.com/buildbuddy-io/buildbuddy",
		"ssh://git@github.com/buildbuddy-io/buildbuddy.git",
		"buildbuddy-io/buildbuddy",
		"file://buildbuddy-io/buildbuddy",
	} {
		url, err := gitutil.NormalizeRepoURL(s)

		assert.NoError(t, err)
		assert.Equal(t, "https://github.com/buildbuddy-io/buildbuddy", url.String())
	}
	url, err := gitutil.NormalizeRepoURL("")
	assert.NoError(t, err)
	assert.Equal(t, "", url.String())
}
