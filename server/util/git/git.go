package git

import (
	"log"
	"net/url"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/whilp/git-urls"
)

const (
	// DefaultUser is the default user set in a repo URL when the username is not
	// known.
	DefaultUser = "buildbuddy"
)

var (
	startsWithDomainRegexp    = regexp.MustCompile(`^[^/]+\.[^/]+`)
	startsWithLocalhostRegexp = regexp.MustCompile(`^localhost(:|/|$)`)
)

// AuthRepoURL returns a Git repo URL with the given credentials set. The
// returned URL can be used as a git remote.
//
// The returned URL accounts for various Git provider quirks, so that if all
// necessary credentials are provided, the returned URL will allow accessing
// the repo. Note that if the credentials are invalid, this function does not
// return an error (other parts of the system are responsible for that).
func AuthRepoURL(repoURL, user, token string) (string, error) {
	if user == "" && token == "" {
		return repoURL, nil
	}
	u, err := parse(repoURL)
	if err != nil {
		return "", status.InvalidArgumentErrorf("invalid repo URL %q", repoURL)
	}
	if user == "" {
		// GitHub allows using only a token for auth, but a bogus (non-empty)
		// username is required. GitLab does not have this requirement, and they
		// simply ignore the username if it is set. Bitbucket *always* requires a
		// username, but we don't handle Bitbucket specially and just let auth fail
		// when querying the remote - the default username set here will probably be
		// incorrect, but it yields the same error as an empty username.
		user = DefaultUser
	}
	u.User = url.UserPassword(user, token)
	return u.String(), nil
}

func StripRepoURLCredentials(repoURL string) string {
	u, err := parse(repoURL)
	if err != nil {
		log.Printf("Failed to parse repo URL while attempting to strip credentials.")
		return repoURL
	}
	u.User = nil
	return u.String()
}

func OwnerRepoFromRepoURL(repoURL string) (string, error) {
	u, err := parse(repoURL)
	if err != nil {
		return "", status.WrapErrorf(err, "failed to parse repo URL %q", repoURL)
	}
	path := u.Path
	path = strings.TrimSuffix(path, ".git")
	path = strings.TrimPrefix(path, "/")
	return path, nil
}

func parse(repoURL string) (*url.URL, error) {
	// The giturls package covers most edge cases, but it's a bit unforgiving if
	// the URL either doesn't look like "git@" or if it fails to specify an
	// explicit protocol. Here, we attempt to salvage the situation if the URL
	// looks like a domain without a protocol -- we prepend https:// except
	// for localhost, which in most cases uses http:// since most people forgo
	// the hassle of setting up HTTPS locally.

	if startsWithLocalhostRegexp.MatchString(repoURL) {
		repoURL = "http://" + repoURL
	} else if !(strings.Contains(repoURL, "@") || strings.Contains(repoURL, "//")) && startsWithDomainRegexp.MatchString(repoURL) {
		repoURL = "https://" + repoURL
	}
	return giturls.Parse(repoURL)
}
