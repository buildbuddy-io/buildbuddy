package git

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"net/url"
)

const (
	// DefaultUser is the default user set in a repo URL when the username is not
	// known.
	DefaultUser = "buildbuddy"
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
	u, err := url.Parse(repoURL)
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
