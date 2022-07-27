package git

import (
	"net"
	"net/url"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	// DefaultUser is the default user set in a repo URL when the username is not
	// known.
	DefaultUser = "buildbuddy"
)

var (
	SchemeRegexp                         = regexp.MustCompile(`^(([a-z0-9+.-]+:)?/)?/`)
	MissingSlashBetweenPortAndPathRegexp = regexp.MustCompile(`^(([a-z0-9+.-]+:)?//([0-9a-zA-Z%._~-]+(:[0-9a-zA-Z%._~-]*)?@)?[0-9a-zA-Z%._~-]+:[0-9]*[^0-9/@])[^@]*$`)
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
	u, err := ParseRepoURL(repoURL)
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
	u, err := ParseRepoURL(repoURL)
	if err != nil {
		log.Warning("Failed to parse repo URL while attempting to strip credentials.")
		return repoURL
	}
	u.User = nil
	return u.String()
}

func OwnerRepoFromRepoURL(repoURL string) (string, error) {
	u, err := NormalizeRepoURL(repoURL)
	if err != nil {
		return "", status.WrapErrorf(err, "failed to parse repo URL %q", repoURL)
	}
	return strings.TrimPrefix(u.Path, "/"), nil
}

func ParseRepoURL(repo string) (*url.URL, error) {
	repo = strings.TrimSpace(repo)
	// We assume https:// when scheme is missing for all domains except localhost,
	// which in most cases uses http:// since most people forgo the hassle of
	// setting up HTTPS locally. We assume "github.com" if no scheme or domain is
	// specified and the path is relative. We strip any trailing slash.

	if SchemeRegexp.FindStringIndex(repo) == nil {
		// convert e.g. user@host:port/path/to/repo -> //user@host:port/path/to/repo
		repo = "//" + repo
	}
	if matches := MissingSlashBetweenPortAndPathRegexp.FindStringSubmatchIndex(repo); matches != nil {
		// convert e.g. //user@host:path/to/repo -> //user@host:/path/to/repo
		repo = repo[:matches[3]-1] + "/" + repo[matches[3]-1:]
	}

	repoURL, err := url.Parse(repo)
	if err != nil {
		return nil, err
	}

	if repoURL.Path != "/" {
		repoURL.Path = strings.TrimSuffix(repoURL.Path, "/")
	}

	// convert e.g file://buildbuddy-io/buildbuddy -> buildbuddy-io/buildbuddy
	// and e.g //buildbuddy-io/buildbuddy -> buildbuddy-io/buildbuddy
	if (repoURL.Scheme == "file" || repoURL.Scheme == "") && repoURL.Host != "" && repoURL.Hostname() != "localhost" && !strings.ContainsAny(repoURL.Host, ".:") && repoURL.Path != "" && !strings.Contains(repoURL.Path[1:], "/") {
		repoURL.Scheme = ""
		repoURL.Path = repoURL.Host + repoURL.Path
		repoURL.Host = ""
	}

	if repoURL.Scheme == "" && repoURL.Host == "" && !strings.HasPrefix(repoURL.Path, "/") {
		if components := strings.Split(repoURL.Path, "/"); strings.ContainsAny(components[0], ".:") || components[0] == "localhost" {
			// convert e.g gitlab.com/buildbuddy-io/buildbuddy -> //gitlab.com/buildbuddy-io/buildbuddy
			repoURL.Host = components[0]
			repoURL.Path = repoURL.Path[len(components[0]):]
		} else if len(components) == 2 {
			// convert e.g buildbuddy-io/buildbuddy -> //github.com/buildbuddy-io/buildbuddy
			repoURL.Host = "github.com"
			repoURL.Path = "/" + repoURL.Path
		}
	}

	// strip trailing ":" on hosts that lack a port.
	host, port, err := net.SplitHostPort(repoURL.Host)
	if err == nil && port == "" {
		repoURL.Host = strings.TrimSuffix(net.JoinHostPort(host, port), ":")
	}

	// assume missing scheme
	if repoURL.String() != "" && repoURL.Scheme == "" {
		if repoURL.Hostname() == "localhost" {
			// assume http for missing localhost scheme.
			repoURL.Scheme = "http"
		} else if repoURL.Host == "" {
			// assume file for missing empty host scheme.
			repoURL.Scheme = "file"
		} else if repoURL.User.Username() != "" {
			// assume ssh for missing scheme with user specified.
			repoURL.Scheme = "ssh"
		} else {
			// assume https for all other missing scheme cases.
			repoURL.Scheme = "https"
		}
	}

	return repoURL, nil
}

func NormalizeRepoURL(repo string) (*url.URL, error) {
	repo = strings.TrimSpace(repo)
	// We coerce https scheme for all domains except localhost. We remove the user
	// info. We strip the ".git" suffix, if it exists.
	repoURL, err := ParseRepoURL(repo)
	if err != nil {
		return nil, err
	}

	// coerce https for all but localhost or known `file` schemes
	if repoURL.Scheme == "file" || (repoURL.Scheme == "" && repoURL.Hostname() == "" && repoURL.Path != "") {
		repoURL.Scheme = "file"
	} else if repoURL.Scheme != "https" && repoURL.Hostname() == "localhost" {
		repoURL.Scheme = "http"
	} else if repoURL.String() != "" {
		repoURL.Scheme = "https"
	}

	repoURL.User = nil
	repoURL.Path = strings.TrimSuffix(repoURL.Path, ".git")

	return repoURL, nil
}
