package build_buddy_url

import (
	"net/url"
	"strings"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/urlutil"
)

var buildBuddyURL = flagtypes.URLFromString("app.build_buddy_url", "http://localhost:8080", "The external URL where your BuildBuddy instance can be found.")

func WithPath(path string) *url.URL {
	return buildBuddyURL.ResolveReference(&url.URL{Path: path})
}

func String() string {
	return buildBuddyURL.String()
}

// ValidateRedirectURL ensures that the provided redirectURL exists on this
// server, otherwise an error is returned.
func ValidateRedirect(redirectURL string) error {
	redir, err := url.Parse(redirectURL)
	if err != nil {
		return err
	}
	if redir.Hostname() == "" {
		return nil
	}

	myDomain := Domain()
	if redir.Hostname() != myDomain && !strings.HasSuffix(redir.Hostname(), "."+myDomain) {
		return status.InvalidArgumentErrorf("Redirect url %q not found on this domain %q", redirectURL, myDomain)
	}
	return nil
}

// Domain returns the domain portion of the BuildBuddy URL.
// e.g. If the URL is "app.buildbuddy.io", the returned domain will be
// "buildbuddy.io".
func Domain() string {
	return urlutil.GetDomain(buildBuddyURL.Hostname())
}
