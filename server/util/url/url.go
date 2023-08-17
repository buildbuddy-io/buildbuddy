package url

import (
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func SameHostname(urlStringA, urlStringB string) bool {
	if urlA, err := url.Parse(urlStringA); err == nil {
		if urlB, err := url.Parse(urlStringB); err == nil {
			return urlA.Hostname() == urlB.Hostname()
		}
	}
	return false
}

// ValidateRedirectURL ensures that the provided redirectURL exists on this
// server, otherwise an error is returned.
func ValidateRedirect(env environment.Env, redirectURL string) error {
	redir, err := url.Parse(redirectURL)
	if err != nil {
		return err
	}
	if redir.Hostname() == "" {
		return nil
	}

	myDomain := build_buddy_url.Domain()
	if redir.Hostname() != myDomain && !strings.HasSuffix(redir.Hostname(), "."+myDomain) {
		return status.InvalidArgumentErrorf("Redirect url %q not found on this domain %q", redirectURL, myDomain)
	}
	return nil
}
