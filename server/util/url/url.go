package url

import (
	"net/url"

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
	myURL, err := url.Parse(env.GetConfigurator().GetAppBuildBuddyURL())
	if err != nil {
		return err
	}
	redir, err := url.Parse(redirectURL)
	if err != nil {
		return err
	}
	if redir.Hostname() != "" {
		if !SameHostname(redirectURL, myURL.String()) {
			return status.InvalidArgumentErrorf("Redirect url %q not found on this domain %q", redirectURL, myURL.Host)
		}
	}
	return nil
}
