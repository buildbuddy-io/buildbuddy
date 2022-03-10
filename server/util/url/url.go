package url

import (
	"flag"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	buildBuddyURL         = flag.String("app.build_buddy_url", "", "The external URL where your BuildBuddy instance can be found.")
	eventsAPIURL          = flag.String("app.events_api_url", "", "Overrides the default build event protocol gRPC address shown by BuildBuddy on the configuration screen.")
	cacheAPIURL           = flag.String("app.cache_api_url", "", "Overrides the default remote cache protocol gRPC address shown by BuildBuddy on the configuration screen.")
	remoteExecutionAPIURL = flag.String("app.remote_execution_api_url", "", "Overrides the default remote execution protocol gRPC address shown by BuildBuddy on the configuration screen.")
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
