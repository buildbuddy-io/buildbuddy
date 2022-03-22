package build_buddy_url

import (
	"flag"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

var buildBuddyURL string

func init() {
	flag.Var(flagutil.NewURLFlag(&buildBuddyURL), "app.build_buddy_url", "The external URL where your BuildBuddy instance can be found.")
}

func BuildBuddyURL(path string) *url.URL {
	u, err := url.Parse(buildBuddyURL)
	if err != nil {
		// Shouldn't happen, URLFlag should validate it.
		alert.UnexpectedEvent("flag app.build_buddy_url was not a parseable URL: %v", err)
	}
	if path == "" {
		return u
	}
	return u.ResolveReference(&url.URL{Path: path})
}

func BuildBuddyURLString() string {
	return buildBuddyURL
}
