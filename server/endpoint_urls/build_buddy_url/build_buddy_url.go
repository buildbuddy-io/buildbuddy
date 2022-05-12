package build_buddy_url

import (
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

var buildBuddyURL = flagutil.URLFromString("app.build_buddy_url", "http://localhost:8080", "The external URL where your BuildBuddy instance can be found.")

func WithPath(path string) *url.URL {
	return buildBuddyURL.ResolveReference(&url.URL{Path: path})
}

func String() string {
	return buildBuddyURL.String()
}
