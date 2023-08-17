package build_buddy_url

import (
	"net/url"
	"strings"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
)

var buildBuddyURL = flagtypes.URLFromString("app.build_buddy_url", "http://localhost:8080", "The external URL where your BuildBuddy instance can be found.")

func WithPath(path string) *url.URL {
	return buildBuddyURL.ResolveReference(&url.URL{Path: path})
}

func String() string {
	return buildBuddyURL.String()
}

// Domain returns the domain portion of the BuildBuddy URL.
// e.g. If the URL is "app.buildbuddy.io", the returned domain will be
// "buildbuddy.io".
func Domain() string {
	hostname := buildBuddyURL.Hostname()
	pts := strings.Split(hostname, ".")
	if len(pts) < 2 {
		return hostname
	}
	return strings.Join(pts[len(pts)-2:], ".")
}
