package urlutil

import (
	"net/url"
	"strings"
)

func SameHostname(urlStringA, urlStringB string) bool {
	if urlA, err := url.Parse(urlStringA); err == nil {
		if urlB, err := url.Parse(urlStringB); err == nil {
			return urlA.Hostname() == urlB.Hostname()
		}
	}
	return false
}

// GetDomain returns the domain portion of the passed host name.
// e.g. app.buildbuddy.io will return buildbuddy.io
//
// N.B. This does not generalize to all domains. Don't use this if you need
// something that works for arbitrary domains.
func GetDomain(hostname string) string {
	pts := strings.Split(hostname, ".")
	if len(pts) < 2 {
		return hostname
	}
	return strings.Join(pts[len(pts)-2:], ".")
}
