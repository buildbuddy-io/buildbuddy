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

// GetDomain returns the domain portion of the passed host, with any port
// number removed.
// e.g. app.buildbuddy.io:80 will return buildbuddy.io
//
// N.B. This does not generalize to all domains. Don't use this if you need
// something that works for arbitrary domains.
func GetDomain(host string) string {
	// If there is a port number, remove it.
	i := strings.IndexByte(host, ':')
	if i >= 0 {
		host = host[:i]
	}
	i = strings.LastIndexByte(host, '.')
	if i < 0 {
		// If there aren't any periods, return the whole host.
		return host
	}
	i = strings.LastIndexByte(host[:i], '.')
	if i < 0 {
		// If there's only 1 period, also return the whole host.
		return host
	}
	// Everything after the second to last period.
	return host[i+1:]
}
