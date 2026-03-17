// Package clientip extracts the real client IP from requests that pass
// through a load balancer or reverse proxy, using the X-Forwarded-For header.
package clientip

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

// ContextKey is the context value key used to store the client IP.
const ContextKey = "clientIP"

var (
	trustXForwardedForHeader = flag.Bool("auth.trust_xforwardedfor_header", false, "If true, client IP information will be retrieved from the X-Forwarded-For header. Should only be enabled if the BuildBuddy server is only accessible behind a trusted proxy.")
)

// Get returns the client IP stored in the context, or empty string if not set.
func Get(ctx context.Context) string {
	if v, ok := ctx.Value(ContextKey).(string); ok {
		return v
	}
	return ""
}

// SetFromXForwardedForHeader parses the X-Forwarded-For header and stores the
// client IP in the context. Returns the original context unchanged if the
// auth.trust_xforwardedfor_header flag is disabled or the header is empty.
func SetFromXForwardedForHeader(ctx context.Context, header string) (context.Context, bool) {
	if !*trustXForwardedForHeader {
		return ctx, false
	}
	ip, ok := FromHeader(header)
	if !ok {
		return ctx, false
	}
	return context.WithValue(ctx, ContextKey, ip), true
}

// FromHeader extracts the client IP from an X-Forwarded-For. It doesn't check
// the trustXForwardedForHeader flag, only used it in cases where you're ok
// with a spoofed IP.
func FromHeader(header string) (string, bool) {
	if header == "" {
		return "", false
	}
	ips := strings.Split(header, ",")

	// If there's only a single IP in the header, return it directly.
	// This handles the header format set by NGINX.
	if len(ips) == 1 {
		return strings.TrimSpace(ips[0]), true
	}

	// For GCLB, the header format is [client supplied IP,]client IP, LB IP
	// We always look at the client IP as seen by GCLB as the client supplied
	// value can't be trusted if it's present.
	return strings.TrimSpace(ips[len(ips)-2]), true
}
