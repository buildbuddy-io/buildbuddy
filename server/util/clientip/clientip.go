package clientip

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

const ContextKey = "clientIP"

var (
	trustXForwardedForHeader = flag.Bool("auth.trust_xforwardedfor_header", false, "If true, client IP information will be retrieved from the X-Forwarded-For header. Should only be enabled if the BuildBuddy server is only accessible behind a trusted proxy.")
)

func Get(ctx context.Context) string {
	if v, ok := ctx.Value(ContextKey).(string); ok {
		return v
	}
	return ""
}

func SetFromXForwardedForHeader(ctx context.Context, header string) (context.Context, bool) {
	if !*trustXForwardedForHeader || header == "" {
		return ctx, false
	}

	ips := strings.Split(header, ",")

	// If there's only a single IP in the header, return it directly.
	// This handles the header format set by NGINX.
	if len(ips) == 1 {
		return context.WithValue(ctx, ContextKey, strings.TrimSpace(ips[0])), true
	}

	// For GCLB, the header format is [client supplied IP,]client IP, LB IP
	// We always look at the client IP as seen by GCLB as the client supplied
	// value can't be trusted if it's present.
	return context.WithValue(ctx, ContextKey, strings.TrimSpace(ips[len(ips)-2])), true
}
