package clientip

import (
	"context"
	"strings"
)

const ContextKey = "clientIP"

func Get(ctx context.Context) string {
	if v, ok := ctx.Value(ContextKey).(string); ok {
		return v
	}
	return ""
}

func SetFromXForwardedForHeader(ctx context.Context, header string) (context.Context, bool) {
	ips := strings.Split(header, ",")
	if len(ips) < 2 {
		return ctx, false
	}
	// For GCLB, the header format is [client supplied IP,]client IP, LB IP
	// We always look at the client IP as seen by GCLB as the client supplied
	// value can't be trusted if it's present.
	return context.WithValue(ctx, ContextKey, ips[len(ips)-2]), true
}
