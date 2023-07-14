package clientip

import "context"

const ContextKey = "clientIP"

func Get(ctx context.Context) string {
	if v, ok := ctx.Value(ContextKey).(string); ok {
		return v
	}
	return ""
}
