package current_request

import "context"

const ClientIPContextKey = "clientIP"

func ClientIP(ctx context.Context) string {
	if v, ok := ctx.Value(ClientIPContextKey).(string); ok {
		return v
	}
	return ""
}
