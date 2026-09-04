package useragent

import (
	"context"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/metadata"
)

const (
	ContextKey = "userAgent"
	HTTPHeader = "User-Agent"
)

// User-Agents are client-supplied. Limit them to this maximum length. Generally
// only the leading portion is meaningful. DB columns storing a user agent must
// be declared at least this wide.
const MaxLength = 1024

// SetFromHeader stores the given User-Agent header value on the context.
func SetFromHeader(ctx context.Context, header string) context.Context {
	header = sanitize(header)
	if header == "" {
		return ctx
	}
	return context.WithValue(ctx, ContextKey, header)
}

// Get returns the client's user agent, or "" if it is unknown. It falls back to
// the gRPC user-agent metadata so that clients calling a gRPC endpoint directly
// (rather than its HTTP/protolet equivalent) are still attributed.
func Get(ctx context.Context) string {
	if v, ok := ctx.Value(ContextKey).(string); ok {
		return v
	}
	if vals := metadata.ValueFromIncomingContext(ctx, "user-agent"); len(vals) > 0 {
		return sanitize(vals[0])
	}
	return ""
}

func sanitize(s string) string {
	// The value reaches a DB column, so drop anything that isn't valid UTF-8
	// before truncating.
	s = strings.ToValidUTF8(s, "")
	if len(s) <= MaxLength {
		return s
	}
	s = s[:MaxLength]
	// Drop the partial rune that the byte-wise cut may have left behind.
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}
