// Package cacheopts carries per-request options for cache operations on the
// context, for callers that can't change the interfaces.Cache signatures.
package cacheopts

import "context"

type maxReadReplicasKey struct{}

// WithMaxReadReplicas limits how many replicas a distributed cache read asks
// before reporting NotFound. n <= 0 means no limit. A limit trades a possible
// false miss for fewer peer calls, so use it only where a miss is cheap.
func WithMaxReadReplicas(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, maxReadReplicasKey{}, n)
}

// MaxReadReplicas returns the limit set by WithMaxReadReplicas, or 0 if none.
func MaxReadReplicas(ctx context.Context) int {
	n, _ := ctx.Value(maxReadReplicasKey{}).(int)
	return n
}
