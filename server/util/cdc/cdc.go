// Package cdc provides shared constants and helpers for content-defined
// chunking (CDC). It is intentionally kept small so lightweight packages
// (e.g. proxy_util) can reference the CDC header name without pulling in the
// full chunking package and its flag registrations.
package cdc

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const EnabledHeaderName = "x-buildbuddy-cdc-enabled"

func EnabledViaHeader(ctx context.Context) bool {
	values := metadata.ValueFromIncomingContext(ctx, EnabledHeaderName)
	return len(values) > 0 && values[0] == "true"
}
