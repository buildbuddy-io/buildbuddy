package cacheutil

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// If set in the context metadata, the requested resource should be written to /
// read from this partition ID if the user is authorized to do so.
const PartitionOverrideKey = "x-buildbuddy-cache-partition"

func PartitionOverride(ctx context.Context) string {
	md := metadata.ValueFromIncomingContext(ctx, PartitionOverrideKey)
	if len(md) > 0 {
		return md[0]
	}
	return ""
}

func SetPartitionOverride(ctx context.Context, val string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, PartitionOverrideKey, val)
}
