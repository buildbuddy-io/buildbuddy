package debugutil

import (
	"context"

	"google.golang.org/grpc/metadata"
)

func Enable(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, "debug", "true")
	return metadata.AppendToOutgoingContext(ctx, "debug", "true")
}

func IsEnabled(ctx context.Context) bool {
	v, ok := ctx.Value("debug").(string)
	return ok && v == "true"
}
