package usageutil

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/pbwireutil"
	"google.golang.org/grpc/metadata"
)

// Client label constants.
const (
	bazelClientLabel    = "bazel"
	executorClientLabel = "executor"
)

// Labels returns usage labels for the given request context.
func Labels(ctx context.Context) (*tables.UsageLabels, error) {
	return &tables.UsageLabels{
		Origin: originLabel(ctx),
		Client: clientLabel(ctx),
	}, nil
}

func originLabel(ctx context.Context) string {
	vals := metadata.ValueFromIncomingContext(ctx, "x-buildbuddy-origin")
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func clientLabel(ctx context.Context) string {
	// Note: we avoid deserializing the RequestMetadata proto here since
	// proto deserialization is too expensive to run on every request.
	b := bazel_request.GetRequestMetadataBytes(ctx)
	if len(b) == 0 {
		return ""
	}
	const (
		mdToolDetailsFieldNumber       = 1
		mdExecutorDetailsFieldNumber   = 1000
		toolDetailsToolNameFieldNumber = 1
	)
	toolDetailsBytes, _ := pbwireutil.ConsumeFirstBytes(b, mdToolDetailsFieldNumber)
	if len(toolDetailsBytes) > 0 {
		toolName, _ := pbwireutil.ConsumeFirstString(toolDetailsBytes, toolDetailsToolNameFieldNumber)
		if toolName == "bazel" {
			return bazelClientLabel
		}
		return ""
	}
	_, n := pbwireutil.ConsumeFirstBytes(b, mdExecutorDetailsFieldNumber)
	if n >= 0 {
		return executorClientLabel
	}
	return ""
}
