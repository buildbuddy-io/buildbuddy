package usageutil

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
)

// Labels returns usage labels for the given request context.
func Labels(ctx context.Context) (*tables.UsageLabels, error) {
	// TODO: populate with real values.
	return &tables.UsageLabels{}, nil
}
