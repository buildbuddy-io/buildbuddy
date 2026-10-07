//go:build !linux || android

package execution_experiments

import (
	"context"

	expb "github.com/buildbuddy-io/buildbuddy/proto/experiments"
)

// TestCPUWeightMultiplier returns 1, since cgroup CPU weights only exist on
// Linux.
func TestCPUWeightMultiplier(ctx context.Context) float64 {
	return 1
}

func evaluatePlatformExperiments(ctx context.Context, opts ...any) []*expb.EvaluatedFlag {
	return nil
}
