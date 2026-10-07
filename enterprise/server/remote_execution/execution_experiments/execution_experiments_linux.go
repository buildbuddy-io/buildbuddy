//go:build linux && !android

package execution_experiments

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"

	expb "github.com/buildbuddy-io/buildbuddy/proto/experiments"
)

var (
	testCPUWeightMultiplier = expflag.Float64("executor.test_cpu_weight_multiplier", 1, "Multiplier applied to the cgroup CPU weight of test actions. Values <= 0 are ignored.")
)

// TestCPUWeightMultiplier returns the multiplier applied to the cgroup CPU
// weight of test actions.
func TestCPUWeightMultiplier(ctx context.Context) float64 {
	return testCPUWeightMultiplier.Get(ctx)
}

func evaluatePlatformExperiments(ctx context.Context, opts ...any) []*expb.EvaluatedFlag {
	return []*expb.EvaluatedFlag{
		testCPUWeightMultiplier.GetProto(ctx, opts...),
	}
}
