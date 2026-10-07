// Package execution_experiments declares experiments that are evaluated by the
// scheduler when an executor leases a task, and read by the executor.
//
// NOTE: when adding a new experiment to this package, also add it to Evaluate
// (or evaluatePlatformExperiments for platform-specific experiments) so that
// the scheduler evaluates it.
package execution_experiments

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"

	expb "github.com/buildbuddy-io/buildbuddy/proto/experiments"
)

var (
	PersistentVolumes = expflag.String("executor.persistent_volumes", "", "Persistent volumes to mount into each task's container, in the same format as the persistent-volumes platform property. When set, this takes precedence over the platform property.")
)

// Evaluate evaluates all experiments in this package, so that the scheduler
// can attach them to a task at lease time.
func Evaluate(ctx context.Context, opts ...any) []*expb.EvaluatedFlag {
	flags := []*expb.EvaluatedFlag{
		PersistentVolumes.GetProto(ctx, opts...),
	}
	return append(flags, evaluatePlatformExperiments(ctx, opts...)...)
}
