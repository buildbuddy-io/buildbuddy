// Package executor_experiments declares experiments that are evaluated by the
// app and read by executors.
//
// Executors cannot evaluate experiments themselves, because they have neither
// the experiment config nor the request attributes, such as the group ID, that
// experiments target. Instead, the execution server evaluates every experiment
// declared here and sends the results to the executor in the ExecutionTask.
// Executors that have opted in via executor.scheduler_controlled_experiments_enabled
// advertise this in their registration, and read the results from the task's
// context, using the provider returned by expflag.NewContextProvider. Other
// executors, such as self-hosted ones, use the values configured on the
// executor. The execution server skips evaluation for pools where no executor
// has opted in.
//
// To add an executor experiment, declare it here and add it to the
// Evaluate list below.
package executor_experiments

import (
	"context"

	expb "github.com/buildbuddy-io/buildbuddy/proto/experiments"
)

// Evaluate returns the executor experiments to send with a task.
func Evaluate(ctx context.Context) []*expb.EvaluatedFlag {
	return []*expb.EvaluatedFlag{
		// Add the GetProto(ctx) result for each experiment that executors read.
	}
}
