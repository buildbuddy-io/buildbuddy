// Package executor_experiments declares experiments that are evaluated by the
// app and read by executors.
//
// To add an executor experiment, declare it here and add it to the
// Evaluate list below.
package executor_experiments

import (
	"context"

	expb "github.com/buildbuddy-io/buildbuddy/proto/experiments"
)

// Evaluate returns the executor experiments to send with a task. The options
// are passed to every experiment, so that targeting rules can use the task's
// attributes.
func Evaluate(ctx context.Context, opts ...any) []*expb.EvaluatedFlag {
	return []*expb.EvaluatedFlag{
		// Add the GetProto(ctx, opts...) result for each experiment that
		// executors read.
	}
}
