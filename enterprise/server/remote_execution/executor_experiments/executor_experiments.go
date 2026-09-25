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

// Evaluate returns the executor experiments to send with a task.
func Evaluate(ctx context.Context) []*expb.EvaluatedFlag {
	return []*expb.EvaluatedFlag{
		// Add the GetProto(ctx) result for each experiment that executors read.
	}
}
