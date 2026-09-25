// Package executor_experiments declares experiments that are evaluated by the
// scheduler and read by executors.
//
// NOTE: when adding a new experiment to this package, also update
// modifyTaskForExperiments in scheduler_server.go to evaluate it when an
// executor leases a task.
package executor_experiments

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"
)

var (
	PersistentVolumes = expflag.String("executor.persistent_volumes", "", "Persistent volumes to mount into each task's container, in the same format as the persistent-volumes platform property. When set, this takes precedence over the platform property.")
)
