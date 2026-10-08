// Package execution_experiments declares experiments that are evaluated by the
// scheduler when an executor leases a task, and read by the executor.
//
// NOTE: when adding a new experiment to this package, also update
// modifyTaskForExperiments in scheduler_server.go to evaluate it.
package execution_experiments

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"
)

var (
	PersistentVolumes        = expflag.String("executor.persistent_volumes", "", "Persistent volumes to mount into each task's container, in the same format as the persistent-volumes platform property. When set, this takes precedence over the platform property.")
	UserspaceNetworking      = expflag.Bool("executor.userspace_networking", false, "Enable userspace networking for OCI and Firecracker containers.")
	RecordInputFetchMetadata = expflag.Bool("executor.record_input_fetch_metadata", true, "If true, record and report metadata describing which action inputs were fetched from remote CAS.", expflag.DeprecatedExperimentName("remote_execution.record_input_fetch_metadata"))
)
