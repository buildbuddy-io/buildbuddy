// Package execution_experiments declares experiments that are evaluated by the
// scheduler when an executor leases a task, and read by the executor.
//
// In addition to the "common" experiment targeting attributes such as group ID,
// invocation ID, action mnemonic, etc., all of the experiments in this package
// have additional targeting attributes available:
//   - executor_hostname (string): the hostname of the leasing executor.
//   - self_hosted (bool): true if the executor is self-hosted rather than
//     managed by BuildBuddy. This is always false if app.client_identity.key
//     is not set, because the scheduler then cannot identify managed executors.
//   - workflow_name (string): the workflow action name, if the task is a
//     workflow run.
//   - os, arch, pool (string): the executor pool that the task was routed to.
//   - requested_isolation_type (string): the workload-isolation-type platform
//     property. This is empty when the task uses the executor's default
//     isolation type, so it is only reliable for targeting tasks that
//     explicitly request an isolation type.
//
// For each execution, the evaluated flag variant is available in the Executions
// table in ClickHouse, which stores a map of experiment name to experiment
// variant. If no variant is selected (e.g. if the flag is not configured in
// flagd), no map entry is recorded.
package execution_experiments

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"
)

var (
	// IMPORTANT: when adding a new experiment to this list, also update
	// modifyTaskForExperiments in scheduler_server.go to evaluate it.

	PersistentVolumes        = expflag.String("executor.persistent_volumes", "", "Persistent volumes to mount into each task's container, in the same format as the persistent-volumes platform property. When set, this takes precedence over the platform property.")
	UserspaceNetworking      = expflag.Bool("executor.userspace_networking", false, "Enable userspace networking for OCI and Firecracker containers.")
	RecordInputFetchMetadata = expflag.Bool("executor.record_input_fetch_metadata", true, "If true, record and report metadata describing which action inputs were fetched from remote CAS.", expflag.DeprecatedExperimentName("remote_execution.record_input_fetch_metadata"))
	WorkspacePoolEnabled     = expflag.Bool("executor.workspace_pool.enabled", false, "If true, keep finished task workspaces in a pool, and set up later tasks with similar input trees from them instead of from an empty directory. See the executor.workspace_pool.* flags.")
	// TODO(bduffany): once expflag supports expflag.Slice, remove this and
	// consolidate with executor.oci.seccomp_additional_syscalls
	SeccompDisableNewUserNS = expflag.Bool("executor.oci.seccomp_disable_new_userns", false, "If true, the OCI seccomp profile prevents containers from creating user namespaces, unless executor.oci.cap_add includes CAP_SYS_ADMIN. This does not restrict syscalls listed in executor.oci.seccomp_additional_syscalls.")
)
