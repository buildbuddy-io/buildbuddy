// Package execution_experiments declares experiments that executors read for
// each task. The values come from one of two places:
//   - If the executor sets executor.scheduler_controlled_experiments_enabled,
//     the scheduler evaluates each experiment with flagd when the executor
//     leases a task, and sends the values with the task. These values,
//     including the scheduler's defaults when no experiment applies, take
//     priority over the executor's config file and command line. This flag is
//     internal and experimental, and only executors that BuildBuddy manages
//     should set it.
//   - Otherwise, the executor uses the values from its config file or command
//     line. All other executors, including on-prem and self-hosted ones,
//     should use this mode.
//
// When the scheduler controls experiments, it provides additional targeting
// attributes beyond the "common" ones such as group ID, invocation ID, action
// mnemonic, etc.:
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
// When the scheduler controls experiments, the Executions table in ClickHouse
// records each execution's experiment variants, as a map of experiment name to
// variant. If no variant is selected (e.g. if the flag is not configured in
// flagd), no map entry is recorded.
package execution_experiments

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"

	expb "github.com/buildbuddy-io/buildbuddy/proto/experiments"
)

var (
	PersistentVolumes                            = register(expflag.String("executor.persistent_volumes", "", "Persistent volumes to mount into each task's container, in the same format as the persistent-volumes platform property. When set, this takes precedence over the platform property."))
	UserspaceNetworking                          = register(expflag.Bool("executor.userspace_networking", false, "Enable userspace networking for OCI and Firecracker containers."))
	RecordInputFetchMetadata                     = register(expflag.Bool("executor.record_input_fetch_metadata", true, "If true, record and report metadata describing which action inputs were fetched from remote CAS.", expflag.DeprecatedExperimentName("remote_execution.record_input_fetch_metadata")))
	WorkspacePoolEnabled                         = register(expflag.Bool("executor.workspace_pool.enabled", false, "If true, keep finished task workspaces in a pool, and set up later tasks with similar input trees from them instead of from an empty directory. See the executor.workspace_pool.* flags."))
	SpliceWithoutValidation                      = register(expflag.Bool("executor.splice_without_validation", true, "If true, ask the cache to splice chunked action outputs without re-verifying each chunk's contents. The cache only honors this from executors with a trusted client identity.", expflag.DeprecatedExperimentName("splice-without-validation")))
	FirecrackerSnapshotKeyIncludesContainerImage = register(expflag.Bool("executor.firecracker.snapshot_key_includes_container_image", false, "If true, include the effective container image, including the executor's default image, in the Firecracker snapshot key."))
	// TODO(bduffany): once expflag supports expflag.Slice, remove this and
	// consolidate with executor.oci.seccomp_additional_syscalls
	SeccompDisableNewUserNS = register(expflag.Bool("executor.oci.seccomp_disable_new_userns", false, "If true, the OCI seccomp profile prevents containers from creating user namespaces, unless executor.oci.cap_add includes CAP_SYS_ADMIN. This does not restrict syscalls listed in executor.oci.seccomp_additional_syscalls."))
)

type protoGetter interface {
	Name() string
	GetProto(ctx context.Context, opts ...any) *expb.EvaluatedFlag
}

// all lists every expflag declared in this package, so that All sends every
// experiment to executors. Declarations add themselves with register, and
// TestAllListsEveryFlag checks that none is declared without it.
var all []protoGetter

func register[F protoGetter](f F) F {
	all = append(all, f)
	return f
}

func All(ctx context.Context, opts ...any) []*expb.EvaluatedFlag {
	flags := make([]*expb.EvaluatedFlag, 0, len(all))
	for _, f := range all {
		flags = append(flags, f.GetProto(ctx, opts...))
	}
	return flags
}
