load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")
load(":defs.bzl", "ts_proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "acl_proto",
    srcs = ["acl.proto"],
    deps = [
        ":user_id_proto",
    ],
)

proto_library(
    name = "api_key_proto",
    srcs = ["api_key.proto"],
    deps = [
        ":context_proto",
    ],
)

proto_library(
    name = "bazel_query_proto",
    srcs = ["bazel_query.proto"],
)

proto_library(
    name = "build_status_proto",
    srcs = [
        "build_status.proto",
    ],
    deps = [
        "@com_google_protobuf//:any_proto",
    ],
)

proto_library(
    name = "build_events_proto",
    srcs = [
        "build_events.proto",
    ],
    deps = [
        ":build_status_proto",
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

proto_library(
    name = "cache_proto",
    srcs = [
        "cache.proto",
    ],
    deps = [
        ":context_proto",
        ":remote_execution_proto",
        ":resource_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:field_mask_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@go_googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "config_proto",
    srcs = [
        "config.proto",
    ],
)

proto_library(
    name = "distributed_cache_proto",
    srcs = [
        "distributed_cache.proto",
    ],
    deps = [
        ":resource_proto",
    ],
)

proto_library(
    name = "resource_proto",
    srcs = [
        "resource.proto",
    ],
    deps = [
        ":remote_execution_proto",
    ],
)

proto_library(
    name = "health_proto",
    srcs = [
        "health.proto",
    ],
)

proto_library(
    name = "execution_stats_proto",
    srcs = [
        "execution_stats.proto",
    ],
    deps = [
        ":acl_proto",
        ":context_proto",
        ":remote_execution_proto",
        ":scheduler_proto",
        "@com_google_protobuf//:duration_proto",
        "@go_googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "failure_details_proto",
    srcs = [
        "failure_details.proto",
    ],
    deps = [
        "@com_google_protobuf//:descriptor_proto",
    ],
)

proto_library(
    name = "github_proto",
    srcs = ["github.proto"],
    deps = [":context_proto"],
)

proto_library(
    name = "pagination_proto",
    srcs = [":pagination.proto"],
)

proto_library(
    name = "quota_proto",
    srcs = [":quota.proto"],
    deps = [
        ":context_proto",
        "@com_google_protobuf//:duration_proto",
    ],
)

proto_library(
    name = "publish_build_event_proto",
    srcs = [
        "publish_build_event.proto",
    ],
    deps = [
        ":build_events_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:empty_proto",
        "@go_googleapis//google/api:annotations_proto",
    ],
)

proto_library(
    name = "raft_proto",
    srcs = [
        "raft.proto",
    ],
    deps = [
        ":remote_execution_proto",
        ":resource_proto",
        "@go_googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "secrets_proto",
    srcs = ["secrets.proto"],
    deps = [
        ":context_proto",
    ],
)

proto_library(
    name = "target_proto",
    srcs = [
        "target.proto",
    ],
    deps = [
        ":context_proto",
        "//proto/api/v1:common_proto",
    ],
)

proto_library(
    name = "target_pagination_proto",
    srcs = [
        "target_pagination.proto",
    ],
)

proto_library(
    name = "usage_proto",
    srcs = [
        "usage.proto",
    ],
    deps = [
        ":context_proto",
    ],
)

proto_library(
    name = "eventlog_proto",
    srcs = [
        "eventlog.proto",
    ],
    deps = [
        ":context_proto",
    ],
)

proto_library(
    name = "workflow_proto",
    srcs = [
        "workflow.proto",
    ],
    deps = [
        ":context_proto",
    ],
)

proto_library(
    name = "runner_proto",
    srcs = [
        "runner.proto",
    ],
    deps = [
        ":context_proto",
    ],
)

proto_library(
    name = "vmexec_proto",
    srcs = ["vmexec.proto"],
    deps = [
        ":remote_execution_proto",
        "@com_google_protobuf//:duration_proto",
        "@go_googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "vmvfs_proto",
    srcs = ["vmvfs.proto"],
    deps = [":remote_execution_proto"],
)

proto_library(
    name = "vfs_proto",
    srcs = ["vfs.proto"],
)

proto_library(
    name = "build_event_stream_proto",
    srcs = ["build_event_stream.proto"],
    deps = [
        ":command_line_proto",
        ":failure_details_proto",
        ":invocation_policy_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

proto_library(
    name = "invocation_policy_proto",
    srcs = ["invocation_policy.proto"],
)

proto_library(
    name = "command_line_proto",
    srcs = ["command_line.proto"],
    deps = [
        ":option_filters_proto",
    ],
)

proto_library(
    name = "option_filters_proto",
    srcs = ["option_filters.proto"],
)

proto_library(
    name = "invocation_proto",
    srcs = ["invocation.proto"],
    deps = [
        ":acl_proto",
        ":api_key_proto",
        ":build_event_stream_proto",
        ":cache_proto",
        ":command_line_proto",
        ":context_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

proto_library(
    name = "user_proto",
    srcs = ["user.proto"],
    exports = [":user_id_proto"],
    deps = [
        ":context_proto",
        ":group_proto",
        ":user_id_proto",
    ],
)

proto_library(
    name = "user_id_proto",
    srcs = ["user_id.proto"],
)

proto_library(
    name = "group_proto",
    srcs = [
        "grp.proto",
    ],
    deps = [
        ":context_proto",
        ":user_id_proto",
    ],
)

proto_library(
    name = "context_proto",
    srcs = ["context.proto"],
    deps = [
        ":user_id_proto",
    ],
)

proto_library(
    name = "raft_service_proto",
    srcs = ["raft_service.proto"],
    deps = [
        ":raft_proto",
    ],
)

proto_library(
    name = "ping_service_proto",
    srcs = ["ping_service.proto"],
)

proto_library(
    name = "buildbuddy_service_proto",
    srcs = ["buildbuddy_service.proto"],
    deps = [
        ":api_key_proto",
        ":bazel_config_proto",
        ":cache_proto",
        ":eventlog_proto",
        ":execution_stats_proto",
        ":github_proto",
        ":group_proto",
        ":invocation_proto",
        ":quota_proto",
        ":resource_proto",
        ":runner_proto",
        ":scheduler_proto",
        ":secrets_proto",
        ":target_proto",
        ":usage_proto",
        ":user_proto",
        ":workflow_proto",
    ],
)

proto_library(
    name = "semver_proto",
    srcs = ["semver.proto"],
)

proto_library(
    name = "sidecar_proto",
    srcs = ["sidecar.proto"],
)

proto_library(
    name = "registry_proto",
    srcs = ["registry.proto"],
    deps = [":remote_execution_proto"],
)

proto_library(
    name = "remote_asset_proto",
    srcs = ["remote_asset.proto"],
    deps = [
        ":remote_execution_proto",
        ":semver_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@go_googleapis//google/api:annotations_proto",
        "@go_googleapis//google/longrunning:longrunning_proto",
        "@go_googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "remote_execution_log_proto",
    srcs = ["remote_execution_log.proto"],
    deps = [
        ":remote_execution_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@go_googleapis//google/api:annotations_proto",
        "@go_googleapis//google/bytestream:bytestream_proto",
        "@go_googleapis//google/longrunning:longrunning_proto",
        "@go_googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "remote_execution_proto",
    srcs = ["remote_execution.proto"],
    deps = [
        ":scheduler_proto",
        ":semver_proto",
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@com_google_protobuf//:wrappers_proto",
        "@go_googleapis//google/api:annotations_proto",
        "@go_googleapis//google/longrunning:longrunning_proto",
        "@go_googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "bazel_config_proto",
    srcs = ["bazel_config.proto"],
    deps = [
        ":api_key_proto",
        ":context_proto",
    ],
)

proto_library(
    name = "telemetry_proto",
    srcs = ["telemetry.proto"],
    deps = [
        "@go_googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "scheduler_proto",
    srcs = ["scheduler.proto"],
    deps = [
        ":acl_proto",
        ":context_proto",
        ":trace_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

proto_library(
    name = "trace_proto",
    srcs = ["trace.proto"],
)

proto_library(
    name = "worker_proto",
    srcs = ["worker.proto"],
)

# Go proto rules below here

go_proto_library(
    name = "acl_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/acl",
    proto = ":acl_proto",
    deps = [
        ":user_id_go_proto",
    ],
)

go_proto_library(
    name = "api_key_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/api_key",
    proto = ":api_key_proto",
    deps = [
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "bazel_query_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/bazel_query",
    proto = ":bazel_query_proto",
)

go_proto_library(
    name = "build_status_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/build_status",
    proto = ":build_status_proto",
)

go_proto_library(
    name = "build_events_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/build_events",
    proto = ":build_events_proto",
    deps = [
        ":build_status_go_proto",
    ],
)

go_proto_library(
    name = "config_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/config",
    proto = ":config_proto",
)

go_proto_library(
    name = "health_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/health",
    proto = ":health_proto",
)

go_proto_library(
    name = "execution_stats_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/execution_stats",
    proto = ":execution_stats_proto",
    deps = [
        ":acl_go_proto",
        ":context_go_proto",
        ":remote_execution_go_proto",
        ":scheduler_go_proto",
        "@go_googleapis//google/rpc:status_go_proto",
    ],
)

go_proto_library(
    name = "failure_details_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/failure_details",
    proto = ":failure_details_proto",
)

go_proto_library(
    name = "github_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/github",
    proto = ":github_proto",
    deps = [
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "pagination_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/pagination",
    proto = ":pagination_proto",
)

go_proto_library(
    name = "publish_build_event_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event",
    proto = ":publish_build_event_proto",
    deps = [
        ":build_events_go_proto",
        "@go_googleapis//google/api:annotations_go_proto",
    ],
)

go_proto_library(
    name = "usage_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/usage",
    proto = ":usage_proto",
    deps = [
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "build_event_stream_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream",
    proto = ":build_event_stream_proto",
    deps = [
        ":command_line_go_proto",
        ":failure_details_go_proto",
        ":invocation_policy_go_proto",
    ],
)

go_proto_library(
    name = "quota_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/quota",
    proto = ":quota_proto",
    deps = [
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "secrets_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/secrets",
    proto = ":secrets_proto",
    deps = [
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "target_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/target",
    proto = ":target_proto",
    deps = [
        ":context_go_proto",
        "//proto/api/v1:common_go_proto",
    ],
)

go_proto_library(
    name = "target_pagination_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/target_pagination",
    proto = ":target_pagination_proto",
)

go_proto_library(
    name = "eventlog_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/eventlog",
    proto = ":eventlog_proto",
    deps = [
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "vmexec_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/vmexec",
    proto = ":vmexec_proto",
    deps = [
        ":remote_execution_go_proto",
        "@go_googleapis//google/rpc:status_go_proto",
    ],
)

go_proto_library(
    name = "vmvfs_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/vmvfs",
    proto = ":vmvfs_proto",
    deps = [
        ":remote_execution_go_proto",
    ],
)

go_proto_library(
    name = "vfs_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/vfs",
    proto = ":vfs_proto",
)

go_proto_library(
    name = "workflow_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/workflow",
    proto = ":workflow_proto",
    deps = [
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "raft_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/raft",
    proto = ":raft_proto",
    deps = [
        ":remote_execution_go_proto",
        ":resource_go_proto",
        "@go_googleapis//google/rpc:status_go_proto",
    ],
)

go_proto_library(
    name = "runner_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/runner",
    proto = ":runner_proto",
    deps = [
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "invocation_policy_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/blaze.invocation_policy",
    proto = ":invocation_policy_proto",
)

go_proto_library(
    name = "command_line_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/command_line",
    proto = ":command_line_proto",
    deps = [
        ":option_filters_go_proto",
    ],
)

go_proto_library(
    name = "resource_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/resource",
    proto = ":resource_proto",
    deps = [
        ":remote_execution_go_proto",
    ],
)

go_proto_library(
    name = "option_filters_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/options",
    proto = ":option_filters_proto",
)

go_proto_library(
    name = "invocation_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/invocation",
    proto = ":invocation_proto",
    deps = [
        ":acl_go_proto",
        ":api_key_go_proto",
        ":build_event_stream_go_proto",
        ":cache_go_proto",
        ":command_line_go_proto",
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "raft_service_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/raft_service",
    proto = ":raft_service_proto",
    deps = [
        ":raft_go_proto",
    ],
)

go_proto_library(
    name = "ping_service_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/ping_service",
    proto = ":ping_service_proto",
)

go_proto_library(
    name = "buildbuddy_service_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service",
    proto = ":buildbuddy_service_proto",
    deps = [
        ":api_key_go_proto",
        ":bazel_config_go_proto",
        ":cache_go_proto",
        ":eventlog_go_proto",
        ":execution_stats_go_proto",
        ":github_go_proto",
        ":group_go_proto",
        ":invocation_go_proto",
        ":quota_go_proto",
        ":resource_go_proto",
        ":runner_go_proto",
        ":scheduler_go_proto",
        ":secrets_go_proto",
        ":target_go_proto",
        ":usage_go_proto",
        ":user_go_proto",
        ":workflow_go_proto",
    ],
)

go_proto_library(
    name = "semver_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/semver",
    proto = ":semver_proto",
)

go_proto_library(
    name = "sidecar_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/sidecar",
    proto = ":sidecar_proto",
)

go_proto_library(
    name = "registry_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/registry",
    proto = ":registry_proto",
    deps = [":remote_execution_go_proto"],
)

go_proto_library(
    name = "user_id_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/user_id",
    proto = ":user_id_proto",
)

go_proto_library(
    name = "user_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/user",
    proto = ":user_proto",
    deps = [
        ":context_go_proto",
        ":group_go_proto",
        ":user_id_go_proto",
    ],
)

go_proto_library(
    name = "group_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/group",
    proto = ":group_proto",
    deps = [
        ":context_go_proto",
        ":user_id_go_proto",
    ],
)

go_proto_library(
    name = "cache_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/cache",
    proto = ":cache_proto",
    deps = [
        ":context_go_proto",
        ":remote_execution_go_proto",
        ":resource_go_proto",
        "@go_googleapis//google/rpc:status_go_proto",
    ],
)

go_proto_library(
    name = "context_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/context",
    proto = ":context_proto",
    deps = [
        ":user_id_go_proto",
    ],
)

go_proto_library(
    name = "bazel_config_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/bazel_config",
    proto = ":bazel_config_proto",
    deps = [
        ":api_key_go_proto",
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "distributed_cache_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/distributed_cache",
    proto = ":distributed_cache_proto",
    deps = [
        ":resource_go_proto",
    ],
)

go_proto_library(
    name = "remote_asset_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/remote_asset",
    proto = ":remote_asset_proto",
    deps = [
        ":remote_execution_go_proto",
        ":semver_go_proto",
        "@go_googleapis//google/api:annotations_go_proto",
        "@go_googleapis//google/longrunning:longrunning_go_proto",
        "@go_googleapis//google/rpc:status_go_proto",
    ],
)

go_proto_library(
    name = "remote_execution_log_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/remote_execution_log",
    proto = ":remote_execution_log_proto",
    deps = [
        ":remote_execution_go_proto",
        "@go_googleapis//google/api:annotations_go_proto",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@go_googleapis//google/longrunning:longrunning_go_proto",
        "@go_googleapis//google/rpc:status_go_proto",
    ],
)

go_proto_library(
    name = "remote_execution_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/remote_execution",
    proto = ":remote_execution_proto",
    deps = [
        ":scheduler_go_proto",
        ":semver_go_proto",
        "@go_googleapis//google/api:annotations_go_proto",
        "@go_googleapis//google/longrunning:longrunning_go_proto",
        "@go_googleapis//google/rpc:status_go_proto",
    ],
)

go_proto_library(
    name = "telemetry_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/telemetry",
    proto = ":telemetry_proto",
    deps = [
        "@go_googleapis//google/rpc:status_go_proto",
    ],
)

go_proto_library(
    name = "scheduler_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/scheduler",
    proto = ":scheduler_proto",
    deps = [
        ":acl_go_proto",
        ":context_go_proto",
        ":trace_go_proto",
    ],
)

go_proto_library(
    name = "trace_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/trace",
    proto = ":trace_proto",
)

go_proto_library(
    name = "worker_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/worker",
    proto = ":worker_proto",
)

# TypeScript proto rules below here

ts_proto_library(
    name = "acl_ts_proto",
    proto = ":acl_proto",
)

ts_proto_library(
    name = "api_key_ts_proto",
    proto = ":api_key_proto",
)

ts_proto_library(
    name = "bazel_config_ts_proto",
    proto = ":bazel_config_proto",
)

ts_proto_library(
    name = "config_ts_proto",
    proto = ":config_proto",
)

ts_proto_library(
    name = "execution_stats_ts_proto",
    proto = ":execution_stats_proto",
)

ts_proto_library(
    name = "github_ts_proto",
    proto = ":github_proto",
)

ts_proto_library(
    name = "quota_ts_proto",
    proto = ":quota_proto",
)

ts_proto_library(
    name = "resource_ts_proto",
    proto = ":resource_proto",
)

ts_proto_library(
    name = "secrets_ts_proto",
    proto = ":secrets_proto",
)

ts_proto_library(
    name = "scheduler_ts_proto",
    proto = ":scheduler_proto",
)

ts_proto_library(
    name = "build_event_stream_ts_proto",
    proto = ":build_event_stream_proto",
)

ts_proto_library(
    name = "target_ts_proto",
    proto = ":target_proto",
)

ts_proto_library(
    name = "eventlog_ts_proto",
    proto = ":eventlog_proto",
)

ts_proto_library(
    name = "workflow_ts_proto",
    proto = ":workflow_proto",
)

ts_proto_library(
    name = "command_line_ts_proto",
    proto = ":command_line_proto",
)

ts_proto_library(
    name = "context_ts_proto",
    proto = ":context_proto",
)

ts_proto_library(
    name = "invocation_ts_proto",
    proto = ":invocation_proto",
)

ts_proto_library(
    name = "usage_ts_proto",
    proto = ":usage_proto",
)

ts_proto_library(
    name = "group_ts_proto",
    proto = ":group_proto",
)

ts_proto_library(
    name = "grpc_code_ts_proto",
    proto = "@go_googleapis//google/rpc:code_proto",
)

ts_proto_library(
    name = "timestamp_ts_proto",
    proto = "@com_google_protobuf//:timestamp_proto",
)

ts_proto_library(
    name = "duration_ts_proto",
    proto = "@com_google_protobuf//:duration_proto",
)

ts_proto_library(
    name = "cache_ts_proto",
    proto = ":cache_proto",
)

ts_proto_library(
    name = "user_id_ts_proto",
    proto = ":user_id_proto",
)

ts_proto_library(
    name = "user_ts_proto",
    proto = ":user_proto",
)

ts_proto_library(
    name = "buildbuddy_service_ts_proto",
    proto = ":buildbuddy_service_proto",
)

ts_proto_library(
    name = "remote_execution_ts_proto",
    proto = ":remote_execution_proto",
)

ts_proto_library(
    name = "runner_ts_proto",
    proto = ":runner_proto",
)
