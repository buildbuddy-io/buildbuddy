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
    name = "auditlog_proto",
    srcs = [
        "auditlog.proto",
    ],
    deps = [
        ":api_key_proto",
        ":context_proto",
        ":encryption_proto",
        ":github_proto",
        ":group_proto",
        ":invocation_proto",
        ":secrets_proto",
        ":workflow_proto",
        "@com_google_protobuf//:timestamp_proto",
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
        "@com_google_protobuf//:wrappers_proto",
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
        "@googleapis//google/rpc:status_proto",
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
        ":invocation_status_proto",
        ":remote_execution_proto",
        ":scheduler_proto",
        ":stat_filter_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@googleapis//google/rpc:status_proto",
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
    name = "firecracker_proto",
    srcs = ["firecracker.proto"],
    deps = [
        ":remote_execution_proto",
    ],
)

proto_library(
    name = "github_proto",
    srcs = ["github.proto"],
    deps = [":context_proto"],
)

proto_library(
    name = "package_load_metrics_proto",
    srcs = ["package_load_metrics.proto"],
    deps = [
        "@com_google_protobuf//:duration_proto",
    ],
)

proto_library(
    name = "pagination_proto",
    srcs = ["pagination.proto"],
)

proto_library(
    name = "quota_proto",
    srcs = ["quota.proto"],
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
        "@googleapis//google/api:annotations_proto",
        "@googleapis//google/api:client_proto",
        "@googleapis//google/api:field_behavior_proto",
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
        "@googleapis//google/rpc:status_proto",
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
    name = "soci_proto",
    srcs = [
        "soci.proto",
    ],
    deps = [
        ":registry_proto",
        ":remote_execution_proto",
    ],
)

proto_library(
    name = "stat_filter_proto",
    srcs = ["stat_filter.proto"],
)

proto_library(
    name = "stats_proto",
    srcs = ["stats.proto"],
    deps = [
        ":context_proto",
        ":invocation_status_proto",
        ":stat_filter_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
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
        ":invocation_status_proto",
        "@com_google_protobuf//:duration_proto",
        "@googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "runner_proto",
    srcs = [
        "runner.proto",
    ],
    deps = [
        ":context_proto",
        ":firecracker_proto",
        ":remote_execution_proto",
    ],
)

proto_library(
    name = "vmexec_proto",
    srcs = ["vmexec.proto"],
    deps = [
        ":remote_execution_proto",
        "@com_google_protobuf//:duration_proto",
        "@googleapis//google/rpc:status_proto",
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
    name = "nbd_proto",
    srcs = ["nbd.proto"],
)

proto_library(
    name = "build_event_stream_proto",
    srcs = ["build_event_stream.proto"],
    deps = [
        ":command_line_proto",
        ":failure_details_proto",
        ":invocation_policy_proto",
        ":package_load_metrics_proto",
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
        ":invocation_status_proto",
        ":stat_filter_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

proto_library(
    name = "invocation_status_proto",
    srcs = ["invocation_status.proto"],
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
        ":auditlog_proto",
        ":bazel_config_proto",
        ":cache_proto",
        ":encryption_proto",
        ":eventlog_proto",
        ":execution_stats_proto",
        ":github_proto",
        ":group_proto",
        ":invocation_proto",
        ":quota_proto",
        ":repo_proto",
        ":resource_proto",
        ":runner_proto",
        ":scheduler_proto",
        ":secrets_proto",
        ":stats_proto",
        ":suggestion_proto",
        ":target_proto",
        ":usage_proto",
        ":user_proto",
        ":workflow_proto",
        ":zip_proto",
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
    name = "repo_proto",
    srcs = ["repo.proto"],
    deps = [
        ":context_proto",
    ],
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
        "@googleapis//google/api:annotations_proto",
        "@googleapis//google/longrunning:operations_proto",
        "@googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "remote_execution_log_proto",
    srcs = ["remote_execution_log.proto"],
    deps = [
        ":remote_execution_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@googleapis//google/api:annotations_proto",
        "@googleapis//google/bytestream:bytestream_proto",
        "@googleapis//google/longrunning:operations_proto",
        "@googleapis//google/rpc:status_proto",
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
        "@googleapis//google/api:annotations_proto",
        "@googleapis//google/longrunning:operations_proto",
        "@googleapis//google/rpc:status_proto",
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
        "@googleapis//google/rpc:status_proto",
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
        "@googleapis//google/rpc:status_proto",
    ],
)

proto_library(
    name = "suggestion_proto",
    srcs = ["suggestion.proto"],
    deps = [
        ":context_proto",
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

proto_library(
    name = "stored_invocation_proto",
    srcs = ["stored_invocation.proto"],
)

proto_library(
    name = "zip_proto",
    srcs = ["zip.proto"],
    deps = [
        ":context_proto",
        ":remote_execution_proto",
    ],
)

proto_library(
    name = "encryption_proto",
    srcs = ["encryption.proto"],
    deps = [
        ":context_proto",
    ],
)

proto_library(
    name = "metrics_proto",
    srcs = ["metrics.proto"],
    deps = [
        "@com_github_prometheus_client_model//io/prometheus/client:io_prometheus_client_proto",
    ],
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
    name = "auditlog_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/auditlog",
    proto = ":auditlog_proto",
    deps = [
        ":api_key_go_proto",
        ":context_go_proto",
        ":encryption_go_proto",
        ":github_go_proto",
        ":group_go_proto",
        ":invocation_go_proto",
        ":secrets_go_proto",
        ":workflow_go_proto",
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
        ":invocation_status_go_proto",
        ":remote_execution_go_proto",
        ":resource_go_proto",
        ":scheduler_go_proto",
        ":stat_filter_go_proto",
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)

go_proto_library(
    name = "failure_details_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/failure_details",
    proto = ":failure_details_proto",
)

go_proto_library(
    name = "firecracker_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/firecracker",
    proto = ":firecracker_proto",
    deps = [
        ":remote_execution_go_proto",
    ],
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
    name = "package_load_metrics_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/package_load_metrics",
    proto = ":package_load_metrics_proto",
)

go_proto_library(
    name = "publish_build_event_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event",
    proto = ":publish_build_event_proto",
    deps = [
        ":build_events_go_proto",
        "@org_golang_google_genproto_googleapis_api//annotations",
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
        ":package_load_metrics_go_proto",
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
    name = "soci_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/soci",
    proto = ":soci_proto",
    deps = [
        ":registry_go_proto",
        ":remote_execution_go_proto",
    ],
)

go_proto_library(
    name = "stat_filter_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/stat_filter",
    proto = ":stat_filter_proto",
)

go_proto_library(
    name = "stats_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/stats",
    proto = ":stats_proto",
    deps = [
        ":context_go_proto",
        ":invocation_status_go_proto",
        ":stat_filter_go_proto",
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
        "@org_golang_google_genproto_googleapis_rpc//status",
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
    name = "nbd_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/nbd",
    proto = ":nbd_proto",
)

go_proto_library(
    name = "workflow_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/workflow",
    proto = ":workflow_proto",
    deps = [
        ":context_go_proto",
        ":invocation_status_go_proto",
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)

go_proto_library(
    name = "raft_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/raft",
    proto = ":raft_proto",
    deps = [
        ":remote_execution_go_proto",
        ":resource_go_proto",
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)

go_proto_library(
    name = "runner_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/runner",
    proto = ":runner_proto",
    deps = [
        ":context_go_proto",
        ":firecracker_go_proto",
        ":remote_execution_go_proto",
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
        ":invocation_status_go_proto",
        ":stat_filter_go_proto",
    ],
)

go_proto_library(
    name = "invocation_status_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/invocation_status",
    proto = ":invocation_status_proto",
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
        ":auditlog_go_proto",
        ":bazel_config_go_proto",
        ":cache_go_proto",
        ":encryption_go_proto",
        ":eventlog_go_proto",
        ":execution_stats_go_proto",
        ":github_go_proto",
        ":group_go_proto",
        ":invocation_go_proto",
        ":quota_go_proto",
        ":repo_go_proto",
        ":resource_go_proto",
        ":runner_go_proto",
        ":scheduler_go_proto",
        ":secrets_go_proto",
        ":stats_go_proto",
        ":suggestion_go_proto",
        ":target_go_proto",
        ":usage_go_proto",
        ":user_go_proto",
        ":workflow_go_proto",
        ":zip_go_proto",
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
        "@org_golang_google_genproto_googleapis_rpc//status",
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
        "@org_golang_google_genproto//googleapis/longrunning",
        "@org_golang_google_genproto_googleapis_api//annotations",
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)

go_proto_library(
    name = "remote_execution_log_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/remote_execution_log",
    proto = ":remote_execution_log_proto",
    deps = [
        ":remote_execution_go_proto",
        "@org_golang_google_genproto//googleapis/longrunning",
        "@org_golang_google_genproto_googleapis_api//annotations",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_genproto_googleapis_rpc//status",
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
        "@org_golang_google_genproto//googleapis/longrunning",
        "@org_golang_google_genproto_googleapis_api//annotations",
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)

go_proto_library(
    name = "repo_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/repo",
    proto = ":repo_proto",
    deps = [
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "telemetry_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/telemetry",
    proto = ":telemetry_proto",
    deps = [
        "@org_golang_google_genproto_googleapis_rpc//status",
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
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)

go_proto_library(
    name = "suggestion_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/suggestion",
    proto = ":suggestion_proto",
    deps = [
        ":context_go_proto",
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

go_proto_library(
    name = "stored_invocation_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation",
    proto = ":stored_invocation_proto",
)

go_proto_library(
    name = "zip_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/zip",
    proto = ":zip_proto",
    deps = [
        ":context_go_proto",
        ":remote_execution_go_proto",
    ],
)

go_proto_library(
    name = "encryption_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/encryption",
    proto = ":encryption_proto",
    deps = [
        ":context_go_proto",
    ],
)

go_proto_library(
    name = "metrics_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/metrics",
    proto = ":metrics_proto",
    deps = [
        "@com_github_prometheus_client_model//go",
    ],
)

# TypeScript proto rules below here

ts_proto_library(
    name = "acl_ts_proto",
    proto = ":acl_proto",
    deps = [
        ":user_id_ts_proto",
    ],
)

ts_proto_library(
    name = "api_key_ts_proto",
    proto = ":api_key_proto",
    deps = [
        ":context_ts_proto",
    ],
)

ts_proto_library(
    name = "auditlog_ts_proto",
    proto = ":auditlog_proto",
    deps = [
        ":api_key_ts_proto",
        ":context_ts_proto",
        ":encryption_ts_proto",
        ":github_ts_proto",
        ":group_ts_proto",
        ":invocation_ts_proto",
        ":secrets_ts_proto",
        ":timestamp_ts_proto",
        ":workflow_ts_proto",
    ],
)

ts_proto_library(
    name = "bazel_config_ts_proto",
    proto = ":bazel_config_proto",
    deps = [
        ":api_key_ts_proto",
        ":context_ts_proto",
    ],
)

ts_proto_library(
    name = "config_ts_proto",
    proto = ":config_proto",
)

ts_proto_library(
    name = "execution_stats_ts_proto",
    proto = ":execution_stats_proto",
    deps = [
        ":acl_ts_proto",
        ":context_ts_proto",
        ":grpc_status_ts_proto",
        ":invocation_status_ts_proto",
        ":remote_execution_ts_proto",
        ":scheduler_ts_proto",
        ":stat_filter_ts_proto",
        ":timestamp_ts_proto",
    ],
)

ts_proto_library(
    name = "github_ts_proto",
    proto = ":github_proto",
    deps = [
        ":context_ts_proto",
    ],
)

ts_proto_library(
    name = "package_load_metrics_ts_proto",
    proto = ":package_load_metrics_proto",
    deps = [
        ":duration_ts_proto",
    ],
)

ts_proto_library(
    name = "quota_ts_proto",
    proto = ":quota_proto",
    deps = [
        ":context_ts_proto",
        ":duration_ts_proto",
    ],
)

ts_proto_library(
    name = "resource_ts_proto",
    proto = ":resource_proto",
    deps = [
        ":remote_execution_ts_proto",
    ],
)

ts_proto_library(
    name = "secrets_ts_proto",
    proto = ":secrets_proto",
    deps = [
        ":context_ts_proto",
    ],
)

ts_proto_library(
    name = "stat_filter_ts_proto",
    proto = ":stat_filter_proto",
)

ts_proto_library(
    name = "stats_ts_proto",
    proto = ":stats_proto",
    deps = [
        ":context_ts_proto",
        ":duration_ts_proto",
        ":invocation_status_ts_proto",
        ":stat_filter_ts_proto",
        ":timestamp_ts_proto",
    ],
)

ts_proto_library(
    name = "scheduler_ts_proto",
    proto = ":scheduler_proto",
    deps = [
        ":acl_ts_proto",
        ":context_ts_proto",
        ":grpc_status_ts_proto",
        ":timestamp_ts_proto",
        ":trace_ts_proto",
    ],
)

ts_proto_library(
    name = "trace_ts_proto",
    proto = ":trace_proto",
)

ts_proto_library(
    name = "suggestion_ts_proto",
    proto = ":suggestion_proto",
    deps = [
        ":context_ts_proto",
    ],
)

ts_proto_library(
    name = "failure_details_ts_proto",
    proto = ":failure_details_proto",
    deps = [
        ":descriptor_ts_proto",
    ],
)

ts_proto_library(
    name = "build_event_stream_ts_proto",
    proto = ":build_event_stream_proto",
    deps = [
        ":command_line_ts_proto",
        ":descriptor_ts_proto",
        ":duration_ts_proto",
        ":failure_details_ts_proto",
        ":invocation_policy_ts_proto",
        ":package_load_metrics_ts_proto",
        ":timestamp_ts_proto",
    ],
)

ts_proto_library(
    name = "build_status_ts_proto",
    proto = ":build_status_proto",
    deps = [
        ":any_ts_proto",
        ":wrappers_ts_proto",
    ],
)

ts_proto_library(
    name = "target_ts_proto",
    proto = ":target_proto",
    deps = [
        ":context_ts_proto",
        "//proto/api/v1:common_ts_proto",
    ],
)

ts_proto_library(
    name = "eventlog_ts_proto",
    proto = ":eventlog_proto",
    deps = [
        ":context_ts_proto",
    ],
)

ts_proto_library(
    name = "workflow_ts_proto",
    proto = ":workflow_proto",
    deps = [
        ":context_ts_proto",
        ":duration_ts_proto",
        ":grpc_status_ts_proto",
        ":invocation_status_ts_proto",
    ],
)

ts_proto_library(
    name = "command_line_ts_proto",
    proto = ":command_line_proto",
    deps = [
        ":option_filters_ts_proto",
    ],
)

ts_proto_library(
    name = "option_filters_ts_proto",
    proto = ":option_filters_proto",
)

ts_proto_library(
    name = "context_ts_proto",
    proto = ":context_proto",
    deps = [
        ":user_id_ts_proto",
    ],
)

ts_proto_library(
    name = "invocation_status_ts_proto",
    proto = ":invocation_status_proto",
)

ts_proto_library(
    name = "invocation_ts_proto",
    proto = ":invocation_proto",
    deps = [
        ":acl_ts_proto",
        ":api_key_ts_proto",
        ":build_event_stream_ts_proto",
        ":cache_ts_proto",
        ":command_line_ts_proto",
        ":context_ts_proto",
        ":duration_ts_proto",
        ":invocation_status_ts_proto",
        ":stat_filter_ts_proto",
        ":timestamp_ts_proto",
    ],
)

ts_proto_library(
    name = "invocation_policy_ts_proto",
    proto = ":invocation_policy_proto",
    deps = [],
)

ts_proto_library(
    name = "usage_ts_proto",
    proto = ":usage_proto",
    deps = [
        ":context_ts_proto",
    ],
)

ts_proto_library(
    name = "group_ts_proto",
    proto = ":group_proto",
    deps = [
        ":context_ts_proto",
        ":user_id_ts_proto",
    ],
)

ts_proto_library(
    name = "grpc_code_ts_proto",
    proto = "@googleapis//google/rpc:code_proto",
)

ts_proto_library(
    name = "grpc_status_ts_proto",
    proto = "@googleapis//google/rpc:status_proto",
    deps = [":any_ts_proto"],
)

ts_proto_library(
    name = "google_api_annotations_ts_proto",
    proto = "@googleapis//google/api:annotations_proto",
    deps = [
        ":descriptor_ts_proto",
        ":duration_ts_proto",
    ],
)

ts_proto_library(
    name = "google_longrunning_ts_proto",
    proto = "@googleapis//google/longrunning:operations_proto",
    deps = [
        ":any_ts_proto",
        ":duration_ts_proto",
        ":empty_ts_proto",
        ":grpc_status_ts_proto",
    ],
)

ts_proto_library(
    name = "empty_ts_proto",
    proto = "@com_google_protobuf//:empty_proto",
)

ts_proto_library(
    name = "timestamp_ts_proto",
    proto = "@com_google_protobuf//:timestamp_proto",
)

ts_proto_library(
    name = "descriptor_ts_proto",
    proto = "@com_google_protobuf//:descriptor_proto",
)

ts_proto_library(
    name = "duration_ts_proto",
    proto = "@com_google_protobuf//:duration_proto",
)

ts_proto_library(
    name = "any_ts_proto",
    proto = "@com_google_protobuf//:any_proto",
)

ts_proto_library(
    name = "wrappers_ts_proto",
    proto = "@com_google_protobuf//:wrappers_proto",
)

ts_proto_library(
    name = "semver_ts_proto",
    proto = ":semver_proto",
)

ts_proto_library(
    name = "field_mask_ts_proto",
    proto = "@com_google_protobuf//:field_mask_proto",
)

ts_proto_library(
    name = "cache_ts_proto",
    proto = ":cache_proto",
    deps = [
        ":context_ts_proto",
        ":duration_ts_proto",
        ":field_mask_ts_proto",
        ":grpc_status_ts_proto",
        ":remote_execution_ts_proto",
        ":resource_ts_proto",
        ":timestamp_ts_proto",
    ],
)

ts_proto_library(
    name = "user_id_ts_proto",
    proto = ":user_id_proto",
)

ts_proto_library(
    name = "user_ts_proto",
    proto = ":user_proto",
    deps = [
        ":context_ts_proto",
        ":group_ts_proto",
        ":user_id_ts_proto",
    ],
)

ts_proto_library(
    name = "encryption_ts_proto",
    proto = ":encryption_proto",
    deps = [":context_ts_proto"],
)

ts_proto_library(
    name = "buildbuddy_service_ts_proto",
    proto = ":buildbuddy_service_proto",
    deps = [
        ":api_key_ts_proto",
        ":auditlog_ts_proto",
        ":bazel_config_ts_proto",
        ":cache_ts_proto",
        ":encryption_ts_proto",
        ":eventlog_ts_proto",
        ":execution_stats_ts_proto",
        ":github_ts_proto",
        ":group_ts_proto",
        ":invocation_ts_proto",
        ":quota_ts_proto",
        ":repo_ts_proto",
        ":runner_ts_proto",
        ":scheduler_ts_proto",
        ":secrets_ts_proto",
        ":stats_ts_proto",
        ":suggestion_ts_proto",
        ":target_ts_proto",
        ":usage_ts_proto",
        ":user_ts_proto",
        ":workflow_ts_proto",
        ":zip_ts_proto",
    ],
)

ts_proto_library(
    name = "repo_ts_proto",
    proto = ":repo_proto",
    deps = [
        ":context_ts_proto",
    ],
)

ts_proto_library(
    name = "remote_execution_ts_proto",
    proto = ":remote_execution_proto",
    deps = [
        ":any_ts_proto",
        ":duration_ts_proto",
        ":google_api_annotations_ts_proto",
        ":google_longrunning_ts_proto",
        ":grpc_status_ts_proto",
        ":scheduler_ts_proto",
        ":semver_ts_proto",
        ":timestamp_ts_proto",
        ":wrappers_ts_proto",
    ],
)

ts_proto_library(
    name = "runner_ts_proto",
    proto = ":runner_proto",
    deps = [
        ":context_ts_proto",
        ":firecracker_ts_proto",
        ":remote_execution_ts_proto",
    ],
)

ts_proto_library(
    name = "firecracker_ts_proto",
    proto = ":firecracker_proto",
    deps = [
        ":remote_execution_ts_proto",
    ],
)

ts_proto_library(
    name = "zip_ts_proto",
    proto = ":zip_proto",
    deps = [":context_ts_proto"],
)
