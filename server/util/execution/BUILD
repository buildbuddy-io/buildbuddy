load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "execution",
    srcs = ["execution.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/execution",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:execution_stats_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/build_event_protocol/invocation_format",
        "//server/remote_cache/digest",
        "//server/tables",
        "//server/util/clickhouse/schema",
        "//server/util/proto",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
