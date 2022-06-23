load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "executor",
    srcs = ["executor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/auth",
        "//enterprise/server/remote_execution/operation",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/alert",
        "//server/util/background",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "//server/util/uuid",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
