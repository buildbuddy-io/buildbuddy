load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["interfaces.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/interfaces",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:execution_stats_go_proto",
        "//proto:invocation_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//proto:telemetry_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//server/tables:go_default_library",
        "@go_googleapis//google/longrunning:longrunning_go_proto",
    ],
)
