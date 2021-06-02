load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "telemetry",
    srcs = ["telemetry_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/telemetry",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:telemetry_go_proto",
        "//server/environment",
        "//server/rpc/filters",
        "//server/tables",
        "//server/util/db",
        "//server/util/log",
        "@com_github_grpc_ecosystem_go_grpc_prometheus//:go-grpc-prometheus",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//codes",
    ],
)
