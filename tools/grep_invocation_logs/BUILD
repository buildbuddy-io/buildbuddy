load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "grep_invocation_logs_lib",
    srcs = ["grep_invocation_logs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/grep_invocation_logs",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//server/util/canary",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "grep_invocation_logs",
    embed = [":grep_invocation_logs_lib"],
    visibility = ["//visibility:public"],
)
