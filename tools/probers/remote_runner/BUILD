load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "remote_runner_lib",
    srcs = ["remote_runner.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/probers/remote_runner",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:git_go_proto",
        "//proto:invocation_go_proto",
        "//proto:runner_go_proto",
        "//server/util/grpc_client",
        "//server/util/log",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "remote_runner",
    embed = [":remote_runner_lib"],
    visibility = ["//visibility:public"],
)
