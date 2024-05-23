load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "workflow_lib",
    srcs = ["workflow.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/probers/workflow",
    visibility = ["//visibility:private"],
    deps = [
        "//proto/api/v1:api_v1_go_proto",
        "//server/util/grpc_client",
        "//server/util/log",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "workflow",
    embed = [":workflow_lib"],
    visibility = ["//visibility:public"],
)
