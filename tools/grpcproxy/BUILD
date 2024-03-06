load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "grpcproxy_lib",
    srcs = ["grpcproxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/grpcproxy",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "@com_github_mwitkow_grpc_proxy//proxy",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "grpcproxy",
    embed = [":grpcproxy_lib"],
    visibility = ["//visibility:public"],
)
