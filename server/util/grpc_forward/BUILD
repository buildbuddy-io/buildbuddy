load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "grpc_forward",
    srcs = ["grpc_forward.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/grpc_forward",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/status",
        "@com_github_mwitkow_grpc_proxy//proxy",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "grpc_forward_test",
    srcs = ["grpc_forward_test.go"],
    embed = [":grpc_forward"],
    deps = [
        "//server/util/grpc_client",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
    ],
)
