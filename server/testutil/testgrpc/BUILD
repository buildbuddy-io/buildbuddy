load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testgrpc",
    testonly = 1,
    srcs = ["testgrpc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testgrpc",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/status",
        "@com_github_mwitkow_grpc_proxy//proxy",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//test/bufconn",
    ],
)
