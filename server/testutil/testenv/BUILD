load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testenv",
    testonly = 1,
    srcs = ["testenv.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testenv",
    visibility = ["//visibility:public"],
    deps = [
        "//server/backends/blobstore",
        "//server/backends/invocationdb",
        "//server/backends/memory_cache",
        "//server/config",
        "//server/real_environment",
        "//server/rpc/filters",
        "//server/testutil/healthcheck",
        "//server/util/db",
        "//server/util/grpc_client",
        "//server/util/log",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//test/bufconn",
    ],
)
