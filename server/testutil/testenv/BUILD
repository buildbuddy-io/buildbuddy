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
        "//server/nullauth",
        "//server/real_environment",
        "//server/rpc/interceptors",
        "//server/testutil/testfs",
        "//server/testutil/testmysql",
        "//server/util/db",
        "//server/util/grpc_client",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/testing/flags",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//test/bufconn",
    ],
)
