load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["test_environment.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/test_environment",
    visibility = ["//visibility:public"],
    deps = [
        "//server/backends/memory_cache:go_default_library",
        "//server/config:go_default_library",
        "//server/real_environment:go_default_library",
        "//server/util/healthcheck:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//test/bufconn:go_default_library",
    ],
)
