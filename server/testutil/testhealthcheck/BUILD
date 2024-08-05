load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testhealthcheck",
    srcs = ["testhealthcheck.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testhealthcheck",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/status",
        "@org_golang_google_grpc//health/grpc_health_v1",
    ],
)
