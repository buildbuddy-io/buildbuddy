load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "redis_key_val_store",
    srcs = ["redis_key_val_store.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/redis_key_val_store",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)
