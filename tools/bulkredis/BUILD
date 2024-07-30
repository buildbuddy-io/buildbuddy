load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "bulkredis_lib",
    srcs = ["bulkredis.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bulkredis",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

go_binary(
    name = "bulkredis",
    embed = [":bulkredis_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)
