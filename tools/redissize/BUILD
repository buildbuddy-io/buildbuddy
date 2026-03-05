load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "redissize_lib",
    srcs = ["redissize.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/redissize",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "@com_github_docker_go_units//:go-units",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_google_uuid//:uuid",
    ],
)

go_binary(
    name = "redissize",
    embed = [":redissize_lib"],
    visibility = ["//visibility:public"],
)
