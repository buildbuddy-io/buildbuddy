load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["environment.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/environment",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:publish_build_event_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/config:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/util/db:go_default_library",
        "@com_github_go_redis_redis_v8//:go_default_library",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
    ],
)
