load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["real_environment.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/real_environment",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/build_event_protocol/build_event_proxy:go_default_library",
        "//server/config:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/util/db:go_default_library",
        "//server/util/healthcheck:go_default_library",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
    ],
)
