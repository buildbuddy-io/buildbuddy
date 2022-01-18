load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "devnull",
    srcs = ["devnull.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/devnull",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:publish_build_event_go_proto",
        "//server/interfaces",
    ],
)
