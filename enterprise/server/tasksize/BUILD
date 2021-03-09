load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["tasksize.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize",
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
    ],
)
