load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "disk",
    srcs = ["disk.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/disk",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/random",
        "//server/util/status",
    ],
)
