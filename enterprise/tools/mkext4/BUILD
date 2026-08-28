load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "mkext4",
    embed = [":mkext4_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "mkext4_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/mkext4",
    deps = ["//enterprise/server/util/ext4writer"],
)
