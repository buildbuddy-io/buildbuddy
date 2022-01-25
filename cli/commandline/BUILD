load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "commandline",
    srcs = ["commandline.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/commandline",
    visibility = ["//visibility:public"],
    deps = ["@com_github_jessevdk_go_flags//:go-flags"],
)
