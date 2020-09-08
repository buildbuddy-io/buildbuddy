load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["version.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/version",
    visibility = ["//visibility:public"],
    x_defs = {
        "commitSha": "{COMMIT_SHA}",
    },
    deps = ["@io_bazel_rules_go//go/tools/bazel:go_default_library"],
)
