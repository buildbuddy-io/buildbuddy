load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "version",
    srcs = ["version.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/version",
    visibility = ["//visibility:public"],
    x_defs = {
        "version": "{VERSION}",
        "commitSha": "{COMMIT_SHA}",
    },
    deps = ["//server/util/log"],
)
