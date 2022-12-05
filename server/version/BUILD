load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "version",
    srcs = ["version.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/version",
    visibility = ["//visibility:public"],
    x_defs = {
        "commitSha": "{STABLE_COMMIT_SHA}",
        "versionTag": "{STABLE_VERSION_TAG}",
    },
    deps = [
        "//server/util/log",
    ],
)
