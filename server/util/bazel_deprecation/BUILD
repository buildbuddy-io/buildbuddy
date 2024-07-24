load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bazel_deprecation",
    srcs = ["bazel_deprecation.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazel_deprecation",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/util/bazel_request",
        "//server/util/claims",
        "//server/util/flag",
        "//server/util/status",
        "@com_github_logrusorgru_aurora//:aurora",
    ],
)
