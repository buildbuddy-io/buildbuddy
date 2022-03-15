load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "download",
    srcs = ["download.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/download",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/logging",
        "//server/util/status",
        "@com_github_google_go_github_v43//github",
    ],
)
