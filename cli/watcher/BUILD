load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "watcher",
    srcs = ["watcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/watcher",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/workspace",
        "//server/util/status",
        "@com_github_bduffany_godemon//:godemon",
        "@com_github_google_shlex//:shlex",
    ],
)
