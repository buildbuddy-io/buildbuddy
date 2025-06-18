load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "watcher",
    srcs = ["watcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/watcher",
    deps = [
        "//cli/log",
        "//cli/workspace",
        "@com_github_bduffany_godemon//:godemon",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
