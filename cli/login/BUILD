load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "login",
    srcs = ["login.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/login",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/parser",
        "//cli/storage",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
