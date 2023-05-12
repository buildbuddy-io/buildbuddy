load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "shortcuts",
    srcs = ["shortcuts.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/shortcuts",
    visibility = ["//visibility:public"],
    deps = ["//cli/arg"],
)

package(default_visibility = ["//cli:__subpackages__"])
