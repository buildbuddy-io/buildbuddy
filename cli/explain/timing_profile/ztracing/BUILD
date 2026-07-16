load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "ztracing",
    srcs = ["ztracing.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/explain/timing_profile/ztracing",
    deps = [
        "//cli/log",
        "//cli/storage",
    ],
)
