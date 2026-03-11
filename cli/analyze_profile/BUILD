load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "analyze_profile",
    srcs = ["analyze_profile.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/analyze_profile",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/timing_profile",
    ],
)
