load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "bb_analyze_profile_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/cmd/bb_analyze_profile",
    visibility = ["//visibility:private"],
    deps = [
        "//cli/analyze_profile",
    ],
)

go_binary(
    name = "bb-analyze-profile",
    embed = [":bb_analyze_profile_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)
