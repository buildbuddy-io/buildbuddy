load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "timing_profile",
    srcs = ["timing_profile.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/timing_profile",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/trace_events",
    ],
)
