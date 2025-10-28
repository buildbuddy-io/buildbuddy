load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "devnull",
    srcs = ["devnull.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/devnull",
    deps = [
        "//proto:publish_build_event_go_proto",
        "//server/interfaces",
    ],
)
