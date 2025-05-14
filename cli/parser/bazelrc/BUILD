load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bazelrc",
    srcs = ["bazelrc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "//server/util/shlex",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
