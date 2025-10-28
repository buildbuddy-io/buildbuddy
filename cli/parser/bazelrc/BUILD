load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "bazelrc",
    srcs = ["bazelrc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "//server/util/lib/set",
        "//server/util/shlex",
    ],
)
