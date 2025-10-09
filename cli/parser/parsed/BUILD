load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "parsed",
    srcs = ["parsed.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/parsed",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "//cli/parser/arguments",
        "//cli/parser/bazelrc",
        "//cli/parser/options",
        "//server/util/lib/set",
    ],
)
