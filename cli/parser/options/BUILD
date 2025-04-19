load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "options",
    srcs = ["options.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/options",
    visibility = ["//visibility:public"],
    deps = ["//proto:bazel_flags_go_proto"],
)

package(default_visibility = ["//cli:__subpackages__"])
