load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "option_definitions",
    srcs = ["option_definitions.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/watcher/option_definitions",
    visibility = ["//visibility:public"],
    deps = ["//cli/parser/options"],
)

package(default_visibility = ["//cli:__subpackages__"])
