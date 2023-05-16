load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "translate",
    srcs = ["translate.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/translate",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/translate/yaml",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
