load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "translate",
    srcs = ["translate.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/translate",
    visibility = ["//visibility:public"],
    deps = ["//cli/translate/js"],
)

go_test(
    name = "translate_test",
    srcs = ["translate_test.go"],
    embed = [":translate"],
)
