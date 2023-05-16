load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "js",
    srcs = ["js.go"],
    embedsrcs = ["translate.js"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/translate/js",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/translate/builtins",
        "@com_github_dop251_goja//:goja",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "js_test",
    srcs = ["js_test.go"],
    embed = [":js"],
    deps = ["@com_github_stretchr_testify//assert"],
)
