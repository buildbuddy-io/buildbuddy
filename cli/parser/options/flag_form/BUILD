load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "flag_form",
    srcs = ["flag_form.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/options/flag_form",
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "flag_form_test",
    srcs = ["flag_form_test.go"],
    deps = [
        ":flag_form",
        "@com_github_stretchr_testify//assert",
    ],
)
