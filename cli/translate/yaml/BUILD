load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "yaml",
    srcs = ["yaml.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/translate/yaml",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/add",
        "//cli/log",
        "//cli/translate/builtins",
        "@in_gopkg_yaml_v2//:yaml_v2",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "yaml_test",
    srcs = ["yaml_test.go"],
    embed = [":yaml"],
    deps = ["@com_github_stretchr_testify//assert"],
)
