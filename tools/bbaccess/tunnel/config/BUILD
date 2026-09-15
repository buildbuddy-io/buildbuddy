load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config",
    visibility = ["//visibility:public"],
    deps = ["@in_gopkg_yaml_v3//:yaml_v3"],
)

go_test(
    name = "config_test",
    srcs = ["config_test.go"],
    embed = [":config"],
    deps = ["@com_github_stretchr_testify//require"],
)
