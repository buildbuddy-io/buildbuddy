load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "tunnelconfig",
    srcs = ["tunnelconfig.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig",
    visibility = ["//visibility:public"],
    deps = ["@in_gopkg_yaml_v3//:yaml_v3"],
)

go_test(
    name = "tunnelconfig_test",
    srcs = ["tunnelconfig_test.go"],
    embed = [":tunnelconfig"],
    deps = ["@com_github_stretchr_testify//require"],
)
