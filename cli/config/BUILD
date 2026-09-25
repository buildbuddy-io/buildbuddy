load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/config",
    deps = [
        "//cli/log",
        "@com_github_docker_go_units//:go-units",
        "@in_gopkg_yaml_v2//:yaml_v2",
    ],
)

go_test(
    name = "config_test",
    srcs = ["config_test.go"],
    embed = [":config"],
)
