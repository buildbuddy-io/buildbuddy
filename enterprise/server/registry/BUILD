load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "registry",
    srcs = [
        "github_registry.go",
        "registry_server.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/registry",
    deps = [
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/log",
    ],
)

go_test(
    name = "registry_test",
    srcs = ["github_registry_test.go"],
    embed = [":registry"],
    deps = ["@com_github_stretchr_testify//assert"],
)
