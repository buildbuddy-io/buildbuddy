load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "scim",
    srcs = ["scim.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scim",
    deps = ["//server/environment"],
)

go_test(
    name = "scim_test",
    size = "small",
    srcs = ["scim_test.go"],
    embed = [":scim"],
)
