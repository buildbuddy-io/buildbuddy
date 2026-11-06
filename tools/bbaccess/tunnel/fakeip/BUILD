load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "fakeip",
    srcs = ["fakeip.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/fakeip",
    visibility = ["//visibility:public"],
)

go_test(
    name = "fakeip_test",
    srcs = ["fakeip_test.go"],
    embed = [":fakeip"],
    deps = ["@com_github_stretchr_testify//require"],
)
