load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "storage",
    srcs = ["storage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/storage",
)

go_test(
    name = "storage_test",
    srcs = ["storage_test.go"],
    embed = [":storage"],
    deps = [
        "//server/testutil/testgit",
        "@com_github_stretchr_testify//require",
    ],
)
