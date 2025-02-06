load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "storage",
    srcs = ["storage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/storage",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/workspace",
        "//server/util/hash",
        "//server/util/uuid",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "storage_test",
    srcs = ["storage_test.go"],
    embed = [":storage"],
    deps = ["@com_github_stretchr_testify//require"],
)
