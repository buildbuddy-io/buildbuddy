load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "stats",
    srcs = ["stats.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/stats",
    deps = [
        "//proto:stats_go_proto",
        "//server/util/flag",
    ],
)

go_test(
    name = "stats_test",
    srcs = ["stats_test.go"],
    embed = [":stats"],
    deps = [
        "//proto:stats_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
