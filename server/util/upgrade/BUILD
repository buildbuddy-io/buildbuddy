load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "upgrade",
    srcs = ["upgrade.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/upgrade",
    deps = [
        "//proto:upgrade_go_proto",
        "//server/util/status",
        "@com_github_masterminds_semver_v3//:semver",
    ],
)

go_test(
    name = "upgrade_test",
    srcs = ["upgrade_test.go"],
    embed = [":upgrade"],
    deps = [
        "//proto:upgrade_go_proto",
        "@com_github_masterminds_semver_v3//:semver",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
