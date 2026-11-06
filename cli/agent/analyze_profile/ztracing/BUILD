load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "ztracing",
    srcs = ["ztracing.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/agent/analyze_profile/ztracing",
    deps = [
        "//cli/log",
        "//cli/storage",
        "//cli/terminal",
    ],
)

go_test(
    name = "ztracing_test",
    srcs = ["ztracing_test.go"],
    embed = [":ztracing"],
    deps = [
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
