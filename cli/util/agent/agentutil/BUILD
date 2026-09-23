load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "agentutil",
    srcs = ["agentutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/util/agent/agentutil",
    visibility = ["//visibility:public"],
    deps = ["@com_github_armon_circbuf//:circbuf"],
)

go_test(
    name = "agentutil_test",
    srcs = ["agentutil_test.go"],
    embed = [":agentutil"],
    deps = [
        "@com_github_armon_circbuf//:circbuf",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
