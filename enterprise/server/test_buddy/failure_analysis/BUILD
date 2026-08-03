load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "failure_analysis",
    srcs = ["failure_analysis.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/test_buddy/failure_analysis",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/backends/openai",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
    ],
)

go_test(
    name = "failure_analysis_test",
    srcs = ["failure_analysis_test.go"],
    deps = [
        ":failure_analysis",
        "//server/environment",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
