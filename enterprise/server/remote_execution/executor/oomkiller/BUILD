load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "oomkiller",
    srcs = ["oomkiller.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor/oomkiller",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/metrics",
        "//server/resources",
        "//server/util/alert",
        "//server/util/flag",
        "//server/util/log",
    ],
)

go_test(
    name = "oomkiller_test",
    srcs = ["oomkiller_test.go"],
    embed = [":oomkiller"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
