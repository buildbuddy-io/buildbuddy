load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "oomkiller",
    srcs = ["oomkiller.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor/oomkiller",
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        "//enterprise/server/remote_execution/cgroup",
        "//proto:remote_execution_go_proto",
        "//server/metrics",
        "//server/util/alert",
        "//server/util/flag",
        "//server/util/log",
        "@com_github_elastic_gosigar//:gosigar",
    ],
)

go_test(
    name = "oomkiller_test",
    srcs = ["oomkiller_test.go"],
    embed = [":oomkiller"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
