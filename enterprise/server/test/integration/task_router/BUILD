load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "task_router_test",
    size = "small",
    srcs = ["task_router_test.go"],
    deps = [
        "//enterprise/server/scheduling/task_router",
        "//enterprise/server/testutil/enterprise_testenv",
        "//enterprise/server/testutil/testredis",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/testutil/testauth",
        "@com_github_stretchr_testify//require",
    ],
)
