load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "task_router_test",
    size = "small",
    srcs = ["task_router_test.go"],
    visibility = ["//visibility:public"],
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
