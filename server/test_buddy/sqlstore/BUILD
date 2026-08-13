load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "sqlstore",
    srcs = ["sqlstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/test_buddy/sqlstore",
    visibility = ["//visibility:public"],
    deps = ["//server/interfaces"],
)

go_test(
    name = "sqlstore_test",
    srcs = ["sqlstore_test.go"],
    deps = [
        ":sqlstore",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "sqlstore_test_mysql",
    srcs = ["sqlstore_test.go"],
    args = [
        "--testenv.database_type=mysql",
        "--testenv.reuse_server",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "mysql",
    },
    tags = ["docker"],
    deps = [
        ":sqlstore",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
    ],
)
