load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "invocationdb",
    srcs = ["invocationdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:acl_go_proto",
        "//proto:api_key_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:telemetry_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/capabilities",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/retry",
        "//server/util/status",
        "@io_gorm_gorm//:gorm",
        "@io_gorm_gorm//clause",
    ],
)

go_test(
    name = "invocationdb_test",
    size = "small",
    srcs = ["invocationdb_test.go"],
    deps = [
        ":invocationdb",
        "//proto:invocation_status_go_proto",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/util/db",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "invocationdb_test_mysql",
    srcs = ["invocationdb_test.go"],
    args = [
        "--testenv.database_type=mysql",
        "--testenv.reuse_server",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        # We don't want different different db tests to be assigned to the samed
        # recycled runner, because we can't fit all db docker images with the
        # default disk limit.
        "test.runner-recycling-key": "mysql",
    },
    tags = ["docker"],
    deps = [
        ":invocationdb",
        "//proto:invocation_status_go_proto",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/util/db",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "invocationdb_test_postgres",
    srcs = ["invocationdb_test.go"],
    args = [
        "--testenv.database_type=postgres",
        "--testenv.reuse_server",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        # We don't want different different db tests to be assigned to the samed
        # recycled runner, because we can't fit all db docker images with the
        # default disk limit.
        "test.runner-recycling-key": "postgres",
    },
    tags = ["docker"],
    deps = [
        ":invocationdb",
        "//proto:invocation_status_go_proto",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/util/db",
        "@com_github_stretchr_testify//require",
    ],
)
