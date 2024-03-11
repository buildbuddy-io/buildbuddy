load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "userdb",
    srcs = ["userdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb",
    deps = [
        "//enterprise/server/util/keystore",
        "//proto:api_key_go_proto",
        "//proto:group_go_proto",
        "//proto:telemetry_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/capabilities",
        "//server/util/db",
        "//server/util/log",
        "//server/util/query_builder",
        "//server/util/role",
        "//server/util/status",
        "//server/util/subdomain",
    ],
)

go_test(
    name = "userdb_test",
    size = "small",
    srcs = ["userdb_test.go"],
    deps = [
        ":userdb",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:api_key_go_proto",
        "//proto:auditlog_go_proto",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauditlog",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/capabilities",
        "//server/util/claims",
        "//server/util/role",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "userdb_test_mysql",
    srcs = ["userdb_test.go"],
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
    shard_count = 5,
    tags = ["docker"],
    deps = [
        ":userdb",
        "//enterprise/server/auditlog",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:api_key_go_proto",
        "//proto:auditlog_go_proto",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauditlog",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/capabilities",
        "//server/util/claims",
        "//server/util/perms",
        "//server/util/role",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "userdb_test_postgres",
    srcs = ["userdb_test.go"],
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
    shard_count = 5,
    tags = ["docker"],
    deps = [
        ":userdb",
        "//enterprise/server/auditlog",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:api_key_go_proto",
        "//proto:auditlog_go_proto",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauditlog",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/capabilities",
        "//server/util/claims",
        "//server/util/perms",
        "//server/util/role",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
