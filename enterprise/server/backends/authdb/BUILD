load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "authdb",
    srcs = ["authdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb",
    deps = [
        "//proto:api_key_go_proto",
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/capabilities",
        "//server/util/db",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/random",
        "//server/util/role",
        "//server/util/status",
        "@org_golang_x_crypto//chacha20",
    ],
)

go_test(
    name = "authdb_test",
    srcs = ["authdb_test.go"],
    deps = [
        ":authdb",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//server/environment",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/util/db",
        "//server/util/role",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "authdb_test_mysql",
    srcs = ["authdb_test.go"],
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
        ":authdb",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//server/environment",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/util/db",
        "//server/util/role",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "authdb_test_postgres",
    srcs = ["authdb_test.go"],
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
        ":authdb",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//server/environment",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/util/db",
        "//server/util/role",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
