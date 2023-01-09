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
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/random",
        "//server/util/role",
        "//server/util/status",
    ],
)

go_test(
    name = "userdb_test",
    size = "small",
    srcs = ["userdb_test.go"],
    deps = [
        ":userdb",
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
