load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "authdb",
    srcs = ["authdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb",
    deps = [
        "//proto:group_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/util/db",
        "//server/util/query_builder",
        "//server/util/status",
    ],
)

go_test(
    name = "authdb_test",
    srcs = ["authdb_test.go"],
    deps = [
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//server/environment",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/util/db",
        "//server/util/role",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
