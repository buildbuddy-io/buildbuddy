load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "iprules",
    srcs = ["iprules.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/iprules",
    deps = [
        "//proto:iprules_go_proto",
        "//proto:server_notification_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/tables",
        "//server/util/alert",
        "//server/util/authutil",
        "//server/util/clientip",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/perms",
        "//server/util/role",
        "//server/util/status",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

go_test(
    name = "iprules_test",
    srcs = ["iprules_test.go"],
    deps = [
        ":iprules",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:context_go_proto",
        "//proto:iprules_go_proto",
        "//server/environment",
        "//server/testutil/testauth",
        "//server/util/clientip",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
