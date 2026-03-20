load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ip_rules_service",
    srcs = ["ip_rules_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/ip_rules_service",
    deps = [
        "//proto:iprules_go_proto",
        "//proto:server_notification_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/clientip",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "ip_rules_service_test",
    srcs = ["ip_rules_service_test.go"],
    deps = [
        ":ip_rules_service",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:context_go_proto",
        "//proto:iprules_go_proto",
        "//proto:server_notification_go_proto",
        "//server/environment",
        "//server/testutil/testauth",
        "//server/util/clientip",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)
