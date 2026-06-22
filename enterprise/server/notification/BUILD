load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "notification",
    srcs = ["notification.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/notification",
    deps = [
        "//enterprise/server/backends/email",
        "//proto:capability_go_proto",
        "//proto:group_go_proto",
        "//proto:notification_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/authutil",
        "//server/util/db",
        "//server/util/lib/set",
        "//server/util/role",
        "//server/util/status",
    ],
)

go_test(
    name = "notification_test",
    srcs = ["notification_test.go"],
    embed = [":notification"],
    deps = [
        "//enterprise/server/backends/email",
        "//proto:capability_go_proto",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:notification_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/claims",
        "//server/util/role",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
