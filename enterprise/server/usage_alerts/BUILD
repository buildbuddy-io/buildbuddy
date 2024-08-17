load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "usage_alerts",
    srcs = ["usage_alerts.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_alerts",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/notifications",
        "//enterprise/server/notifications/email",
        "//enterprise/server/util/redisutil",
        "//proto:notification_go_proto",
        "//proto:usage_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/interfaces",
        "//server/tables",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "//server/util/subdomain",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

go_test(
    name = "usage_alerts_test",
    srcs = ["usage_alerts_test.go"],
    deps = [
        ":usage_alerts",
        "//enterprise/server/notifications",
        "//enterprise/server/testutil/testredis",
        "//proto:notification_go_proto",
        "//proto:usage_go_proto",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/util/hash",
        "@com_github_google_go_cmp//cmp",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
