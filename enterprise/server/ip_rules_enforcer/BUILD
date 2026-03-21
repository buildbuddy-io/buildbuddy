load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ip_rules_enforcer",
    srcs = ["ip_rules_enforcer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/ip_rules_enforcer",
    deps = [
        "//proto:context_go_proto",
        "//proto:iprules_go_proto",
        "//proto:server_notification_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/tables",
        "//server/util/alert",
        "//server/util/clientip",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/proto",
        "//server/util/status",
        "//third_party/singleflight",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "ip_rules_enforcer_test",
    srcs = ["ip_rules_enforcer_test.go"],
    deps = [
        ":ip_rules_enforcer",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:iprules_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testgrpc",
        "//server/util/authutil",
        "//server/util/clientip",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
        "@org_uber_go_goleak//:goleak",
    ],
)
