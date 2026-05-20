load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "usagealert_lib",
    srcs = ["usagealert.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/usagealert",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/backends/email",
        "//enterprise/server/usage_service",
        "//proto:group_go_proto",
        "//proto:usage_go_proto",
        "//server/config",
        "//server/endpoint_urls/build_buddy_url",
        "//server/interfaces",
        "//server/real_environment",
        "//server/tables",
        "//server/util/clickhouse",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/role",
        "//server/util/status",
        "//server/util/subdomain",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_client_golang//prometheus/push",
    ],
)

go_binary(
    name = "usagealert",
    embed = [":usagealert_lib"],
    visibility = ["//visibility:public"],
)

go_test(
    name = "usagealert_test",
    srcs = ["usagealert_test.go"],
    embed = [":usagealert_lib"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse25.3",
        "test.EstimatedComputeUnits": "4",
    },
    shard_count = 4,
    tags = ["docker"],
    deps = [
        "//enterprise/server/backends/email",
        "//enterprise/server/usage_service",
        "//proto:group_go_proto",
        "//proto:usage_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/usage/sku",
        "//server/util/clickhouse/schema",
        "//server/util/role",
        "//server/util/testing/flags",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_client_golang//prometheus/testutil",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
