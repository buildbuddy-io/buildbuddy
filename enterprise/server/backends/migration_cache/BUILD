load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "migration_cache",
    srcs = [
        "config.go",
        "migration_cache.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/migration_cache",
    deps = [
        "//enterprise/server/backends/pebble_cache",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/backends/disk_cache",
        "//server/cache/config",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/util/alert",
        "//server/util/background",
        "//server/util/claims",
        "//server/util/compression",
        "//server/util/disk",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_time//rate",
    ],
)

go_test(
    name = "migration_cache_test",
    size = "medium",
    srcs = ["migration_cache_test.go"],
    deps = [
        ":migration_cache",
        "//enterprise/server/backends/pebble_cache",
        "//enterprise/server/experiments",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/backends/disk_cache",
        "//server/backends/memory_cache",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk//openfeature/memprovider",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)
