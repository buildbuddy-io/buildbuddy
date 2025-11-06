load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "cache_test",
    timeout = "long",
    srcs = ["cache_test.go"],
    args = [
        "-test.bench=.",
    ],
    tags = ["performance"],
    deps = [
        "//enterprise/server/backends/cache_config",
        "//enterprise/server/backends/distributed",
        "//enterprise/server/backends/migration_cache",
        "//enterprise/server/backends/pebble_cache",
        "//enterprise/server/experiments",
        "//proto:resource_go_proto",
        "//server/backends/disk_cache",
        "//server/backends/memory_cache",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/testing/flags",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk//openfeature/memprovider",
        "@com_github_stretchr_testify//require",
    ],
)
