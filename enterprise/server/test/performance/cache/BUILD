load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "cache_test",
    timeout = "long",
    srcs = ["cache_test.go"],
    args = [
        "-test.bench=.",
    ],
    tags = [
        "manual",
        "performance",
    ],
    deps = [
        "//enterprise/server/backends/distributed",
        "//enterprise/server/backends/migration_cache",
        "//enterprise/server/backends/pebble_cache",
        "//proto:resource_go_proto",
        "//server/backends/disk_cache",
        "//server/backends/memory_cache",
        "//server/environment",
        "//server/interfaces",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/testing/flags",
    ],
)
