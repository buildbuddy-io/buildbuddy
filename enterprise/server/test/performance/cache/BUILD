load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "cache_test",
    srcs = ["cache_test.go"],
    args = [
        "-test.bench=.",
    ],
    tags = ["performance"],
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

package(default_visibility = ["//enterprise:__subpackages__"])
