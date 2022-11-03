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
        "//proto:resource_go_proto",
        "//server/backends/disk_cache",
        "//server/backends/memory_cache",
        "//server/environment",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/util/log",
        "//server/util/prefix",
    ],
)
