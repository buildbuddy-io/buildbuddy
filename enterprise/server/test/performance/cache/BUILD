load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "cache_test",
    srcs = ["cache_test.go"],
    args = [
        "-test.bench=.",
    ],
    tags = ["manual"],
    deps = [
        "//enterprise/server/backends/distributed",
        "//proto:remote_execution_go_proto",
        "//server/backends/disk_cache",
        "//server/backends/memory_cache",
        "//server/config",
        "//server/environment",
        "//server/interfaces",
        "//server/testutil/app",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/testing/flags",
    ],
)
