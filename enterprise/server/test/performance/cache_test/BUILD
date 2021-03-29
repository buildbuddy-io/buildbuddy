load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "go_default_test",
    srcs = ["cache_test.go"],
    args = [
        "-test.bench=.",
    ],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/backends/disk_cache",
        "//server/backends/memory_cache",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/prefix",
        "//server/util/testing/flags",
    ],
)

go_test(
    name = "cache_test_test",
    srcs = ["cache_test.go"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/backends/disk_cache",
        "//server/backends/memory_cache",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/prefix",
        "//server/util/testing/flags",
    ],
)
