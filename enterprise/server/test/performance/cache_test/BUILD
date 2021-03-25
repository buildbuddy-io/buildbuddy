load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "go_default_test",
    srcs = ["cache_test.go"],
    args = [
        "-test.bench=.",
    ],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/backends/disk_cache:go_default_library",
        "//server/backends/memory_cache:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/testutil/testauth:go_default_library",
        "//server/testutil/testdigest:go_default_library",
        "//server/testutil/testenv:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/testing/flags:go_default_library",
    ],
)
