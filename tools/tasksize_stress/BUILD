load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "tasksize_stress_test",
    srcs = ["tasksize_stress_test.go"],
    shard_count = 6,
    tags = [
        "manual",
        "no-cache",
    ],
)
