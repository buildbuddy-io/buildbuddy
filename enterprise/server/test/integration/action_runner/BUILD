load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "go_default_test",
    srcs = ["action_runner_test.go"],
    data = ["//enterprise/server/cmd/action_runner"],
    tags = ["manual"],
    deps = [
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
