load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "gitmirror_test",
    timeout = "long",
    srcs = ["gitmirror_test.go"],
    args = [
        "-test.bench=.",
    ],
    tags = ["performance"],
)
