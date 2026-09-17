load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "chunking_test",
    srcs = ["chunking_test.go"],
    deps = [
        "//enterprise/server/backends/pebble_cache",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/remote_cache/chunking",
        "//server/remote_cache/digest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/prefix",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
