load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ext4",
    srcs = ["ext4.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4",
    deps = [
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
    ],
)

go_test(
    name = "ext4_test",
    srcs = ["ext4_test.go"],
    tags = ["manual"],  # This won't work on mac.
    deps = [
        ":ext4",
        "//server/remote_cache/digest",
        "//server/testutil/testdigest",
        "//server/testutil/testfs",
        "//server/util/disk",
    ],
)
