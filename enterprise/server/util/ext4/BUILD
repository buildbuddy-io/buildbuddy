load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "ext4",
    srcs = ["ext4.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4",
    visibility = ["//visibility:public"],
    deps = ["//server/util/status"],
)

go_test(
    name = "ext4_test",
    srcs = ["ext4_test.go"],
    deps = [
        ":ext4",
        "//server/remote_cache/digest",
        "//server/testutil/testdigest",
        "//server/util/disk",
        "//server/util/log",
    ],
)
