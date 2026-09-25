load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "testport",
    srcs = [
        "lock_other.go",
        "lock_unix.go",
        "testport.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testport",
    visibility = ["//visibility:public"],
)

go_test(
    name = "testport_test",
    size = "small",
    srcs = ["lock_unix_test.go"],
    embed = [":testport"],
)
