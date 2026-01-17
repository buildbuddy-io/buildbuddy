load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "fsync",
    srcs = ["fsync.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/fsync",
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "fsync_test",
    srcs = ["fsync_test.go"],
    embed = [":fsync"],
)
