load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "testserver",
    srcs = ["testserver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testserver",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

go_test(
    name = "testserver_test",
    srcs = ["testserver_test.go"],
    embed = [":testserver"],
    target_compatible_with = ["@platforms//os:linux"],
)
